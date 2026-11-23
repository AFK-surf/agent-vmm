//! A window without a screen: the shipping views drawn by the real renderer on
//! a clock the caller steps, driven through the input events a person causes.

use crate::{
    ConsoleView, Page,
    backend::FixtureBackend,
    theme::{Theme, register_fonts},
};
use std::{cell::RefCell, rc::Rc, sync::Arc, time::Duration};
use ui::{
    App, ApplicationHandle, AuditViolation, Bounds, Context, Entity, EntityInputHandler,
    FrameSnapshot, HeadlessPlatform, HeadlessWindow, Modifiers, MouseButton, PlatformInput, Render,
    Window, WindowBounds, WindowHandle, WindowOptions, audit_frame_snapshot, point, prelude::*, px,
    size,
};

pub struct Session<V> {
    platform: Rc<HeadlessPlatform>,
    app: ApplicationHandle,
    window: WindowHandle<V>,
    headless: HeadlessWindow,
}

impl<V: Render + 'static> Session<V> {
    /// Opens a `width` × `height` window of `scale` device pixels per pixel
    /// on the view `build` makes.
    pub fn open(
        width: u32,
        height: u32,
        scale: f32,
        build: impl FnOnce(&mut Window, &mut App) -> Entity<V> + 'static,
    ) -> Self {
        let platform = HeadlessPlatform::new(width, height, scale);
        let opened = Rc::new(RefCell::new(None));
        let app = ui::Application::with_platform(platform.clone()).run_embedded({
            let opened = opened.clone();
            move |cx| {
                register_fonts(cx).expect("the bundled fonts register");
                let bounds = Bounds::new(
                    point(px(0.), px(0.)),
                    size(px(width as f32), px(height as f32)),
                );
                let options = WindowOptions {
                    window_bounds: Some(WindowBounds::Windowed(bounds)),
                    ..WindowOptions::default()
                };
                let window = cx
                    .open_window(options, build)
                    .expect("a headless window opens");
                *opened.borrow_mut() = Some(window);
            }
        });
        let window = opened.borrow_mut().take().expect("the application ran");
        let headless = platform.windows().into_iter().next().expect("one window");
        let session = Self {
            platform,
            app,
            window,
            headless,
        };
        session.settle(0);
        session
    }

    pub fn update<R>(&self, f: impl FnOnce(&mut V, &mut Window, &mut Context<V>) -> R) -> R {
        self.app
            .update(|cx| self.window.update(cx, f))
            .expect("the window is open")
    }

    /// Runs what is queued, lets `millis` pass on the clock, and runs what that woke.
    pub fn settle(&self, millis: u64) {
        self.platform.run_until_parked();
        if millis > 0 {
            self.platform.advance_clock(Duration::from_millis(millis));
            self.platform.run_until_parked();
        }
    }

    /// Draws a frame as a window does: only the views that changed are built again.
    pub fn draw(&self) {
        self.settle(0);
        self.headless.request_frame();
        // A frame can ask for another: a scroll position clamped while
        // painting, a list measured for the first time.
        self.settle(0);
        self.headless.request_frame();
    }

    /// Builds every view again and reads the elements the frame recorded. A
    /// view that a frame reuses records none.
    pub fn frame(&self) -> FrameSnapshot {
        self.settle(0);
        self.update(|_, window, _| window.refresh());
        self.headless.request_frame();
        self.settle(0);
        self.update(|_, window, _| window.refresh());
        self.headless.request_frame();
        self.update(|_, window, _| window.frame_snapshot().clone())
    }

    /// Draws a frame and reads its pixels.
    pub fn image(&self) -> image::RgbaImage {
        self.draw();
        self.update(|_, window, _| window.render_to_image())
            .expect("the frame renders")
    }

    /// The structural faults of the current frame. An element inside a
    /// scrolling area (recorded as `….scroll`) may be cut by its fold; one
    /// that sticks out sideways, or out of anything else, is a fault.
    pub fn audit(&self) -> Vec<AuditViolation> {
        let frame = self.frame();
        let folds: Vec<_> = frame
            .elements
            .iter()
            .filter(|(id, _)| id.ends_with(".scroll"))
            .map(|(_, element)| element.bounds)
            .collect();
        audit_frame_snapshot(&frame)
            .into_iter()
            .filter(|violation| {
                let Some(element) = frame.elements.get(&violation.element_id) else {
                    return true;
                };
                let (bounds, mask) = (element.bounds, element.content_mask);
                let within_sides = bounds.x + 0.5 >= mask.x && bounds.right() - 0.5 <= mask.right();
                !(violation.rule == "parent-clip-fault" && within_sides && folds.contains(&mask))
            })
            .collect()
    }

    /// The middle of the element recorded as `id`.
    pub fn center(&self, id: &str) -> (f32, f32) {
        let frame = self.frame();
        let element = frame
            .elements
            .get(id)
            .unwrap_or_else(|| panic!("no element {id} in {:?}", frame.elements.keys()));
        (
            element.bounds.x + element.bounds.width / 2.,
            element.bounds.y + element.bounds.height / 2.,
        )
    }

    pub fn has(&self, id: &str) -> bool {
        self.frame().elements.contains_key(id)
    }

    pub fn pointer_move(&self, x: f32, y: f32, pressed: bool) {
        self.input(PlatformInput::MouseMove(ui::MouseMoveEvent {
            position: point(px(x), px(y)),
            pressed_button: pressed.then_some(MouseButton::Left),
            modifiers: Modifiers::default(),
        }));
    }

    pub fn pointer_down(&self, x: f32, y: f32) {
        self.input(PlatformInput::MouseDown(ui::MouseDownEvent {
            button: MouseButton::Left,
            position: point(px(x), px(y)),
            modifiers: Modifiers::default(),
            click_count: 1,
            first_mouse: false,
        }));
    }

    pub fn pointer_up(&self, x: f32, y: f32) {
        self.input(PlatformInput::MouseUp(ui::MouseUpEvent {
            button: MouseButton::Left,
            position: point(px(x), px(y)),
            modifiers: Modifiers::default(),
            click_count: 1,
        }));
    }

    pub fn click(&self, x: f32, y: f32) {
        self.pointer_move(x, y, false);
        self.pointer_down(x, y);
        self.pointer_up(x, y);
    }

    /// Clicks the middle of the element recorded as `id`.
    pub fn click_id(&self, id: &str) {
        let (x, y) = self.center(id);
        self.click(x, y);
    }

    /// Turns the wheel at `(x, y)`: a positive `down` moves toward the end.
    pub fn scroll(&self, x: f32, y: f32, down: f32) {
        self.input(PlatformInput::Scroll(ui::ScrollEvent {
            position: point(px(x), px(y)),
            delta: ui::ScrollDelta::Pixels(point(px(0.), px(-down))),
            modifiers: Modifiers::default(),
            touch_phase: ui::TouchPhase::Moved,
            source: ui::ScrollSource::Pointer,
        }));
    }

    pub fn key(&self, key: &str, shift: bool, command: bool) {
        self.input(PlatformInput::KeyDown(ui::KeyDownEvent {
            keystroke: ui::Keystroke {
                key: key.into(),
                key_char: None,
                modifiers: Modifiers {
                    shift,
                    platform: command,
                    ..Default::default()
                },
            },
            is_held: false,
            prefer_character_input: false,
        }));
    }

    pub fn clipboard(&self) -> Option<String> {
        self.app
            .update(|cx| cx.read_from_clipboard().and_then(|item| item.text()))
    }

    fn input(&self, event: PlatformInput) {
        self.headless.dispatch_input(event);
        self.platform.run_until_parked();
    }
}

/// The console on the fixture Host.
pub struct Console {
    pub session: Session<ConsoleView>,
    pub backend: Arc<FixtureBackend>,
}

/// A state of the console worth a picture and an audit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    Host,
    HostStopConfirmation,
    HostStopped,
    Environments,
    EnvironmentsLongName,
    EnvironmentsDiscard,
    Logs,
    LogsSearch,
    LogsMenu,
    Access,
    AccessToken,
    AccessRotation,
}

impl State {
    pub const ALL: [Self; 12] = [
        Self::Host,
        Self::HostStopConfirmation,
        Self::HostStopped,
        Self::Environments,
        Self::EnvironmentsLongName,
        Self::EnvironmentsDiscard,
        Self::Logs,
        Self::LogsSearch,
        Self::LogsMenu,
        Self::Access,
        Self::AccessToken,
        Self::AccessRotation,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Self::Host => "host",
            Self::HostStopConfirmation => "host-stop-confirmation",
            Self::HostStopped => "host-stopped",
            Self::Environments => "environments",
            Self::EnvironmentsLongName => "environments-long-name",
            Self::EnvironmentsDiscard => "environments-discard",
            Self::Logs => "logs",
            Self::LogsSearch => "logs-search",
            Self::LogsMenu => "logs-menu",
            Self::Access => "access",
            Self::AccessToken => "access-token",
            Self::AccessRotation => "access-rotation",
        }
    }

    /// An element that is there only in this state.
    pub fn landmark(self) -> &'static str {
        match self {
            Self::Host => "host.stop",
            Self::HostStopConfirmation => "host.stop.confirm",
            Self::HostStopped => "host.start",
            Self::Environments => "environments.reset",
            Self::EnvironmentsLongName => "environments.row.3",
            Self::EnvironmentsDiscard => "environments.confirm",
            Self::Logs => "logs.view",
            Self::LogsSearch => "logs.matches",
            Self::LogsMenu => "logs.environment.menu",
            Self::Access => "access.token.create",
            Self::AccessToken => "access.token.copy",
            Self::AccessRotation => "access.rotate.confirm",
        }
    }
}

impl Console {
    pub fn open(width: u32, height: u32, scale: f32, theme: Theme) -> Self {
        let backend = Arc::new(FixtureBackend::new());
        let session = Session::open(width, height, scale, {
            let backend = backend.clone();
            move |window, cx| cx.new(|cx| ConsoleView::new(backend, theme, window, cx))
        });
        Self { session, backend }
    }

    pub fn show(&self, page: Page) {
        if self.session.update(|console, _, _| console.page()) != page {
            self.session.click_id(&format!("tab.{}", page.slug()));
        }
    }

    /// Types into the search field of the Logs page, as an input method would.
    pub fn type_search(&self, text: &str) {
        self.session.update(|console, window, cx| {
            let search = console.logs().read(cx).search().clone();
            search.update(cx, |search, cx| {
                search.replace_text_in_range(None, text, window, cx)
            });
        });
        self.session.settle(0);
    }

    /// Brings a console that was just opened into `state`, through the
    /// clicks and keys a person would use.
    pub fn enter(&self, state: State) {
        let session = &self.session;
        match state {
            State::Host => {}
            State::HostStopConfirmation => session.click_id("host.stop"),
            State::HostStopped => {
                session.click_id("host.stop");
                session.click_id("host.stop.confirm");
                // The operation is read twice, and the page samples the stopped VM.
                session.settle(600);
                session.settle(600);
            }
            State::Environments => self.show(Page::Environments),
            State::EnvironmentsLongName => {
                self.show(Page::Environments);
                session.click_id("environments.more");
                session.click_id("environments.row.3");
            }
            State::EnvironmentsDiscard => {
                self.show(Page::Environments);
                session.click_id("environments.discard");
            }
            State::Logs => self.show(Page::Logs),
            State::LogsSearch => {
                self.show(Page::Logs);
                session.click_id("logs.search");
                self.type_search("timeout");
            }
            State::LogsMenu => {
                self.show(Page::Logs);
                session.click_id("logs.environment");
            }
            State::Access => self.show(Page::Access),
            State::AccessToken => {
                self.show(Page::Access);
                session.click_id("access.token.create");
            }
            State::AccessRotation => {
                self.show(Page::Access);
                session.click_id("access.rotate");
            }
        }
        session.settle(0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_vmm_proto::host::v1 as h;

    /// The window sizes every state is audited at: the default and the smallest.
    const SIZES: [(u32, u32); 2] = [(1200, 800), (880, 620)];

    #[test]
    fn every_state_is_structurally_sound_at_both_sizes() {
        for theme in [Theme::Dark, Theme::Light] {
            for (width, height) in SIZES {
                for state in State::ALL {
                    let console = Console::open(width, height, 1.0, theme);
                    console.enter(state);
                    let at = format!("{} at {width}×{height}, {}", state.name(), theme.name());
                    assert!(
                        console.session.has(state.landmark()),
                        "{at}: no {}",
                        state.landmark()
                    );
                    let violations = console.session.audit();
                    assert!(violations.is_empty(), "{at}: {violations:#?}");
                }
            }
        }
    }

    #[test]
    fn stopping_the_vm_asks_first_and_reports_the_operation() {
        let console = Console::open(1200, 800, 1.0, Theme::Dark);
        let session = &console.session;
        let vm_state =
            || session.update(|console, _, cx| console.host().read(cx).vm().map(|vm| vm.state()));
        assert_eq!(vm_state(), Some(h::VmState::Running));

        session.click_id("host.stop");
        assert!(session.has("host.stop.confirmation"));
        session.click_id("host.stop.cancel");
        assert!(!session.has("host.stop.confirmation"));
        session.settle(1_000);
        assert_eq!(vm_state(), Some(h::VmState::Running));

        session.click_id("host.stop");
        session.click_id("host.stop.confirm");
        session.settle(1_000);
        session.settle(1_000);
        assert_eq!(vm_state(), Some(h::VmState::Stopped));
        assert!(session.has("host.start"));
    }

    #[test]
    fn the_host_is_sampled_only_while_its_page_is_in_sight() {
        let console = Console::open(1200, 800, 1.0, Theme::Dark);
        let sampling = || {
            console
                .session
                .update(|console, _, cx| console.host().read(cx).is_sampling())
        };
        assert!(sampling());
        console.show(Page::Logs);
        assert!(!sampling());
        console.show(Page::Host);
        assert!(sampling());
    }

    #[test]
    fn the_log_view_searches_pauses_and_copies() {
        let console = Console::open(1200, 800, 1.0, Theme::Dark);
        let session = &console.session;
        console.show(Page::Logs);
        let view = session.update(|console, _, cx| console.logs().read(cx).view().clone());
        let lines = || session.update(|_, _, cx| view.read(cx).line_count());
        assert_eq!(lines(), 420);
        assert!(session.has("logs.gaps"));

        session.click_id("logs.search");
        console.type_search("TIMEOUT");
        assert!(session.has("logs.matches"));
        let (count, matching) = session.update(|_, _, cx| {
            let view = view.read(cx);
            (view.match_count(), view.matching_line_count())
        });
        assert!(count > 0 && count == matching, "{count} in {matching}");

        // New lines arrive while following, and while paused.
        assert!(session.update(|_, _, cx| view.read(cx).is_following()));
        console
            .backend
            .push_log("a line while following", h::LogStream::Stdout, 1_000);
        session.settle(0);
        session.click_id("logs.pause");
        assert!(!session.update(|_, _, cx| view.read(cx).is_following()));
        console
            .backend
            .push_log("a line while paused", h::LogStream::Stderr, 1_001);
        session.settle(0);
        assert_eq!(lines(), 422);

        // Dragging over two rows selects them; Copy puts their text on the clipboard.
        let (x, y) = session.center("logs.view");
        session.pointer_move(x - 300., y, false);
        session.pointer_down(x - 300., y);
        session.pointer_move(x + 200., y + 18., true);
        session.pointer_up(x + 200., y + 18.);
        session.click_id("logs.copy");
        let copied = session.clipboard().expect("the selection was copied");
        assert_eq!(copied.lines().count(), 2, "{copied:?}");
        assert!(!copied.contains('\u{1b}'));
    }

    #[test]
    fn ten_thousand_lines_are_painted_a_screen_at_a_time() {
        let console = Console::open(1200, 800, 1.0, Theme::Dark);
        let session = &console.session;
        console.show(Page::Logs);
        let view = session.update(|console, _, cx| console.logs().read(cx).view().clone());
        let push = |count: u64| {
            for index in 0..count {
                let line = format!("line {index} of a long build");
                console
                    .backend
                    .push_log(&line, h::LogStream::Stdout, 1_000 + index);
            }
            session.frame();
        };
        let state = || {
            session.update(|_, _, cx| {
                let view = view.read(cx);
                (view.line_count(), view.rows_painted(), view.is_following())
            })
        };

        push(10_000);
        let (lines, painted, following) = state();
        assert_eq!(lines, 10_420);
        // Following: the newest line is painted, and a screen of rows with it.
        assert!(following);
        assert_eq!(painted.end, 10_420);
        assert!((30..=800 / 18 + 16).contains(&painted.len()), "{painted:?}");

        // The wheel goes back to the first line and stops following.
        let (x, y) = session.center("logs.view");
        session.scroll(x, y, -1e7);
        session.frame();
        let (_, painted, following) = state();
        assert!(!following);
        assert_eq!(painted.start, 0);
        assert!(painted.len() <= 800 / 18 + 16, "{painted:?}");

        // The view keeps 20,000 lines and drops the oldest.
        push(12_000);
        let (lines, painted, _) = state();
        assert_eq!(lines, 20_000);
        assert_eq!(
            session.update(|_, _, cx| view.read(cx).dropped_count()),
            2_420
        );
        assert!(painted.len() <= 800 / 18 + 16, "{painted:?}");
    }

    #[test]
    fn log_text_stops_before_the_scrollbar() {
        for (width, height) in SIZES {
            let console = Console::open(width, height, 1.0, Theme::Dark);
            let session = &console.session;
            console.show(Page::Logs);
            session.frame();
            let view = session.update(|console, _, cx| console.logs().read(cx).view().clone());
            let (text, thumb) =
                session.update(|_, window, cx| view.read(cx).text_and_thumb(window));
            let frame = session.frame().elements["logs.view"].bounds;
            // The text keeps clear of the thumb, and both stay inside the frame.
            assert!(text.end + 4. <= thumb.start, "{width}: {text:?} {thumb:?}");
            assert!(text.start > frame.x && thumb.end < frame.right(), "{width}");
        }
    }

    #[test]
    fn glyphs_reach_the_pixels() {
        let console = Console::open(1200, 800, 1.0, Theme::Dark);
        let tab = console.session.frame().elements["tab.environments"].bounds;
        let image = console.session.image();
        let background = image.get_pixel(tab.x as u32 + 1, tab.y as u32 + 4).0;
        let (mut inked, mut total) = (0_u32, 0_u32);
        for y in tab.y as u32..tab.bottom() as u32 {
            for x in tab.x as u32..tab.right() as u32 {
                let pixel = image.get_pixel(x, y).0;
                total += 1;
                inked += u32::from(
                    pixel
                        .iter()
                        .zip(background)
                        .any(|(a, b)| a.abs_diff(b) > 40),
                );
            }
        }
        // A label is some ink on its background: not none, and not a block.
        let share = inked as f32 / total as f32;
        assert!((0.02..0.5).contains(&share), "{inked} of {total} pixels");
    }

    #[test]
    fn a_token_is_shown_once() {
        let console = Console::open(880, 620, 1.0, Theme::Dark);
        let session = &console.session;
        console.enter(State::AccessToken);
        session.click_id("access.token.copy");
        let token = session.clipboard().expect("the token was copied");
        assert!(token.starts_with("avmm_mcp_1."));
        session.click_id("access.token.dismiss");
        assert!(!session.has("access.token"));
        assert!(session.has("access.token.create"));
    }

    #[test]
    fn an_open_list_stays_inside_the_window_and_closes_on_a_press_outside() {
        let console = Console::open(880, 620, 1.0, Theme::Dark);
        let session = &console.session;
        console.enter(State::LogsMenu);
        let frame = session.frame();
        let menu = &frame.elements["logs.environment.menu"];
        assert!(
            menu.bounds.x >= 0. && menu.bounds.right() <= 880.,
            "{:?}",
            menu.bounds
        );
        assert!(menu.bounds.bottom() <= 620., "{:?}", menu.bounds);
        // The whole of the longest name is on it.
        let option = &frame.elements["logs.environment.option.3"];
        assert!(option.bounds.right() <= menu.bounds.right());
        // The list is as wide as the field it belongs to.
        let field = &frame.elements["logs.environment"];
        assert_eq!(menu.bounds.width, field.bounds.width);
        assert_eq!(menu.bounds.x, field.bounds.x);
        session.click(20., 600.);
        assert!(!session.has("logs.environment.menu"));
        // The press that closed the list did nothing else.
        assert_eq!(session.update(|console, _, _| console.page()), Page::Logs);
    }
}
