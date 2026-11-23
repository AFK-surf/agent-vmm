//! What the containers of an environment write, as it arrives.

use super::failure;
use crate::{
    backend::{CallError, OperatorBackend},
    format,
    log_view::{CAPACITY, VirtualLogView},
    text_input::{TextInput, TextInputEvent},
    theme::{CONTROL, PAD, Palette, RADIUS, RADIUS_INNER, SMALL_LINE, Theme},
    widgets::{
        ButtonKind, Tone, button, disabled_button, floating, mono, notice, segment, segmented,
        small,
    },
};
use agent_vmm_proto::host::v1 as h;
use futures::StreamExt;
use std::{cell::Cell, rc::Rc, sync::Arc};
use ui::{
    AnyElement, Bounds, Context, Div, ElementId, Entity, MouseButton, Pixels, SharedString,
    Stateful, Subscription, Task, Window, anchored, canvas, deferred, div, fill, point, prelude::*,
    px, relative, size,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Menu {
    Environment,
    Container,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Shown {
    Both,
    Stdout,
    Stderr,
}

/// Records the guest could not deliver: dropped while its buffer was full,
/// or cut because a record was too long.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Gaps {
    dropped_records: u64,
    truncated_records: u64,
}

pub struct LogsPage {
    backend: Arc<dyn OperatorBackend>,
    theme: Theme,
    environments: Vec<String>,
    environment: Option<String>,
    containers: Vec<String>,
    container: Option<String>,
    menu: Option<Menu>,
    /// How wide each field was last painted: its list is as wide.
    field_widths: [Rc<Cell<Pixels>>; 2],
    shown: Shown,
    only_matches: bool,
    search: Entity<TextInput>,
    view: Entity<VirtualLogView>,
    gaps: Gaps,
    /// Why the stream ended, if the Host ended it.
    ended: Option<Result<(), CallError>>,
    last_cursor: String,
    listing: Option<Task<()>>,
    streaming: Option<Task<()>>,
    _search_changed: Subscription,
}

impl LogsPage {
    pub fn new(backend: Arc<dyn OperatorBackend>, theme: Theme, cx: &mut Context<Self>) -> Self {
        let search = cx.new(|cx| TextInput::new("logs.search", "Search these lines", theme, cx));
        let view = cx.new(|cx| VirtualLogView::new(theme, cx));
        let search_changed = cx.subscribe(&search, |this, search, event, cx| {
            if matches!(event, TextInputEvent::Changed) {
                let needle = search.read(cx).text().to_owned();
                this.view
                    .update(cx, |view, cx| view.set_search(&needle, cx));
                cx.notify();
            }
        });
        Self {
            backend,
            theme,
            environments: Vec::new(),
            environment: None,
            containers: Vec::new(),
            container: None,
            menu: None,
            field_widths: Default::default(),
            shown: Shown::Both,
            only_matches: false,
            search,
            view,
            gaps: Gaps::default(),
            ended: None,
            last_cursor: String::new(),
            listing: None,
            streaming: None,
            _search_changed: search_changed,
        }
    }

    pub fn search(&self) -> &Entity<TextInput> {
        &self.search
    }

    pub fn view(&self) -> &Entity<VirtualLogView> {
        &self.view
    }

    pub fn set_theme(&mut self, theme: Theme, cx: &mut Context<Self>) {
        self.theme = theme;
        self.search
            .update(cx, |search, cx| search.set_theme(theme, cx));
        self.view.update(cx, |view, cx| view.set_theme(theme, cx));
        cx.notify();
    }

    /// Reads which environments exist, and opens the first one's first
    /// container if none is open.
    pub fn refresh(&mut self, cx: &mut Context<Self>) {
        let backend = self.backend.clone();
        self.listing = Some(cx.spawn(async move |this, cx| {
            let mut ids = Vec::new();
            let mut after = String::new();
            loop {
                let Ok(page) = backend.environment_page(after).await else {
                    break;
                };
                ids.extend(page.environments.into_iter().map(|row| row.id));
                if page.next_cursor.is_empty() {
                    break;
                }
                after = page.next_cursor;
            }
            let _ = this.update(cx, |this, cx| {
                this.environments = ids;
                if this.environment.is_none()
                    && let Some(first) = this.environments.first().cloned()
                {
                    this.choose_environment(first, cx);
                }
                cx.notify();
            });
        }));
    }

    fn choose_environment(&mut self, environment_id: String, cx: &mut Context<Self>) {
        self.menu = None;
        self.environment = Some(environment_id.clone());
        self.containers.clear();
        self.container = None;
        self.streaming = None;
        self.reset(cx);
        let detail = self.backend.environment(environment_id);
        self.listing = Some(cx.spawn(async move |this, cx| {
            let detail = detail.await;
            let _ = this.update(cx, |this, cx| {
                match detail {
                    Ok(environment) => {
                        this.containers =
                            environment.containers.into_iter().map(|c| c.id).collect();
                        if let Some(first) = this.containers.first().cloned() {
                            this.choose_container(first, cx);
                        }
                    }
                    Err(error) => this.ended = Some(Err(error)),
                }
                cx.notify();
            });
        }));
        cx.notify();
    }

    fn choose_container(&mut self, container_id: String, cx: &mut Context<Self>) {
        self.menu = None;
        self.container = Some(container_id.clone());
        self.reset(cx);
        let Some(environment_id) = self.environment.clone() else {
            return;
        };
        let mut records = self.backend.logs(h::OperatorServiceLogsRequest {
            environment_id,
            query: Some(h::LogsRequest {
                container_id,
                tail_records: CAPACITY as u64,
                since: None,
                after_cursor: String::new(),
                follow: true,
            }),
        });
        self.streaming = Some(cx.spawn(async move |this, cx| {
            loop {
                let Some(first) = records.next().await else {
                    let _ = this.update(cx, |this, cx| {
                        this.ended = Some(Ok(()));
                        this.view.update(cx, |view, cx| view.finish(cx));
                        cx.notify();
                    });
                    return;
                };
                // Take what has arrived with it, so a burst costs one frame.
                let mut batch = vec![first];
                while batch.len() < 512
                    && let Some(Some(next)) = futures::FutureExt::now_or_never(records.next())
                {
                    batch.push(next);
                }
                let failed = batch.last().is_some_and(|record| record.is_err());
                if this
                    .update(cx, |this, cx| this.received(batch, cx))
                    .is_err()
                    || failed
                {
                    return;
                }
            }
        }));
        cx.notify();
    }

    fn reset(&mut self, cx: &mut Context<Self>) {
        self.gaps = Gaps::default();
        self.ended = None;
        self.last_cursor.clear();
        self.view.update(cx, |view, cx| view.clear(cx));
    }

    fn received(
        &mut self,
        batch: Vec<Result<h::OperatorServiceLogsResponse, CallError>>,
        cx: &mut Context<Self>,
    ) {
        for record in batch {
            let record = match record {
                Ok(record) => record,
                Err(error) => {
                    self.ended = Some(Err(error));
                    break;
                }
            };
            self.gaps.dropped_records += record.dropped_records;
            self.gaps.truncated_records += u64::from(record.truncated);
            let stderr = record.stream() == h::LogStream::Stderr;
            let time: SharedString = format::clock_millis(record.timestamp.as_ref()).into();
            self.view
                .update(cx, |view, cx| view.push(stderr, time, &record.data, cx));
            self.last_cursor = record.cursor;
        }
        cx.notify();
    }

    fn show(&mut self, shown: Shown, cx: &mut Context<Self>) {
        self.shown = shown;
        let stderr = match shown {
            Shown::Both => None,
            Shown::Stdout => Some(false),
            Shown::Stderr => Some(true),
        };
        self.view.update(cx, |view, cx| view.set_stream(stderr, cx));
        cx.notify();
    }

    fn set_only_matches(&mut self, only_matches: bool, cx: &mut Context<Self>) {
        self.only_matches = only_matches;
        self.view
            .update(cx, |view, cx| view.set_only_matches(only_matches, cx));
        cx.notify();
    }

    fn set_follow(&mut self, follow: bool, cx: &mut Context<Self>) {
        self.view.update(cx, |view, cx| view.set_follow(follow, cx));
        cx.notify();
    }

    fn toggle_menu(&mut self, menu: Menu, cx: &mut Context<Self>) {
        self.menu = (self.menu != Some(menu)).then_some(menu);
        cx.notify();
    }
}

/// A triangle that points down, in rows of whole pixels.
fn caret_down(palette: &Palette) -> impl IntoElement {
    let color = palette.muted;
    canvas(
        |_, _, _| (),
        move |bounds: Bounds<Pixels>, _, window, _| {
            let top = bounds.top() + ((bounds.size.height - px(4.)) / 2.).round();
            for row in 0..4 {
                let inset = px(row as f32);
                let line = Bounds::new(
                    point(bounds.left() + inset, top + inset),
                    size(px(7.) - inset * 2., px(1.)),
                );
                window.paint_quad(fill(line, color));
            }
        },
    )
    .flex_none()
    .w(px(7.))
    .h(px(SMALL_LINE))
}

impl LogsPage {
    /// A field that opens a list of `options` below it.
    #[allow(clippy::too_many_arguments)]
    fn select(
        &self,
        menu: Menu,
        id: &'static str,
        label: &'static str,
        value: Option<&String>,
        options: &[String],
        palette: &Palette,
        cx: &mut Context<Self>,
    ) -> Div {
        let (hover, press) = (palette.hover, palette.press);
        let (over, chosen_fill) = (palette.floating_hover, palette.floating_chosen);
        let open = self.menu == Some(menu);
        let painted_width = self.field_widths[menu as usize].clone();
        let width = painted_width.get().max(px(240.));
        let trigger = div()
            .id(id)
            .snapshot(id, None)
            .relative()
            .w_full()
            .min_h(px(CONTROL))
            .px(px(10.))
            .py(px(5.))
            .flex()
            .items_start()
            .gap(px(8.))
            .rounded(px(RADIUS_INNER))
            .border_1()
            .border_color(if open { palette.accent } else { palette.border })
            .bg(palette.raised)
            .cursor_pointer()
            .hover(move |style| style.bg(hover))
            .active(move |style| style.bg(press))
            .on_click(cx.listener(move |this, _, _, cx| this.toggle_menu(menu, cx)))
            .child(small(label).flex_none().text_color(palette.muted))
            .child(match value {
                Some(value) => mono(value.clone()).flex_1().min_w(px(0.)),
                None => mono("none")
                    .flex_1()
                    .min_w(px(0.))
                    .text_color(palette.faint),
            })
            .child(caret_down(palette))
            .child(
                canvas(
                    |_, _, _| (),
                    // The field's inside is two pixels of border narrower than it.
                    move |bounds: Bounds<Pixels>, _, _, _| {
                        painted_width.set(bounds.size.width + px(2.))
                    },
                )
                .absolute()
                .inset_0(),
            );
        let list = open.then(|| {
            let items = options.iter().enumerate().map(|(index, option)| {
                let chosen = value == Some(option);
                let option = option.clone();
                div()
                    .id(ElementId::NamedInteger(id.into(), index as u64))
                    .snapshot(format!("{id}.option.{index}"), None)
                    .min_h(px(CONTROL))
                    .px(px(8.))
                    .py(px(6.))
                    .rounded(px(RADIUS_INNER))
                    .cursor_pointer()
                    .when(chosen, |item| item.bg(chosen_fill))
                    .hover(move |style| style.bg(over))
                    .active(move |style| style.bg(chosen_fill))
                    .child(mono(option.clone()))
                    .on_click(cx.listener(move |this, _, _, cx| match menu {
                        Menu::Environment => this.choose_environment(option.clone(), cx),
                        Menu::Container => this.choose_container(option.clone(), cx),
                    }))
            });
            // Below the field, above the page, never cut by a panel.
            div()
                .absolute()
                .left_0()
                .top(relative(1.))
                .mt(px(4.))
                .child(
                    deferred(
                        anchored().child(
                            floating(palette)
                                .id(ElementId::Name(format!("{id}.menu").into()))
                                .snapshot(format!("{id}.menu"), None)
                                .occlude()
                                .w(width)
                                .p(px(RADIUS - RADIUS_INNER))
                                .flex()
                                .flex_col()
                                .gap(px(2.))
                                .children(items)
                                .when(options.is_empty(), |list| {
                                    list.child(
                                        small("Nothing to choose from.")
                                            .px(px(8.))
                                            .py(px(6.))
                                            .text_color(palette.faint),
                                    )
                                }),
                        ),
                    )
                    .priority(2),
                )
        });
        div().relative().child(trigger).children(list)
    }

    /// While a list is open, a press anywhere else closes it and does nothing more.
    fn scrim(&self, window: &Window, cx: &mut Context<Self>) -> Option<AnyElement> {
        self.menu?;
        let viewport = window.viewport_size();
        Some(
            deferred(
                anchored().position(point(px(0.), px(0.))).child(
                    div()
                        .id("logs-menu-scrim")
                        .w(viewport.width)
                        .h(viewport.height)
                        .occlude()
                        .on_mouse_down(
                            MouseButton::Left,
                            cx.listener(|this, _, _, cx| {
                                this.menu = None;
                                cx.notify();
                            }),
                        ),
                ),
            )
            .priority(1)
            .into_any_element(),
        )
    }

    fn toolbar(&self, palette: &Palette, cx: &mut Context<Self>) -> Div {
        let view = self.view.read(cx);
        let (following, has_selection) = (view.is_following(), view.has_selection());
        let needle = self.search.read(cx).text().to_owned();
        let found = (!needle.is_empty()).then(|| {
            let count = view.match_count();
            let lines = view.matching_line_count();
            small(match count {
                0 => "No match".to_owned(),
                1 => "1 match".to_owned(),
                _ => format!(
                    "{} matches in {} lines",
                    format::count(count as u64),
                    format::count(lines as u64)
                ),
            })
            .snapshot("logs.matches", None)
            .flex_none()
            .py(px(6.))
            .text_color(if count == 0 {
                palette.warn
            } else {
                palette.muted
            })
        });
        let stream = |id: &'static str, label: &'static str, shown: Shown| {
            segment(
                id,
                label,
                self.shown == shown,
                palette,
                cx.listener(move |this, _, _, cx| this.show(shown, cx)),
            )
        };
        let copy: Stateful<Div> = if has_selection {
            let view = self.view.clone();
            button(
                "logs.copy",
                "Copy",
                ButtonKind::Plain,
                palette,
                move |_, _, cx| {
                    view.update(cx, |view, cx| view.copy_selection(cx));
                },
            )
        } else {
            disabled_button("logs.copy", "Copy", palette)
        };
        div()
            .flex()
            .flex_wrap()
            .items_start()
            .gap(px(8.))
            .child(
                segmented(palette)
                    .child(stream("logs.stream.both", "Both", Shown::Both))
                    .child(stream("logs.stream.stdout", "stdout", Shown::Stdout))
                    .child(stream("logs.stream.stderr", "stderr", Shown::Stderr)),
            )
            .child(div().flex_1().min_w(px(180.)).child(self.search.clone()))
            .children(found)
            .child(
                segmented(palette)
                    .child(segment(
                        "logs.lines.all",
                        "All lines",
                        !self.only_matches,
                        palette,
                        cx.listener(|this, _, _, cx| this.set_only_matches(false, cx)),
                    ))
                    .child(segment(
                        "logs.lines.matches",
                        "Matches",
                        self.only_matches,
                        palette,
                        cx.listener(|this, _, _, cx| this.set_only_matches(true, cx)),
                    )),
            )
            .child(
                segmented(palette)
                    .child(segment(
                        "logs.follow",
                        "Follow",
                        following,
                        palette,
                        cx.listener(|this, _, _, cx| this.set_follow(true, cx)),
                    ))
                    .child(segment(
                        "logs.pause",
                        "Pause",
                        !following,
                        palette,
                        cx.listener(|this, _, _, cx| this.set_follow(false, cx)),
                    )),
            )
            .child(copy)
    }

    fn status(&self, palette: &Palette, cx: &mut Context<Self>) -> Div {
        let view = self.view.read(cx);
        let pair = |label: &'static str, value: String| {
            div()
                .flex()
                .items_start()
                .gap(px(6.))
                .child(small(label).text_color(palette.faint))
                .child(mono(value).text_color(palette.muted))
        };
        let mut status = div()
            .flex()
            .flex_wrap()
            .items_start()
            .gap(px(24.))
            .child(pair(
                "Lines",
                format!(
                    "{} of {}",
                    format::count(view.line_count() as u64),
                    format::count(CAPACITY as u64)
                ),
            ));
        if view.dropped_count() > 0 {
            status = status.child(pair("Rolled off", format::count(view.dropped_count())));
        }
        if !self.last_cursor.is_empty() {
            status = status.child(pair("Cursor", self.last_cursor.clone()));
        }
        status.child(
            small(match (&self.ended, &self.container) {
                (Some(Ok(())), _) => "The stream ended",
                (Some(Err(_)), _) => "The stream failed",
                (None, None) => "No container is open",
                (None, Some(_)) => "Streaming",
            })
            .text_color(palette.faint),
        )
    }
}

impl Render for LogsPage {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let palette = self.theme.palette();
        let mut page = div()
            .size_full()
            .p(px(PAD))
            .flex()
            .flex_col()
            .gap(px(12.))
            .child(
                div()
                    .flex()
                    .flex_wrap()
                    .items_start()
                    .gap(px(8.))
                    .child(div().flex_1().min_w(px(320.)).child(self.select(
                        Menu::Environment,
                        "logs.environment",
                        "Environment",
                        self.environment.as_ref(),
                        &self.environments,
                        &palette,
                        cx,
                    )))
                    .child(div().flex_none().w(px(264.)).child(self.select(
                        Menu::Container,
                        "logs.container",
                        "Container",
                        self.container.as_ref(),
                        &self.containers,
                        &palette,
                        cx,
                    ))),
            )
            .child(self.toolbar(&palette, cx));
        if self.gaps != Gaps::default() {
            let mut parts = Vec::new();
            if self.gaps.dropped_records > 0 {
                parts.push(format!(
                    "{} records were dropped in the guest before they could be read",
                    format::count(self.gaps.dropped_records)
                ));
            }
            if self.gaps.truncated_records > 0 {
                parts.push(format!(
                    "{} records were cut short",
                    format::count(self.gaps.truncated_records)
                ));
            }
            page = page.child(
                notice(Tone::Warn, &palette)
                    .snapshot("logs.gaps", None)
                    .child(small(format!(
                        "This log has gaps: {}.",
                        parts.join(", and ")
                    ))),
            );
        }
        if let Some(Err(error)) = &self.ended {
            page = page.child(failure("The log stream stopped", error, &palette));
        }
        page.child(
            div()
                .snapshot("logs.view", None)
                .flex_1()
                .min_h_0()
                .rounded(px(RADIUS))
                .border_1()
                .border_color(palette.hairline)
                .bg(palette.well)
                .overflow_hidden()
                .child(self.view.clone()),
        )
        .child(self.status(&palette, cx))
        .children(self.scrim(window, cx))
    }
}
