//! The window: a header that names the Host's state and switches pages, and
//! the page in sight.

use crate::{
    backend::OperatorBackend,
    theme::{FONT_UI, PAD, Palette, SMALL, SMALL_LINE, TEXT, TEXT_LINE, Theme},
    views::{AccessPage, EnvironmentsPage, HostPage, LogsPage},
    widgets::{ButtonKind, Tone, button, dot, text},
};
use agent_vmm_proto::host::v1 as h;
use std::sync::Arc;
use ui::{
    Context, Div, ElementId, Entity, FontWeight, Stateful, Subscription, Window, WindowVisibility,
    div, prelude::*, px,
};

const HEADER: f32 = 48.0;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Page {
    Host,
    Environments,
    Logs,
    Access,
}

impl Page {
    pub const ALL: [Self; 4] = [Self::Host, Self::Environments, Self::Logs, Self::Access];

    pub fn title(self) -> &'static str {
        match self {
            Self::Host => "Host & Memory",
            Self::Environments => "Environments",
            Self::Logs => "Logs",
            Self::Access => "Access & Appliances",
        }
    }

    /// The name of the page in element IDs and file names.
    pub fn slug(self) -> &'static str {
        match self {
            Self::Host => "host",
            Self::Environments => "environments",
            Self::Logs => "logs",
            Self::Access => "access",
        }
    }
}

pub struct ConsoleView {
    theme: Theme,
    page: Page,
    visible: bool,
    host: Entity<HostPage>,
    environments: Entity<EnvironmentsPage>,
    logs: Entity<LogsPage>,
    access: Entity<AccessPage>,
    _observers: [Subscription; 2],
}

impl ConsoleView {
    pub fn new(
        backend: Arc<dyn OperatorBackend>,
        theme: Theme,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let host = cx.new(|_| HostPage::new(backend.clone(), theme));
        let environments = cx.new(|_| EnvironmentsPage::new(backend.clone(), theme));
        let logs = cx.new(|cx| LogsPage::new(backend.clone(), theme, cx));
        let access = cx.new(|_| AccessPage::new(backend, theme));
        // The header shows the VM's state, which the Host page samples.
        let host_changed = cx.observe(&host, |_, _, cx| cx.notify());
        let visibility = cx.observe_window_visibility(window, |this, visibility, _, cx| {
            this.visible = visibility == WindowVisibility::Visible;
            this.sample(cx);
        });
        let mut this = Self {
            theme,
            page: Page::Host,
            visible: window.visibility() == WindowVisibility::Visible,
            host,
            environments,
            logs,
            access,
            _observers: [host_changed, visibility],
        };
        this.sample(cx);
        this
    }

    pub fn page(&self) -> Page {
        self.page
    }

    pub fn theme(&self) -> Theme {
        self.theme
    }

    pub fn host(&self) -> &Entity<HostPage> {
        &self.host
    }

    pub fn logs(&self) -> &Entity<LogsPage> {
        &self.logs
    }

    /// Shows `page`. A page reads the Host when it comes into sight, and what
    /// it read last is there at once.
    pub fn show(&mut self, page: Page, cx: &mut Context<Self>) {
        self.page = page;
        self.sample(cx);
        match page {
            Page::Host => {}
            Page::Environments => self.environments.update(cx, |page, cx| page.refresh(cx)),
            Page::Logs => self.logs.update(cx, |page, cx| page.refresh(cx)),
            Page::Access => self.access.update(cx, |page, cx| page.refresh(cx)),
        }
        cx.notify();
    }

    pub fn set_theme(&mut self, theme: Theme, cx: &mut Context<Self>) {
        self.theme = theme;
        self.host.update(cx, |page, cx| page.set_theme(theme, cx));
        self.environments
            .update(cx, |page, cx| page.set_theme(theme, cx));
        self.logs.update(cx, |page, cx| page.set_theme(theme, cx));
        self.access.update(cx, |page, cx| page.set_theme(theme, cx));
        cx.notify();
    }

    /// The Host page samples once a second, and only while someone can see it.
    fn sample(&mut self, cx: &mut Context<Self>) {
        let watching = self.visible && self.page == Page::Host;
        self.host
            .update(cx, |page, cx| page.set_visible(watching, cx));
    }

    fn tab(&self, page: Page, palette: &Palette, cx: &mut Context<Self>) -> Stateful<Div> {
        let selected = self.page == page;
        let id = format!("tab.{}", page.slug());
        let hover = palette.text;
        div()
            .id(ElementId::Name(id.clone().into()))
            .snapshot(id, None)
            .snapshot_text(page.title())
            .relative()
            .flex_none()
            .h(px(HEADER))
            .px(px(12.))
            .flex()
            .items_center()
            .text_size(px(TEXT))
            .line_height(px(TEXT_LINE))
            .whitespace_nowrap()
            .map(|tab| {
                if selected {
                    tab.text_color(palette.text).child(
                        div()
                            .absolute()
                            .left(px(12.))
                            .right(px(12.))
                            .bottom(px(0.))
                            .h(px(2.))
                            .bg(palette.accent),
                    )
                } else {
                    tab.text_color(palette.muted)
                        .cursor_pointer()
                        .hover(move |style| style.text_color(hover))
                        .active(move |style| style.text_color(hover).opacity(0.7))
                        .on_click(cx.listener(move |this, _, _, cx| this.show(page, cx)))
                }
            })
            .child(page.title())
    }

    fn header(&self, palette: &Palette, cx: &mut Context<Self>) -> Div {
        let state = self.host.read(cx).vm().map(|vm| vm.state());
        let (label, tone) = match state {
            Some(h::VmState::Running) => ("VM running", Tone::Ok),
            Some(h::VmState::Starting) => ("VM starting", Tone::Accent),
            Some(h::VmState::Stopping) => ("VM stopping", Tone::Warn),
            Some(h::VmState::Stopped) => ("VM stopped", Tone::Neutral),
            Some(h::VmState::Failed) => ("VM failed", Tone::Danger),
            Some(h::VmState::Unspecified) | None => ("Host not read", Tone::Neutral),
        };
        let next = self.theme.toggled();
        div()
            .snapshot("header", None)
            .flex_none()
            .h(px(HEADER))
            .px(px(PAD))
            .flex()
            .items_center()
            .gap(px(12.))
            .border_b_1()
            .border_color(palette.hairline)
            .bg(palette.surface)
            .child(
                text("Agent VMM")
                    .flex_none()
                    .font_weight(FontWeight::MEDIUM)
                    .whitespace_nowrap(),
            )
            .child(
                div()
                    .snapshot("header.vm", None)
                    .flex_none()
                    .flex()
                    .items_center()
                    .gap(px(6.))
                    .child(dot(tone, TEXT_LINE, palette))
                    .child(text(label).text_color(palette.muted).whitespace_nowrap()),
            )
            .child(
                div()
                    .flex_1()
                    .min_w(px(0.))
                    .flex()
                    .children(Page::ALL.map(|page| self.tab(page, palette, cx))),
            )
            .child(button(
                "theme",
                match next {
                    Theme::Light => "Light",
                    Theme::Dark => "Dark",
                },
                ButtonKind::Plain,
                palette,
                cx.listener(move |this, _, _, cx| this.set_theme(next, cx)),
            ))
    }
}

impl Render for ConsoleView {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let palette = self.theme.palette();
        div()
            .size_full()
            .flex()
            .flex_col()
            .bg(palette.app)
            .text_color(palette.text)
            .font_family(FONT_UI)
            .text_size(px(SMALL))
            .line_height(px(SMALL_LINE))
            .child(self.header(&palette, cx))
            .child(div().flex_1().min_h_0().map(|body| match self.page {
                Page::Host => body.child(self.host.clone()),
                Page::Environments => body.child(self.environments.clone()),
                Page::Logs => body.child(self.logs.clone()),
                Page::Access => body.child(self.access.clone()),
            }))
    }
}
