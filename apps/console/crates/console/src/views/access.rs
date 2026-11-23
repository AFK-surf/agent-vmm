//! Who this Host is, what may reach it, and the appliances it runs.

use super::{Load, failure, follow_operation, operation_status, page};
use crate::{
    backend::{CallError, OperatorBackend},
    format,
    theme::{FONT_MONO, PAD, Palette, RADIUS_INNER, SMALL, SMALL_LINE, Theme},
    widgets::{
        ButtonKind, Column, Tone, badge, button, dot, field, heading, mono, panel, small,
        table_head, table_row, text,
    },
};
use agent_vmm_proto::{appliance::v1 as a, host::v1 as h};
use futures::FutureExt;
use std::sync::Arc;
use ui::{ClipboardItem, Context, Div, FontWeight, Task, Window, div, prelude::*, px};

pub struct AccessPage {
    backend: Arc<dyn OperatorBackend>,
    theme: Theme,
    identity: Load<h::GetDeviceIdentityResponse>,
    registrations: Load<Vec<h::LocalRegistrationMetadata>>,
    sockets: Load<Vec<h::HostSocket>>,
    appliances: Load<Vec<a::ApplianceStatus>>,
    /// A token the Host issued just now. It is shown until dismissed and
    /// kept nowhere else.
    token: Option<Result<String, CallError>>,
    token_copied: bool,
    confirming_rotation: bool,
    rotation: Option<Result<h::Operation, CallError>>,
    reading: Option<Task<()>>,
    issuing: Option<Task<()>>,
    following: Option<Task<()>>,
}

impl AccessPage {
    pub fn new(backend: Arc<dyn OperatorBackend>, theme: Theme) -> Self {
        Self {
            backend,
            theme,
            identity: Load::Pending,
            registrations: Load::Pending,
            sockets: Load::Pending,
            appliances: Load::Pending,
            token: None,
            token_copied: false,
            confirming_rotation: false,
            rotation: None,
            reading: None,
            issuing: None,
            following: None,
        }
    }

    pub fn set_theme(&mut self, theme: Theme, cx: &mut Context<Self>) {
        self.theme = theme;
        cx.notify();
    }

    pub fn refresh(&mut self, cx: &mut Context<Self>) {
        let backend = self.backend.clone();
        self.reading = Some(cx.spawn(async move |this, cx| {
            let registrations = async {
                let mut rows = Vec::new();
                let mut after = String::new();
                loop {
                    let page = backend.registration_page(after).await?;
                    rows.extend(page.registrations);
                    if page.next_cursor.is_empty() {
                        return Ok::<_, CallError>(rows);
                    }
                    after = page.next_cursor;
                }
            };
            // The list names each appliance; its status has the frontends.
            let appliances = async {
                let mut appliances = backend.appliances().await?;
                for appliance in &mut appliances {
                    let id = appliance.installation_id.clone();
                    *appliance = backend.appliance_status(id).await?;
                }
                Ok::<_, CallError>(appliances)
            };
            let (identity, registrations, sockets, appliances) = futures::join!(
                backend.device_identity(),
                registrations,
                backend.host_sockets(),
                appliances
            );
            let _ = this.update(cx, |this, cx| {
                this.identity = identity.into();
                this.registrations = registrations.into();
                this.sockets = sockets.into();
                this.appliances = appliances.into();
                cx.notify();
            });
        }));
    }

    fn create_token(&mut self, cx: &mut Context<Self>) {
        let token = self.backend.create_mcp_token();
        self.issuing = Some(cx.spawn(async move |this, cx| {
            let token = token.await;
            let _ = this.update(cx, |this, cx| {
                this.token = Some(token);
                this.token_copied = false;
                cx.notify();
            });
        }));
    }

    fn copy_token(&mut self, cx: &mut Context<Self>) {
        if let Some(Ok(token)) = &self.token {
            cx.write_to_clipboard(ClipboardItem::new_string(token.clone()));
            self.token_copied = true;
            cx.notify();
        }
    }

    fn dismiss_token(&mut self, cx: &mut Context<Self>) {
        self.token = None;
        cx.notify();
    }

    fn confirm_rotation(&mut self, confirming: bool, cx: &mut Context<Self>) {
        self.confirming_rotation = confirming;
        cx.notify();
    }

    fn rotate(&mut self, cx: &mut Context<Self>) {
        self.confirming_rotation = false;
        // A rotation answers the ID of its operation: read it to follow it.
        let backend = self.backend.clone();
        let rotating = self.backend.rotate_mcp_token_key();
        let started = async move { backend.operation(rotating.await?).await }.boxed();
        self.following = Some(follow_operation(
            self.backend.clone(),
            started,
            cx,
            |this, operation, cx| {
                // Tokens of the old key no longer work: do not show one.
                this.token = None;
                this.rotation = Some(operation);
                cx.notify();
            },
        ));
        cx.notify();
    }
}

fn registration_state(state: h::RemoteRegistrationState) -> (&'static str, Tone) {
    match state {
        h::RemoteRegistrationState::Enabled => ("Enabled", Tone::Ok),
        h::RemoteRegistrationState::Draining => ("Draining", Tone::Warn),
        h::RemoteRegistrationState::Revoked => ("Revoked", Tone::Danger),
        h::RemoteRegistrationState::Unspecified => ("Unknown", Tone::Neutral),
    }
}

fn appliance_state(state: a::ApplianceState) -> (&'static str, Tone) {
    match state {
        a::ApplianceState::Running => ("Running", Tone::Ok),
        a::ApplianceState::Starting => ("Starting", Tone::Accent),
        a::ApplianceState::Stopping => ("Stopping", Tone::Warn),
        a::ApplianceState::Stopped => ("Stopped", Tone::Neutral),
        a::ApplianceState::Failed => ("Failed", Tone::Danger),
        a::ApplianceState::Sealed => ("Sealed", Tone::Neutral),
        a::ApplianceState::Unspecified => ("Unknown", Tone::Neutral),
    }
}

fn channel_state(state: a::ControllerChannelState) -> &'static str {
    match state {
        a::ControllerChannelState::Active => "active",
        a::ControllerChannelState::Reconciling => "reconciling",
        a::ControllerChannelState::Connecting => "connecting",
        a::ControllerChannelState::Enrolling => "enrolling",
        a::ControllerChannelState::Disconnected => "disconnected",
        a::ControllerChannelState::Revoked => "revoked",
        a::ControllerChannelState::Unspecified => "unknown",
    }
}

const FRONTENDS: [Column; 4] = [
    Column::fixed("Frontend", 88.),
    Column::fixed("Exposure", 104.),
    Column::flex("Endpoint"),
    Column::fixed("State", 104.),
];

/// A way into an appliance. Why it is not available stands where its
/// endpoint would.
fn frontend_row(index: usize, frontend: &a::ApplianceFrontendStatus, palette: &Palette) -> Div {
    let exposure = match frontend.exposure() {
        a::FrontendExposure::HostLoopback => "host loopback",
        a::FrontendExposure::OutboundOnly => "outbound only",
        a::FrontendExposure::LanHttp => "LAN HTTP",
        a::FrontendExposure::Unspecified => "unknown",
    };
    let endpoint = if !frontend.endpoint.is_empty() {
        mono(frontend.endpoint.clone())
    } else if !frontend.error_reason.is_empty() {
        small(frontend.error_reason.clone()).text_color(palette.muted)
    } else {
        mono("none").text_color(palette.faint)
    };
    let (state, tone) = if frontend.available {
        ("Available", Tone::Ok)
    } else {
        ("Unavailable", Tone::Warn)
    };
    table_row(
        &FRONTENDS,
        index == 0,
        [
            mono(frontend.name.clone()).into_any_element(),
            small(exposure).text_color(palette.muted).into_any_element(),
            endpoint.into_any_element(),
            div()
                .flex()
                .items_start()
                .gap(px(8.))
                .child(dot(tone, SMALL_LINE, palette))
                .child(small(state))
                .into_any_element(),
        ],
        palette,
    )
}

/// A section whose content is `load`, or the reason it is not there.
fn loaded<T>(
    title: &'static str,
    snapshot: &'static str,
    load: &Load<T>,
    palette: &Palette,
    content: impl FnOnce(&T) -> Div,
) -> Div {
    let section = panel(palette)
        .snapshot(snapshot, None)
        .p(px(PAD))
        .gap(px(12.))
        .child(heading(title, palette));
    match load {
        Load::Ready(value) => section.child(content(value)),
        Load::Pending => section.child(small("Reading…").text_color(palette.faint)),
        Load::Failed(error) => section.child(failure("Not available", error, palette)),
    }
}

impl AccessPage {
    fn identity_panel(&self, palette: &Palette) -> Div {
        loaded(
            "Device identity",
            "access.identity",
            &self.identity,
            palette,
            |response| {
                let Some(identity) = &response.identity else {
                    return small("This Host has no device identity.").text_color(palette.muted);
                };
                let (state, tone) = match response.state() {
                    h::DeviceIdentityState::Ready => ("Ready", Tone::Ok),
                    h::DeviceIdentityState::NeedsRepair => ("Needs repair", Tone::Danger),
                    h::DeviceIdentityState::Unspecified => ("Unknown", Tone::Neutral),
                };
                div()
                    .flex()
                    .flex_col()
                    .gap(px(6.))
                    .child(field("Device", mono(identity.device_id.clone()), palette))
                    .child(field(
                        "State",
                        div()
                            .flex()
                            .items_start()
                            .gap(px(8.))
                            .child(dot(tone, SMALL_LINE, palette))
                            .child(small(state)),
                        palette,
                    ))
                    .child(field(
                        "Key revision",
                        mono(identity.root_key_revision.to_string()),
                        palette,
                    ))
                    .child(field(
                        "Root public key",
                        mono(format::hex_groups(&identity.root_public_key))
                            .text_color(palette.muted),
                        palette,
                    ))
                    .child(field(
                        "Operator socket",
                        mono(self.backend.endpoint()),
                        palette,
                    ))
            },
        )
    }

    fn token_panel(&self, palette: &Palette, cx: &mut Context<Self>) -> Div {
        let mut section = panel(palette)
            .snapshot("access.tokens", None)
            .p(px(PAD))
            .gap(px(12.))
            .child(heading("MCP tokens", palette))
            .child(
                small(
                    "A token lets an agent reach this Host's MCP appliance. \
                     The Host shows it once and keeps no copy.",
                )
                .text_color(palette.muted),
            );
        match &self.token {
            Some(Ok(token)) => {
                section = section.child(
                    div()
                        .snapshot("access.token", None)
                        .flex()
                        .flex_col()
                        .gap(px(8.))
                        .child(
                            div()
                                .p(px(10.))
                                .rounded(px(RADIUS_INNER))
                                .border_1()
                                .border_color(palette.hairline)
                                .bg(palette.well)
                                .font_family(FONT_MONO)
                                .text_size(px(SMALL))
                                .line_height(px(SMALL_LINE))
                                .child(token.clone()),
                        )
                        .child(
                            div()
                                .flex()
                                .flex_wrap_reverse()
                                .gap(px(8.))
                                .child(button(
                                    "access.token.copy",
                                    if self.token_copied {
                                        "Copied"
                                    } else {
                                        "Copy token"
                                    },
                                    ButtonKind::Primary,
                                    palette,
                                    cx.listener(|this, _, _, cx| this.copy_token(cx)),
                                ))
                                .child(button(
                                    "access.token.dismiss",
                                    "Done",
                                    ButtonKind::Plain,
                                    palette,
                                    cx.listener(|this, _, _, cx| this.dismiss_token(cx)),
                                )),
                        ),
                );
            }
            Some(Err(error)) => {
                section = section.child(failure("The Host issued no token", error, palette));
            }
            None => {}
        }
        if self.confirming_rotation {
            return section.child(
                div()
                    .snapshot("access.rotation", None)
                    .flex()
                    .flex_col()
                    .gap(px(12.))
                    .pt(px(12.))
                    .border_t(px(1.))
                    .border_color(palette.hairline)
                    .child(
                        text("Rotating the key ends every token issued so far.")
                            .font_weight(FontWeight::MEDIUM),
                    )
                    .child(
                        small("Agents that hold one lose access until they get a new token.")
                            .text_color(palette.muted),
                    )
                    .child(
                        div()
                            .flex()
                            .flex_wrap_reverse()
                            .gap(px(8.))
                            .child(button(
                                "access.rotate.confirm",
                                "Rotate key",
                                ButtonKind::Danger,
                                palette,
                                cx.listener(|this, _, _, cx| this.rotate(cx)),
                            ))
                            .child(button(
                                "access.rotate.cancel",
                                "Keep the key",
                                ButtonKind::Plain,
                                palette,
                                cx.listener(|this, _, _, cx| this.confirm_rotation(false, cx)),
                            )),
                    ),
            );
        }
        if self.token.as_ref().is_none_or(|token| token.is_err()) {
            section = section.child(
                div()
                    .flex()
                    .flex_wrap_reverse()
                    .gap(px(8.))
                    .child(button(
                        "access.token.create",
                        "Create token",
                        ButtonKind::Primary,
                        palette,
                        cx.listener(|this, _, _, cx| this.create_token(cx)),
                    ))
                    .child(button(
                        "access.rotate",
                        "Rotate key…",
                        ButtonKind::Plain,
                        palette,
                        cx.listener(|this, _, _, cx| this.confirm_rotation(true, cx)),
                    )),
            );
        }
        if let Some(rotation) = &self.rotation {
            section = section.child(operation_status(rotation, palette));
        }
        section
    }

    fn registrations_panel(&self, palette: &Palette) -> Div {
        const COLUMNS: [Column; 3] = [
            Column::flex("Registration"),
            Column::fixed("State", 104.),
            Column::number("Revision", 64.),
        ];
        loaded(
            "Remote registrations",
            "access.registrations",
            &self.registrations,
            palette,
            |registrations| {
                if registrations.is_empty() {
                    return small("No controller is registered with this Host.")
                        .text_color(palette.faint);
                }
                div()
                    .flex()
                    .flex_col()
                    .child(table_head(&COLUMNS, palette))
                    .children(registrations.iter().enumerate().map(|(index, row)| {
                        let (state, tone) = registration_state(row.state());
                        table_row(
                            &COLUMNS,
                            index == 0,
                            [
                                mono(row.id.clone()).into_any_element(),
                                div()
                                    .flex()
                                    .items_start()
                                    .gap(px(8.))
                                    .child(dot(tone, SMALL_LINE, palette))
                                    .child(small(state))
                                    .into_any_element(),
                                mono(row.revision.to_string())
                                    .text_color(palette.muted)
                                    .into_any_element(),
                            ],
                            palette,
                        )
                    }))
            },
        )
    }

    fn sockets_panel(&self, palette: &Palette) -> Div {
        const COLUMNS: [Column; 3] = [
            Column::flex("Host socket"),
            Column::flex("Environment"),
            Column::number("Port", 48.),
        ];
        loaded(
            "Host sockets",
            "access.sockets",
            &self.sockets,
            palette,
            |sockets| {
                if sockets.is_empty() {
                    return small("No environment has a socket on this Host.")
                        .text_color(palette.faint);
                }
                div()
                    .flex()
                    .flex_col()
                    .child(table_head(&COLUMNS, palette))
                    .children(sockets.iter().enumerate().map(|(index, socket)| {
                        table_row(
                            &COLUMNS,
                            index == 0,
                            [
                                mono(socket.path.clone()).into_any_element(),
                                mono(socket.environment_id.clone())
                                    .text_color(palette.muted)
                                    .into_any_element(),
                                mono(socket.port.to_string()).into_any_element(),
                            ],
                            palette,
                        )
                    }))
            },
        )
    }

    fn appliances_panel(&self, palette: &Palette) -> Div {
        loaded(
            "Appliances",
            "access.appliances",
            &self.appliances,
            palette,
            |appliances| {
                if appliances.is_empty() {
                    return small("No appliance is installed.").text_color(palette.faint);
                }
                div()
                    .flex()
                    .flex_col()
                    .gap(px(PAD))
                    .children(appliances.iter().map(|appliance| {
                        let (state, tone) = appliance_state(appliance.appliance_state());
                        let healthy = appliance.health.as_ref();
                        div()
                            .flex()
                            .flex_col()
                            .gap(px(8.))
                            .child(
                                div()
                                    .flex()
                                    .flex_wrap()
                                    .items_start()
                                    .gap(px(8.))
                                    .child(mono(appliance.product_id.clone()).py(px(2.)))
                                    .child(badge(state, tone, palette))
                                    .when(!appliance.enabled, |row| {
                                        row.child(badge("Disabled", Tone::Neutral, palette))
                                    }),
                            )
                            .child(field(
                                "Installation",
                                mono(appliance.installation_id.clone()),
                                palette,
                            ))
                            .child(field(
                                "Artifact",
                                mono(appliance.artifact_id.clone()).text_color(palette.muted),
                                palette,
                            ))
                            .child(field(
                                "Controller",
                                mono(channel_state(appliance.channel_state())),
                                palette,
                            ))
                            .when_some(healthy, |section, health| {
                                section.child(field(
                                    "Health",
                                    div()
                                        .flex()
                                        .items_start()
                                        .gap(px(8.))
                                        .child(dot(
                                            if health.healthy {
                                                Tone::Ok
                                            } else {
                                                Tone::Danger
                                            },
                                            SMALL_LINE,
                                            palette,
                                        ))
                                        .child(small(health.status.clone())),
                                    palette,
                                ))
                            })
                            .child(
                                div()
                                    .flex()
                                    .flex_col()
                                    .pt(px(4.))
                                    .child(table_head(&FRONTENDS, palette))
                                    .children(appliance.frontends.iter().enumerate().map(
                                        |(index, frontend)| frontend_row(index, frontend, palette),
                                    )),
                            )
                    }))
            },
        )
    }
}

impl Render for AccessPage {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let palette = self.theme.palette();
        let compact = window.viewport_size().width < px(super::COMPACT_BELOW);
        let left = div()
            .flex()
            .flex_col()
            .gap(px(PAD))
            .child(self.identity_panel(&palette))
            .child(self.token_panel(&palette, cx))
            .child(self.registrations_panel(&palette));
        let right = div()
            .flex()
            .flex_col()
            .gap(px(PAD))
            .child(self.appliances_panel(&palette))
            .child(self.sockets_panel(&palette));
        page("access-page", "access.scroll").child(if compact {
            div()
                .flex()
                .flex_col()
                .gap(px(PAD))
                .child(left)
                .child(right)
        } else {
            div()
                .flex()
                .items_start()
                .gap(px(PAD))
                .child(left.flex_1().min_w(px(0.)))
                .child(right.flex_1().min_w(px(0.)))
        })
    }
}
