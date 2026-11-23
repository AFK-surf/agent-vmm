//! The environments on this Host: what runs in each, and how to clean one up.

use super::{
    COMPACT_BELOW, Load, failure, follow_operation, host::health_state, operation_status,
    request_id,
};
use crate::{
    backend::{CallError, OperatorBackend},
    format,
    theme::{PAD, Palette, RADIUS_INNER, SMALL_LINE, Theme},
    widgets::{
        ButtonKind, Column, Tone, badge, button, dot, field, heading, mono, notice, number, panel,
        small, table_cells, table_head, table_row, text,
    },
};
use agent_vmm_proto::host::v1 as h;
use prost_types::Timestamp;
use std::sync::Arc;
use ui::{Context, Div, ElementId, FontWeight, SharedString, Task, Window, div, prelude::*, px};

/// A cleanup the operator asked to see before it runs.
enum Preview {
    None,
    Reading(&'static str),
    Cleanup(h::CleanupPlan),
    Disposal(h::LocalDisposalPlan),
}

pub struct EnvironmentsPage {
    backend: Arc<dyn OperatorBackend>,
    theme: Theme,
    rows: Vec<h::LocalEnvironmentSummary>,
    next_cursor: String,
    collected_at: Option<Timestamp>,
    list: Load<()>,
    selected: Option<String>,
    detail: Load<h::AgentEnvironment>,
    preview: Preview,
    operation: Option<Result<h::Operation, CallError>>,
    listing: Option<Task<()>>,
    reading: Option<Task<()>>,
    following: Option<Task<()>>,
}

impl EnvironmentsPage {
    pub fn new(backend: Arc<dyn OperatorBackend>, theme: Theme) -> Self {
        Self {
            backend,
            theme,
            rows: Vec::new(),
            next_cursor: String::new(),
            collected_at: None,
            list: Load::Pending,
            selected: None,
            detail: Load::Pending,
            preview: Preview::None,
            operation: None,
            listing: None,
            reading: None,
            following: None,
        }
    }

    pub fn set_theme(&mut self, theme: Theme, cx: &mut Context<Self>) {
        self.theme = theme;
        cx.notify();
    }

    /// Reads the list again from its start. The page shows what it had until
    /// the answer is here.
    pub fn refresh(&mut self, cx: &mut Context<Self>) {
        self.list_page(String::new(), cx);
    }

    pub fn select(&mut self, environment_id: String, cx: &mut Context<Self>) {
        if self.selected.as_ref() != Some(&environment_id) {
            self.preview = Preview::None;
            self.operation = None;
            self.detail = Load::Pending;
        }
        self.selected = Some(environment_id.clone());
        let detail = self.backend.environment(environment_id);
        self.reading = Some(cx.spawn(async move |this, cx| {
            let detail = detail.await;
            let _ = this.update(cx, |this, cx| {
                this.detail = detail.into();
                cx.notify();
            });
        }));
        cx.notify();
    }

    fn list_page(&mut self, after_id: String, cx: &mut Context<Self>) {
        let first = after_id.is_empty();
        let page = self.backend.environment_page(after_id);
        self.listing = Some(cx.spawn(async move |this, cx| {
            let page = page.await;
            let _ = this.update(cx, |this, cx| {
                match page {
                    Ok(page) => {
                        if first {
                            this.rows.clear();
                        }
                        this.rows.extend(page.environments);
                        this.next_cursor = page.next_cursor;
                        this.collected_at = page.collected_at;
                        this.list = Load::Ready(());
                        let gone = this
                            .selected
                            .as_ref()
                            .is_some_and(|id| first && !this.rows.iter().any(|row| &row.id == id));
                        if this.selected.is_none() || gone {
                            match this.rows.first().map(|row| row.id.clone()) {
                                Some(id) => this.select(id, cx),
                                None => this.selected = None,
                            }
                        }
                    }
                    Err(error) => this.list = Load::Failed(error),
                }
                cx.notify();
            });
        }));
    }

    fn preview_cleanup(&mut self, kind: h::CleanupKind, cx: &mut Context<Self>) {
        let Some(id) = self.selected.clone() else {
            return;
        };
        self.operation = None;
        self.preview = Preview::Reading(if kind == h::CleanupKind::Discard {
            "discard"
        } else {
            "reset"
        });
        let plan = self.backend.plan_cleanup(kind, id);
        self.reading = Some(cx.spawn(async move |this, cx| {
            let plan = plan.await;
            let _ = this.update(cx, |this, cx| {
                this.previewed(plan.map(Preview::Cleanup), cx)
            });
        }));
        cx.notify();
    }

    fn preview_disposal(&mut self, cx: &mut Context<Self>) {
        let Some(id) = self.selected.clone() else {
            return;
        };
        self.operation = None;
        self.preview = Preview::Reading("disposal");
        let plan = self.backend.plan_disposal(id);
        self.reading = Some(cx.spawn(async move |this, cx| {
            let plan = plan.await;
            let _ = this.update(cx, |this, cx| {
                this.previewed(plan.map(Preview::Disposal), cx)
            });
        }));
        cx.notify();
    }

    fn previewed(&mut self, preview: Result<Preview, CallError>, cx: &mut Context<Self>) {
        match preview {
            Ok(preview) => self.preview = preview,
            Err(error) => {
                self.preview = Preview::None;
                self.operation = Some(Err(error));
            }
        }
        cx.notify();
    }

    fn cancel(&mut self, cx: &mut Context<Self>) {
        self.preview = Preview::None;
        cx.notify();
    }

    fn execute(&mut self, cx: &mut Context<Self>) {
        let started = match std::mem::replace(&mut self.preview, Preview::None) {
            Preview::Cleanup(plan) => self.backend.execute_cleanup(request_id(), plan),
            Preview::Disposal(plan) => self.backend.execute_disposal(request_id(), plan),
            Preview::None | Preview::Reading(_) => return,
        };
        self.following = Some(follow_operation(
            self.backend.clone(),
            started,
            cx,
            |this, operation, cx| {
                let done = operation
                    .as_ref()
                    .is_ok_and(|operation| operation.state() == h::OperationState::Succeeded);
                this.operation = Some(operation);
                if done {
                    // The environment changed or is gone: read both again.
                    this.refresh(cx);
                    if let Some(id) = this.selected.clone() {
                        this.select(id, cx);
                    }
                }
                cx.notify();
            },
        ));
        cx.notify();
    }
}

fn summary_state(state: &str) -> (&'static str, Tone) {
    match state {
        "observed_active" => ("Active", Tone::Ok),
        "retired" => ("Retired", Tone::Neutral),
        "vm_stopped" => ("VM stopped", Tone::Warn),
        _ => ("Unknown", Tone::Neutral),
    }
}

fn container_state(container: &h::Container) -> (&'static str, Tone) {
    match container.state() {
        h::ContainerState::Running => ("Running", Tone::Ok),
        h::ContainerState::Created => ("Created", Tone::Accent),
        h::ContainerState::Stopped if container.exit_code.unwrap_or(0) != 0 => {
            ("Stopped", Tone::Danger)
        }
        h::ContainerState::Stopped => ("Stopped", Tone::Neutral),
        h::ContainerState::Unknown | h::ContainerState::Unspecified => ("Unknown", Tone::Warn),
    }
}

impl EnvironmentsPage {
    fn list_panel(&self, palette: &Palette, cx: &mut Context<Self>) -> Div {
        let (hover, press) = (palette.hover, palette.press);
        let rows = self.rows.iter().enumerate().map(|(index, row)| {
            let selected = self.selected.as_ref() == Some(&row.id);
            let (state, tone) = summary_state(&row.state);
            let id = row.id.clone();
            div()
                .id(ElementId::NamedInteger("environment".into(), index as u64))
                .snapshot(format!("environments.row.{index}"), None)
                .flex()
                .flex_col()
                .gap(px(4.))
                .px(px(8.))
                .py(px(8.))
                .rounded(px(RADIUS_INNER))
                .map(|this| {
                    if selected {
                        this.bg(press)
                    } else {
                        this.cursor_pointer()
                            .hover(move |style| style.bg(hover))
                            .active(move |style| style.bg(press))
                            .on_click(cx.listener(move |this, _, _, cx| {
                                this.select(id.clone(), cx);
                            }))
                    }
                })
                .child(mono(row.id.clone()))
                .child(
                    div()
                        .flex()
                        .items_start()
                        .gap(px(8.))
                        .child(dot(tone, SMALL_LINE, palette))
                        .child(small(state).text_color(palette.muted))
                        .child(small(row.origin.clone()).text_color(palette.faint))
                        .child(div().flex_1())
                        .child(
                            mono(
                                row.private_disk_bytes
                                    .map(format::bytes)
                                    .unwrap_or_default(),
                            )
                            .flex_none()
                            .text_color(palette.muted),
                        ),
                )
        });
        let mut list = div()
            .id("environment-list")
            .snapshot("environments.list.scroll", None)
            .flex_1()
            .min_h_0()
            .overflow_y_scroll()
            .p(px(8.))
            .flex()
            .flex_col()
            .gap(px(2.))
            .children(rows);
        if !self.next_cursor.is_empty() {
            let cursor = self.next_cursor.clone();
            list = list.child(div().pt(px(6.)).flex().child(button(
                "environments.more",
                "Load more",
                ButtonKind::Plain,
                palette,
                cx.listener(move |this, _, _, cx| this.list_page(cursor.clone(), cx)),
            )));
        }
        if let Some(error) = self.list.error() {
            list = list.child(failure("The list is not available", error, palette));
        }
        panel(palette)
            .h_full()
            .child(
                div()
                    .flex()
                    .items_start()
                    .justify_between()
                    .gap(px(12.))
                    .px(px(PAD))
                    .py(px(12.))
                    .border_b_1()
                    .border_color(palette.hairline)
                    .child(heading("Environments", palette))
                    .child(
                        mono(format::clock(self.collected_at.as_ref())).text_color(palette.faint),
                    ),
            )
            .child(list)
    }

    fn detail_panel(&self, palette: &Palette, cx: &mut Context<Self>) -> Div {
        let frame = panel(palette).h_full();
        let body = div()
            .id("environment-detail")
            .snapshot("environments.detail.scroll", None)
            .size_full()
            .overflow_y_scroll()
            .p(px(PAD))
            .flex()
            .flex_col()
            .gap(px(PAD));
        let environment = match &self.detail {
            Load::Ready(environment) => environment,
            Load::Pending => {
                let message = if self.selected.is_some() {
                    "Reading…"
                } else {
                    "No environment exists on this Host."
                };
                return frame.child(body.child(small(message).text_color(palette.faint)));
            }
            Load::Failed(error) => {
                return frame.child(body.child(failure(
                    "The environment is not available",
                    error,
                    palette,
                )));
            }
        };

        let remote = environment.origin() == h::EnvironmentOrigin::Remote;
        let mut badges = div().flex().flex_wrap().gap(px(6.)).child(badge(
            if remote { "Remote" } else { "Local" },
            Tone::Neutral,
            palette,
        ));
        if let Some(health) = &environment.health {
            let (label, tone) = health_state(health.state());
            badges = badges.child(badge(label, tone, palette));
        }
        let (inventory, inventory_tone) = match environment.inventory_state() {
            h::InventoryState::Available => ("Inventory current", Tone::Ok),
            h::InventoryState::VmStopped => ("VM stopped", Tone::Warn),
            h::InventoryState::Stale => ("Inventory stale", Tone::Warn),
            h::InventoryState::Unavailable => ("Inventory unavailable", Tone::Danger),
            h::InventoryState::Unspecified => ("Inventory unknown", Tone::Neutral),
        };
        badges = badges.child(badge(inventory, inventory_tone, palette));

        let owner = environment.owner.clone().unwrap_or_default();
        let limits = environment.limits.as_ref();
        let pid_limit = limits.and_then(|limits| limits.pids);
        let disk_limit = limits.and_then(|limits| limits.disk_bytes);
        let limit = |value: Option<String>| value.unwrap_or_else(|| "none".to_owned());
        let mut content = body
            .child(
                div()
                    .flex()
                    .flex_col()
                    .gap(px(6.))
                    .child(text(environment.display_name.clone()).font_weight(FontWeight::MEDIUM))
                    .child(mono(environment.id.clone()).text_color(palette.muted))
                    .child(badges),
            )
            // Above the details, so a preview opens in sight.
            .child(self.actions(environment, palette, cx))
            .when_some(self.operation.as_ref(), |content, operation| {
                content.child(operation_status(operation, palette))
            })
            .when(!environment.inventory_message.is_empty(), |content| {
                content.child(
                    notice(Tone::Warn, palette).child(small(environment.inventory_message.clone())),
                )
            })
            .child(
                div()
                    .flex()
                    .flex_col()
                    .gap(px(6.))
                    .child(field(
                        "Namespace",
                        mono(environment.namespace_id.clone()),
                        palette,
                    ))
                    .when(remote, |fields| {
                        fields
                            .child(field(
                                "Registration",
                                mono(owner.registration_id.clone()),
                                palette,
                            ))
                            .child(field(
                                "Allocation",
                                mono(owner.allocation_id.clone()),
                                palette,
                            ))
                    })
                    .child(field(
                        "Egress",
                        small(match environment.egress_mode() {
                            h::EgressMode::PublicInternet => "Public internet",
                            h::EgressMode::DenyAll => "Deny all",
                            h::EgressMode::Unspecified => "Unspecified",
                        }),
                        palette,
                    ))
                    .child(field(
                        "PID limit",
                        mono(limit(pid_limit.map(|pids| format::count(pids.into())))),
                        palette,
                    ))
                    .child(field(
                        "Disk limit",
                        mono(limit(disk_limit.map(format::bytes))),
                        palette,
                    ))
                    .child(field(
                        "Policy revision",
                        mono(environment.policy_revision.to_string()),
                        palette,
                    )),
            );

        if let Some(usage) = &environment.usage {
            content = content.child(self.usage_section(usage, palette));
        }
        content = content.child(self.containers_section(environment, palette));
        if !environment.volumes.is_empty() {
            content = content.child(self.volumes_section(environment, palette));
        }
        frame.child(content)
    }

    fn usage_section(&self, usage: &h::Usage, palette: &Palette) -> Div {
        const COLUMNS: [Column; 6] = [
            Column::number("Memory", 72.),
            Column::number("CPU time", 88.),
            Column::number("PIDs", 48.),
            Column::number("Disk", 72.),
            Column::number("Received", 72.),
            Column::number("Sent", 72.),
        ];
        let sampled = format::age(usage.sampled_at.as_ref(), self.collected_at.as_ref());
        let mut title = div()
            .flex()
            .flex_wrap()
            .items_start()
            .gap(px(8.))
            .child(heading("Resources", palette))
            .child(
                mono(format!(
                    "sampled {}",
                    format::clock(usage.sampled_at.as_ref())
                ))
                .text_color(palette.faint),
            );
        if usage.stale {
            title = title.child(small(format!("stale, {sampled}")).text_color(palette.warn));
        }
        let mut section = div().flex().flex_col().gap(px(8.)).child(title);
        if !usage.unavailable_reason.is_empty() {
            section = section
                .child(notice(Tone::Warn, palette).child(small(usage.unavailable_reason.clone())));
        }
        section.child(
            div()
                .max_w(px(504.))
                .flex()
                .flex_col()
                .child(table_head(&COLUMNS, palette))
                .child(table_row(
                    &COLUMNS,
                    true,
                    [
                        number(format::bytes(usage.memory_bytes), 72.).into_any_element(),
                        number(format::cpu_time(usage.cpu_nanos), 88.).into_any_element(),
                        number(format::count(usage.pids.into()), 48.).into_any_element(),
                        number(format::bytes(usage.disk_bytes), 72.).into_any_element(),
                        number(format::bytes(usage.network_rx_bytes), 72.).into_any_element(),
                        number(format::bytes(usage.network_tx_bytes), 72.).into_any_element(),
                    ],
                    palette,
                )),
        )
    }

    fn containers_section(&self, environment: &h::AgentEnvironment, palette: &Palette) -> Div {
        const COLUMNS: [Column; 5] = [
            Column::flex("Container"),
            Column::fixed("State", 136.),
            Column::number("Memory", 64.),
            Column::number("CPU time", 84.),
            Column::number("PIDs", 40.),
        ];
        let section = div()
            .flex()
            .flex_col()
            .gap(px(8.))
            .child(heading("Containers", palette));
        if environment.containers.is_empty() {
            return section.child(
                small("No container exists in this environment.").text_color(palette.faint),
            );
        }
        section.child(
            div()
                .flex()
                .flex_col()
                .child(table_head(&COLUMNS, palette))
                .children(
                    environment
                        .containers
                        .iter()
                        .enumerate()
                        .map(|(index, container)| {
                            let (label, tone) = container_state(container);
                            let usage = container.usage.clone().unwrap_or_default();
                            let mut state = div()
                                .flex()
                                .items_start()
                                .gap(px(8.))
                                .child(dot(tone, SMALL_LINE, palette))
                                .child(small(label));
                            if let Some(code) = container.exit_code {
                                state = state.child(mono(format!("exit {code}")).text_color(
                                    if code == 0 {
                                        palette.muted
                                    } else {
                                        palette.danger
                                    },
                                ));
                            }
                            // The image and the command run the width of the
                            // table: both can be long.
                            let (repository, digest) = container
                                .image
                                .split_once('@')
                                .unwrap_or((container.image.as_str(), ""));
                            div()
                                .flex()
                                .flex_col()
                                .gap(px(2.))
                                .py(px(8.))
                                .when(index > 0, |group| {
                                    group.border_t(px(1.)).border_color(palette.hairline)
                                })
                                .child(table_cells(
                                    &COLUMNS,
                                    [
                                        mono(container.id.clone()).into_any_element(),
                                        state.into_any_element(),
                                        number(format::bytes(usage.memory_bytes), 64.)
                                            .into_any_element(),
                                        number(format::cpu_time(usage.cpu_nanos), 84.)
                                            .into_any_element(),
                                        number(format::count(usage.pids.into()), 40.)
                                            .into_any_element(),
                                    ],
                                ))
                                .child(mono(repository.to_owned()).text_color(palette.muted))
                                .when(!digest.is_empty(), |group| {
                                    group.child(mono(digest.to_owned()).text_color(palette.muted))
                                })
                                .child(mono(container.argv.join(" ")).text_color(palette.faint))
                        }),
                ),
        )
    }

    fn volumes_section(&self, environment: &h::AgentEnvironment, palette: &Palette) -> Div {
        const COLUMNS: [Column; 4] = [
            Column::flex("Volume"),
            Column::flex("Mounted by"),
            Column::number("Used", 72.),
            Column::number("Limit", 72.),
        ];
        div()
            .flex()
            .flex_col()
            .gap(px(8.))
            .child(heading("Volumes", palette))
            .child(
                div()
                    .flex()
                    .flex_col()
                    .child(table_head(&COLUMNS, palette))
                    .children(
                        environment
                            .volumes
                            .iter()
                            .enumerate()
                            .map(|(index, volume)| {
                                let used =
                                    volume.usage.as_ref().map_or(0, |usage| usage.disk_bytes);
                                table_row(
                                    &COLUMNS,
                                    index == 0,
                                    [
                                        mono(volume.id.clone()).into_any_element(),
                                        mono(volume.mounted_by.join(", "))
                                            .text_color(palette.muted)
                                            .into_any_element(),
                                        number(format::bytes(used), 72.).into_any_element(),
                                        number(format::bytes(volume.limit_bytes), 72.)
                                            .text_color(palette.muted)
                                            .into_any_element(),
                                    ],
                                    palette,
                                )
                            }),
                    ),
            )
    }

    /// The cleanups an operator can ask for, or the one being previewed.
    fn actions(
        &self,
        environment: &h::AgentEnvironment,
        palette: &Palette,
        cx: &mut Context<Self>,
    ) -> Div {
        let frame = div().flex().flex_col().gap(px(12.));
        // The action that confirms keeps its corner when the row wraps.
        let confirm = |label: &'static str, cx: &mut Context<Self>| {
            div()
                .flex()
                .flex_wrap_reverse()
                .gap(px(8.))
                .child(button(
                    "environments.confirm",
                    label,
                    ButtonKind::Danger,
                    palette,
                    cx.listener(|this, _, _, cx| this.execute(cx)),
                ))
                .child(button(
                    "environments.cancel",
                    "Cancel",
                    ButtonKind::Plain,
                    palette,
                    cx.listener(|this, _, _, cx| this.cancel(cx)),
                ))
        };
        match &self.preview {
            Preview::None => frame.child(
                div()
                    .flex()
                    .flex_wrap_reverse()
                    .gap(px(8.))
                    .child(button(
                        "environments.reset",
                        "Reset…",
                        ButtonKind::Plain,
                        palette,
                        cx.listener(|this, _, _, cx| {
                            this.preview_cleanup(h::CleanupKind::Reset, cx)
                        }),
                    ))
                    .child(button(
                        "environments.discard",
                        "Discard…",
                        ButtonKind::Plain,
                        palette,
                        cx.listener(|this, _, _, cx| {
                            this.preview_cleanup(h::CleanupKind::Discard, cx)
                        }),
                    ))
                    .child(button(
                        "environments.dispose",
                        "Dispose locally…",
                        ButtonKind::Plain,
                        palette,
                        cx.listener(|this, _, _, cx| this.preview_disposal(cx)),
                    )),
            ),
            Preview::Reading(what) => {
                frame.child(small(format!("Reading the {what} plan…")).text_color(palette.faint))
            }
            Preview::Cleanup(plan) => {
                const COLUMNS: [Column; 3] = [
                    Column::fixed("Resource", 168.),
                    Column::flex("Effect"),
                    Column::fixed("Undo", 96.),
                ];
                let discard = plan.kind() == h::CleanupKind::Discard;
                let title: SharedString = if discard {
                    "Discard deletes the environment and everything in it."
                } else {
                    "Reset deletes the containers and keeps the named volumes."
                }
                .into();
                frame
                    .snapshot("environments.preview", None)
                    .child(text(title).font_weight(FontWeight::MEDIUM))
                    .child(
                        div()
                            .flex()
                            .flex_col()
                            .child(table_head(&COLUMNS, palette))
                            .children(plan.impacts.iter().enumerate().map(|(index, impact)| {
                                let target = impact.resource.clone().unwrap_or_default();
                                let undo = if impact.blocker {
                                    badge("Blocks", Tone::Danger, palette)
                                } else if impact.irreversible {
                                    badge("Not possible", Tone::Danger, palette)
                                } else {
                                    badge("Kept", Tone::Neutral, palette)
                                };
                                table_row(
                                    &COLUMNS,
                                    index == 0,
                                    [
                                        div()
                                            .flex()
                                            .flex_col()
                                            .gap(px(2.))
                                            .child(small(target.kind).text_color(palette.muted))
                                            .child(mono(target.resource_id))
                                            .into_any_element(),
                                        small(impact.effect.clone()).into_any_element(),
                                        div().flex().child(undo).into_any_element(),
                                    ],
                                    palette,
                                )
                            })),
                    )
                    .child(confirm(
                        if discard {
                            "Discard environment"
                        } else {
                            "Reset environment"
                        },
                        cx,
                    ))
            }
            Preview::Disposal(plan) => frame
                .snapshot("environments.preview", None)
                .child(
                    text("Disposal removes the environment's local state from this Host. It cannot be undone.")
                        .font_weight(FontWeight::MEDIUM),
                )
                .when(plan.activity_unknown, |frame| {
                    frame.child(notice(Tone::Warn, palette).child(small(
                        "The Host cannot tell whether a workload still runs in it.",
                    )))
                })
                .child(
                    div()
                        .flex()
                        .flex_col()
                        .gap(px(6.))
                        .child(field("Environment", mono(environment.id.clone()), palette))
                        .child(field("Namespace", mono(plan.namespace_id.clone()), palette))
                        .child(field(
                            "Plan revision",
                            mono(plan.revision.to_string()),
                            palette,
                        )),
                )
                .child(confirm("Dispose environment", cx)),
        }
    }
}

impl Render for EnvironmentsPage {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let palette = self.theme.palette();
        let compact = window.viewport_size().width < px(COMPACT_BELOW);
        div()
            .size_full()
            .p(px(PAD))
            .flex()
            .gap(px(PAD))
            .child(
                div()
                    .flex_none()
                    .w(px(if compact { 272. } else { 328. }))
                    .h_full()
                    .child(self.list_panel(&palette, cx)),
            )
            .child(
                div()
                    .flex_1()
                    .min_w(px(0.))
                    .h_full()
                    .child(self.detail_panel(&palette, cx)),
            )
    }
}
