//! The virtual machine, and where its memory and disk are.

use super::{COMPACT_BELOW, Load, failure, follow_operation, operation_status, page, request_id};
use crate::{
    backend::{CallError, MemorySummary, OperatorBackend},
    format,
    theme::{
        DISPLAY, DISPLAY_LINE, FONT_MONO, MONO_METRICS, PAD, Palette, SMALL, SMALL_LINE, TEXT,
        TEXT_LINE, Theme, UI_METRICS, baseline,
    },
    widgets::{
        ButtonKind, Column, Part, Tone, bar, button, disabled_button, dot, heading, mono, notice,
        number, panel, small, swatch, table_head, table_row, text, trailing,
    },
};
use agent_vmm_proto::host::v1 as h;
use std::{sync::Arc, time::Duration};
use ui::{AnyElement, Context, Div, FontWeight, SharedString, Task, Window, div, prelude::*, px};

/// What the operator is doing about stopping the VM.
enum Stop {
    Idle,
    /// Reading what a stop would take down.
    Reading,
    /// Showing it, waiting for the operator to confirm.
    Confirming(h::GetVmImpactSummaryResponse),
}

pub struct HostPage {
    backend: Arc<dyn OperatorBackend>,
    theme: Theme,
    vm: Load<h::Vm>,
    health: Load<h::GetSystemHealthSummaryResponse>,
    disk: Load<h::GetLocalDiskSummaryResponse>,
    memory: Load<MemorySummary>,
    stop: Stop,
    operation: Option<Result<h::Operation, CallError>>,
    /// Samples the Host once a second while the page is in sight.
    sampling: Option<Task<()>>,
    following: Option<Task<()>>,
}

impl HostPage {
    pub fn new(backend: Arc<dyn OperatorBackend>, theme: Theme) -> Self {
        Self {
            backend,
            theme,
            vm: Load::Pending,
            health: Load::Pending,
            disk: Load::Pending,
            memory: Load::Pending,
            stop: Stop::Idle,
            operation: None,
            sampling: None,
            following: None,
        }
    }

    pub fn set_theme(&mut self, theme: Theme, cx: &mut Context<Self>) {
        self.theme = theme;
        cx.notify();
    }

    pub fn vm(&self) -> Option<&h::Vm> {
        self.vm.ready()
    }

    pub fn is_sampling(&self) -> bool {
        self.sampling.is_some()
    }

    /// The VM is known not to run, and is not on its way up or down.
    fn is_down(&self) -> bool {
        self.vm()
            .is_some_and(|vm| matches!(vm.state(), h::VmState::Stopped | h::VmState::Failed))
    }

    /// Samples while `visible`, and not at all otherwise.
    pub fn set_visible(&mut self, visible: bool, cx: &mut Context<Self>) {
        if !visible {
            self.sampling = None;
        } else if self.sampling.is_none() {
            let backend = self.backend.clone();
            self.sampling = Some(cx.spawn(async move |this, cx| {
                loop {
                    let (vm, health, disk, memory) = futures::join!(
                        backend.vm(),
                        backend.health(),
                        backend.disk(),
                        backend.memory()
                    );
                    let updated = this.update(cx, |this, cx| {
                        this.vm = vm.into();
                        this.health = health.into();
                        this.disk = disk.into();
                        this.memory = memory.into();
                        cx.notify();
                    });
                    if updated.is_err() {
                        return;
                    }
                    cx.background_executor().timer(Duration::from_secs(1)).await;
                }
            }));
        }
        cx.notify();
    }

    fn ask_to_stop(&mut self, cx: &mut Context<Self>) {
        self.stop = Stop::Reading;
        self.operation = None;
        let impact = self.backend.vm_impact();
        self.following = Some(cx.spawn(async move |this, cx| {
            let impact = impact.await;
            let _ = this.update(cx, |this, cx| {
                match impact {
                    Ok(impact) => this.stop = Stop::Confirming(impact),
                    Err(error) => {
                        this.stop = Stop::Idle;
                        this.operation = Some(Err(error));
                    }
                }
                cx.notify();
            });
        }));
        cx.notify();
    }

    fn cancel_stop(&mut self, cx: &mut Context<Self>) {
        self.stop = Stop::Idle;
        cx.notify();
    }

    fn confirm_stop(&mut self, cx: &mut Context<Self>) {
        let Stop::Confirming(impact) = std::mem::replace(&mut self.stop, Stop::Idle) else {
            return;
        };
        let vm = impact.vm.unwrap_or_default();
        // The fence makes the Host refuse the stop if the VM is no longer the
        // one whose impact the operator saw.
        let fence = h::VmMutationFence {
            state: vm.state,
            boot_id: vm.boot_id,
            observed_workloads: impact.known_workloads,
            whole_vm_impact_acknowledged: true,
        };
        let started = self.backend.stop_vm(request_id(), fence);
        self.follow(started, cx);
    }

    fn start(&mut self, cx: &mut Context<Self>) {
        let started = self.backend.start_vm(request_id());
        self.follow(started, cx);
    }

    fn follow(&mut self, started: crate::backend::Call<h::Operation>, cx: &mut Context<Self>) {
        self.following = Some(follow_operation(
            self.backend.clone(),
            started,
            cx,
            |this, operation, cx| {
                this.operation = Some(operation);
                cx.notify();
            },
        ));
        cx.notify();
    }
}

fn vm_state(state: h::VmState) -> (&'static str, Tone) {
    match state {
        h::VmState::Running => ("Running", Tone::Ok),
        h::VmState::Starting => ("Starting", Tone::Accent),
        h::VmState::Stopping => ("Stopping", Tone::Warn),
        h::VmState::Stopped => ("Stopped", Tone::Neutral),
        h::VmState::Failed => ("Failed", Tone::Danger),
        h::VmState::Unspecified => ("Unknown", Tone::Neutral),
    }
}

pub fn health_state(state: h::HealthState) -> (&'static str, Tone) {
    match state {
        h::HealthState::Healthy => ("Healthy", Tone::Ok),
        h::HealthState::Degraded => ("Degraded", Tone::Warn),
        h::HealthState::Unavailable => ("Unavailable", Tone::Danger),
        h::HealthState::Unspecified => ("Unknown", Tone::Neutral),
    }
}

/// A large figure with a caption on its baseline.
fn figure(value: String, caption: impl Into<SharedString>, palette: &Palette) -> Div {
    let drop =
        baseline(DISPLAY, DISPLAY_LINE, MONO_METRICS) - baseline(SMALL, SMALL_LINE, UI_METRICS);
    div()
        .flex()
        .items_start()
        .gap(px(10.))
        .child(
            div()
                .flex_none()
                .font_family(FONT_MONO)
                .text_size(px(DISPLAY))
                .line_height(px(DISPLAY_LINE))
                .whitespace_nowrap()
                .child(value),
        )
        .child(
            small(caption)
                // The nearest half pixel: a whole device pixel on a 2× display.
                .mt(px((drop * 2.).round() / 2.))
                .text_color(palette.muted),
        )
}

impl HostPage {
    fn vm_panel(&self, palette: &Palette, cx: &mut Context<Self>) -> Div {
        let vm = self.vm.ready();
        let state = vm.map_or(h::VmState::Unspecified, |vm| vm.state());
        let (label, tone) = vm_state(state);
        let health = self
            .health
            .ready()
            .and_then(|health| health.health.as_ref());
        let collected = self.health.ready().and_then(|h| h.collected_at.as_ref());

        let action: AnyElement = match (&self.stop, state) {
            (Stop::Confirming(_), _) => div().into_any_element(),
            (Stop::Reading, _) => {
                disabled_button("host.stop", "Reading impact", palette).into_any_element()
            }
            (Stop::Idle, h::VmState::Running) => button(
                "host.stop",
                "Stop VM…",
                ButtonKind::Plain,
                palette,
                cx.listener(|this, _, _, cx| this.ask_to_stop(cx)),
            )
            .into_any_element(),
            (Stop::Idle, h::VmState::Stopped | h::VmState::Failed) => button(
                "host.start",
                "Start VM",
                ButtonKind::Primary,
                palette,
                cx.listener(|this, _, _, cx| this.start(cx)),
            )
            .into_any_element(),
            (Stop::Idle, _) => disabled_button("host.stop", "Stop VM…", palette).into_any_element(),
        };

        // A name and its value, side by side with the next.
        // A fact shares the baseline of the state on its line. When the row
        // wraps, the next line starts under the state, at the left edge.
        let first_line_drop =
            baseline(TEXT, TEXT_LINE, UI_METRICS) - baseline(SMALL, SMALL_LINE, UI_METRICS);
        let fact = |name: &'static str, value: Div| {
            div()
                .flex_none()
                .flex()
                .items_start()
                .gap(px(8.))
                .mr(px(24.))
                .pt(px((first_line_drop * 2.).round() / 2.))
                .child(small(name).text_color(palette.muted))
                .child(value)
        };
        let mut summary = div()
            .flex()
            .items_start()
            .gap(px(16.))
            .child(
                div()
                    .flex_1()
                    .min_w(px(0.))
                    .flex()
                    .flex_wrap()
                    .items_start()
                    .gap(px(4.))
                    .child(
                        div()
                            .flex_none()
                            .flex()
                            .items_start()
                            .gap(px(8.))
                            .mr(px(24.))
                            .child(dot(tone, TEXT_LINE, palette))
                            .child(text(label).font_weight(FontWeight::MEDIUM)),
                    )
                    .child(fact(
                        "Boot ID",
                        match vm {
                            Some(vm) if !vm.boot_id.is_empty() => mono(vm.boot_id.clone()),
                            _ => mono("none").text_color(palette.faint),
                        },
                    ))
                    .when_some(health, |facts, health| {
                        let (label, tone) = health_state(health.state());
                        facts.child(fact(
                            "Health",
                            div()
                                .flex()
                                .items_start()
                                .gap(px(8.))
                                .child(dot(tone, SMALL_LINE, palette))
                                .child(small(label)),
                        ))
                    })
                    .child(fact(
                        "Collected",
                        mono(format::clock(collected)).text_color(palette.muted),
                    )),
            )
            // Centered on the first line, whatever wraps below it.
            .child(
                div()
                    .flex_none()
                    .h(px(TEXT_LINE))
                    .flex()
                    .items_center()
                    .child(action),
            );
        if let Some(error) = vm.filter(|vm| !vm.error.is_empty()).map(|vm| &vm.error) {
            summary = summary.child(
                notice(Tone::Danger, palette)
                    .w_full()
                    .child(small(error.clone())),
            );
        }

        let mut body = panel(palette)
            .snapshot("host.vm", None)
            .p(px(PAD))
            .gap(px(12.))
            .child(summary);
        if self.is_down() {
            body = body.child(
                small("Memory, health and disk are reported while the VM runs.")
                    .text_color(palette.muted),
            );
        }
        if let Some(error) = self.vm.error() {
            body = body.child(failure("The Host does not answer", error, palette));
        }
        if let Stop::Confirming(impact) = &self.stop {
            body = body.child(self.stop_confirmation(impact, palette, cx));
        }
        if let Some(operation) = &self.operation {
            body = body.child(operation_status(operation, palette));
        }
        body
    }

    /// What stopping the VM takes down, and the two ways out.
    fn stop_confirmation(
        &self,
        impact: &h::GetVmImpactSummaryResponse,
        palette: &Palette,
        cx: &mut Context<Self>,
    ) -> Div {
        const COLUMNS: [Column; 3] = [
            Column::fixed("Kind", 96.),
            Column::flex("Environment"),
            Column::flex("Owner"),
        ];
        let count = impact.known_workloads.len();
        let sentence = match count {
            0 => "Stopping the VM stops no workload the Host knows of.".to_owned(),
            1 => "Stopping the VM stops 1 workload.".to_owned(),
            n => format!("Stopping the VM stops {n} workloads."),
        };
        let mut band = div()
            .snapshot("host.stop.confirmation", None)
            .flex()
            .flex_col()
            .gap(px(12.))
            .pt(px(12.))
            .border_t(px(1.))
            .border_color(palette.hairline)
            .child(text(sentence).font_weight(FontWeight::MEDIUM));
        if !impact.complete || impact.overflow {
            let reason = if impact.overflow {
                "More workloads exist than the Host lists here.".to_owned()
            } else {
                format!("The list is not complete: {}", impact.issue)
            };
            band = band.child(notice(Tone::Warn, palette).child(small(reason)));
        }
        if count > 0 {
            band = band.child(
                div()
                    .flex()
                    .flex_col()
                    .child(table_head(&COLUMNS, palette))
                    .children(impact.known_workloads.iter().enumerate().map(
                        |(index, workload)| {
                            let owner = if workload.kind == "registration" {
                                mono(workload.resource_id.clone())
                            } else {
                                mono("local").text_color(palette.muted)
                            };
                            table_row(
                                &COLUMNS,
                                index == 0,
                                [
                                    small(workload.kind.clone())
                                        .text_color(palette.muted)
                                        .into_any_element(),
                                    mono(workload.environment_id.clone()).into_any_element(),
                                    owner.into_any_element(),
                                ],
                                palette,
                            )
                        },
                    )),
            );
        }
        band.child(
            // The confirming button keeps its corner when the row wraps.
            div()
                .flex()
                .flex_wrap_reverse()
                .gap(px(8.))
                .child(button(
                    "host.stop.confirm",
                    "Stop VM",
                    ButtonKind::Danger,
                    palette,
                    cx.listener(|this, _, _, cx| this.confirm_stop(cx)),
                ))
                .child(button(
                    "host.stop.cancel",
                    "Keep running",
                    ButtonKind::Plain,
                    palette,
                    cx.listener(|this, _, _, cx| this.cancel_stop(cx)),
                )),
        )
    }

    fn memory_panel(&self, palette: &Palette) -> Div {
        let live = self.sampling.is_some() && self.memory.ready().is_some();
        let title = div()
            .flex()
            .items_start()
            .justify_between()
            .gap(px(12.))
            .child(heading("Physical memory", palette))
            .child(
                div()
                    .flex()
                    .items_start()
                    .gap(px(8.))
                    .children(self.memory.ready().map(|memory| {
                        mono(format::clock(memory.sampled_at.as_ref())).text_color(palette.faint)
                    }))
                    .children(live.then(|| small("1 Hz").text_color(palette.ok))),
            );
        let body = panel(palette)
            .snapshot("host.memory", None)
            .p(px(PAD))
            .gap(px(12.))
            .child(title);
        let memory = match &self.memory {
            Load::Ready(memory) => memory,
            Load::Pending => return body.child(small("Reading…").text_color(palette.faint)),
            Load::Failed(error) if error.is_unimplemented() => {
                return body.child(
                    small(
                        "This Host does not report its memory by layer. \
                         The layers appear here once it serves GetMemorySummary.",
                    )
                    .text_color(palette.muted),
                );
            }
            Load::Failed(_)
                if self
                    .vm()
                    .is_some_and(|vm| vm.state() != h::VmState::Running) =>
            {
                return body.child(
                    small("No guest memory exists while the VM is not running.")
                        .text_color(palette.muted),
                );
            }
            Load::Failed(error) => {
                return body.child(failure(
                    "The memory layers are not available",
                    error,
                    palette,
                ));
            }
        };

        let pool = &memory.pool;
        let total =
            pool.guest_plugged_bytes + pool.free_bytes + pool.inflated_bytes + pool.clean_bytes;
        let workloads: u64 = memory.environments.iter().map(|e| e.plugged_bytes).sum();
        let dom0 = Part::Solid(palette.layer_dom0);
        let free = Part::Solid(palette.layer_free);
        let inflated = Part::Dashed(palette.faint);
        let clean = Part::Hollow(palette.border);
        let mut parts = vec![(memory.dom0_limit_bytes, dom0)];
        parts.extend(
            memory
                .environments
                .iter()
                .enumerate()
                .map(|(index, e)| (e.plugged_bytes, Part::Solid(palette.layer_guest[index % 2]))),
        );
        parts.extend([
            (pool.free_bytes, free),
            (pool.inflated_bytes, inflated),
            (pool.clean_bytes, clean),
        ]);

        const COLUMNS: [Column; 4] = [
            Column::flex("Layer"),
            Column::number("Bytes", 80.),
            Column::number("Share", 64.),
            Column::fixed("On the host", 176.),
        ];
        let rows = [
            (
                dom0,
                "Dom0",
                memory.dom0_limit_bytes,
                format!("{} in use", format::bytes(memory.dom0_used_bytes)),
            ),
            (
                Part::Solid(palette.layer_guest[0]),
                "Workload VMs",
                workloads,
                "resident when touched".to_owned(),
            ),
            (free, "Free pool", pool.free_bytes, "resident".to_owned()),
            (
                inflated,
                "Inflated",
                pool.inflated_bytes,
                "returned to macOS".to_owned(),
            ),
            (clean, "Clean", pool.clean_bytes, "never touched".to_owned()),
        ];
        body.child(figure(
            format::bytes(memory.carrier_footprint_bytes),
            format!(
                "charged to the carrier by macOS, of {} guest memory",
                format::bytes(total)
            ),
            palette,
        ))
        .child(bar(parts, Vec::new(), 20., palette))
        .child(
            div()
                .flex()
                .flex_col()
                .child(table_head(&COLUMNS, palette))
                .children(rows.into_iter().enumerate().map(
                    |(index, (part, name, bytes, note))| {
                        table_row(
                            &COLUMNS,
                            index == 0,
                            [
                                div()
                                    .flex()
                                    .items_start()
                                    .gap(px(8.))
                                    .child(swatch(part))
                                    .child(small(name))
                                    .into_any_element(),
                                number(format::bytes(bytes), 80.).into_any_element(),
                                number(format::percent(bytes, total), 64.)
                                    .text_color(palette.muted)
                                    .into_any_element(),
                                small(note).text_color(palette.muted).into_any_element(),
                            ],
                            palette,
                        )
                    },
                )),
        )
    }

    fn workloads_panel(&self, palette: &Palette) -> Option<Div> {
        let memory = self.memory.ready()?;
        const COLUMNS: [Column; 7] = [
            Column::flex("Environment"),
            Column::number("Boot", 72.),
            Column::number("Plugged", 72.),
            Column::number("Grant", 72.),
            Column::fixed("Plugged of grant", 120.),
            Column::number("zram", 72.),
            Column::number("EBUSY", 48.),
        ];
        Some(
            panel(palette)
                .snapshot("host.workloads", None)
                .p(px(PAD))
                .gap(px(12.))
                .child(heading("Workload VM memory", palette))
                .child(
                    div()
                        .flex()
                        .flex_col()
                        .child(table_head(&COLUMNS, palette))
                        .children(memory.environments.iter().enumerate().map(|(index, e)| {
                            let usage = bar(
                                vec![
                                    (e.plugged_bytes, Part::Solid(palette.layer_guest[index % 2])),
                                    (
                                        e.grant_bytes.saturating_sub(e.plugged_bytes),
                                        Part::Solid(palette.hairline),
                                    ),
                                ],
                                vec![e.boot_bytes],
                                8.,
                                palette,
                            );
                            let busy = number(e.ebusy_count.to_string(), 48.).text_color(
                                if e.ebusy_count > 0 {
                                    palette.warn
                                } else {
                                    palette.faint
                                },
                            );
                            table_row(
                                &COLUMNS,
                                index == 0,
                                [
                                    mono(e.environment_id.clone()).into_any_element(),
                                    number(format::bytes(e.boot_bytes), 72.)
                                        .text_color(palette.muted)
                                        .into_any_element(),
                                    number(format::bytes(e.plugged_bytes), 72.).into_any_element(),
                                    number(format::bytes(e.grant_bytes), 72.)
                                        .text_color(palette.muted)
                                        .into_any_element(),
                                    // Centered on the first line of the row.
                                    div()
                                        .w_full()
                                        .h(px(SMALL_LINE))
                                        .flex()
                                        .items_center()
                                        .child(usage)
                                        .into_any_element(),
                                    number(format::bytes(e.zram_bytes), 72.).into_any_element(),
                                    busy.into_any_element(),
                                ],
                                palette,
                            )
                        })),
                ),
        )
    }

    fn health_panel(&self, palette: &Palette) -> Div {
        const COLUMNS: [Column; 3] = [
            Column::flex("Component"),
            Column::fixed("State", 96.),
            Column::number("Observed", 80.),
        ];
        let body = panel(palette)
            .snapshot("host.health", None)
            .p(px(PAD))
            .gap(px(12.))
            .child(heading("Health", palette));
        let health = match &self.health {
            Load::Ready(health) => health,
            Load::Pending => return body.child(small("Reading…").text_color(palette.faint)),
            Load::Failed(error) => {
                return body.child(failure("Health is not available", error, palette));
            }
        };
        let components = health
            .health
            .as_ref()
            .map(|health| health.components.as_slice())
            .unwrap_or_default();
        body.child(
            div()
                .flex()
                .flex_col()
                .child(table_head(&COLUMNS, palette))
                .children(components.iter().enumerate().map(|(index, component)| {
                    let (label, tone) = health_state(component.state());
                    let mut name = div()
                        .flex()
                        .flex_col()
                        .gap(px(2.))
                        .child(mono(component.component.clone()));
                    if !component.reason.is_empty() {
                        name =
                            name.child(small(component.reason.clone()).text_color(palette.muted));
                    }
                    table_row(
                        &COLUMNS,
                        index == 0,
                        [
                            name.into_any_element(),
                            div()
                                .flex()
                                .items_start()
                                .gap(px(8.))
                                .child(dot(tone, SMALL_LINE, palette))
                                .child(small(label))
                                .into_any_element(),
                            trailing(
                                format::age(
                                    component.observed_at.as_ref(),
                                    health.collected_at.as_ref(),
                                ),
                                80.,
                            )
                            .text_color(palette.muted)
                            .into_any_element(),
                        ],
                        palette,
                    )
                })),
        )
    }

    fn disk_panel(&self, palette: &Palette) -> Div {
        const COLUMNS: [Column; 2] = [Column::flex("Use"), Column::number("Bytes", 88.)];
        let body = panel(palette)
            .snapshot("host.disk", None)
            .p(px(PAD))
            .gap(px(12.))
            .child(heading("Guest state disk", palette));
        let disk = match &self.disk {
            Load::Ready(disk) => disk,
            Load::Pending => return body.child(small("Reading…").text_color(palette.faint)),
            Load::Failed(error) => {
                return body.child(failure("The disk is not available", error, palette));
            }
        };
        let Some(fs) = &disk.guest_state_fs else {
            return body.child(small(disk.unavailable_reason.clone()).text_color(palette.muted));
        };
        let used = fs.used_bytes.unwrap_or(0);
        let capacity = fs.capacity_bytes.unwrap_or(0);
        let reserved = fs.import_reservation_bytes.unwrap_or(0);
        // A Host that does not break the use down reports only the total.
        let known = [
            (
                "Images and runtime",
                fs.image_runtime_bytes,
                Part::Solid(palette.layer_dom0),
            ),
            (
                "Environments",
                fs.private_bytes,
                Part::Solid(palette.layer_guest[0]),
            ),
            (
                "Build cache",
                fs.build_cache_bytes,
                Part::Solid(palette.layer_zram),
            ),
            ("Other", fs.other_bytes, Part::Solid(palette.layer_free)),
        ];
        let mut uses: Vec<_> = known
            .into_iter()
            .filter_map(|(name, bytes, part)| Some((name, bytes?, part)))
            .collect();
        if uses.is_empty() {
            uses.push(("Used", used, Part::Solid(palette.layer_guest[0])));
        }
        uses.push((
            "Reserved for imports",
            reserved,
            Part::Hollow(palette.border),
        ));
        let shown: u64 = uses.iter().map(|(_, bytes, _)| *bytes).sum();
        let mut parts: Vec<_> = uses
            .iter()
            .map(|(_, bytes, part)| (*bytes, *part))
            .collect();
        parts.push((
            capacity.saturating_sub(shown),
            Part::Solid(palette.hairline),
        ));
        body.child(figure(
            format::bytes(used),
            format!("used of {}", format::bytes(capacity)),
            palette,
        ))
        .child(bar(parts, Vec::new(), 12., palette))
        .child(
            div()
                .flex()
                .flex_col()
                .child(table_head(&COLUMNS, palette))
                .children(
                    uses.into_iter()
                        .enumerate()
                        .map(|(index, (name, bytes, part))| {
                            table_row(
                                &COLUMNS,
                                index == 0,
                                [
                                    div()
                                        .flex()
                                        .items_start()
                                        .gap(px(8.))
                                        .child(swatch(part))
                                        .child(small(name))
                                        .into_any_element(),
                                    number(format::bytes(bytes), 88.).into_any_element(),
                                ],
                                palette,
                            )
                        }),
                ),
        )
    }
}

impl Render for HostPage {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let palette = self.theme.palette();
        let compact = window.viewport_size().width < px(COMPACT_BELOW);
        let vm = self.vm_panel(&palette, cx);
        if self.is_down() {
            // Nothing below the VM exists to report on.
            return page("host-page", "host.scroll").child(vm);
        }
        let memory = div()
            .flex()
            .flex_col()
            .gap(px(PAD))
            .child(self.memory_panel(&palette))
            .children(self.workloads_panel(&palette));
        let side = div()
            .flex()
            .flex_col()
            .gap(px(PAD))
            .child(self.health_panel(&palette))
            .child(self.disk_panel(&palette));
        let columns = if compact {
            div()
                .flex()
                .flex_col()
                .gap(px(PAD))
                .child(memory)
                .child(side)
        } else {
            div()
                .flex()
                .items_start()
                .gap(px(PAD))
                .child(memory.flex_1().min_w(px(0.)))
                .child(side.flex_none().w(px(372.)))
        };
        page("host-page", "host.scroll").child(
            div()
                .flex()
                .flex_col()
                .gap(px(PAD))
                .child(vm)
                .child(columns),
        )
    }
}
