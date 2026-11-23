//! The four pages of the console and what they share.

mod access;
mod environments;
mod host;
mod logs;

pub use access::AccessPage;
pub use environments::EnvironmentsPage;
pub use host::HostPage;
pub use logs::LogsPage;

use crate::{
    backend::{Call, CallError, OperatorBackend},
    format,
    theme::{PAD, Palette},
    widgets::{Tone, badge, mono, notice, small},
};
use agent_vmm_proto::host::v1 as h;
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use ui::{Context, Div, SharedString, Stateful, Task, div, prelude::*, px};

/// What a page knows of something it asked the Host for.
#[derive(Clone, Debug, Default)]
pub enum Load<T> {
    #[default]
    Pending,
    Ready(T),
    Failed(CallError),
}

impl<T> Load<T> {
    pub fn ready(&self) -> Option<&T> {
        match self {
            Self::Ready(value) => Some(value),
            _ => None,
        }
    }

    pub fn error(&self) -> Option<&CallError> {
        match self {
            Self::Failed(error) => Some(error),
            _ => None,
        }
    }
}

impl<T> From<Result<T, CallError>> for Load<T> {
    fn from(result: Result<T, CallError>) -> Self {
        match result {
            Ok(value) => Self::Ready(value),
            Err(error) => Self::Failed(error),
        }
    }
}

/// An ID for a mutation, so the Host can tell a retry from a new request.
pub fn request_id() -> String {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    format!(
        "console-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

/// Follows an operation from the call that starts it until it ends, giving
/// every state it is seen in to `report`.
pub fn follow_operation<V: 'static>(
    backend: Arc<dyn OperatorBackend>,
    started: Call<h::Operation>,
    cx: &mut Context<V>,
    report: impl Fn(&mut V, Result<h::Operation, CallError>, &mut Context<V>) + 'static,
) -> Task<()> {
    cx.spawn(async move |this, cx| {
        let mut next = started;
        loop {
            let result = next.await;
            let running = result
                .as_ref()
                .is_ok_and(|operation| operation.state() == h::OperationState::Running);
            let id = result.as_ref().ok().map(|operation| operation.id.clone());
            if this
                .update(cx, |this, cx| report(this, result, cx))
                .is_err()
                || !running
            {
                return;
            }
            cx.background_executor()
                .timer(Duration::from_millis(500))
                .await;
            next = backend.operation(id.unwrap_or_default());
        }
    })
}

/// The state of an operation the operator started, or why it did not start.
pub fn operation_status(result: &Result<h::Operation, CallError>, palette: &Palette) -> Div {
    let operation = match result {
        Ok(operation) => operation,
        Err(error) => return failure("The Host refused the request", error, palette),
    };
    let (label, tone) = match operation.state() {
        h::OperationState::Running => ("Running", Tone::Accent),
        h::OperationState::Succeeded => ("Succeeded", Tone::Ok),
        h::OperationState::Failed => ("Failed", Tone::Danger),
        h::OperationState::Interrupted => ("Interrupted", Tone::Warn),
        h::OperationState::Unspecified => ("Unknown", Tone::Neutral),
    };
    let mut status = div()
        .flex()
        .flex_wrap()
        .items_start()
        .gap(px(8.))
        .child(badge(label, tone, palette))
        .child(mono(operation.kind.clone()).py(px(2.)))
        .child(
            mono(operation.id.clone())
                .py(px(2.))
                .text_color(palette.muted),
        );
    if !operation.stage.is_empty() {
        status = status.child(
            small(operation.stage.clone())
                .py(px(2.))
                .text_color(palette.muted),
        );
    }
    status = status.child(
        mono(format::clock(operation.updated_at.as_ref()))
            .py(px(2.))
            .text_color(palette.faint),
    );
    match &operation.error {
        Some(error) => div().flex().flex_col().gap(px(8.)).child(status).child(
            notice(Tone::Danger, palette)
                .child(small(format!("{}: {}", error.reason, error.message))),
        ),
        None => status,
    }
}

/// Why a call failed, in the place its answer would be.
pub fn failure(what: impl Into<SharedString>, error: &CallError, palette: &Palette) -> Div {
    notice(Tone::Danger, palette).child(
        div()
            .flex_1()
            .min_w(px(0.))
            .flex()
            .flex_col()
            .gap(px(2.))
            .child(small(what))
            .child(mono(error.to_string()).text_color(palette.muted)),
    )
}

/// The scrolling body of a page. `id` names it in the frame snapshot, where
/// `.scroll` marks what a fold may cut.
pub fn page(id: &'static str, snapshot: &'static str) -> Stateful<Div> {
    div()
        .id(id)
        .snapshot(snapshot, None)
        .size_full()
        .overflow_y_scroll()
        .p(px(PAD))
}

/// Windows narrower than this stack what wide ones put side by side.
pub const COMPACT_BELOW: f32 = 1000.0;
