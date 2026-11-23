//! The Agent VMM console: a window on one Host, for the people who build and
//! debug it.

pub mod app;
pub mod backend;
mod format;
#[cfg(any(test, feature = "harness"))]
pub mod harness;
mod log_view;
mod text_input;
pub mod theme;
mod views;
mod widgets;

pub use app::{ConsoleView, Page};
pub use log_view::VirtualLogView;
pub use text_input::TextInput;
pub use views::{HostPage, LogsPage};
