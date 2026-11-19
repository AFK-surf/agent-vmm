//! End-to-end tests: the `vmm-guest-proto` client calls a real server over TCP.
#![cfg(target_os = "linux")]

mod exec;
mod health;
mod lifecycle;
mod logs;
mod process;
mod support;
mod workspace;
