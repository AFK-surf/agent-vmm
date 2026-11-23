#[cfg(target_os = "macos")]
fn main() {
    use agent_vmm_console::{
        ConsoleView,
        backend::{UdsBackend, default_socket_path},
        theme::{Theme, register_fonts},
    };
    use std::sync::Arc;
    use ui::{Bounds, WindowBounds, WindowOptions, prelude::*, px, size};

    let backend = match UdsBackend::new(default_socket_path()) {
        Ok(backend) => Arc::new(backend),
        Err(error) => {
            eprintln!("agent-vmm-console: the RPC thread did not start: {error}");
            std::process::exit(1);
        }
    };
    let platform = std::rc::Rc::new(ui::MacPlatform::new(false));
    ui::Application::with_platform(platform).run(move |cx| {
        if let Err(error) = register_fonts(cx) {
            eprintln!("agent-vmm-console: the fonts did not register: {error:#}");
        }
        let options = WindowOptions {
            window_bounds: Some(WindowBounds::Windowed(Bounds::centered(
                None,
                size(px(1200.), px(800.)),
                cx,
            ))),
            window_min_size: Some(size(px(860.), px(600.))),
            titlebar: Some(ui::TitlebarOptions {
                title: Some("Agent VMM".into()),
                ..Default::default()
            }),
            ..WindowOptions::default()
        };
        let opened = cx.open_window(options, |window, cx| {
            cx.new(|cx| ConsoleView::new(backend, Theme::default(), window, cx))
        });
        if let Err(error) = opened {
            eprintln!("agent-vmm-console: the window did not open: {error:#}");
            cx.quit();
        }
        // The console holds no state of its own: closing its window ends it.
        cx.set_quit_mode(ui::QuitMode::LastWindowClosed);
    });
}

#[cfg(not(target_os = "macos"))]
fn main() {
    eprintln!("agent-vmm-console opens a window on macOS only.");
    std::process::exit(2);
}
