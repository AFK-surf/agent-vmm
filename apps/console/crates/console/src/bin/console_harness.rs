//! Draws the console without a screen: pictures of its states, and the frames
//! of a walk through it.

use agent_vmm_console::{
    Page,
    harness::{Console, State},
    theme::Theme,
};
use agent_vmm_proto::host::v1 as h;
use anyhow::{Context as _, Result, bail};
use std::path::{Path, PathBuf};

const USAGE: &str = "usage: console-harness shots <directory> [--scale <factor>]
       console-harness film <directory> [--scale <factor>]";

fn main() -> Result<()> {
    let mut arguments = std::env::args().skip(1);
    let command = arguments.next().context(USAGE)?;
    let directory = PathBuf::from(arguments.next().context(USAGE)?);
    let mut scale = 2.0;
    while let Some(flag) = arguments.next() {
        match flag.as_str() {
            "--scale" => scale = arguments.next().context(USAGE)?.parse()?,
            _ => bail!("{USAGE}"),
        }
    }
    std::fs::create_dir_all(&directory)?;
    match command.as_str() {
        "shots" => shots(&directory, scale),
        "film" => film(&directory, scale),
        _ => bail!("{USAGE}"),
    }
}

/// One picture of every state at the default and the smallest window size.
fn shots(directory: &Path, scale: f32) -> Result<()> {
    for theme in [Theme::Dark, Theme::Light] {
        for (width, height) in [(1200, 800), (880, 620)] {
            for state in State::ALL {
                let console = Console::open(width, height, scale, theme);
                console.enter(state);
                let name = format!("{}-{width}x{height}-{}.png", state.name(), theme.name());
                console.session.image().save(directory.join(&name))?;
                println!("{name}");
            }
        }
    }
    Ok(())
}

const FPS: u64 = 60;

/// The pointer drawn into the frames, its hot spot at the top left: `#` is
/// its outline and `o` its fill.
const POINTER: [&str; 17] = [
    "#           ",
    "##          ",
    "#o#         ",
    "#oo#        ",
    "#ooo#       ",
    "#oooo#      ",
    "#ooooo#     ",
    "#oooooo#    ",
    "#ooooooo#   ",
    "#oooooooo#  ",
    "#ooooooooo# ",
    "#oooooo#####",
    "#ooo#oo#    ",
    "#oo# #oo#   ",
    "#o#  #oo#   ",
    "##    #oo#  ",
    "       ##   ",
];

/// Drives a console through input events and saves every frame at 60 per second.
struct Recorder<'a> {
    console: &'a Console,
    directory: &'a Path,
    scale: f32,
    frames: usize,
    pointer: (f32, f32),
    pressed: bool,
    /// Lines sent to the open log stream so far.
    lines: u64,
}

impl Recorder<'_> {
    /// Lets one frame pass on the clock, draws it and saves it.
    fn frame(&mut self) -> Result<()> {
        self.console.session.settle(1000 / FPS);
        // A container that writes ten lines a second.
        if self.frames.is_multiple_of(6) {
            let (line, stream) = &live_lines()[self.lines as usize % 5];
            self.console
                .backend
                .push_log(line, *stream, 2_000 + self.lines);
            self.lines += 1;
        }
        let mut image = self.console.session.image();
        self.draw_pointer(&mut image);
        image.save(self.directory.join(format!("frame-{:05}.png", self.frames)))?;
        self.frames += 1;
        Ok(())
    }

    fn hold(&mut self, frames: usize) -> Result<()> {
        (0..frames).try_for_each(|_| self.frame())
    }

    fn draw_pointer(&self, image: &mut image::RgbaImage) {
        let scale = self.scale.round().max(1.) as i64;
        let (x, y) = (
            (self.pointer.0 * self.scale).round() as i64,
            (self.pointer.1 * self.scale).round() as i64,
        );
        let (width, height) = (i64::from(image.width()), i64::from(image.height()));
        let mut blend = |x: i64, y: i64, color: [u8; 3], alpha: f32| {
            if (0..width).contains(&x) && (0..height).contains(&y) {
                let pixel = image.get_pixel_mut(x as u32, y as u32);
                for (channel, value) in pixel.0.iter_mut().zip(color) {
                    *channel = (f32::from(*channel) * (1. - alpha) + f32::from(value) * alpha)
                        .round() as u8;
                }
            }
        };
        if self.pressed {
            let radius = 10. * self.scale;
            let reach = radius.ceil() as i64;
            for dy in -reach..=reach {
                for dx in -reach..=reach {
                    let distance = ((dx * dx + dy * dy) as f32).sqrt();
                    let coverage = (radius + 0.5 - distance).clamp(0., 1.);
                    blend(x + dx, y + dy, [122, 165, 216], 0.4 * coverage);
                }
            }
        }
        for (row, line) in POINTER.iter().enumerate() {
            for (column, cell) in line.bytes().enumerate() {
                let color = match cell {
                    b'#' => [255, 255, 255],
                    b'o' => [20, 20, 20],
                    _ => continue,
                };
                for dy in 0..scale {
                    for dx in 0..scale {
                        blend(
                            x + column as i64 * scale + dx,
                            y + row as i64 * scale + dy,
                            color,
                            1.,
                        );
                    }
                }
            }
        }
    }

    /// Glides the pointer to `target` over `frames` frames.
    fn move_to(&mut self, target: (f32, f32), frames: usize) -> Result<()> {
        let from = self.pointer;
        for step in 1..=frames {
            let t = step as f32 / frames as f32;
            let eased = t * t * (3. - 2. * t);
            self.pointer = (
                from.0 + (target.0 - from.0) * eased,
                from.1 + (target.1 - from.1) * eased,
            );
            self.console
                .session
                .pointer_move(self.pointer.0, self.pointer.1, self.pressed);
            self.frame()?;
        }
        Ok(())
    }

    /// Glides to the middle of the element recorded as `id` and clicks it.
    fn click(&mut self, id: &str) -> Result<()> {
        let target = self.console.session.center(id);
        self.move_to(target, 30)?;
        self.console
            .session
            .pointer_down(self.pointer.0, self.pointer.1);
        self.pressed = true;
        self.hold(4)?;
        self.console
            .session
            .pointer_up(self.pointer.0, self.pointer.1);
        self.pressed = false;
        self.hold(20)
    }

    fn type_text(&mut self, text: &str) -> Result<()> {
        let mut typed = String::new();
        for character in text.chars() {
            typed.push(character);
            self.console.type_search(&character.to_string());
            self.hold(7)?;
        }
        Ok(())
    }
}

fn live_lines() -> [(String, h::LogStream); 5] {
    let line = |level: &str, module: &str, message: &str| {
        format!("{level}\x1b[0m \x1b[1m{module}\x1b[0m {message}")
    };
    [
        (
            line(
                "\x1b[32m INFO",
                "cue_backend::router",
                "GET /v1/sessions \x1b[32m200\x1b[0m in 6 ms",
            ),
            h::LogStream::Stdout,
        ),
        (
            line(
                "\x1b[32m INFO",
                "cue_backend::router",
                "POST /v1/ifc/labels:batch \x1b[32m200\x1b[0m in 19 ms",
            ),
            h::LogStream::Stdout,
        ),
        (
            line(
                "\x1b[33m WARN",
                "cue_backend::db",
                "slow query took \x1b[1m1184 ms\x1b[0m: SELECT id FROM sessions WHERE workspace_id = $1",
            ),
            h::LogStream::Stdout,
        ),
        (
            line(
                "\x1b[32m INFO",
                "cue_backend::router",
                "GET /healthz \x1b[32m200\x1b[0m in 2 ms",
            ),
            h::LogStream::Stdout,
        ),
        (
            line(
                "\x1b[31mERROR",
                "cue_backend::provider",
                "upstream timeout after 30000 ms: anthropic request 5d0c2e7a",
            ),
            h::LogStream::Stderr,
        ),
    ]
}

/// A walk through the four pages: the memory of the Host as it is sampled,
/// an environment, a log searched, paused and followed again, a token, and
/// the VM stopped after its impact was confirmed.
fn film(directory: &Path, scale: f32) -> Result<()> {
    let console = Console::open(1200, 800, scale, Theme::Dark);
    let mut recorder = Recorder {
        console: &console,
        directory,
        scale,
        frames: 0,
        pointer: (640., 420.),
        pressed: false,
        lines: 0,
    };
    let chapter = |recorder: &Recorder, name: &str| {
        println!("frame {:05} {name}", recorder.frames);
    };

    chapter(&recorder, "host");
    recorder.hold(150)?;

    chapter(&recorder, "environments");
    recorder.click("tab.environments")?;
    recorder.hold(40)?;
    recorder.click("environments.more")?;
    recorder.click("environments.row.3")?;
    recorder.hold(70)?;

    chapter(&recorder, "logs");
    recorder.click("tab.logs")?;
    recorder.hold(60)?;
    recorder.click("logs.search")?;
    recorder.type_text("timeout")?;
    recorder.hold(60)?;
    recorder.click("logs.lines.matches")?;
    recorder.hold(70)?;
    recorder.click("logs.lines.all")?;
    recorder.hold(30)?;
    recorder.click("logs.pause")?;
    recorder.hold(100)?;
    recorder.click("logs.follow")?;
    recorder.hold(70)?;

    chapter(&recorder, "access");
    recorder.click("tab.access")?;
    recorder.hold(50)?;
    recorder.click("access.token.create")?;
    recorder.hold(80)?;

    chapter(&recorder, "stop");
    recorder.click("tab.host")?;
    recorder.hold(40)?;
    recorder.click("host.stop")?;
    recorder.hold(100)?;
    recorder.click("host.stop.confirm")?;
    recorder.hold(150)?;
    debug_assert_eq!(
        console.session.update(|console, _, _| console.page()),
        Page::Host
    );
    println!("{} frames at {FPS} per second", recorder.frames);
    Ok(())
}
