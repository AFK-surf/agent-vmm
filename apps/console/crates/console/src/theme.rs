//! Colors, type and spacing of the console.
//!
//! Graphite neutrals carry the structure. Hue appears only where it reports
//! state: health, a destructive action, a memory layer.

use std::borrow::Cow;
use ui::{App, Hsla, rgb, rgba};

pub const FONT_UI: &str = "Inter";
pub const FONT_MONO: &str = "IBM Plex Mono";

/// Body text and control labels.
pub const TEXT: f32 = 13.0;
pub const TEXT_LINE: f32 = 20.0;
/// Column headings, captions and every monospaced value.
pub const SMALL: f32 = 12.0;
pub const SMALL_LINE: f32 = 16.0;
/// The one large figure of a page.
pub const DISPLAY: f32 = 24.0;
pub const DISPLAY_LINE: f32 = 32.0;

/// Radius of a panel, and of a control outside a panel.
pub const RADIUS: f32 = 8.0;
/// Radius of anything inside a panel: `max(RADIUS - padding, 4)`.
pub const RADIUS_INNER: f32 = 4.0;
/// Height of a button, an input and a segmented control.
pub const CONTROL: f32 = 28.0;
/// Space between a panel's edge and its content.
pub const PAD: f32 = 16.0;

/// How far the baseline of `size` text sits below the top of its `line` box,
/// for text whose line box is centered on the font's ascent and descent.
/// `metrics` is the font's `(ascent, descent)` in ems.
pub fn baseline(size: f32, line: f32, metrics: (f32, f32)) -> f32 {
    let (ascent, descent) = metrics;
    (line - (ascent + descent) * size) / 2.0 + ascent * size
}

/// `(ascent, descent)` of IBM Plex Mono, in ems.
pub const MONO_METRICS: (f32, f32) = (1.025, 0.275);
/// `(ascent, descent)` of Inter, in ems.
pub const UI_METRICS: (f32, f32) = (0.96875, 0.2415);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum Theme {
    #[default]
    Dark,
    Light,
}

impl Theme {
    pub fn toggled(self) -> Self {
        match self {
            Self::Dark => Self::Light,
            Self::Light => Self::Dark,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Dark => "dark",
            Self::Light => "light",
        }
    }

    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "dark" => Some(Self::Dark),
            "light" => Some(Self::Light),
            _ => None,
        }
    }

    pub fn palette(self) -> Palette {
        let c = |hex: u32| -> Hsla { rgb(hex).into() };
        match self {
            Self::Dark => Palette {
                app: c(0x0f1012),
                surface: c(0x15171a),
                raised: c(0x1c1f23),
                well: c(0x0a0b0c),
                hairline: c(0x24272c),
                border: c(0x343840),
                text: c(0xe4e6ea),
                muted: c(0xa0a6af),
                faint: c(0x6c727c),
                hover: c(0x22252a),
                press: c(0x2b2f35),
                floating: c(0x262a31),
                floating_edge: c(0x4b525d),
                floating_hover: c(0x30353d),
                floating_chosen: c(0x383e47),
                shadow: rgba(0x0000008c).into(),
                accent: c(0x7aa5d8),
                accent_hover: c(0x8db3e0),
                accent_press: c(0x6893c6),
                on_accent: c(0x0d1218),
                ok: c(0x62b58c),
                warn: c(0xd3a556),
                danger: c(0xe07a68),
                danger_fill: c(0xb9483a),
                danger_hover: c(0xc9584a),
                danger_press: c(0xa33d30),
                on_danger: c(0xfff3f0),
                selection: c(0x2c4668),
                found: c(0x5c4a1c),
                layer_dom0: c(0x7d8fb9),
                layer_guest: [c(0x58a6a0), c(0x3f8782)],
                layer_free: c(0x8b9099),
                layer_zram: c(0xa98bc0),
                ansi: [
                    c(0x5c626b),
                    c(0xe07a68),
                    c(0x62b58c),
                    c(0xd3a556),
                    c(0x7aa5d8),
                    c(0xb693cf),
                    c(0x5bb5bd),
                    c(0xc9ccd1),
                    c(0x7d838c),
                    c(0xf09a8a),
                    c(0x86cfa8),
                    c(0xe6c07a),
                    c(0x9dc0ea),
                    c(0xcdb1e0),
                    c(0x84d0d7),
                    c(0xf1f2f4),
                ],
            },
            Self::Light => Palette {
                app: c(0xf3f4f5),
                surface: c(0xffffff),
                raised: c(0xffffff),
                well: c(0xf7f8f9),
                hairline: c(0xe3e5e8),
                border: c(0xc9cdd3),
                text: c(0x1a1c1f),
                muted: c(0x555b64),
                faint: c(0x858b94),
                hover: c(0xeceef0),
                press: c(0xe1e4e7),
                floating: c(0xffffff),
                floating_edge: c(0xb4bac2),
                floating_hover: c(0xeceef0),
                floating_chosen: c(0xe1e4e7),
                shadow: rgba(0x1a1c1f33).into(),
                accent: c(0x2c66a8),
                accent_hover: c(0x3874b8),
                accent_press: c(0x235790),
                on_accent: c(0xffffff),
                ok: c(0x2a7f5a),
                warn: c(0x9a6c14),
                danger: c(0xb9412f),
                danger_fill: c(0xb9412f),
                danger_hover: c(0xc8503d),
                danger_press: c(0xa03625),
                on_danger: c(0xffffff),
                selection: c(0xc9dcf3),
                found: c(0xf6e2a8),
                layer_dom0: c(0x5d73a6),
                layer_guest: [c(0x3d8e88), c(0x62aaa4)],
                layer_free: c(0x9aa0a9),
                layer_zram: c(0x8a68a6),
                ansi: [
                    c(0x8a9099),
                    c(0xb9412f),
                    c(0x2a7f5a),
                    c(0x9a6c14),
                    c(0x2c66a8),
                    c(0x7d549c),
                    c(0x1f7f88),
                    c(0x3a3e45),
                    c(0x6b717a),
                    c(0xcf5441),
                    c(0x34956b),
                    c(0xb07d1c),
                    c(0x3b7bc4),
                    c(0x9368b4),
                    c(0x2a97a1),
                    c(0x16181b),
                ],
            },
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Palette {
    /// The window behind every page.
    pub app: Hsla,
    /// A panel.
    pub surface: Hsla,
    /// A control or a floating layer on a panel.
    pub raised: Hsla,
    /// A recessed area that holds raw output.
    pub well: Hsla,
    /// A line that separates rows and sections.
    pub hairline: Hsla,
    /// The outline of a control.
    pub border: Hsla,
    pub text: Hsla,
    pub muted: Hsla,
    pub faint: Hsla,
    pub hover: Hsla,
    pub press: Hsla,
    /// A layer above the page, lighter than a panel so it reads as nearer,
    /// with its edge, the rows of it under the pointer and chosen, and the
    /// shadow it casts.
    pub floating: Hsla,
    pub floating_edge: Hsla,
    pub floating_hover: Hsla,
    pub floating_chosen: Hsla,
    pub shadow: Hsla,
    pub accent: Hsla,
    pub accent_hover: Hsla,
    pub accent_press: Hsla,
    pub on_accent: Hsla,
    pub ok: Hsla,
    pub warn: Hsla,
    pub danger: Hsla,
    pub danger_fill: Hsla,
    pub danger_hover: Hsla,
    pub danger_press: Hsla,
    pub on_danger: Hsla,
    /// Selected text.
    pub selection: Hsla,
    /// A search match.
    pub found: Hsla,
    pub layer_dom0: Hsla,
    /// Workload VMs alternate between the two, so neighbors stay apart.
    pub layer_guest: [Hsla; 2],
    pub layer_free: Hsla,
    pub layer_zram: Hsla,
    /// SGR colors 30–37 and 90–97.
    pub ansi: [Hsla; 16],
}

/// Registers the fonts the console draws with. The UI library embeds none.
pub fn register_fonts(cx: &App) -> anyhow::Result<()> {
    cx.text_system().add_fonts(vec![
        Cow::Borrowed(include_bytes!("../assets/fonts/Inter-Variable.ttf").as_slice()),
        Cow::Borrowed(include_bytes!("../assets/fonts/IBMPlexMono-Regular.ttf").as_slice()),
        Cow::Borrowed(include_bytes!("../assets/fonts/IBMPlexMono-Bold.ttf").as_slice()),
    ])
}
