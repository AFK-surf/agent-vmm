//! The parts every page is built from.

use crate::theme::{
    CONTROL, FONT_MONO, Palette, RADIUS, RADIUS_INNER, SMALL, SMALL_LINE, TEXT, TEXT_LINE,
};
use smallvec::smallvec;
use ui::{
    AnyElement, App, BorderStyle, Bounds, BoxShadow, ClickEvent, Corners, Div, Edges, ElementId,
    FontWeight, Hsla, Pixels, SharedString, Stateful, TextAlign, Window, canvas, div, point,
    prelude::*, px, quad, size,
};

/// Body text.
pub fn text(content: impl Into<SharedString>) -> Div {
    div()
        .text_size(px(TEXT))
        .line_height(px(TEXT_LINE))
        .child(content.into())
}

/// A caption or a column heading.
pub fn small(content: impl Into<SharedString>) -> Div {
    div()
        .text_size(px(SMALL))
        .line_height(px(SMALL_LINE))
        .child(content.into())
}

/// A value read character by character: an ID, a path, a quantity.
pub fn mono(content: impl Into<SharedString>) -> Div {
    div()
        .font_family(FONT_MONO)
        .text_size(px(SMALL))
        .line_height(px(SMALL_LINE))
        .child(content.into())
}

/// A quantity in a column: monospaced, so the digits of the rows align.
pub fn number(content: impl Into<SharedString>, width: f32) -> Div {
    mono(content)
        .flex_none()
        .w(px(width))
        .text_align(TextAlign::Right)
        .whitespace_nowrap()
}

/// The heading of a section of a page.
/// Words at the end of a row, such as an age.
pub fn trailing(content: impl Into<SharedString>, width: f32) -> Div {
    small(content)
        .flex_none()
        .w(px(width))
        .text_align(TextAlign::Right)
        .whitespace_nowrap()
}

pub fn heading(title: impl Into<SharedString>, palette: &Palette) -> Div {
    small(title)
        .font_weight(FontWeight::MEDIUM)
        .text_color(palette.muted)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Tone {
    Neutral,
    Accent,
    Ok,
    Warn,
    Danger,
}

impl Tone {
    pub fn color(self, palette: &Palette) -> Hsla {
        match self {
            Self::Neutral => palette.muted,
            Self::Accent => palette.accent,
            Self::Ok => palette.ok,
            Self::Warn => palette.warn,
            Self::Danger => palette.danger,
        }
    }
}

/// A state in a word, tinted by what it means.
pub fn badge(label: impl Into<SharedString>, tone: Tone, palette: &Palette) -> Div {
    let color = tone.color(palette);
    small(label)
        .flex_none()
        .h(px(20.))
        .px(px(6.))
        .py(px(2.))
        .rounded(px(RADIUS_INNER))
        .bg(color.opacity(0.14))
        .text_color(color)
        .font_weight(FontWeight::MEDIUM)
        .whitespace_nowrap()
}

/// A state in a dot, centered on the first line of the text beside it.
pub fn dot(tone: Tone, line: f32, palette: &Palette) -> Div {
    div()
        .flex_none()
        .w(px(8.))
        .h(px(line))
        .flex()
        .items_center()
        .child(
            div()
                .w(px(8.))
                .h(px(8.))
                .rounded(px(4.))
                .bg(tone.color(palette)),
        )
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ButtonKind {
    /// The action a panel is for.
    Primary,
    /// Any other action.
    Plain,
    /// An action that deletes or stops something.
    Danger,
}

/// A button named `id` in the frame snapshot.
pub fn button(
    id: impl Into<SharedString>,
    label: impl Into<SharedString>,
    kind: ButtonKind,
    palette: &Palette,
    on_click: impl Fn(&ClickEvent, &mut Window, &mut App) + 'static,
) -> Stateful<Div> {
    let (fill, hover, press, ink, edge) = match kind {
        ButtonKind::Primary => (
            palette.accent,
            palette.accent_hover,
            palette.accent_press,
            palette.on_accent,
            palette.accent,
        ),
        ButtonKind::Plain => (
            palette.raised,
            palette.hover,
            palette.press,
            palette.text,
            palette.border,
        ),
        ButtonKind::Danger => (
            palette.danger_fill,
            palette.danger_hover,
            palette.danger_press,
            palette.on_danger,
            palette.danger_fill,
        ),
    };
    button_frame(id, label)
        .bg(fill)
        .border_color(edge)
        .text_color(ink)
        .cursor_pointer()
        .hover(move |style| style.bg(hover))
        .active(move |style| style.bg(press))
        .on_click(on_click)
}

/// A button that cannot act now. It takes no pointer and shows no hand.
pub fn disabled_button(
    id: impl Into<SharedString>,
    label: impl Into<SharedString>,
    palette: &Palette,
) -> Stateful<Div> {
    button_frame(id, label)
        .bg(palette.surface)
        .border_color(palette.hairline)
        .text_color(palette.faint)
}

fn button_frame(id: impl Into<SharedString>, label: impl Into<SharedString>) -> Stateful<Div> {
    let id: SharedString = id.into();
    let label: SharedString = label.into();
    div()
        .id(ElementId::Name(id.clone()))
        .snapshot(id, None)
        .snapshot_text(label.clone())
        .flex_none()
        .h(px(CONTROL))
        .px(px(12.))
        .flex()
        .items_center()
        .justify_center()
        .rounded(px(RADIUS_INNER))
        .border_1()
        .text_size(px(SMALL))
        .line_height(px(SMALL_LINE))
        .font_weight(FontWeight::MEDIUM)
        .whitespace_nowrap()
        .child(label)
}

/// One choice of a segmented control named `id`.
pub fn segment(
    id: impl Into<SharedString>,
    label: impl Into<SharedString>,
    selected: bool,
    palette: &Palette,
    on_click: impl Fn(&ClickEvent, &mut Window, &mut App) + 'static,
) -> Stateful<Div> {
    let id: SharedString = id.into();
    let label: SharedString = label.into();
    let (hover, press) = (palette.hover, palette.press);
    div()
        .id(ElementId::Name(id.clone()))
        .snapshot(id, None)
        .snapshot_text(label.clone())
        .flex_none()
        .h(px(CONTROL - 4.))
        .px(px(10.))
        .flex()
        .items_center()
        .rounded(px(RADIUS_INNER))
        .text_size(px(SMALL))
        .line_height(px(SMALL_LINE))
        .font_weight(FontWeight::MEDIUM)
        .whitespace_nowrap()
        .map(|this| {
            if selected {
                this.bg(palette.press).text_color(palette.text)
            } else {
                this.text_color(palette.muted)
                    .cursor_pointer()
                    .hover(move |style| style.bg(hover))
                    .active(move |style| style.bg(press))
                    .on_click(on_click)
            }
        })
        .child(label)
}

/// The frame of a segmented control: its segments sit two pixels inside it.
pub fn segmented(palette: &Palette) -> Div {
    div()
        .flex_none()
        .h(px(CONTROL))
        .p(px(1.))
        .flex()
        .items_center()
        .gap(px(2.))
        .rounded(px(RADIUS_INNER + 2.))
        .border_1()
        .border_color(palette.border)
        .bg(palette.well)
}

/// A panel: one subject of a page.
pub fn panel(palette: &Palette) -> Div {
    div()
        .flex()
        .flex_col()
        .rounded(px(RADIUS))
        .border_1()
        .border_color(palette.hairline)
        .bg(palette.surface)
}

/// A layer above the page: a menu, a confirmation.
pub fn floating(palette: &Palette) -> Div {
    div()
        .rounded(px(RADIUS))
        .border_1()
        .border_color(palette.floating_edge)
        .bg(palette.floating)
        .shadow(smallvec![
            BoxShadow {
                color: palette.shadow,
                offset: point(px(0.), px(12.)),
                blur_radius: px(32.),
                spread_radius: px(0.),
                inset: false,
            },
            BoxShadow {
                color: palette.shadow.opacity(0.5),
                offset: point(px(0.), px(1.)),
                blur_radius: px(3.),
                spread_radius: px(0.),
                inset: false,
            },
        ])
}

/// A label and its value on one row. A long value wraps under itself.
pub fn field(label: impl Into<SharedString>, value: impl IntoElement, palette: &Palette) -> Div {
    div()
        .flex()
        .items_start()
        .gap(px(12.))
        .child(
            small(label)
                .flex_none()
                .w(px(112.))
                .text_color(palette.muted),
        )
        .child(div().flex_1().min_w(px(0.)).child(value))
}

/// A column of a table.
#[derive(Clone, Copy)]
pub struct Column {
    pub title: &'static str,
    /// Its width, or `None` to share what the fixed columns leave.
    pub width: Option<f32>,
    pub right: bool,
}

impl Column {
    pub const fn flex(title: &'static str) -> Self {
        Self {
            title,
            width: None,
            right: false,
        }
    }

    pub const fn fixed(title: &'static str, width: f32) -> Self {
        Self {
            title,
            width: Some(width),
            right: false,
        }
    }

    pub const fn number(title: &'static str, width: f32) -> Self {
        Self {
            title,
            width: Some(width),
            right: true,
        }
    }

    fn cell(&self) -> Div {
        let cell = match self.width {
            Some(width) => div().flex_none().w(px(width)),
            None => div().flex_1().min_w(px(0.)),
        };
        if self.right {
            cell.flex().justify_end()
        } else {
            cell
        }
    }
}

const TABLE_GAP: f32 = 16.0;

pub fn table_head(columns: &[Column], palette: &Palette) -> Div {
    div()
        .flex()
        .gap(px(TABLE_GAP))
        .pb(px(6.))
        .border_b_1()
        .border_color(palette.hairline)
        .children(columns.iter().map(|column| {
            column
                .cell()
                .child(heading(column.title, palette).whitespace_nowrap())
        }))
}

/// A row of a table. Cells align on their first line; a line parts it from the
/// row above, so the last row leaves none hanging.
pub fn table_row(
    columns: &[Column],
    first: bool,
    cells: impl IntoIterator<Item = AnyElement>,
    palette: &Palette,
) -> Div {
    table_cells(columns, cells).py(px(8.)).when(!first, |row| {
        row.border_t(px(1.)).border_color(palette.hairline)
    })
}

/// The cells of a row in their columns, without the space and the line
/// around a row.
pub fn table_cells(columns: &[Column], cells: impl IntoIterator<Item = AnyElement>) -> Div {
    div().flex().items_start().gap(px(TABLE_GAP)).children(
        columns
            .iter()
            .zip(cells)
            .map(|(column, cell)| column.cell().child(cell)),
    )
}

/// How a part of a bar is drawn.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Part {
    /// Memory or disk that is in use: a solid fill.
    Solid(Hsla),
    /// Reserved, but backed by nothing: an outline.
    Hollow(Hsla),
    /// Given back: a dashed outline.
    Dashed(Hsla),
}

/// A bar divided in proportion to `parts`, on whole device pixels. A mark is
/// a line across the bar at a quantity, such as a limit.
pub fn bar(
    parts: Vec<(u64, Part)>,
    marks: Vec<u64>,
    height: f32,
    palette: &Palette,
) -> impl IntoElement {
    let track = palette.hairline;
    let ink = palette.text;
    canvas(
        |_, _, _| (),
        move |bounds: Bounds<Pixels>, _, window, _| {
            let total: u64 = parts.iter().map(|(amount, _)| *amount).sum();
            window.paint_quad(quad(
                bounds,
                Corners::all(px(2.)),
                track,
                Edges::all(px(0.)),
                Hsla::default(),
                BorderStyle::Solid,
            ));
            if total == 0 {
                return;
            }
            let x_of = |amount: u64| {
                bounds.left() + (bounds.size.width * (amount as f32 / total as f32)).round()
            };
            let mut before = 0;
            for (index, (amount, part)) in parts.iter().enumerate() {
                let (left, right) = (x_of(before), x_of(before + amount));
                before += amount;
                // One pixel of the track shows between two parts.
                let gap = if index + 1 < parts.len() {
                    px(1.)
                } else {
                    px(0.)
                };
                let width = right - left - gap;
                if width <= px(0.) {
                    continue;
                }
                let part_bounds =
                    Bounds::new(point(left, bounds.top()), size(width, bounds.size.height));
                let (fill, edge, style) = match *part {
                    Part::Solid(color) => (color, color, BorderStyle::Solid),
                    Part::Hollow(color) => (color.opacity(0.0), color, BorderStyle::Solid),
                    Part::Dashed(color) => (color.opacity(0.0), color, BorderStyle::Dashed),
                };
                let edges = if matches!(part, Part::Solid(_)) {
                    px(0.)
                } else {
                    px(1.)
                };
                window.paint_quad(quad(
                    part_bounds,
                    Corners::all(px(2.)),
                    fill,
                    Edges::all(edges),
                    edge,
                    style,
                ));
            }
            for mark in &marks {
                let line = Bounds::new(
                    point(x_of(*mark) - px(1.), bounds.top() - px(2.)),
                    size(px(1.), bounds.size.height + px(4.)),
                );
                window.paint_quad(ui::fill(line, ink));
            }
        },
    )
    .w_full()
    .h(px(height))
    .flex_none()
}

/// A square of the color a bar part has, for its legend.
pub fn swatch(part: Part) -> Div {
    let square = div().w(px(8.)).h(px(8.)).rounded(px(2.));
    div()
        .flex_none()
        .w(px(8.))
        .h(px(SMALL_LINE))
        .flex()
        .items_center()
        .child(match part {
            Part::Solid(color) => square.bg(color),
            Part::Hollow(color) | Part::Dashed(color) => square.border_1().border_color(color),
        })
}

/// A notice across a panel: something the operator should know before acting.
pub fn notice(tone: Tone, palette: &Palette) -> Div {
    let color = tone.color(palette);
    div()
        .flex()
        .items_start()
        .gap(px(8.))
        .px(px(12.))
        .py(px(8.))
        .rounded(px(RADIUS_INNER))
        .bg(color.opacity(0.13))
        .text_color(palette.text)
}
