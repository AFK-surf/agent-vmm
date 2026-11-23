//! A log viewer that draws only the rows in sight.
//!
//! The scroll container holds one spacer as tall as every row together, so
//! the wheel and the scroll handle work as for any list. The rows themselves
//! are painted over it, shaped only for the part of the buffer the viewport
//! shows.

mod ansi;
mod buffer;

use crate::theme::{FONT_MONO, Palette, SMALL, Theme};
use ansi::{AnsiParser, Sgr, Span};
use buffer::{LogBuffer, LogLine, Stream, visible_rows};
use std::{cell::Cell, ops::Range, rc::Rc};
use ui::{
    App, Bounds, ClipboardItem, ContentMask, Context, Corners, FocusHandle, Font, FontWeight, Hsla,
    KeyDownEvent, MouseButton, MouseDownEvent, MouseMoveEvent, MouseUpEvent, Pixels, Point,
    ScrollDelta, ScrollEvent, ScrollHandle, ShapedLine, SharedString, TextAlign, TextRun, Window,
    canvas, div, fill, font, point, prelude::*, px, size,
};

/// Lines the view keeps. Older ones are dropped.
pub const CAPACITY: usize = 20_000;
pub const ROW_HEIGHT: f32 = 18.0;
/// Rows shaped beyond each edge of the viewport.
const OVERSCAN: usize = 8;
const GUTTER: f32 = 12.0;
/// Space above the first row and below the last.
const MARGIN: f32 = 6.0;
/// The strip at the right edge that belongs to the scrollbar alone.
const SCROLLBAR: f32 = 16.0;
const THUMB: f32 = 4.0;

/// Where the text and the scrollbar of a view are, left to right.
#[derive(Clone, Copy, Debug, PartialEq)]
struct Columns {
    /// The left edge of the text.
    text: Pixels,
    /// The right edge of the text: a whole number of characters from its
    /// left edge, so no character is cut in half.
    text_end: Pixels,
    thumb: Pixels,
}

impl Columns {
    fn of(bounds: Bounds<Pixels>, advance: Pixels) -> Self {
        let text = bounds.left() + px(GUTTER) + advance * TIME_COLUMNS;
        let room = (bounds.right() - px(SCROLLBAR) - text).max(px(0.));
        Self {
            text,
            text_end: text + advance * (room / advance).floor(),
            thumb: bounds.right() - px((SCROLLBAR + THUMB) / 2.),
        }
    }
}
/// Columns of the time of day, `09:14:07.412`, and the gap after it.
const TIME_COLUMNS: f32 = 14.0;

/// A place in the text: a line and a byte of it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Place {
    sequence: u64,
    byte: usize,
}

pub struct VirtualLogView {
    buffer: LogBuffer,
    parsers: [AnsiParser; 2],
    scroll: ScrollHandle,
    focus: FocusHandle,
    theme: Theme,
    follow: bool,
    /// Where a selection began and where it ends now.
    selection: Option<(Place, Place)>,
    selecting: bool,
    /// How far the text is moved left to show the end of long lines.
    scroll_left: Pixels,
    /// The widest line last painted, which bounds `scroll_left`.
    widest: Rc<Cell<Pixels>>,
}

impl VirtualLogView {
    pub fn new(theme: Theme, cx: &mut Context<Self>) -> Self {
        Self {
            buffer: LogBuffer::new(CAPACITY),
            parsers: Default::default(),
            scroll: ScrollHandle::new(),
            focus: cx.focus_handle(),
            theme,
            follow: true,
            selection: None,
            selecting: false,
            scroll_left: px(0.),
            widest: Rc::default(),
        }
    }

    /// Adds what a container wrote. `data` can end in the middle of a line.
    pub fn push(&mut self, stderr: bool, time: SharedString, data: &[u8], cx: &mut Context<Self>) {
        let stream = if stderr {
            Stream::Stderr
        } else {
            Stream::Stdout
        };
        let Self {
            buffer, parsers, ..
        } = self;
        let mut rows_dropped = 0;
        parsers[stderr as usize].feed(data, |line| {
            rows_dropped += buffer.push(stream, time.clone(), line).rows_dropped;
        });
        if self.follow {
            self.scroll.scroll_to_bottom();
        } else if rows_dropped > 0 {
            // The rows in sight moved up by what was dropped above them.
            let offset = self.scroll.offset();
            self.scroll.set_offset(point(
                offset.x,
                offset.y + px(rows_dropped as f32 * ROW_HEIGHT),
            ));
        }
        cx.notify();
    }

    /// Shows what the containers wrote last without ending the line: no
    /// more will come.
    pub fn finish(&mut self, cx: &mut Context<Self>) {
        let Self {
            buffer, parsers, ..
        } = self;
        for (parser, stream) in parsers.iter_mut().zip([Stream::Stdout, Stream::Stderr]) {
            if let Some(line) = parser.flush() {
                buffer.push(stream, SharedString::default(), line);
            }
        }
        cx.notify();
    }

    pub fn clear(&mut self, cx: &mut Context<Self>) {
        self.buffer.clear();
        self.parsers = Default::default();
        self.selection = None;
        cx.notify();
    }

    pub fn set_theme(&mut self, theme: Theme, cx: &mut Context<Self>) {
        self.theme = theme;
        cx.notify();
    }

    pub fn set_search(&mut self, needle: &str, cx: &mut Context<Self>) {
        self.buffer.set_search(needle);
        self.keep_in_range();
        cx.notify();
    }

    pub fn set_only_matches(&mut self, only_matches: bool, cx: &mut Context<Self>) {
        self.buffer.set_only_matches(only_matches);
        self.keep_in_range();
        cx.notify();
    }

    /// Shows what was written to standard error, to standard output, or both.
    pub fn set_stream(&mut self, stderr: Option<bool>, cx: &mut Context<Self>) {
        self.buffer.set_stream(stderr.map(|stderr| {
            if stderr {
                Stream::Stderr
            } else {
                Stream::Stdout
            }
        }));
        self.keep_in_range();
        cx.notify();
    }

    /// Following keeps the newest line in sight. Not following keeps the
    /// lines in sight where they are while new ones arrive below.
    pub fn set_follow(&mut self, follow: bool, cx: &mut Context<Self>) {
        self.follow = follow;
        if follow {
            self.scroll.scroll_to_bottom();
        }
        cx.notify();
    }

    pub fn is_following(&self) -> bool {
        self.follow
    }

    pub fn is_filtered(&self) -> bool {
        self.buffer.is_filtered()
    }

    pub fn match_count(&self) -> usize {
        self.buffer.match_count()
    }

    pub fn matching_line_count(&self) -> usize {
        self.buffer.matching_line_count()
    }

    pub fn line_count(&self) -> usize {
        self.buffer.line_count()
    }

    pub fn dropped_count(&self) -> u64 {
        self.buffer.dropped_count()
    }

    pub fn has_selection(&self) -> bool {
        self.selected().is_some()
    }

    pub fn focus_handle(&self) -> &FocusHandle {
        &self.focus
    }

    /// The selected text, one line per row, without the escape sequences.
    pub fn selected_text(&self) -> Option<String> {
        let (start, end) = self.selected()?;
        let rows = self.buffer.row_of(start.sequence)..self.buffer.row_count();
        let mut text = String::new();
        for line in rows.map_while(|row| self.buffer.row(row)) {
            if line.sequence > end.sequence {
                break;
            }
            let from = if line.sequence == start.sequence {
                start.byte.min(line.text.len())
            } else {
                0
            };
            let to = if line.sequence == end.sequence {
                end.byte.min(line.text.len())
            } else {
                line.text.len()
            };
            if !text.is_empty() || line.sequence > start.sequence {
                text.push('\n');
            }
            text.push_str(&line.text[from..to.max(from)]);
        }
        Some(text)
    }

    pub fn copy_selection(&self, cx: &mut App) {
        if let Some(text) = self.selected_text() {
            cx.write_to_clipboard(ClipboardItem::new_string(text));
        }
    }

    pub fn select_all(&mut self, cx: &mut Context<Self>) {
        let rows = self.buffer.row_count();
        let ends = rows
            .checked_sub(1)
            .and_then(|last| Some((self.buffer.row(0)?, self.buffer.row(last)?)));
        self.selection = ends.map(|(first, last)| {
            (
                Place {
                    sequence: first.sequence,
                    byte: 0,
                },
                Place {
                    sequence: last.sequence,
                    byte: last.text.len(),
                },
            )
        });
        cx.notify();
    }

    /// The selection in reading order, unless it is empty.
    fn selected(&self) -> Option<(Place, Place)> {
        let (anchor, head) = self.selection?;
        (anchor != head).then(|| (anchor.min(head), anchor.max(head)))
    }

    /// A search or a filter changes the rows: scroll back into them.
    fn keep_in_range(&mut self) {
        if self.follow {
            self.scroll.scroll_to_bottom();
        } else {
            self.scroll.set_offset(self.scroll.offset());
        }
    }

    /// The place in the text under `position`.
    fn place_at(&self, position: Point<Pixels>, window: &mut Window) -> Option<Place> {
        let bounds = self.scroll.bounds();
        let palette = self.theme.palette();
        let scroll_top = -self.scroll.offset().y;
        let row = ((position.y - bounds.top() - px(MARGIN) + scroll_top) / px(ROW_HEIGHT)).floor();
        let rows = self.buffer.row_count();
        let line = self
            .buffer
            .row((row.max(0.0) as usize).min(rows.checked_sub(1)?))?;
        let shaped = shape(line, &palette, window);
        let x = position.x - Columns::of(bounds, char_width(window)).text + self.scroll_left;
        let byte = if row < 0.0 {
            0
        } else if row >= rows as f32 {
            line.text.len()
        } else {
            shaped.closest_index_for_x(x)
        };
        Some(Place {
            sequence: line.sequence,
            byte,
        })
    }

    fn mouse_down(&mut self, event: &MouseDownEvent, window: &mut Window, cx: &mut Context<Self>) {
        self.focus.focus(window, cx);
        let Some(place) = self.place_at(event.position, window) else {
            return;
        };
        self.selection = Some(match self.selection {
            Some((anchor, _)) if event.modifiers.shift => (anchor, place),
            _ => (place, place),
        });
        self.selecting = true;
        cx.notify();
    }

    fn mouse_move(&mut self, event: &MouseMoveEvent, window: &mut Window, cx: &mut Context<Self>) {
        if !self.selecting || event.pressed_button != Some(MouseButton::Left) {
            return;
        }
        if let (Some(place), Some((anchor, head))) =
            (self.place_at(event.position, window), self.selection)
            && place != head
        {
            self.selection = Some((anchor, place));
            cx.notify();
        }
    }

    fn mouse_up(&mut self, _: &MouseUpEvent, _: &mut Window, cx: &mut Context<Self>) {
        if std::mem::take(&mut self.selecting) {
            cx.notify();
        }
    }

    fn scrolled(&mut self, event: &ScrollEvent, window: &mut Window, cx: &mut Context<Self>) {
        let delta = match event.delta {
            ScrollDelta::Pixels(delta) => delta,
            ScrollDelta::Lines(delta) => point(px(delta.x * ROW_HEIGHT), px(delta.y * ROW_HEIGHT)),
        };
        // Scrolling back to read is a request to stop following.
        if delta.y > px(0.) && self.follow {
            self.follow = false;
            cx.notify();
        }
        if delta.x != px(0.) {
            // The text moves by whole characters, so none is cut at either edge.
            let advance = char_width(window);
            let columns = Columns::of(self.scroll.bounds(), advance);
            let hidden = (self.widest.get() - (columns.text_end - columns.text)).max(px(0.));
            let furthest = advance * (hidden / advance).ceil();
            let wanted = advance * ((self.scroll_left - delta.x) / advance).round();
            let scroll_left = wanted.clamp(px(0.), furthest);
            if scroll_left != self.scroll_left {
                self.scroll_left = scroll_left;
                cx.notify();
            }
        }
    }

    fn key_down(&mut self, event: &KeyDownEvent, _: &mut Window, cx: &mut Context<Self>) {
        let keystroke = &event.keystroke;
        if !(keystroke.modifiers.platform || keystroke.modifiers.control) {
            return;
        }
        match keystroke.key.as_str() {
            "c" => self.copy_selection(cx),
            "a" => self.select_all(cx),
            _ => return,
        }
        cx.stop_propagation();
    }
}

fn char_width(window: &mut Window) -> Pixels {
    let run = TextRun {
        len: 1,
        font: font(FONT_MONO),
        color: Hsla::default(),
        background_color: None,
        underline: None,
        strikethrough: None,
    };
    window
        .text_system()
        .shape_line("0".into(), px(SMALL), &[run], None)
        .width()
}

fn run(len: usize, weight: FontWeight, color: Hsla) -> TextRun {
    TextRun {
        len,
        font: Font {
            weight,
            ..font(FONT_MONO)
        },
        color,
        background_color: None,
        underline: None,
        strikethrough: None,
    }
}

fn color_of(style: Sgr, palette: &Palette) -> Hsla {
    let color = style
        .color
        .map_or(palette.text, |index| palette.ansi[index as usize % 16]);
    if style.dim {
        color.opacity(0.62)
    } else {
        color
    }
}

fn shape(line: &LogLine, palette: &Palette, window: &mut Window) -> ShapedLine {
    let mut runs = Vec::with_capacity(line.spans.len().max(1));
    let mut start = 0;
    for Span { end, style } in &line.spans {
        let weight = if style.bold {
            FontWeight::BOLD
        } else {
            FontWeight::NORMAL
        };
        runs.push(run(end - start, weight, color_of(*style, palette)));
        start = *end;
    }
    if start < line.text.len() {
        runs.push(run(
            line.text.len() - start,
            FontWeight::NORMAL,
            palette.text,
        ));
    }
    window
        .text_system()
        .shape_line(line.text.clone(), px(SMALL), &runs, None)
}

#[cfg(test)]
impl VirtualLogView {
    /// Where the text may be painted, and where the scrollbar's thumb is.
    pub(crate) fn text_and_thumb(&self, window: &mut Window) -> (Range<f32>, Range<f32>) {
        let columns = Columns::of(self.scroll.bounds(), char_width(window));
        (
            columns.text.into()..columns.text_end.into(),
            columns.thumb.into()..f32::from(columns.thumb) + THUMB,
        )
    }

    /// The rows a frame shapes and paints now.
    pub(crate) fn rows_painted(&self) -> Range<usize> {
        let rows = self.rows_in(self.scroll.bounds().size.height);
        match (rows.first(), rows.last()) {
            (Some(first), Some(last)) => first.index..last.index + 1,
            _ => 0..0,
        }
    }
}

/// What the painter needs of one row, taken out of the buffer before painting.
struct Row {
    index: usize,
    line: LogLine,
    found: Vec<Range<usize>>,
    selected: Option<Range<usize>>,
}

impl VirtualLogView {
    fn rows_in(&self, viewport: Pixels) -> Vec<Row> {
        let scroll_top = -self.scroll.offset().y;
        let rows = visible_rows(
            self.buffer.row_count(),
            ROW_HEIGHT,
            viewport.into(),
            f32::from(scroll_top) - MARGIN,
            OVERSCAN,
        );
        let selected = self.selected();
        rows.filter_map(|index| {
            let line = self.buffer.row(index)?;
            let selected = selected.and_then(|(start, end)| {
                if line.sequence < start.sequence || line.sequence > end.sequence {
                    return None;
                }
                let from = if line.sequence == start.sequence {
                    start.byte.min(line.text.len())
                } else {
                    0
                };
                let to = if line.sequence == end.sequence {
                    end.byte.min(line.text.len())
                } else {
                    // One column past the end stands for the line break.
                    line.text.len() + 1
                };
                Some(from..to)
            });
            Some(Row {
                index,
                found: self.buffer.matches_in(line).collect(),
                line: line.clone(),
                selected,
            })
        })
        .collect()
    }
}

/// One frame of the view: the rows in sight, read out of the view before
/// painting needs the app.
struct Painter {
    rows: Vec<Row>,
    palette: Palette,
    scroll_top: Pixels,
    scroll_left: Pixels,
    max_scroll: Pixels,
    widest: Rc<Cell<Pixels>>,
}

impl Painter {
    fn paint(&self, bounds: Bounds<Pixels>, window: &mut Window, cx: &mut App) {
        let Self {
            rows,
            palette,
            scroll_top,
            scroll_left,
            max_scroll,
            widest,
        } = self;
        let (scroll_top, max_scroll) = (*scroll_top, *max_scroll);
        let advance = char_width(window);
        let columns = Columns::of(bounds, advance);
        let text_left = columns.text - *scroll_left;
        // The times stay where they are while the text moves under this mask,
        // which ends before the scrollbar.
        let text_mask = ContentMask {
            bounds: Bounds::from_corners(
                point(columns.text, bounds.top()),
                point(columns.text_end, bounds.bottom()),
            ),
        };
        let row_height = px(ROW_HEIGHT);
        let mut widest_painted = px(0.);
        window.with_content_mask(Some(ContentMask { bounds }), |window| {
            for row in rows {
                let top = bounds.top() + px(MARGIN) + row_height * row.index as f32 - scroll_top;
                let shaped = shape(&row.line, palette, window);
                widest_painted = widest_painted.max(shaped.width());
                let x_of = |byte: usize| {
                    if byte > row.line.text.len() {
                        shaped.width() + advance
                    } else {
                        shaped.x_for_index(byte)
                    }
                };
                let band = |range: &Range<usize>| {
                    Bounds::new(
                        point(text_left + x_of(range.start), top),
                        size(x_of(range.end) - x_of(range.start), row_height),
                    )
                };
                if row.line.stream == Stream::Stderr {
                    let mark = Bounds::new(
                        point(bounds.left() + px(4.), top + px(3.)),
                        size(px(2.), row_height - px(6.)),
                    );
                    window.paint_quad(fill(mark, palette.danger));
                }
                window.with_content_mask(Some(text_mask), |window| {
                    for range in &row.found {
                        window.paint_quad(fill(band(range), palette.found));
                    }
                    if let Some(range) = &row.selected {
                        window.paint_quad(fill(band(range), palette.selection));
                    }
                    let origin = point(text_left, top);
                    let _ = shaped.paint(origin, row_height, TextAlign::Left, None, window, cx);
                });
                let time = window.text_system().shape_line(
                    row.line.time.clone(),
                    px(SMALL),
                    &[run(row.line.time.len(), FontWeight::NORMAL, palette.faint)],
                    None,
                );
                let origin = point(bounds.left() + px(GUTTER), top);
                let _ = time.paint(origin, row_height, TextAlign::Left, None, window, cx);
            }
            if max_scroll > px(0.) {
                let track = bounds.size.height - px(8.);
                let thumb =
                    (track * (bounds.size.height / (bounds.size.height + max_scroll))).max(px(24.));
                let top = bounds.top() + px(4.) + (track - thumb) * (scroll_top / max_scroll);
                let thumb = Bounds::new(
                    point(columns.thumb, top.round()),
                    size(px(THUMB), thumb.round()),
                );
                window.paint_quad(ui::quad(
                    thumb,
                    Corners::all(px(2.)),
                    palette.faint.opacity(0.55),
                    px(0.),
                    Hsla::default(),
                    Default::default(),
                ));
            }
        });
        widest.set(widest_painted);
    }
}

impl Render for VirtualLogView {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let palette = self.theme.palette();
        let rows = self.buffer.row_count();
        let view = cx.entity();
        let empty = (rows == 0).then(|| {
            let message = if self.buffer.line_count() == 0 {
                "No log record has arrived yet.".to_owned()
            } else if self.buffer.search().is_empty() {
                "This stream has no lines.".to_owned()
            } else {
                format!("No line here contains “{}”.", self.buffer.search())
            };
            div()
                .absolute()
                .inset_0()
                .flex()
                .items_center()
                .justify_center()
                .text_size(px(crate::theme::TEXT))
                .text_color(palette.faint)
                .child(message)
        });
        div()
            .id("log-view")
            .relative()
            .size_full()
            .bg(palette.well)
            .cursor(ui::CursorStyle::IBeam)
            .track_focus(&self.focus)
            .on_key_down(cx.listener(Self::key_down))
            .on_mouse_down(MouseButton::Left, cx.listener(Self::mouse_down))
            .on_mouse_move(cx.listener(Self::mouse_move))
            .on_mouse_up(MouseButton::Left, cx.listener(Self::mouse_up))
            .on_mouse_up_out(MouseButton::Left, cx.listener(Self::mouse_up))
            .child(
                div()
                    .id("log-scroll")
                    .absolute()
                    .inset_0()
                    .overflow_y_scroll()
                    .map(|mut scroll| {
                        // A sideways turn of the wheel moves the text, not the rows.
                        scroll.style().restrict_scroll_to_axis = Some(true);
                        scroll
                    })
                    .track_scroll(&self.scroll)
                    .on_scroll(cx.listener(Self::scrolled))
                    .child(div().w_full().h(px(rows as f32 * ROW_HEIGHT + 2. * MARGIN))),
            )
            .child(
                canvas(
                    |_, _, _| (),
                    move |bounds, _, window, cx| {
                        let view = view.read(cx);
                        let painter = Painter {
                            rows: view.rows_in(bounds.size.height),
                            palette: view.theme.palette(),
                            scroll_top: -view.scroll.offset().y,
                            scroll_left: view.scroll_left,
                            max_scroll: view.scroll.max_offset().y,
                            widest: view.widest.clone(),
                        };
                        painter.paint(bounds, window, cx);
                    },
                )
                .absolute()
                .inset_0(),
            )
            .children(empty)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_ends_on_a_whole_character_before_the_scrollbar() {
        let advance = px(7.2);
        for width in (240..1600).step_by(7) {
            let bounds = Bounds::new(point(px(16.), px(100.)), size(px(width as f32), px(400.)));
            let columns = Columns::of(bounds, advance);
            let characters = (columns.text_end - columns.text) / advance;
            assert!(
                (characters - characters.round()).abs() < 1e-3,
                "{width}: {characters}"
            );
            assert!(
                columns.text_end <= bounds.right() - px(SCROLLBAR),
                "{width}"
            );
            assert!(columns.text_end + px(4.) <= columns.thumb, "{width}");
            assert!(columns.thumb + px(THUMB) < bounds.right(), "{width}");
        }
    }
}
