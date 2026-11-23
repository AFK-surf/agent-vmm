//! A one-line text field: caret, selection, clipboard and input-method composition.

use crate::theme::{CONTROL, FONT_UI, Palette, RADIUS_INNER, TEXT, Theme};
use std::{cell::Cell, ops::Range, rc::Rc};
use ui::{
    App, Bounds, ClipboardItem, Context, CursorStyle, EntityInputHandler, EventEmitter,
    FocusHandle, Hsla, KeyDownEvent, MouseButton, MouseDownEvent, MouseMoveEvent, MouseUpEvent,
    Pixels, Point, ShapedLine, SharedString, TextAlign, TextRun, UTF16Selection, Window, canvas,
    div, fill, font, point, prelude::*, px, size,
};
use unicode_segmentation::UnicodeSegmentation;

const PADDING: f32 = 8.0;

pub enum TextInputEvent {
    /// The text changed, a composition in progress included.
    Changed,
    /// Return was pressed.
    Submitted,
}

pub struct TextInput {
    id: &'static str,
    placeholder: SharedString,
    text: String,
    /// Byte range of the selection. Empty at the caret.
    selection: Range<usize>,
    /// The caret is at the start of the selection, not its end.
    reversed: bool,
    /// Byte range of the text an input method is still composing.
    marked: Option<Range<usize>>,
    focus: FocusHandle,
    theme: Theme,
    selecting: bool,
    /// How far the text is scrolled left to keep the caret in sight, and
    /// where it was last painted. The painter sets both.
    scroll: Rc<Cell<Pixels>>,
    text_bounds: Rc<Cell<Bounds<Pixels>>>,
}

impl EventEmitter<TextInputEvent> for TextInput {}

impl TextInput {
    pub fn new(
        id: &'static str,
        placeholder: impl Into<SharedString>,
        theme: Theme,
        cx: &mut Context<Self>,
    ) -> Self {
        Self {
            id,
            placeholder: placeholder.into(),
            text: String::new(),
            selection: 0..0,
            reversed: false,
            marked: None,
            focus: cx.focus_handle(),
            theme,
            selecting: false,
            scroll: Rc::default(),
            text_bounds: Rc::default(),
        }
    }

    pub fn text(&self) -> &str {
        &self.text
    }

    pub fn focus_handle(&self) -> &FocusHandle {
        &self.focus
    }

    pub fn set_theme(&mut self, theme: Theme, cx: &mut Context<Self>) {
        self.theme = theme;
        cx.notify();
    }

    pub fn set_text(&mut self, text: &str, cx: &mut Context<Self>) {
        self.text = text.to_owned();
        self.selection = self.text.len()..self.text.len();
        self.marked = None;
        self.changed(cx);
    }

    fn changed(&mut self, cx: &mut Context<Self>) {
        cx.emit(TextInputEvent::Changed);
        cx.notify();
    }

    fn caret(&self) -> usize {
        if self.reversed {
            self.selection.start
        } else {
            self.selection.end
        }
    }

    fn move_to(&mut self, offset: usize, extend: bool, cx: &mut Context<Self>) {
        if extend {
            let anchor = if self.reversed {
                self.selection.end
            } else {
                self.selection.start
            };
            self.reversed = offset < anchor;
            self.selection = offset.min(anchor)..offset.max(anchor);
        } else {
            self.selection = offset..offset;
            self.reversed = false;
        }
        cx.notify();
    }

    /// The boundary of the user-perceived character before or after `offset`.
    fn boundary(&self, offset: usize, forward: bool) -> usize {
        if forward {
            self.text[offset..]
                .grapheme_indices(true)
                .nth(1)
                .map_or(self.text.len(), |(index, _)| offset + index)
        } else {
            self.text[..offset]
                .grapheme_indices(true)
                .next_back()
                .map_or(0, |(index, _)| index)
        }
    }

    fn replace(&mut self, range: Range<usize>, text: &str, cx: &mut Context<Self>) {
        // A field of one line takes a pasted paragraph as one line.
        let text: String = text
            .chars()
            .map(|c| if c == '\n' || c == '\r' { ' ' } else { c })
            .collect();
        self.text.replace_range(range.clone(), &text);
        let caret = range.start + text.len();
        self.selection = caret..caret;
        self.reversed = false;
        self.marked = None;
        self.changed(cx);
    }

    fn utf16_to_byte(&self, offset: usize) -> usize {
        let mut units = 0;
        for (index, character) in self.text.char_indices() {
            if units >= offset {
                return index;
            }
            units += character.len_utf16();
        }
        self.text.len()
    }

    fn byte_to_utf16(&self, offset: usize) -> usize {
        self.text[..offset].encode_utf16().count()
    }

    fn range_to_utf16(&self, range: &Range<usize>) -> Range<usize> {
        self.byte_to_utf16(range.start)..self.byte_to_utf16(range.end)
    }

    fn range_from_utf16(&self, range: &Range<usize>) -> Range<usize> {
        self.utf16_to_byte(range.start)..self.utf16_to_byte(range.end)
    }

    fn key_down(&mut self, event: &KeyDownEvent, _: &mut Window, cx: &mut Context<Self>) {
        let keystroke = &event.keystroke;
        let command = keystroke.modifiers.platform || keystroke.modifiers.control;
        let shift = keystroke.modifiers.shift;
        // While an input method composes, the keys are its own.
        if self.marked.is_some() {
            return;
        }
        match keystroke.key.as_str() {
            "left" if command => self.move_to(0, shift, cx),
            "right" if command => self.move_to(self.text.len(), shift, cx),
            "home" => self.move_to(0, shift, cx),
            "end" => self.move_to(self.text.len(), shift, cx),
            "left" => {
                let to = if shift || self.selection.is_empty() {
                    self.boundary(self.caret(), false)
                } else {
                    self.selection.start
                };
                self.move_to(to, shift, cx);
            }
            "right" => {
                let to = if shift || self.selection.is_empty() {
                    self.boundary(self.caret(), true)
                } else {
                    self.selection.end
                };
                self.move_to(to, shift, cx);
            }
            "backspace" | "delete" => {
                let range = if !self.selection.is_empty() {
                    self.selection.clone()
                } else if keystroke.key == "backspace" {
                    self.boundary(self.caret(), false)..self.caret()
                } else {
                    self.caret()..self.boundary(self.caret(), true)
                };
                if !range.is_empty() {
                    self.replace(range, "", cx);
                }
            }
            "a" if command => {
                self.selection = 0..self.text.len();
                self.reversed = false;
                cx.notify();
            }
            "c" | "x" if command => {
                if !self.selection.is_empty() {
                    let text = self.text[self.selection.clone()].to_owned();
                    cx.write_to_clipboard(ClipboardItem::new_string(text));
                    if keystroke.key == "x" {
                        self.replace(self.selection.clone(), "", cx);
                    }
                }
            }
            "v" if command => {
                if let Some(text) = cx.read_from_clipboard().and_then(|item| item.text()) {
                    self.replace(self.selection.clone(), &text, cx);
                }
            }
            "escape" if !self.text.is_empty() => self.replace(0..self.text.len(), "", cx),
            "enter" => cx.emit(TextInputEvent::Submitted),
            _ => return,
        }
        cx.stop_propagation();
    }

    fn shape(&self, window: &mut Window) -> ShapedLine {
        let palette = self.theme.palette();
        let (text, color): (SharedString, Hsla) = if self.text.is_empty() {
            (self.placeholder.clone(), palette.faint)
        } else {
            (self.text.clone().into(), palette.text)
        };
        let run = TextRun {
            len: text.len(),
            font: font(FONT_UI),
            color,
            background_color: None,
            underline: None,
            strikethrough: None,
        };
        window
            .text_system()
            .shape_line(text, px(TEXT), &[run], None)
    }

    fn offset_at(&self, position: Point<Pixels>, window: &mut Window) -> usize {
        if self.text.is_empty() {
            return 0;
        }
        let x = position.x - self.text_bounds.get().left() + self.scroll.get();
        self.shape(window).closest_index_for_x(x)
    }

    fn mouse_down(&mut self, event: &MouseDownEvent, window: &mut Window, cx: &mut Context<Self>) {
        self.focus.focus(window, cx);
        if event.click_count >= 2 {
            self.selection = 0..self.text.len();
            self.reversed = false;
            cx.notify();
            return;
        }
        let offset = self.offset_at(event.position, window);
        self.move_to(offset, event.modifiers.shift, cx);
        self.selecting = true;
    }

    fn mouse_move(&mut self, event: &MouseMoveEvent, window: &mut Window, cx: &mut Context<Self>) {
        if self.selecting && event.pressed_button == Some(MouseButton::Left) {
            let offset = self.offset_at(event.position, window);
            if offset != self.caret() {
                self.move_to(offset, true, cx);
            }
        }
    }

    fn mouse_up(&mut self, _: &MouseUpEvent, _: &mut Window, _: &mut Context<Self>) {
        self.selecting = false;
    }

    /// Paints the text, the selection and the caret, and takes text input
    /// while focused.
    fn paint(view: ui::Entity<Self>, bounds: Bounds<Pixels>, window: &mut Window, cx: &mut App) {
        let focus = view.read(cx).focus.clone();
        if focus.is_focused(window) {
            window.handle_input(
                &focus,
                ui::ElementInputHandler::new(bounds, view.clone()),
                cx,
            );
        }
        let shaped = view.read(cx).shape(window);
        let (palette, empty, selection, caret, marked, scrolled, painted) = {
            let this = view.read(cx);
            (
                this.theme.palette(),
                this.text.is_empty(),
                this.selection.clone(),
                this.caret(),
                this.marked.clone(),
                this.scroll.clone(),
                this.text_bounds.clone(),
            )
        };
        let x_of = |offset: usize| {
            if empty {
                px(0.)
            } else {
                shaped.x_for_index(offset)
            }
        };
        let caret_x = x_of(caret);
        // Keep the caret inside the field with one pixel to spare.
        let room = bounds.size.width - px(1.);
        let widest = (if empty { px(0.) } else { shaped.width() } - room).max(px(0.));
        let scroll = scrolled
            .get()
            .clamp(caret_x - room, caret_x)
            .clamp(px(0.), widest)
            .round();
        scrolled.set(scroll);
        painted.set(bounds);
        let line_height = px(20.);
        let top = bounds.top() + ((bounds.size.height - line_height) / 2.).round();
        let band = |range: &Range<usize>, top: Pixels, height: Pixels| {
            Bounds::new(
                point(bounds.left() + x_of(range.start) - scroll, top),
                size(x_of(range.end) - x_of(range.start), height),
            )
        };
        window.with_content_mask(Some(ui::ContentMask { bounds }), |window| {
            let focused = focus.is_focused(window);
            if focused && !selection.is_empty() && marked.is_none() {
                window.paint_quad(fill(band(&selection, top, line_height), palette.selection));
            }
            let origin = point(bounds.left() - scroll, top);
            let _ = shaped.paint(origin, line_height, TextAlign::Left, None, window, cx);
            if let Some(marked) = &marked {
                // Text an input method is composing is underlined until it commits.
                let line = band(marked, top + line_height - px(2.), px(1.));
                window.paint_quad(fill(line, palette.muted));
            }
            if focused {
                let caret = Bounds::new(
                    point(bounds.left() + caret_x - scroll, top + px(2.)),
                    size(px(1.), line_height - px(4.)),
                );
                window.paint_quad(fill(caret, palette.text));
            }
        });
    }
}

impl EntityInputHandler for TextInput {
    fn text_for_range(
        &mut self,
        range: Range<usize>,
        adjusted: &mut Option<Range<usize>>,
        _: &mut Window,
        _: &mut Context<Self>,
    ) -> Option<String> {
        let range = self.range_from_utf16(&range);
        *adjusted = Some(self.range_to_utf16(&range));
        Some(self.text[range].to_owned())
    }

    fn selected_text_range(
        &mut self,
        _: bool,
        _: &mut Window,
        _: &mut Context<Self>,
    ) -> Option<UTF16Selection> {
        Some(UTF16Selection {
            range: self.range_to_utf16(&self.selection),
            reversed: self.reversed,
        })
    }

    fn marked_text_range(&self, _: &mut Window, _: &mut Context<Self>) -> Option<Range<usize>> {
        self.marked.as_ref().map(|range| self.range_to_utf16(range))
    }

    fn unmark_text(&mut self, _: &mut Window, cx: &mut Context<Self>) {
        self.marked = None;
        cx.notify();
    }

    fn replace_text_in_range(
        &mut self,
        range: Option<Range<usize>>,
        text: &str,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let range = range
            .map(|range| self.range_from_utf16(&range))
            .or_else(|| self.marked.clone())
            .unwrap_or_else(|| self.selection.clone());
        self.replace(range, text, cx);
    }

    fn replace_and_mark_text_in_range(
        &mut self,
        range: Option<Range<usize>>,
        text: &str,
        selected: Option<Range<usize>>,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let range = range
            .map(|range| self.range_from_utf16(&range))
            .or_else(|| self.marked.clone())
            .unwrap_or_else(|| self.selection.clone());
        self.text.replace_range(range.clone(), text);
        let marked = range.start..range.start + text.len();
        self.selection = match selected {
            // The selection the input method reports is relative to its text.
            Some(selected) => {
                let units = |units: usize| {
                    let mut seen = 0;
                    text.char_indices()
                        .find(|(_, character)| {
                            let found = seen >= units;
                            seen += character.len_utf16();
                            found
                        })
                        .map_or(text.len(), |(index, _)| index)
                };
                marked.start + units(selected.start)..marked.start + units(selected.end)
            }
            None => marked.end..marked.end,
        };
        self.reversed = false;
        self.marked = (!text.is_empty()).then_some(marked);
        self.changed(cx);
    }

    fn bounds_for_range(
        &mut self,
        range: Range<usize>,
        bounds: Bounds<Pixels>,
        window: &mut Window,
        _: &mut Context<Self>,
    ) -> Option<Bounds<Pixels>> {
        if self.text.is_empty() {
            return Some(Bounds::new(bounds.origin, size(px(1.), bounds.size.height)));
        }
        let range = self.range_from_utf16(&range);
        let shaped = self.shape(window);
        let scroll = self.scroll.get();
        let left = bounds.left() + shaped.x_for_index(range.start) - scroll;
        let right = bounds.left() + shaped.x_for_index(range.end) - scroll;
        Some(Bounds::new(
            point(left, bounds.top()),
            size((right - left).max(px(1.)), bounds.size.height),
        ))
    }

    fn character_index_for_point(
        &mut self,
        position: Point<Pixels>,
        window: &mut Window,
        _: &mut Context<Self>,
    ) -> Option<usize> {
        Some(self.byte_to_utf16(self.offset_at(position, window)))
    }
}

impl Render for TextInput {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let palette: Palette = self.theme.palette();
        let focused = self.focus.is_focused(window);
        let view = cx.entity();
        div()
            .id(self.id)
            .snapshot(self.id, None)
            .snapshot_focused(focused)
            .snapshot_text(self.text.clone())
            .flex_none()
            .w_full()
            .h(px(CONTROL))
            .px(px(PADDING))
            .rounded(px(RADIUS_INNER))
            .border_1()
            .border_color(if focused {
                palette.accent
            } else {
                palette.border
            })
            .bg(palette.well)
            .cursor(CursorStyle::IBeam)
            .track_focus(&self.focus)
            .on_key_down(cx.listener(Self::key_down))
            .on_mouse_down(MouseButton::Left, cx.listener(Self::mouse_down))
            .on_mouse_move(cx.listener(Self::mouse_move))
            .on_mouse_up(MouseButton::Left, cx.listener(Self::mouse_up))
            .on_mouse_up_out(MouseButton::Left, cx.listener(Self::mouse_up))
            .child(
                canvas(
                    |_, _, _| (),
                    move |bounds, _, window, cx| Self::paint(view, bounds, window, cx),
                )
                .size_full(),
            )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::harness::Session;

    /// Runs `f` on a field in a window, as the platform's input handler would.
    fn with_field<R>(
        f: impl FnOnce(&mut TextInput, &mut Window, &mut Context<TextInput>) -> R,
    ) -> R {
        let session = Session::open(320, 80, 1.0, |_, cx| {
            cx.new(|cx| TextInput::new("field", "Search", Theme::Dark, cx))
        });
        session.update(f)
    }

    fn key(key: &str, shift: bool, command: bool) -> KeyDownEvent {
        KeyDownEvent {
            keystroke: ui::Keystroke {
                key: key.into(),
                key_char: None,
                modifiers: ui::Modifiers {
                    shift,
                    platform: command,
                    ..Default::default()
                },
            },
            is_held: false,
            prefer_character_input: false,
        }
    }

    #[test]
    fn typing_moving_and_deleting_follow_whole_characters() {
        with_field(|field, window, cx| {
            field.replace_text_in_range(None, "ab👨‍👩‍👧é", window, cx);
            assert_eq!(field.selection, 22..22);
            // (key, shift, command) -> (text, selection, reversed)
            let steps = [
                (("left", false, false), ("ab👨‍👩‍👧é", 20..20, false)),
                (("left", true, false), ("ab👨‍👩‍👧é", 2..20, true)),
                (("right", false, false), ("ab👨‍👩‍👧é", 20..20, false)),
                (("backspace", false, false), ("abé", 2..2, false)),
                (("home", true, false), ("abé", 0..2, true)),
                (("delete", false, false), ("é", 0..0, false)),
                (("delete", false, false), ("", 0..0, false)),
                (("backspace", false, false), ("", 0..0, false)),
            ];
            for ((name, shift, command), (text, selection, reversed)) in steps {
                field.key_down(&key(name, shift, command), window, cx);
                assert_eq!(
                    (field.text.as_str(), field.selection.clone(), field.reversed),
                    (text, selection, reversed),
                    "after {name}"
                );
            }
        });
    }

    #[test]
    fn the_clipboard_takes_and_gives_the_selection() {
        with_field(|field, window, cx| {
            field.replace_text_in_range(None, "timeout budget", window, cx);
            field.key_down(&key("a", false, true), window, cx);
            field.key_down(&key("x", false, true), window, cx);
            assert_eq!(field.text, "");
            cx.write_to_clipboard(ClipboardItem::new_string("upstream\ntimeout".into()));
            field.key_down(&key("v", false, true), window, cx);
            assert_eq!(field.text, "upstream timeout");
            field.key_down(&key("escape", false, false), window, cx);
            assert_eq!(field.text, "");
        });
    }

    #[test]
    fn composition_marks_text_until_it_commits() {
        with_field(|field, window, cx| {
            field.replace_text_in_range(None, "连接 ", window, cx);
            field.replace_and_mark_text_in_range(None, "chao", Some(4..4), window, cx);
            assert_eq!(field.text, "连接 chao");
            assert_eq!(field.marked_text_range(window, cx), Some(3..7));
            // Keys belong to the input method while it composes.
            field.key_down(&key("backspace", false, false), window, cx);
            assert_eq!(field.text, "连接 chao");
            field.replace_and_mark_text_in_range(None, "超时", Some(2..2), window, cx);
            assert_eq!(field.text, "连接 超时");
            assert_eq!(field.marked_text_range(window, cx), Some(3..5));
            let selected = field
                .selected_text_range(false, window, cx)
                .map(|s| s.range);
            assert_eq!(selected, Some(5..5));
            field.replace_text_in_range(None, "超时", window, cx);
            assert_eq!(field.text, "连接 超时");
            assert_eq!(field.marked_text_range(window, cx), None);
            // Ranges the platform names are in UTF-16 units.
            let mut adjusted = None;
            let text = field.text_for_range(3..5, &mut adjusted, window, cx);
            assert_eq!((text.as_deref(), adjusted), (Some("超时"), Some(3..5)));
            field.replace_text_in_range(Some(0..2), "😀", window, cx);
            assert_eq!(field.text, "😀 超时");
            assert_eq!(
                field
                    .selected_text_range(false, window, cx)
                    .map(|s| s.range),
                Some(2..2)
            );
        });
    }
}
