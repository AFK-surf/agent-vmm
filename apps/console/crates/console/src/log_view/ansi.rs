//! Turns the bytes a container wrote to a terminal into styled lines.
//!
//! A chunk can end anywhere: inside a line, an escape sequence or a UTF-8
//! character. The parser keeps what is unfinished and continues with the next
//! chunk.

/// The longest line kept whole. A longer one continues on the next row.
const MAX_LINE_BYTES: usize = 4096;

/// What SGR sequences have selected for the text that follows.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Sgr {
    /// One of the 16 terminal colors, or the default foreground.
    pub color: Option<u8>,
    pub bold: bool,
    pub dim: bool,
}

/// A stretch of a line in one style, up to byte `end`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Span {
    pub end: usize,
    pub style: Sgr,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StyledLine {
    pub text: String,
    /// In order and without gaps: the last one ends at `text.len()`.
    pub spans: Vec<Span>,
}

#[derive(Default)]
enum State {
    #[default]
    Text,
    Escape,
    Csi,
    /// An operating system command, up to BEL or ESC `\`.
    Osc,
    OscEscape,
}

#[derive(Default)]
pub struct AnsiParser {
    state: State,
    style: Sgr,
    params: Vec<u8>,
    text: String,
    /// Where each style of `text` begins.
    starts: Vec<(usize, Sgr)>,
    /// The bytes of a UTF-8 character that has not ended.
    partial: Vec<u8>,
    /// A carriage return that a line feed has not followed yet.
    carriage_return: bool,
}

impl AnsiParser {
    /// Parses `bytes`, giving every line they complete to `emit`.
    pub fn feed(&mut self, bytes: &[u8], mut emit: impl FnMut(StyledLine)) {
        for &byte in bytes {
            match self.state {
                State::Text => self.text_byte(byte, &mut emit),
                State::Escape => {
                    self.state = match byte {
                        b'[' => {
                            self.params.clear();
                            State::Csi
                        }
                        b']' => State::Osc,
                        _ => State::Text,
                    }
                }
                State::Csi => match byte {
                    0x20..=0x3f => self.params.push(byte),
                    b'm' => {
                        self.select_graphic_rendition();
                        self.state = State::Text;
                    }
                    _ => self.state = State::Text,
                },
                State::Osc => match byte {
                    0x07 => self.state = State::Text,
                    0x1b => self.state = State::OscEscape,
                    _ => {}
                },
                State::OscEscape => {
                    self.state = if byte == b'\\' {
                        State::Text
                    } else {
                        State::Osc
                    }
                }
            }
        }
    }

    /// The line that has no line feed yet, if it has any text.
    pub fn flush(&mut self) -> Option<StyledLine> {
        self.end_character();
        (!self.text.is_empty()).then(|| self.take_line())
    }

    fn text_byte(&mut self, byte: u8, emit: &mut impl FnMut(StyledLine)) {
        if byte == b'\n' {
            self.end_character();
            self.carriage_return = false;
            emit(self.take_line());
            return;
        }
        if std::mem::take(&mut self.carriage_return) {
            // A carriage return without a line feed rewrites the line, as a
            // progress bar does.
            self.text.clear();
            self.starts.clear();
        }
        match byte {
            b'\r' => {
                self.end_character();
                self.carriage_return = true;
            }
            0x1b => {
                self.end_character();
                self.state = State::Escape;
            }
            b'\t' => self.push_str("    "),
            0x00..=0x1f | 0x7f => self.end_character(),
            0x00..=0x7f => {
                self.end_character();
                self.push_char(byte as char);
            }
            0x80..=0xbf if !self.partial.is_empty() => {
                self.partial.push(byte);
                let wanted = match self.partial[0] {
                    0xc0..=0xdf => 2,
                    0xe0..=0xef => 3,
                    _ => 4,
                };
                if self.partial.len() == wanted {
                    let character = std::str::from_utf8(&self.partial)
                        .ok()
                        .and_then(|text| text.chars().next())
                        .unwrap_or(char::REPLACEMENT_CHARACTER);
                    self.partial.clear();
                    self.push_char(character);
                }
            }
            0xc0..=0xf7 => {
                self.end_character();
                self.partial.push(byte);
            }
            _ => {
                self.end_character();
                self.push_char(char::REPLACEMENT_CHARACTER);
            }
        }
        if self.text.len() >= MAX_LINE_BYTES && self.partial.is_empty() {
            emit(self.take_line());
        }
    }

    /// Replaces a UTF-8 character that ended early.
    fn end_character(&mut self) {
        if !self.partial.is_empty() {
            self.partial.clear();
            self.push_char(char::REPLACEMENT_CHARACTER);
        }
    }

    fn push_char(&mut self, character: char) {
        self.open_span();
        self.text.push(character);
    }

    fn push_str(&mut self, text: &str) {
        self.end_character();
        self.open_span();
        self.text.push_str(text);
    }

    fn open_span(&mut self) {
        match self.starts.last_mut() {
            Some((_, style)) if *style == self.style => {}
            Some((start, style)) if *start == self.text.len() => *style = self.style,
            _ => self.starts.push((self.text.len(), self.style)),
        }
    }

    fn take_line(&mut self) -> StyledLine {
        let text = std::mem::take(&mut self.text);
        let mut spans: Vec<Span> = Vec::with_capacity(self.starts.len());
        for (start, style) in self.starts.drain(..) {
            if let Some(previous) = spans.last_mut() {
                previous.end = start;
            }
            spans.push(Span {
                end: text.len(),
                style,
            });
        }
        spans.retain(|span| span.end > 0);
        StyledLine { text, spans }
    }

    fn select_graphic_rendition(&mut self) {
        // A private or intermediate byte makes this another sequence that
        // happens to end in `m`.
        if self
            .params
            .iter()
            .any(|byte| !byte.is_ascii_digit() && *byte != b';')
        {
            return;
        }
        let params = std::mem::take(&mut self.params);
        let mut codes = params.split(|byte| *byte == b';').map(|digits| {
            digits
                .iter()
                .fold(0_u32, |value, digit| value * 10 + u32::from(digit - b'0'))
        });
        while let Some(code) = codes.next() {
            match code {
                0 => self.style = Sgr::default(),
                1 => self.style.bold = true,
                2 => self.style.dim = true,
                22 => {
                    self.style.bold = false;
                    self.style.dim = false;
                }
                30..=37 => self.style.color = Some((code - 30) as u8),
                39 => self.style.color = None,
                90..=97 => self.style.color = Some((code - 90 + 8) as u8),
                // 256-color and direct colors have no place in the 16-color
                // palette: skip their arguments and keep the current color.
                38 | 48 => match codes.next() {
                    Some(5) => {
                        codes.next();
                    }
                    Some(2) => {
                        codes.nth(2);
                    }
                    _ => {}
                },
                _ => {}
            }
        }
        self.params = params;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(chunks: &[&[u8]]) -> Vec<StyledLine> {
        let mut parser = AnsiParser::default();
        let mut lines = Vec::new();
        for chunk in chunks {
            parser.feed(chunk, |line| lines.push(line));
        }
        lines.extend(parser.flush());
        lines
    }

    fn styled(line: &StyledLine) -> Vec<(&str, Sgr)> {
        let mut start = 0;
        line.spans
            .iter()
            .map(|span| {
                let part = (&line.text[start..span.end], span.style);
                start = span.end;
                part
            })
            .collect()
    }

    const RED: Sgr = Sgr {
        color: Some(1),
        bold: false,
        dim: false,
    };

    #[test]
    fn styles_follow_sgr_sequences() {
        let plain = Sgr::default();
        let bold = Sgr {
            bold: true,
            ..plain
        };
        let dim = Sgr { dim: true, ..plain };
        let bright_green_bold = Sgr {
            color: Some(10),
            bold: true,
            dim: false,
        };
        let cases: [(&[u8], Vec<(&str, Sgr)>); 6] = [
            (b"plain", vec![("plain", plain)]),
            (
                b"\x1b[31mERROR\x1b[0m boom",
                vec![("ERROR", RED), (" boom", plain)],
            ),
            (
                b"\x1b[2m09:07\x1b[22m \x1b[1mmodule\x1b[0m",
                vec![("09:07", dim), (" ", plain), ("module", bold)],
            ),
            (b"\x1b[1;92mok\x1b[m", vec![("ok", bright_green_bold)]),
            // Colors outside the 16 keep the current one; other sequences vanish.
            (
                b"\x1b[31m\x1b[38;5;208ma\x1b[38;2;1;2;3mb\x1b[2K\x1b[?25lc\x1b]0;title\x07d",
                vec![("abcd", RED)],
            ),
            (b"a\x1b[39;49mb", vec![("ab", plain)]),
        ];
        for (input, expected) in cases {
            let lines = parse(&[input]);
            assert_eq!(lines.len(), 1, "{:?}", String::from_utf8_lossy(input));
            assert_eq!(styled(&lines[0]), expected);
        }
    }

    #[test]
    fn chunks_can_end_anywhere() {
        let whole: &[u8] =
            "\x1b[31m错误 ERROR\x1b[0m: 连接 reset\r\nnext \x1b[1mline\x1b[0m\n".as_bytes();
        let expected = parse(&[whole]);
        assert_eq!(expected.len(), 2);
        assert_eq!(expected[0].text, "错误 ERROR: 连接 reset");
        assert_eq!(styled(&expected[0])[0], ("错误 ERROR", RED));
        for split in 1..whole.len() {
            let (head, tail) = whole.split_at(split);
            assert_eq!(parse(&[head, tail]), expected, "split at {split}");
        }
        let bytes: Vec<&[u8]> = whole.chunks(1).collect();
        assert_eq!(parse(&bytes), expected);
    }

    #[test]
    fn a_style_carries_over_to_the_next_line() {
        let lines = parse(&[b"\x1b[31mfirst\nsecond\x1b[0m\nthird\n"]);
        let styles: Vec<_> = lines.iter().map(|line| styled(line)).collect();
        assert_eq!(
            styles,
            [
                vec![("first", RED)],
                vec![("second", RED)],
                vec![("third", Sgr::default())]
            ]
        );
    }

    #[test]
    fn control_bytes_and_bad_utf8_do_not_break_lines() {
        let lines = parse(&[b"progress 10%\rprogress 100%\n\ta\x00b\xffc\xe4\xb8\n\n"]);
        let text: Vec<_> = lines.iter().map(|line| line.text.as_str()).collect();
        assert_eq!(text, ["progress 100%", "    ab\u{fffd}c\u{fffd}", ""]);
        assert!(lines[2].spans.is_empty());
    }

    #[test]
    fn a_long_line_continues_on_the_next_row() {
        let long = "é".repeat(MAX_LINE_BYTES);
        let lines = parse(&[long.as_bytes(), b"\n"]);
        assert_eq!(lines.len(), 3);
        assert_eq!(lines[0].text.len(), MAX_LINE_BYTES);
        assert_eq!(lines[1].text.len(), MAX_LINE_BYTES);
        assert_eq!(lines[2].text, "");
        assert_eq!(
            lines[0].spans.last().map(|span| span.end),
            Some(MAX_LINE_BYTES)
        );
    }
}
