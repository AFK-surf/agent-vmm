//! The lines a log view holds, and the rows it shows of them.

use super::ansi::{Span, StyledLine};
use std::{collections::VecDeque, ops::Range};
use ui::SharedString;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stream {
    Stdout,
    Stderr,
}

#[derive(Clone, Debug)]
pub struct LogLine {
    /// Counts every line ever pushed, so it names a line while others are dropped.
    pub sequence: u64,
    pub stream: Stream,
    /// The time of day of the record the line came in.
    pub time: SharedString,
    pub text: SharedString,
    pub spans: Vec<Span>,
    /// How often the search text occurs in the line.
    matches: u32,
}

/// A bounded buffer of lines: a new line pushes out the oldest once it is full.
///
/// With a search text every line knows its number of matches. The rows are
/// all lines, or only those of one stream, or only those with a match.
pub struct LogBuffer {
    lines: VecDeque<LogLine>,
    capacity: usize,
    next_sequence: u64,
    needle: String,
    only_matches: bool,
    stream: Option<Stream>,
    match_count: usize,
    matching_lines: usize,
    /// While the rows are filtered: the sequences of the lines shown, oldest first.
    shown: VecDeque<u64>,
}

/// What a push changed for a view that keeps its place.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Pushed {
    /// Rows that left at the front.
    pub rows_dropped: usize,
}

impl LogBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            lines: VecDeque::new(),
            capacity,
            next_sequence: 0,
            needle: String::new(),
            only_matches: false,
            stream: None,
            match_count: 0,
            matching_lines: 0,
            shown: VecDeque::new(),
        }
    }

    pub fn push(&mut self, stream: Stream, time: SharedString, line: StyledLine) -> Pushed {
        let mut pushed = Pushed::default();
        if self.lines.len() == self.capacity {
            let dropped = self.lines.pop_front().expect("a full buffer has a line");
            self.match_count -= dropped.matches as usize;
            self.matching_lines -= usize::from(dropped.matches > 0);
            if !self.is_filtered() {
                pushed.rows_dropped = 1;
            } else if self.shown.front() == Some(&dropped.sequence) {
                self.shown.pop_front();
                pushed.rows_dropped = 1;
            }
        }
        let matches = find(&line.text, &self.needle).count() as u32;
        self.match_count += matches as usize;
        self.matching_lines += usize::from(matches > 0);
        let line = LogLine {
            sequence: self.next_sequence,
            stream,
            time,
            text: line.text.into(),
            spans: line.spans,
            matches,
        };
        if self.is_filtered() && self.shows(&line) {
            self.shown.push_back(line.sequence);
        }
        self.lines.push_back(line);
        self.next_sequence += 1;
        pushed
    }

    pub fn clear(&mut self) {
        self.lines.clear();
        self.shown.clear();
        self.match_count = 0;
        self.matching_lines = 0;
    }

    fn shows(&self, line: &LogLine) -> bool {
        self.stream.is_none_or(|stream| stream == line.stream)
            && (!self.only_matches || self.needle.is_empty() || line.matches > 0)
    }

    fn filter(&mut self) {
        self.shown.clear();
        if self.is_filtered() {
            let shown = self.lines.iter().filter(|line| self.shows(line));
            self.shown = shown.map(|line| line.sequence).collect();
        }
    }

    /// Searches for `needle`, ignoring ASCII case. An empty text ends the search.
    pub fn set_search(&mut self, needle: &str) {
        if self.needle == needle {
            return;
        }
        self.needle = needle.to_owned();
        self.match_count = 0;
        self.matching_lines = 0;
        for line in &mut self.lines {
            line.matches = find(&line.text, needle).count() as u32;
            self.match_count += line.matches as usize;
            self.matching_lines += usize::from(line.matches > 0);
        }
        self.filter();
    }

    pub fn set_only_matches(&mut self, only_matches: bool) {
        self.only_matches = only_matches;
        self.filter();
    }

    /// Shows the lines of one stream, or of both.
    pub fn set_stream(&mut self, stream: Option<Stream>) {
        self.stream = stream;
        self.filter();
    }

    pub fn search(&self) -> &str {
        &self.needle
    }

    /// Whether the rows are fewer than the lines.
    pub fn is_filtered(&self) -> bool {
        self.stream.is_some() || (self.only_matches && !self.needle.is_empty())
    }

    pub fn match_count(&self) -> usize {
        self.match_count
    }

    pub fn matching_line_count(&self) -> usize {
        self.matching_lines
    }

    pub fn line_count(&self) -> usize {
        self.lines.len()
    }

    /// How many lines the buffer has dropped to stay within its capacity.
    pub fn dropped_count(&self) -> u64 {
        self.next_sequence - self.lines.len() as u64
    }

    pub fn row_count(&self) -> usize {
        if self.is_filtered() {
            self.shown.len()
        } else {
            self.lines.len()
        }
    }

    pub fn row(&self, row: usize) -> Option<&LogLine> {
        if self.is_filtered() {
            self.line(*self.shown.get(row)?)
        } else {
            self.lines.get(row)
        }
    }

    pub fn line(&self, sequence: u64) -> Option<&LogLine> {
        let first = self.lines.front()?.sequence;
        self.lines
            .get(usize::try_from(sequence.checked_sub(first)?).ok()?)
    }

    /// The row that shows the line `sequence`, or the first row after it.
    pub fn row_of(&self, sequence: u64) -> usize {
        if self.is_filtered() {
            self.shown.partition_point(|shown| *shown < sequence)
        } else {
            let first = self.lines.front().map_or(0, |line| line.sequence);
            (sequence.saturating_sub(first) as usize).min(self.lines.len())
        }
    }

    /// Where the search text occurs in `line`.
    pub fn matches_in<'a>(&'a self, line: &'a LogLine) -> impl Iterator<Item = Range<usize>> + 'a {
        let needle = if line.matches > 0 {
            self.needle.as_str()
        } else {
            ""
        };
        find(&line.text, needle)
    }
}

/// The places `needle` occurs in `text`, ignoring ASCII case, without overlaps.
fn find<'a>(text: &'a str, needle: &'a str) -> impl Iterator<Item = Range<usize>> + 'a {
    let (text, needle) = (text.as_bytes(), needle.as_bytes());
    let mut from = 0;
    std::iter::from_fn(move || {
        if needle.is_empty() {
            return None;
        }
        while from + needle.len() <= text.len() {
            let range = from..from + needle.len();
            if text[range.clone()].eq_ignore_ascii_case(needle) {
                from = range.end;
                return Some(range);
            }
            from += 1;
        }
        None
    })
}

/// The rows to draw for a viewport `viewport` high scrolled `scroll_top` down
/// a list of `row_count` rows: those in sight and `overscan` rows around them.
pub fn visible_rows(
    row_count: usize,
    row_height: f32,
    viewport: f32,
    scroll_top: f32,
    overscan: usize,
) -> Range<usize> {
    if row_count == 0 || viewport <= 0.0 {
        return 0..0;
    }
    let first = (scroll_top.max(0.0) / row_height).floor() as usize;
    let last = ((scroll_top.max(0.0) + viewport) / row_height).ceil() as usize;
    first.saturating_sub(overscan).min(row_count)..(last + overscan).min(row_count)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn line(text: &str) -> StyledLine {
        StyledLine {
            text: text.into(),
            spans: Vec::new(),
        }
    }

    fn filled(capacity: usize, count: usize) -> LogBuffer {
        let mut buffer = LogBuffer::new(capacity);
        for index in 0..count {
            let text = if index % 10 == 3 {
                format!("line {index} upstream Timeout, then timeout again")
            } else {
                format!("line {index} ok")
            };
            buffer.push(Stream::Stdout, "".into(), line(&text));
        }
        buffer
    }

    #[test]
    fn ten_thousand_lines_draw_only_the_rows_in_sight() {
        let buffer = filled(20_000, 10_000);
        assert_eq!(buffer.row_count(), 10_000);
        // (viewport, scroll top) -> rows
        let cases = [
            ((540.0, 0.0), 0..38),
            ((540.0, 9.0), 0..39),
            ((540.0, 90_000.0), 4_992..5_038),
            ((540.0, 179_460.0), 9_962..10_000),
            ((540.0, 1e9), 10_000..10_000),
            ((0.0, 0.0), 0..0),
        ];
        for ((viewport, scroll_top), rows) in cases {
            let visible = visible_rows(buffer.row_count(), 18.0, viewport, scroll_top, 8);
            assert_eq!(visible, rows, "viewport {viewport} at {scroll_top}");
            assert!(visible.len() <= (viewport / 18.0) as usize + 1 + 16);
        }
    }

    #[test]
    fn a_full_buffer_drops_its_oldest_lines() {
        let mut buffer = filled(100, 100);
        assert_eq!(buffer.dropped_count(), 0);
        let pushed = buffer.push(Stream::Stderr, "".into(), line("newest"));
        assert_eq!(pushed.rows_dropped, 1);
        assert_eq!(buffer.line_count(), 100);
        assert_eq!(buffer.dropped_count(), 1);
        assert_eq!(buffer.row(0).map(|line| line.sequence), Some(1));
        assert_eq!(buffer.row(99).map(|line| &*line.text), Some("newest"));
        assert!(buffer.line(0).is_none());
        assert_eq!(
            buffer.line(100).map(|line| line.stream),
            Some(Stream::Stderr)
        );
    }

    #[test]
    fn search_counts_matches_and_filters_rows() {
        let mut buffer = filled(1_000, 1_000);
        buffer.set_search("TIMEOUT");
        assert_eq!(buffer.match_count(), 200);
        assert_eq!(buffer.matching_line_count(), 100);
        assert_eq!(buffer.row_count(), 1_000);
        let third = buffer.row(3).expect("row 3");
        assert_eq!(
            buffer.matches_in(third).collect::<Vec<_>>(),
            [16..23, 30..37]
        );
        assert_eq!(buffer.matches_in(buffer.row(4).expect("row 4")).count(), 0);

        buffer.set_only_matches(true);
        assert_eq!(buffer.row_count(), 100);
        assert_eq!(buffer.row(1).map(|line| line.sequence), Some(13));
        assert_eq!(buffer.row_of(13), 1);
        assert_eq!(buffer.row_of(14), 2);

        // Lines that come and go keep the counts right: the fourth push drops
        // line 3, the first row.
        let dropped: Vec<_> = (0..4)
            .map(|_| {
                buffer
                    .push(Stream::Stdout, "".into(), line("no match"))
                    .rows_dropped
            })
            .collect();
        assert_eq!(dropped, [0, 0, 0, 1]);
        assert_eq!(buffer.match_count(), 198);
        assert_eq!(buffer.row(0).map(|line| line.sequence), Some(13));
        buffer.push(Stream::Stdout, "".into(), line("a timeout"));
        assert_eq!(buffer.match_count(), 199);
        assert_eq!(buffer.row_count(), 100);

        buffer.set_search("");
        assert_eq!((buffer.match_count(), buffer.row_count()), (0, 1_000));
        assert!(!buffer.is_filtered());
    }

    #[test]
    fn one_stream_can_be_shown_alone() {
        let mut buffer = LogBuffer::new(100);
        for index in 0..10 {
            let stream = if index % 5 == 0 {
                Stream::Stderr
            } else {
                Stream::Stdout
            };
            buffer.push(stream, "".into(), line(&format!("line {index} timeout")));
        }
        buffer.set_stream(Some(Stream::Stderr));
        let shown = |buffer: &LogBuffer| -> Vec<u64> {
            (0..buffer.row_count())
                .filter_map(|row| buffer.row(row).map(|line| line.sequence))
                .collect()
        };
        assert_eq!(shown(&buffer), [0, 5]);
        buffer.push(Stream::Stderr, "".into(), line("fatal"));
        buffer.push(Stream::Stdout, "".into(), line("fine"));
        assert_eq!(shown(&buffer), [0, 5, 10]);
        // Both filters together show the lines that pass both.
        buffer.set_search("timeout");
        buffer.set_only_matches(true);
        assert_eq!(shown(&buffer), [0, 5]);
        assert_eq!(buffer.matching_line_count(), 10);
        buffer.set_stream(None);
        assert_eq!(buffer.row_count(), 10);
    }

    #[test]
    fn search_matches_whole_characters() {
        let mut buffer = LogBuffer::new(8);
        buffer.push(Stream::Stdout, "".into(), line("连接超时 Aaa"));
        buffer.set_search("aa");
        let row = buffer.row(0).expect("row");
        assert_eq!(buffer.matches_in(row).collect::<Vec<_>>(), [13..15]);
        buffer.set_search("超时");
        let row = buffer.row(0).expect("row");
        assert_eq!(buffer.matches_in(row).collect::<Vec<_>>(), [6..12]);
    }
}
