//! Reads the Host and guest log files of the service user in bounded pages.
//! A cursor names the file, the offset and the rotation that it was read in.

use crate::envelope::{LOG_BYTES, LogRecord, redact_text};
use crate::facts::{self, Facts};
use crate::helper;
use crate::options::Options;
use crate::runtime::LogPage;
use rustix::fs::{Mode, OFlags};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use uzers::os::unix::UserExt;
use vmm_lifecycle::timefmt;

struct Segment {
    file: File,
    identity: String,
    size: u64,
    modified: (i64, u32),
    offset: u64,
}

pub fn read_local(status: &Facts, options: &Options) -> Result<LogPage, String> {
    let account = uzers::get_user_by_name(facts::string(status, "serviceUser")).ok_or("service user log directory is unavailable")?;
    let home = account.home_dir();
    let paths = if options.component == "guest" {
        let directory = helper::service_log_directory(home);
        vec![directory.join("serial.log.previous"), directory.join("serial.log")]
    } else if facts::string(status, "hostJob").starts_with("system/") {
        vec![PathBuf::from("/Library/Logs/AgentVMM/host.stderr.log")]
    } else {
        vec![helper::service_output_log_directory(home).join("host.stderr.log")]
    };
    read_files(&paths, options)
}

fn open(path: &Path) -> Result<Option<Segment>, String> {
    // A link in place of the log is refused.
    let descriptor = match rustix::fs::open(path, OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC, Mode::empty()) {
        Ok(descriptor) => descriptor,
        Err(rustix::io::Errno::NOENT) => return Ok(None),
        Err(error) => return Err(std::io::Error::from(error).to_string()),
    };
    let file = File::from(descriptor);
    let metadata = file.metadata().ok().filter(|metadata| metadata.is_file()).ok_or("log is not a regular file")?;
    let identity = format!("{}:{}", metadata.dev(), metadata.ino());
    Ok(Some(Segment { file, identity, size: metadata.len(), modified: (metadata.mtime(), metadata.mtime_nsec() as u32), offset: 0 }))
}

/// Reads the files from oldest to newest. The newest bytes are kept when the page is full.
pub fn read_files(paths: &[PathBuf], options: &Options) -> Result<LogPage, String> {
    let mut segments: Vec<Segment> = Vec::new();
    for path in paths {
        let Some(segment) = open(path)? else { continue };
        // The same file under two names is one segment.
        if segments.last().is_some_and(|last| last.identity == segment.identity) {
            segments.pop();
        }
        segments.push(segment);
    }
    let rotation = match segments.as_slice() {
        [previous, _, ..] => format!("{}:{}", previous.identity, i128::from(previous.modified.0) * 1_000_000_000 + i128::from(previous.modified.1)),
        _ => "none".to_string(),
    };
    let mut first = 0;
    let mut gap = false;
    if !options.after_cursor.is_empty() {
        let (cursor_offset, cursor_rotation) = options.after_cursor.split_once('|').unwrap_or((&options.after_cursor, ""));
        let known = options.after_cursor.contains('|') && cursor_rotation == rotation;
        let position = segments.iter().enumerate().find_map(|(index, segment)| Some((index, cursor_offset.strip_prefix(&format!("{}:", segment.identity))?)));
        match position.filter(|_| known).and_then(|(index, offset)| Some((index, offset.parse::<u64>().ok().filter(|offset| *offset <= segments[index].size)?))) {
            Some((index, offset)) => {
                first = index;
                segments[index].offset = offset;
            }
            None => gap = true,
        }
    }
    let cursor = |segment: &Segment| format!("{}:{}|{rotation}", segment.identity, segment.size);
    let latest = segments.last().map_or_else(|| options.after_cursor.clone(), cursor);
    if options.limit > 0 && segments.len() - first.min(segments.len()) > options.limit {
        first = segments.len() - options.limit;
        gap = true;
    }
    let mut remaining = LOG_BYTES as u64;
    let mut records = Vec::new();
    for segment in segments.iter_mut().skip(first).rev() {
        let mut start = segment.offset;
        let truncated = segment.size - start > remaining;
        if truncated {
            start = segment.size - remaining;
            gap = true;
        }
        if start == segment.size {
            continue;
        }
        let mut data = Vec::new();
        segment.file.seek(SeekFrom::Start(start)).and_then(|_| (&segment.file).take(segment.size - start).read_to_end(&mut data)).map_err(|error| error.to_string())?;
        remaining -= data.len() as u64;
        let mut text = redact_text(&String::from_utf8_lossy(&data));
        if !options.operation_id.is_empty() {
            let wanted = format!("\"operation_id\":\"{}\"", options.operation_id);
            text = text.split('\n').filter(|line| line.contains(&wanted)).collect::<Vec<_>>().join("\n");
        }
        records.push(LogRecord { cursor: cursor(segment), timestamp: timefmt::format(segment.modified.0, segment.modified.1), stream: "stderr".into(), data: text, truncated, dropped: 0 });
    }
    records.reverse();
    Ok(LogPage { records, cursor: latest, gap })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn options(after_cursor: &str, limit: usize) -> Options {
        Options { component: "guest".into(), after_cursor: after_cursor.into(), limit, ..Options::default() }
    }

    #[test]
    fn a_cursor_continues_in_the_same_file_and_a_rotation_reports_a_gap() {
        let directory = tempfile::tempdir().unwrap();
        let (previous, current) = (directory.path().join("serial.log.previous"), directory.path().join("serial.log"));
        let paths = [previous.clone(), current.clone()];
        std::fs::write(&current, "one\n").unwrap();
        let first = read_files(&paths, &options("", 50)).unwrap();
        assert_eq!((first.records.len(), first.records[0].data.as_str(), first.gap), (1, "one\n", false));
        assert!(first.cursor.ends_with(":4|none") && first.records[0].cursor == first.cursor);

        // Nothing new: the page is empty and the cursor does not move.
        let idle = read_files(&paths, &options(&first.cursor, 50)).unwrap();
        assert_eq!((idle.records.len(), idle.cursor.as_str(), idle.gap), (0, first.cursor.as_str(), false));

        std::fs::OpenOptions::new().append(true).open(&current).unwrap().write_all(b"two\n").unwrap();
        let second = read_files(&paths, &options(&first.cursor, 50)).unwrap();
        assert_eq!((second.records[0].data.as_str(), second.gap), ("two\n", false));

        // A rotation invalidates the cursor: the reader starts again and says so.
        std::fs::rename(&current, &previous).unwrap();
        std::fs::write(&current, "three\n").unwrap();
        let rotated = read_files(&paths, &options(&second.cursor, 50)).unwrap();
        assert_eq!(rotated.records.iter().map(|record| record.data.as_str()).collect::<Vec<_>>(), ["one\ntwo\n", "three\n"]);
        assert!(rotated.gap && !rotated.cursor.ends_with("|none"));
        let newest = read_files(&paths, &options("", 1)).unwrap();
        assert_eq!((newest.records.len(), newest.records[0].data.as_str(), newest.gap), (1, "three\n", true));
    }

    #[test]
    fn secrets_and_other_operations_are_left_out() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("host.stderr.log");
        std::fs::write(&path, "{\"token\":\"abc\",\"operation_id\":\"o1\"}\n{\"operation_id\":\"o2\"}\n").unwrap();
        let page = read_files(std::slice::from_ref(&path), &Options { operation_id: "o1".into(), limit: 50, ..Options::default() }).unwrap();
        assert_eq!(page.records[0].data, "{\"token\":[REDACTED],\"operation_id\":\"o1\"}");
        assert_eq!(page.records[0].stream, "stderr");

        let link = directory.path().join("link.log");
        std::os::unix::fs::symlink(&path, &link).unwrap();
        assert!(read_files(&[link], &options("", 50)).is_err(), "a link is not followed");
        assert_eq!(read_files(&[directory.path().join("absent.log")], &options("stale", 50)).unwrap(), LogPage { records: Vec::new(), cursor: "stale".into(), gap: true });
    }
}
