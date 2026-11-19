//! Bounded in-memory log of the main process output.
//!
//! The ring keeps the newest records within a byte limit. Each record has a
//! cursor that increases by one, so a reader can resume and can count the
//! records that left the ring before it read them.

use std::collections::VecDeque;
use std::sync::{Condvar, Mutex};
use std::time::Duration;

use vmm_guest_proto::rpc::{LogRecord, LogStream};

use crate::{lock, unix_nanos};

/// Accounting cost of one record besides its data.
const RECORD_OVERHEAD: usize = 32;
const MAX_RECORD_BYTES: usize = 16 * 1024;

struct Record {
    cursor: u64,
    timestamp_unix_nanos: i64,
    stream: LogStream,
    data: Vec<u8>,
}

#[derive(Default)]
struct Inner {
    records: VecDeque<Record>,
    bytes: usize,
    last_cursor: u64,
    writers: usize,
}

pub struct LogRing {
    limit: usize,
    inner: Mutex<Inner>,
    changed: Condvar,
}

/// Records after a position, and the state of the ring behind them.
pub struct Batch {
    pub records: Vec<LogRecord>,
    /// More records after the last one are ready.
    pub more: bool,
    /// Cursor of the newest record that the ring ever held.
    pub last_cursor: u64,
    /// No writer remains, so the ring gets no new record.
    pub ended: bool,
}

impl LogRing {
    pub fn new(limit_bytes: usize) -> Self {
        Self {
            limit: limit_bytes,
            inner: Mutex::default(),
            changed: Condvar::new(),
        }
    }

    /// Registers a source of records. The ring ends when each source closed.
    pub fn open_writer(&self) {
        lock(&self.inner).writers += 1;
    }

    pub fn close_writer(&self) {
        lock(&self.inner).writers -= 1;
        self.changed.notify_all();
    }

    pub fn push(&self, stream: LogStream, data: &[u8]) {
        let record_bytes = MAX_RECORD_BYTES.min(self.limit / 4).max(1);
        let timestamp_unix_nanos = unix_nanos();
        let mut inner = lock(&self.inner);
        for chunk in data.chunks(record_bytes) {
            inner.last_cursor += 1;
            let cursor = inner.last_cursor;
            inner.bytes += chunk.len() + RECORD_OVERHEAD;
            inner.records.push_back(Record {
                cursor,
                timestamp_unix_nanos,
                stream,
                data: chunk.to_vec(),
            });
            while inner.bytes > self.limit && inner.records.len() > 1 {
                let dropped = inner
                    .records
                    .pop_front()
                    .expect("the ring holds two records");
                inner.bytes -= dropped.data.len() + RECORD_OVERHEAD;
            }
        }
        drop(inner);
        self.changed.notify_all();
    }

    /// The position after which only the newest `count` matching records follow.
    /// `count` is at least one.
    pub fn tail_position(&self, count: u64, since_unix_nanos: Option<i64>) -> u64 {
        let inner = lock(&self.inner);
        let since = since_unix_nanos.unwrap_or(i64::MIN);
        inner
            .records
            .iter()
            .rev()
            .filter(|record| record.timestamp_unix_nanos >= since)
            .nth(count.saturating_sub(1) as usize)
            .map_or(0, |record| record.cursor - 1)
            .max(
                inner
                    .records
                    .front()
                    .map_or(inner.last_cursor, |record| record.cursor - 1),
            )
    }

    /// Reads at most `limit` records after the cursor `after`.
    ///
    /// The first record counts, in `dropped_records`, the records after `after`
    /// that left the ring.
    pub fn read(&self, after: u64, since_unix_nanos: Option<i64>, limit: usize) -> Batch {
        let inner = lock(&self.inner);
        let since = since_unix_nanos.unwrap_or(i64::MIN);
        let oldest = inner
            .records
            .front()
            .map_or(inner.last_cursor + 1, |record| record.cursor);
        let mut dropped = oldest.saturating_sub(after + 1);
        let start = inner
            .records
            .partition_point(|record| record.cursor <= after);
        let mut matching = inner
            .records
            .range(start..)
            .filter(|record| record.timestamp_unix_nanos >= since);
        let records = matching
            .by_ref()
            .take(limit)
            .map(|record| LogRecord {
                cursor: record.cursor,
                timestamp_unix_nanos: record.timestamp_unix_nanos,
                stream: record.stream,
                data: record.data.clone(),
                dropped_records: std::mem::take(&mut dropped),
            })
            .collect();
        Batch {
            records,
            more: matching.next().is_some(),
            last_cursor: inner.last_cursor,
            ended: inner.writers == 0,
        }
    }

    /// Waits until a record after `after` exists or the ring ends. Returns false at the timeout.
    pub fn wait(&self, after: u64, timeout: Duration) -> bool {
        let inner = lock(&self.inner);
        let (inner, _) = self
            .changed
            .wait_timeout_while(inner, timeout, |inner| {
                inner.last_cursor <= after && inner.writers > 0
            })
            .unwrap_or_else(|error| error.into_inner());
        inner.last_cursor > after || inner.writers == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cursors(batch: &Batch) -> Vec<u64> {
        batch.records.iter().map(|record| record.cursor).collect()
    }

    #[test]
    fn a_full_ring_drops_the_oldest_records_and_reports_the_gap() {
        // Each record costs 8 + 32 bytes, so the ring holds five of them.
        let ring = LogRing::new(200);
        ring.open_writer();
        for index in 0..12u8 {
            ring.push(LogStream::Stdout, &[index; 8]);
        }

        let all = ring.read(0, None, 100);
        assert_eq!(cursors(&all), [8, 9, 10, 11, 12]);
        assert_eq!(all.records[0].dropped_records, 7);
        assert!(
            all.records[1..]
                .iter()
                .all(|record| record.dropped_records == 0)
        );
        assert_eq!(all.records[0].data, [7; 8]);
        assert!(!all.more && !all.ended);

        let resumed = ring.read(10, None, 100);
        assert_eq!(cursors(&resumed), [11, 12]);
        assert_eq!(resumed.records[0].dropped_records, 0);

        ring.push(LogStream::Stderr, b"next");
        let incremental = ring.read(12, None, 100);
        assert_eq!(cursors(&incremental), [13]);
        assert_eq!(incremental.records[0].stream, LogStream::Stderr);

        let stale = ring.read(2, None, 2);
        assert_eq!(cursors(&stale), [9, 10]);
        assert_eq!(stale.records[0].dropped_records, 6);
        assert!(stale.more);

        ring.close_writer();
        assert!(ring.read(13, None, 100).ended);
    }

    #[test]
    fn tail_and_since_select_the_newest_matching_records() {
        let ring = LogRing::new(1 << 20);
        for index in 0..6u8 {
            ring.push(LogStream::Stdout, &[index]);
        }
        assert_eq!(
            cursors(&ring.read(ring.tail_position(2, None), None, 100)),
            [5, 6]
        );
        assert_eq!(ring.tail_position(100, None), 0);

        let future = unix_nanos() + 1_000_000_000;
        assert!(ring.read(0, Some(future), 100).records.is_empty());
        assert_eq!(ring.read(0, Some(0), 100).records.len(), 6);
    }

    #[test]
    fn a_large_write_is_split_and_never_exceeds_the_limit() {
        let ring = LogRing::new(4096);
        ring.push(LogStream::Stdout, &vec![b'x'; 10_000]);
        let batch = ring.read(0, None, 100);
        let bytes: usize = batch.records.iter().map(|record| record.data.len()).sum();
        assert!(bytes <= 4096 && bytes > 2048, "{bytes}");
        assert!(batch.records[0].dropped_records > 0);
    }

    #[test]
    fn wait_returns_for_a_new_record_and_at_the_end() {
        let ring = std::sync::Arc::new(LogRing::new(4096));
        ring.open_writer();
        assert!(!ring.wait(0, Duration::from_millis(20)));
        let writer = ring.clone();
        let thread = std::thread::spawn(move || {
            writer.push(LogStream::Stdout, b"line");
            writer.close_writer();
        });
        assert!(ring.wait(0, Duration::from_secs(10)));
        thread.join().unwrap();
        assert!(ring.wait(1, Duration::from_secs(10)));
        assert!(ring.read(1, None, 100).ended);
    }
}
