//! [`Journal`] — the append-only write-ahead log, and the engine's **only**
//! atomicity mechanism.
//!
//! Durability lives here: a batch is durable once [`Journal::append`]
//! returns, because append writes the frame and `fsync`s before yielding.
//! Everything else in the engine — pages, the page directories, the allocator —
//! is a reconstruction rooted in journal frames.
//!
//! A store opened with a durability window takes the other door —
//! [`append_deferred`](Journal::append_deferred) plus a windowed
//! [`sync`](Journal::sync) — and the promise weakens accordingly (RFC 0061).
//!
//! ## Files
//!
//! Journals are timestamped: `journal_<nanos>.log`. Rotation creates a new
//! file and redirects appends; the old journal is retired by a [`Commit`]
//! frame written into the **new** journal (see `crate::commit`), then
//! deleted. A `data.bin` with **no** journal present is corrupt — the
//! journal chain is the recovery root, its absence means history was lost.
//!
//! ## Frame format
//!
//! ```text
//! [ payload_len (u32 LE) ][ payload = to_wire_checked(JournalFrame) ]
//!                           └── [ crc32 (u32 LE) ][ wire bytes ] ──┘
//! ```
//!
//! Two frame kinds:
//! - [`JournalFrame::Batch`] — one all-or-nothing batch of [`Write`]s (the
//!   unit [`Store::apply`](wavedb_core::Store::apply) commits);
//! - [`JournalFrame::Commit`] — "journal `<ts>` is fully settled into
//!   `data.bin`": the retired journal's timestamp plus the head of the
//!   addressing chain. One frame, so the framing crc makes the
//!   whole commit atomic — a torn commit is ignored and the retired journal
//!   (still on disk) rules.
//!
//! Replay stops at the first frame whose length runs past EOF, whose crc
//! fails, or whose payload does not decode — a torn tail from a crash
//! mid-append — and truncates the file back to the last whole frame.
//!
//! [`Commit`]: JournalFrame::Commit

use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use wavedb_core::Write;
use wavedb_core::wire::{WaveWire, from_wire_checked, to_wire_checked};

use crate::error::{StorageError, StorageResult};

/// Per-frame prefix: `payload_len (u32)`.
const FRAME_PREFIX: usize = 4;

/// One journal frame — see the module docs for the two kinds.
#[derive(Debug, Clone, PartialEq, Eq, WaveWire)]
pub enum JournalFrame {
    /// One atomic batch of writes.
    Batch(Vec<Write>),
    /// The retirement record of an older journal.
    Commit(CommitFrame),
}

/// "Journal `journal_ts` is **done**" — its writes are settled into
/// `data.bin` — plus where to find the addressing state that makes those
/// pages findable.
///
/// The frame is a **pointer**, not the state: the descriptors themselves ride
/// the settle windows that changed them
/// ([RFC 0046](../../../rfcs/0046-directory-deltas-in-the-window.md)), and each
/// chunk names the one before it, so this needs to name only the newest
/// ([RFC 0048](../../../rfcs/0048-chained-addressing-log.md)).
///
/// **Fixed size.** It was a list of every chunk since the last snapshot, which
/// meant appending the whole list again at every checkpoint to name state the
/// previous frame already named — quadratic in the compaction interval, and so
/// a ceiling on that interval. Now it is 16 bytes whether the log holds one
/// chunk or a thousand.
///
/// Appended **after** the page window is synced: physical order is the
/// contract, and a frame is only meaningful once the blocks it addresses
/// are durable.
#[derive(Debug, Clone, PartialEq, Eq, WaveWire)]
pub struct CommitFrame {
    /// Timestamp of the journal this commit retires — the DONE marker.
    pub journal_ts: u64,
    /// Raw descriptor of the newest edit chunk; the walk goes back from here
    /// to the snapshot. `0` = no addressing log yet (a first-generation
    /// database whose directories are still empty).
    pub head: u64,
}

/// An append-only log of [`JournalFrame`]s in one timestamped file.
#[derive(Debug)]
pub struct Journal {
    file: File,
    path: PathBuf,
    ts: u64,
    /// Byte offset of the end of the last whole frame (next append lands here).
    end: u64,
    /// Barriers taken on this file (see [`syncs`](Self::syncs)).
    syncs: u64,
    /// When the last barrier was taken — the durability window is measured
    /// against this (RFC 0061). Monotonic, deliberately: a rewound wall clock
    /// would either stall the window forever or sync on every append.
    last_sync: Instant,
}

/// The journal files under `dir`, sorted by timestamp (oldest first).
///
/// # Errors
/// [`StorageError::Io`] on a directory read fault.
pub fn scan(dir: &Path) -> StorageResult<Vec<(u64, PathBuf)>> {
    let mut found = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if let Some(ts) = name
            .strip_prefix("journal_")
            .and_then(|r| r.strip_suffix(".log"))
            .and_then(|t| t.parse::<u64>().ok())
        {
            found.push((ts, path));
        }
    }
    found.sort_unstable_by_key(|(ts, _)| *ts);
    Ok(found)
}

impl Journal {
    /// Create a fresh journal `journal_<ts>.log` under `dir`.
    ///
    /// # Errors
    /// [`StorageError::Io`] on a filesystem failure.
    pub fn create(dir: &Path, ts: u64) -> StorageResult<Self> {
        let path = dir.join(format!("journal_{ts}.log"));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)?;
        Ok(Self {
            file,
            path,
            ts,
            end: 0,
            syncs: 0,
            last_sync: Instant::now(),
        })
    }

    /// Open an existing journal file. Call [`replay`](Self::replay) to read
    /// its frames (and truncate any torn tail) before appending.
    ///
    /// # Errors
    /// [`StorageError::Io`] on a filesystem failure.
    pub fn open(path: &Path, ts: u64) -> StorageResult<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let end = file.metadata()?.len();
        Ok(Self {
            file,
            path: path.to_path_buf(),
            ts,
            end,
            syncs: 0,
            last_sync: Instant::now(),
        })
    }

    /// This journal's timestamp (its identity in `Commit` frames).
    #[must_use]
    pub const fn ts(&self) -> u64 {
        self.ts
    }

    /// Total bytes of committed frames.
    #[must_use]
    pub const fn len_bytes(&self) -> u64 {
        self.end
    }

    /// Append one frame and `fsync`. Durable once this returns — **and so is
    /// every frame appended before it**, including any left deferred by
    /// [`append_deferred`](Self::append_deferred): `fsync` flushes the file,
    /// not one write.
    ///
    /// # Errors
    /// [`StorageError::Io`] if the write or sync fails.
    pub fn append(&mut self, frame: &JournalFrame) -> StorageResult<()> {
        self.write_frame(frame)?;
        self.sync() // the durability point
    }

    /// Append one frame **without** the barrier — the bytes are in the page
    /// cache, durable only once someone syncs this file.
    ///
    /// A checkpoint's [`Commit`](JournalFrame::Commit) rides this: it is a
    /// pointer into state `data.bin` already made durable, so it costs no
    /// barrier of its own and the next ordinary `append` carries it
    /// ([RFC 0046](../../../rfcs/0046-directory-deltas-in-the-window.md)).
    /// Until then the retired journal stays on disk and rules, so a crash in
    /// the window simply replays it.
    ///
    /// # Errors
    /// [`StorageError::Io`] if the write fails.
    pub fn append_deferred(
        &mut self,
        frame: &JournalFrame,
    ) -> StorageResult<()> {
        self.write_frame(frame)
    }

    /// `fsync` this journal, making every deferred append durable.
    ///
    /// # Errors
    /// [`StorageError::Io`] if the sync fails.
    pub fn sync(&mut self) -> StorageResult<()> {
        self.file.sync_all()?;
        self.syncs += 1;
        self.last_sync = Instant::now();
        Ok(())
    }

    /// Elapsed time since the last barrier — what a durability window is
    /// compared against (RFC 0061). Zero-window stores never ask.
    #[must_use]
    pub fn since_last_sync(&self) -> Duration {
        self.last_sync.elapsed()
    }

    /// Barriers this journal has taken — the accounting behind "a checkpoint
    /// costs one barrier", asserted in `page_store`'s tests.
    #[must_use]
    pub const fn barriers(&self) -> u64 {
        self.syncs
    }

    /// Write one framed payload at the tail, no barrier.
    fn write_frame(&mut self, frame: &JournalFrame) -> StorageResult<()> {
        let payload = to_wire_checked(frame);
        let mut buf = Vec::with_capacity(FRAME_PREFIX + payload.len());
        buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        buf.extend_from_slice(&payload);

        self.file.write_all_at(&buf, self.end)?;
        self.end += buf.len() as u64;
        Ok(())
    }

    /// Read every committed frame back, in append order.
    ///
    /// A torn tail (a frame whose length overruns EOF, or whose crc
    /// mismatches — the signature of a crash mid-append) is dropped and the
    /// file truncated to the last whole frame, so the next append writes
    /// clean.
    ///
    /// # Errors
    /// [`StorageError::Io`] on a read fault. Mid-log corruption surfaces as
    /// a truncated tail, not an error (an append-only log is unreliable past
    /// its first bad frame).
    pub fn replay(&mut self) -> StorageResult<Vec<JournalFrame>> {
        let mut buf = vec![0u8; self.end as usize];
        self.file.read_exact_at(&mut buf, 0)?;

        let mut frames = Vec::new();
        let mut pos = 0usize;
        let mut good_end = 0usize;
        while pos + FRAME_PREFIX <= buf.len() {
            let len = u32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap())
                as usize;
            let body_start = pos + FRAME_PREFIX;
            let Some(body_end) = body_start.checked_add(len) else {
                break; // absurd length — torn tail
            };
            if body_end > buf.len() {
                break; // frame overruns EOF — torn tail
            }
            // crc verification + decode in one step; any fault = torn tail.
            match from_wire_checked::<JournalFrame>(&buf[body_start..body_end])
            {
                Ok(frame) => frames.push(frame),
                Err(_) => break,
            }
            pos = body_end;
            good_end = body_end;
        }

        if (good_end as u64) < self.end {
            self.file.set_len(good_end as u64)?;
            self.file.sync_all()?;
            self.end = good_end as u64;
        }
        Ok(frames)
    }

    /// Delete this (retired) journal file.
    ///
    /// # Errors
    /// [`StorageError::Io`] if the unlink fails.
    pub fn delete(self) -> StorageResult<()> {
        std::fs::remove_file(&self.path)?;
        Ok(())
    }
}

/// Delete a retired journal by path (recovery cleanup of a
/// committed-but-undeleted file).
///
/// # Errors
/// [`StorageError::Io`] if the unlink fails.
pub fn delete_file(path: &Path) -> StorageResult<()> {
    std::fs::remove_file(path)?;
    Ok(())
}

/// A timestamp for a new journal: strictly greater than `after` (existing
/// files), wall-clock nanos otherwise.
#[must_use]
pub fn next_ts(after: u64) -> u64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX));
    now.max(after + 1)
}

/// The invariant check for an existing database: `data.bin` without any
/// journal means the recovery root was lost.
///
/// # Errors
/// [`StorageError::Corrupt`] when violated.
pub const fn require_journal_for(
    data_bin_existed: bool,
    journals: &[(u64, PathBuf)],
) -> StorageResult<()> {
    if data_bin_existed && journals.is_empty() {
        return Err(StorageError::Corrupt(
            "data.bin present but no journal — recovery root lost",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{Journal, JournalFrame, next_ts, require_journal_for, scan};
    use crate::error::StorageResult;
    use std::os::unix::fs::FileExt;
    use wavedb_core::{Id, U48, Write};

    fn id(key: u64) -> Id {
        Id::new(key, U48::from(1u32), false, key as u16)
    }

    /// The journal frames bytes; it never routes, so any hash serves.
    const SH: u64 = 0xABCD;

    fn batch(frames: &[Write]) -> JournalFrame {
        JournalFrame::Batch(frames.to_vec())
    }

    #[test]
    fn append_then_replay() -> StorageResult<()> {
        let d = tempfile::tempdir().unwrap();
        {
            let mut j = Journal::create(d.path(), 7)?;
            j.append(&batch(&[
                Write::Put(id(1), vec![1, 2, 3]),
                Write::Put(id(2), vec![4]),
            ]))?;
            j.append(&batch(&[Write::Remove(SH, id(1))]))?;
        }
        let (ts, path) = scan(d.path())?.pop().unwrap();
        assert_eq!(ts, 7);
        let mut j = Journal::open(&path, ts)?;
        let frames = j.replay()?;
        assert_eq!(
            frames,
            vec![
                batch(&[
                    Write::Put(id(1), vec![1, 2, 3]),
                    Write::Put(id(2), vec![4])
                ]),
                batch(&[Write::Remove(SH, id(1))]),
            ]
        );
        Ok(())
    }

    #[test]
    fn scan_sorts_and_next_ts_is_monotonic() -> StorageResult<()> {
        let d = tempfile::tempdir().unwrap();
        Journal::create(d.path(), 30)?;
        Journal::create(d.path(), 10)?;
        Journal::create(d.path(), 20)?;
        let found = scan(d.path())?;
        let ts: Vec<u64> = found.iter().map(|(t, _)| *t).collect();
        assert_eq!(ts, vec![10, 20, 30]);
        assert!(next_ts(30) > 30);
        assert_eq!(next_ts(u64::MAX - 1), u64::MAX);
        Ok(())
    }

    #[test]
    fn missing_journal_with_data_is_refused() {
        assert!(require_journal_for(true, &[]).is_err());
        assert!(require_journal_for(false, &[]).is_ok());
        assert!(
            require_journal_for(true, &[(1, std::path::PathBuf::new())])
                .is_ok()
        );
    }

    #[test]
    fn torn_tail_is_discarded_and_truncated() -> StorageResult<()> {
        let d = tempfile::tempdir().unwrap();
        {
            let mut j = Journal::create(d.path(), 1)?;
            j.append(&batch(&[Write::Put(id(1), vec![1, 2, 3])]))?;
            j.append(&batch(&[Write::Put(id(2), vec![4, 5, 6])]))?;
        }
        let (ts, path) = scan(d.path())?.pop().unwrap();
        let whole = std::fs::metadata(&path)?.len();
        // Simulate a crash mid-append: tack on a half-written frame.
        {
            let f = std::fs::OpenOptions::new().write(true).open(&path)?;
            f.write_all_at(&[0xFF; 5], whole)?; // garbage, shorter than a frame
        }
        let mut j = Journal::open(&path, ts)?;
        assert_eq!(j.replay()?.len(), 2, "torn tail must be ignored");
        // File truncated back to the last whole frame.
        assert_eq!(std::fs::metadata(&path)?.len(), whole);
        Ok(())
    }

    #[test]
    fn corrupt_frame_body_treated_as_tail() -> StorageResult<()> {
        let d = tempfile::tempdir().unwrap();
        {
            let mut j = Journal::create(d.path(), 1)?;
            j.append(&batch(&[Write::Put(id(1), vec![1, 2, 3])]))?;
        }
        let (ts, path) = scan(d.path())?.pop().unwrap();
        // Flip a byte in the payload → crc fails → whole frame dropped.
        {
            let f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)?;
            let len = std::fs::metadata(&path)?.len();
            let mut b = [0u8];
            f.read_exact_at(&mut b, len - 1)?;
            b[0] ^= 0xFF;
            f.write_all_at(&b, len - 1)?;
        }
        let mut j = Journal::open(&path, ts)?;
        let observed = j.replay()?;
        assert!(observed.is_empty(), "{observed:?}");
        Ok(())
    }

    #[test]
    fn delete_removes_the_file() -> StorageResult<()> {
        let d = tempfile::tempdir().unwrap();
        Journal::create(d.path(), 5)?;
        let (ts, path) = scan(d.path())?.pop().unwrap();
        Journal::open(&path, ts)?.delete()?;
        let observed = scan(d.path())?;
        assert!(observed.is_empty(), "{observed:?}");
        Ok(())
    }
}
