//! One type's record log ([RFC 0068]) — the bytes of its cached records, in
//! commit order, in chunks.
//!
//! An entry is `[Id u128 LE][len u32 LE][bytes]`; a remove is a **tombstone**,
//! `len = u32::MAX` and no bytes. A rewritten record is appended again and its
//! old copy stays where it was, dead, until its chunk goes.
//!
//! Chunks hold up to [`CHUNK`] bytes. The chunk being filled starts at
//! [`MIN_CHUNK`] and doubles as it fills, so a type that is barely written
//! holds a few hundred bytes rather than a whole chunk, and the room a chunk
//! has not used yet never exceeds what it already holds. A record larger
//! than a chunk gets one of its own size.
//!
//! Positions are **logical**: a byte count over every entry ever appended, so
//! a chunk starts where the previous one ended and three plain numbers say
//! everything about settledness:
//!
//! - `w` — where the next entry goes;
//! - `s` — how far the settle has reached: before it, every entry's page is
//!   written;
//! - `r` — `w` when a settle cycle last opened a batch: it owes a settle
//!   while `s < r`.
//!
//! [RFC 0068]: ../../../rfcs/0068-record-cache-as-a-log-WIP.md

use std::collections::VecDeque;
use std::mem::size_of;

use crate::record_index::Loc;

/// An entry's head: the id and the length.
const HEAD: usize = 20;
/// The first capacity of a chunk being filled.
const MIN_CHUNK: usize = 512;
/// A chunk is sealed once the next entry would take it past this.
pub const CHUNK: usize = 64 << 10;
/// The `len` that marks a tombstone.
const TOMBSTONE: u32 = u32::MAX;
/// The largest record a log can hold.
pub const MAX_RECORD: usize = (TOMBSTONE - 1) as usize;

/// One entry, borrowed from its chunk. `bytes` is `None` for a tombstone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Entry<'a> {
    pub id: u128,
    pub bytes: Option<&'a [u8]>,
}

#[derive(Debug)]
struct Chunk {
    /// Logical position of its first byte.
    start: u64,
    /// When it was started, on the store's clock — eviction's age.
    born: u64,
    bytes: Vec<u8>,
}

impl Chunk {
    const fn end(&self) -> u64 {
        self.start + self.bytes.len() as u64
    }
}

/// The entry at `at` in `bytes`, and its length.
fn parse(bytes: &[u8], at: usize) -> Option<(Entry<'_>, usize)> {
    let head = bytes.get(at..at + HEAD)?;
    let id = u128::from_le_bytes(head[..16].try_into().ok()?);
    let len = u32::from_le_bytes(head[16..].try_into().ok()?);
    if len == TOMBSTONE {
        return Some((Entry { id, bytes: None }, HEAD));
    }
    let len = len as usize;
    let body = bytes.get(at + HEAD..at + HEAD + len)?;
    Some((
        Entry {
            id,
            bytes: Some(body),
        },
        HEAD + len,
    ))
}

/// One type's log (see the module docs).
#[derive(Debug)]
pub struct TypeLog {
    /// The type's slot in its store, stamped into every [`Loc`].
    slot: u16,
    chunks: VecDeque<Chunk>,
    /// Sequence number of `chunks[0]`; a [`Loc`] carries it truncated.
    first: u64,
    w: u64,
    s: u64,
    r: u64,
}

impl TypeLog {
    pub const fn new(slot: u16) -> Self {
        Self {
            slot,
            chunks: VecDeque::new(),
            first: 0,
            w: 0,
            s: 0,
            r: 0,
        }
    }

    /// Append `id`'s bytes — `None` for a tombstone — and say where they
    /// went. `clock` stamps a chunk started for it, and ticks.
    ///
    /// `bytes` must not exceed [`MAX_RECORD`]; the commit path refuses such a
    /// record before anything is committed.
    pub fn append(
        &mut self,
        id: u128,
        bytes: Option<&[u8]>,
        clock: &mut u64,
    ) -> Loc {
        let len = HEAD + bytes.map_or(0, <[u8]>::len);
        let fits = self
            .chunks
            .back()
            .is_some_and(|c| c.bytes.len() + len <= CHUNK);
        if !fits {
            let cap = if len > CHUNK {
                len
            } else {
                len.next_power_of_two().max(MIN_CHUNK)
            };
            self.chunks.push_back(Chunk {
                start: self.w,
                born: *clock,
                bytes: Vec::with_capacity(cap),
            });
            *clock += 1;
        }
        let last = self.chunks.len() - 1;
        let seq = self.first + last as u64;
        let chunk = &mut self.chunks[last];
        let offset = chunk.bytes.len();
        let room = chunk.bytes.capacity() - offset;
        if room < len {
            // Double toward `CHUNK`; `offset + len <= CHUNK` here.
            let want = (offset + len).next_power_of_two().min(CHUNK);
            chunk.bytes.reserve_exact(want - offset);
        }
        chunk.bytes.extend_from_slice(&id.to_le_bytes());
        match bytes {
            Some(body) => {
                chunk
                    .bytes
                    .extend_from_slice(&(body.len() as u32).to_le_bytes());
                chunk.bytes.extend_from_slice(body);
            }
            None => chunk.bytes.extend_from_slice(&TOMBSTONE.to_le_bytes()),
        }
        self.w += len as u64;
        Loc {
            slot: self.slot,
            offset: offset as u16,
            chunk: seq as u32,
        }
    }

    /// The entry at `loc`, while its chunk is held.
    pub fn entry(&self, loc: Loc) -> Option<Entry<'_>> {
        let at = loc.chunk.wrapping_sub(self.first as u32) as usize;
        let chunk = self.chunks.get(at)?;
        parse(&chunk.bytes, loc.offset as usize).map(|(entry, _)| entry)
    }

    /// The entries from `s` up to `until`, each with its location and the
    /// position just past it.
    pub fn scan(&self, until: u64) -> Scan<'_> {
        let at = self.chunks.partition_point(|c| c.end() <= self.s);
        let offset = self
            .chunks
            .get(at)
            .map_or(0, |c| (self.s.saturating_sub(c.start)) as usize);
        Scan {
            log: self,
            at,
            offset,
            pos: self.s,
            until,
        }
    }

    pub const fn written(&self) -> u64 {
        self.w
    }

    pub const fn rotated(&self) -> u64 {
        self.r
    }

    /// Bytes from `s` to the end.
    pub const fn unsettled(&self) -> u64 {
        self.w - self.s
    }

    /// Whether anything from `s` on may still be owed to a page.
    pub const fn pending(&self) -> bool {
        self.s < self.w
    }

    /// Whether the open batch still owes a settle here.
    pub const fn owes(&self) -> bool {
        self.s < self.r
    }

    /// A batch opened: what is written so far is owed to it.
    pub const fn rotate(&mut self) {
        self.r = self.w;
    }

    /// A round's pages are written up to `to`.
    pub fn advance(&mut self, to: u64) {
        self.s = self.s.max(to).min(self.w);
    }

    /// The age of the oldest chunk, when it is wholly settled.
    pub fn oldest_settled(&self) -> Option<u64> {
        self.chunks
            .front()
            .filter(|c| c.end() <= self.s)
            .map(|c| c.born)
    }

    /// Drop the oldest chunk if it is wholly settled, telling `each` the
    /// location and id of every entry it held.
    pub fn drop_oldest(&mut self, mut each: impl FnMut(Loc, u128)) -> bool {
        if self.oldest_settled().is_none() {
            return false;
        }
        let Some(chunk) = self.chunks.pop_front() else {
            return false;
        };
        let seq = self.first as u32;
        self.first += 1;
        let mut at = 0;
        while let Some((entry, len)) = parse(&chunk.bytes, at) {
            let loc = Loc {
                slot: self.slot,
                offset: at as u16,
                chunk: seq,
            };
            each(loc, entry.id);
            at += len;
        }
        true
    }

    /// Bytes the log holds: every chunk's capacity, and the queue of them.
    pub fn footprint(&self) -> usize {
        self.chunks
            .iter()
            .map(|c| c.bytes.capacity())
            .sum::<usize>()
            + self.chunks.capacity() * size_of::<Chunk>()
    }
}

/// [`TypeLog::scan`]'s walk.
pub struct Scan<'a> {
    log: &'a TypeLog,
    at: usize,
    offset: usize,
    pos: u64,
    until: u64,
}

impl<'a> Iterator for Scan<'a> {
    type Item = (Loc, Entry<'a>, u64);

    fn next(&mut self) -> Option<Self::Item> {
        while self.pos < self.until {
            let chunk = self.log.chunks.get(self.at)?;
            let Some((entry, len)) = parse(&chunk.bytes, self.offset) else {
                self.at += 1;
                self.offset = 0;
                continue;
            };
            let loc = Loc {
                slot: self.log.slot,
                offset: self.offset as u16,
                chunk: (self.log.first + self.at as u64) as u32,
            };
            self.offset += len;
            self.pos += len as u64;
            return Some((loc, entry, self.pos));
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::{CHUNK, HEAD, MIN_CHUNK, TypeLog};

    fn filled(n: usize, size: usize) -> (TypeLog, u64) {
        let mut log = TypeLog::new(3);
        let mut clock = 0;
        for i in 0..n {
            log.append(i as u128, Some(&vec![i as u8; size]), &mut clock);
        }
        (log, clock)
    }

    #[test]
    fn an_entry_and_a_tombstone_read_back_where_they_were_put() {
        let mut log = TypeLog::new(3);
        let mut clock = 0;
        let put = log.append(7, Some(b"seven"), &mut clock);
        let gone = log.append(7, None, &mut clock);
        assert_eq!(put.slot, 3);
        let entry = log.entry(put).unwrap();
        assert_eq!((entry.id, entry.bytes), (7, Some(&b"seven"[..])));
        assert_eq!(log.entry(gone).unwrap().bytes, None);
        assert_eq!(log.written(), (HEAD * 2 + 5) as u64);
        assert!(log.pending());
    }

    /// The chunk being filled doubles from `MIN_CHUNK`, is sealed at
    /// `CHUNK`, and never holds more room than it has used.
    #[test]
    fn a_chunk_doubles_as_it_fills_and_is_sealed_at_its_cap() {
        let (log, clock) = filled(1, 10);
        assert_eq!(log.chunks[0].bytes.capacity(), MIN_CHUNK);
        assert_eq!(clock, 1);

        let (log, _) = filled(5_000, 100);
        assert!(log.chunks.len() > 1);
        for c in log.chunks.iter().take(log.chunks.len() - 1) {
            assert!(c.bytes.len() > CHUNK - 120, "sealed chunks are full");
            assert_eq!(c.bytes.capacity(), CHUNK);
        }
        let last = log.chunks.back().unwrap();
        assert!(last.bytes.capacity() <= (last.bytes.len() * 2).max(MIN_CHUNK));
    }

    #[test]
    fn a_record_larger_than_a_chunk_gets_its_own() {
        let mut log = TypeLog::new(0);
        let mut clock = 0;
        log.append(1, Some(b"small"), &mut clock);
        let big = vec![9u8; CHUNK * 2];
        let at = log.append(2, Some(&big), &mut clock);
        let next = log.append(3, Some(b"after"), &mut clock);
        assert_eq!((at.offset, at.chunk), (0, 1));
        assert_eq!(next.chunk, 2, "nothing shares the oversized chunk");
        assert_eq!(
            log.entry(at).unwrap().bytes.map(<[u8]>::len),
            Some(CHUNK * 2)
        );
    }

    /// A scan starts at `s`, crosses chunks, and stops at `until` — the
    /// shape a settle round takes.
    #[test]
    fn a_scan_walks_from_the_settle_cursor_to_its_limit() {
        let (mut log, _) = filled(2_000, 100);
        let all: Vec<_> = log
            .scan(log.written())
            .map(|(loc, e, next)| (loc, e.id, next))
            .collect();
        assert_eq!(all.len(), 2_000);
        let (_, _, mid) = all[1_000];
        log.advance(mid);
        let rest: Vec<u128> =
            log.scan(log.written()).map(|(_, e, _)| e.id).collect();
        assert_eq!(rest, (1_001..2_000).collect::<Vec<u128>>());
        let (_, _, limit) = all[1_010];
        assert_eq!(log.scan(limit).count(), 10);
        for (loc, entry, _) in log.scan(limit) {
            assert_eq!(log.entry(loc), Some(entry));
        }
    }

    /// Only chunks wholly before `s` may go, oldest first, and dropping one
    /// names every entry it held.
    #[test]
    fn only_wholly_settled_chunks_are_dropped() {
        let (mut log, _) = filled(2_000, 100);
        assert!(!log.drop_oldest(|_, _| {}), "nothing is settled yet");
        let all: Vec<_> = log
            .scan(log.written())
            .map(|(loc, e, next)| (loc, e.id, next))
            .collect();
        let (_, _, mid) = all[1_000];
        log.advance(mid);
        let mut dropped = Vec::new();
        while log.drop_oldest(|loc, id| dropped.push((loc, id))) {}
        assert_ne!(dropped.len(), 0);
        assert!(dropped.len() <= 1_001, "never past s");
        for (i, (loc, id)) in dropped.iter().enumerate() {
            assert_eq!((*id, *loc), (i as u128, all[i].0));
            assert_eq!(log.entry(*loc), None, "its chunk is gone");
        }
        let (survivor, _, _) = all[dropped.len()];
        assert!(log.entry(survivor).is_some());
        assert_eq!(log.scan(log.written()).count(), 999);
    }

    #[test]
    fn a_rotation_is_owed_until_the_settle_passes_it() {
        let (mut log, _) = filled(10, 10);
        assert!(!log.owes());
        log.rotate();
        assert!(log.owes());
        log.advance(log.rotated());
        assert!(!log.owes() && !log.pending());
        log.advance(u64::MAX);
        assert_eq!(log.scan(u64::MAX).count(), 0, "s never passes w");
    }
}
