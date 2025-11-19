//! [`Dictionary`] — the per-`STRUCT_HASH` raw-content zstd dictionary.
//!
//! A page holds records of exactly one type, so each type gets one dictionary
//! with nothing foreign diluting it. zstd accepts **any bytes** as a
//! dictionary (no `ZDICT` trainer): this is simply a capped, **append-only**
//! buffer of record bytes sampled as writes settle.
//!
//! ## Versioning — a version is a prefix
//!
//! A page compressed against dictionary state X must be decompressed against
//! the exact same bytes. Because the buffer only ever appends, a state is
//! fully identified by its **length**: a page stamps the buffer length at
//! compression time (`dict_len`), and decompression binds `buf[..dict_len]`.
//! Every superseded version is a prefix of the same live buffer, so nothing
//! is ever freed or recompressed to stay readable.
//!
//! ## Storage & recovery
//!
//! The dictionary is stored in the data files like a page: a block run handed out
//! by the allocator, holding `[len (u32 LE)][to_wire_checked(buf)]`, repointed
//! by its [`Directory`](crate::directory::Directory) whenever the buffer
//! grows (which stops at the cap). On open the run the newest header names is
//! read back, so every `dict_len` a page stamps resolves against it.

use wavedb_core::wire::{from_wire_checked, to_wire_checked};

use crate::block::BlockDescriptor;
use crate::error::{StorageError, StorageResult};

/// Per-run prefix: `payload_len (u32)`.
const LEN_PREFIX: usize = 4;

/// Sampling stops once the buffer holds this many bytes. Sized like a typical
/// trained zstd dictionary: big enough to seed repetitive record prefixes,
/// small enough to sit in cache during (de)compression.
pub const DICT_CAP: usize = 64 * 1024;

/// A capped, append-only byte buffer used as a zstd dictionary.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Dictionary {
    buf: Vec<u8>,
}

impl Dictionary {
    /// An empty dictionary (compression starts dictionary-less and warms up).
    #[must_use]
    pub const fn new() -> Self {
        Self { buf: Vec::new() }
    }

    /// Sample a settling record's bytes into the buffer, until the cap.
    /// Append-only: bytes already sampled never change or move.
    pub fn sample(&mut self, record: &[u8]) {
        let room = DICT_CAP.saturating_sub(self.buf.len());
        if room == 0 {
            return;
        }
        let take = record.len().min(room);
        self.buf.extend_from_slice(&record[..take]);
    }

    /// The current state's identity: the buffer length a page stamps as its
    /// `dict_len` at compression time.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.buf.len()
    }

    /// `true` while nothing has been sampled.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// The exact dictionary bytes for state `len` — the prefix a page stamped
    /// with `dict_len == len` must decompress against. `None` if `len`
    /// overruns the buffer (a foreign or corrupt stamp).
    #[must_use]
    pub fn state(&self, len: usize) -> Option<&[u8]> {
        self.buf.get(..len)
    }

    /// The latest state, for compressing a page being written now.
    #[must_use]
    pub fn latest(&self) -> &[u8] {
        &self.buf
    }

    /// Serialise for the dictionary's block run: `[len][checked wire]` — the
    /// same framing every engine structure uses.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let payload = to_wire_checked(&self.buf);
        let mut out = Vec::with_capacity(LEN_PREFIX + payload.len());
        out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        out.extend_from_slice(&payload);
        out
    }

    /// Parse a dictionary back from its (zero-padded) block run.
    ///
    /// # Errors
    /// [`StorageError::Corrupt`] on a truncated buffer, an out-of-range
    /// length, a crc mismatch, or an undecodable body.
    pub fn from_bytes(buf: &[u8]) -> StorageResult<Self> {
        let len_bytes: [u8; LEN_PREFIX] = buf
            .get(..LEN_PREFIX)
            .and_then(|s| s.try_into().ok())
            .ok_or(StorageError::Corrupt("dictionary length truncated"))?;
        let len = u32::from_le_bytes(len_bytes) as usize;
        let end = LEN_PREFIX
            .checked_add(len)
            .filter(|&end| end <= buf.len())
            .ok_or(StorageError::Corrupt("dictionary length out of range"))?;
        let buf = from_wire_checked(&buf[LEN_PREFIX..end])
            .map_err(|_| StorageError::Corrupt("dictionary body"))?;
        Ok(Self { buf })
    }
}

/// One type's whole compression state.
///
/// The policy (on/off, fixed per type at compile time), the append-only
/// [`Dictionary`], and the block run it is persisted in. This is the third
/// slot of a type's `StructStorage` static — compression is per-type, so its
/// state lives with the type.
#[derive(Debug)]
pub struct DictState {
    enabled: bool,
    dict: Dictionary,
    desc: BlockDescriptor,
}

impl DictState {
    /// Fresh state. `enabled = false` never samples, never persists a run —
    /// that type's pages are always stored `Raw`.
    #[must_use]
    pub const fn new(enabled: bool) -> Self {
        Self {
            enabled,
            dict: Dictionary::new(),
            desc: BlockDescriptor::EMPTY,
        }
    }

    /// Whether this type's pages run through zstd at all.
    #[must_use]
    pub const fn enabled(&self) -> bool {
        self.enabled
    }

    /// The dictionary itself (what pages (de)compress against).
    #[must_use]
    pub const fn dictionary(&self) -> &Dictionary {
        &self.dict
    }

    /// The block run the dictionary is persisted in
    /// ([`BlockDescriptor::EMPTY`] while nothing has been sampled).
    #[must_use]
    pub const fn descriptor(&self) -> BlockDescriptor {
        self.desc
    }

    /// Warm the dictionary with a settling record (append-only, capped),
    /// reporting whether the buffer actually grew — a grown buffer must be
    /// re-persisted, which the settle does by placing [`image`](Self::image)
    /// in its window and then [`repoint`](Self::repoint)ing this state at it.
    /// A disabled state never samples and never allocates a run.
    pub(crate) fn sample(&mut self, record: &[u8]) -> bool {
        if !self.enabled {
            return false;
        }
        let before = self.dict.len();
        self.dict.sample(record);
        self.dict.len() != before
    }

    /// The buffer's current on-disk image (`[len][to_wire_checked(buf)]`).
    pub(crate) fn image(&self) -> Vec<u8> {
        self.dict.to_bytes()
    }

    /// Adopt the run the latest [`image`](Self::image) was written to,
    /// returning the superseded descriptor for the caller to free.
    pub(crate) const fn repoint(
        &mut self,
        desc: BlockDescriptor,
    ) -> BlockDescriptor {
        std::mem::replace(&mut self.desc, desc)
    }

    /// Adopt a persisted dictionary and the run it lives in — the open's
    /// path, where the buffer is read back from the run the headers name.
    pub(crate) fn load(&mut self, dict: Dictionary, desc: BlockDescriptor) {
        self.dict = dict;
        self.desc = desc;
    }
}

#[cfg(test)]
mod tests {
    use super::{DICT_CAP, DictState, Dictionary};
    use crate::block::{BlockDescriptor, Run};

    // Sampling reports growth so the settle knows to place a fresh image;
    // the image round-trips byte-identically and `repoint` hands back the run
    // to free. A disabled state never samples and never claims a run.
    #[test]
    fn sampling_reports_growth_and_disabled_is_noop() {
        let mut on = DictState::new(true);
        assert!(!on.descriptor().is_allocated());
        assert!(on.sample(&[0xAB; 100]), "a first sample grows the buffer");

        let stored = Dictionary::from_bytes(&on.image()).unwrap();
        assert_eq!(stored.latest(), &[0xAB; 100][..]);

        // Placing the image hands back the superseded descriptor.
        let first = BlockDescriptor::from_run(Run::new(7, 1));
        assert!(!on.repoint(first).is_allocated());
        assert_eq!(on.descriptor(), first);
        let second = BlockDescriptor::from_run(Run::new(9, 1));
        assert_eq!(on.repoint(second), first, "the old run comes back to free");

        let mut off = DictState::new(false);
        assert!(!off.sample(&[0xCD; 100]), "compression off ⇒ never grows");
        assert!(!off.descriptor().is_allocated());
        assert!(off.dictionary().is_empty());
    }

    #[test]
    fn samples_append_only_and_cap() {
        let mut d = Dictionary::new();
        assert!(d.is_empty());
        d.sample(b"alpha");
        d.sample(b"beta");
        assert_eq!(d.latest(), b"alphabeta");
        // A prior state stays byte-identical after growth.
        assert_eq!(d.state(5), Some(&b"alpha"[..]));
        // Beyond the buffer = unknown state.
        assert_eq!(d.state(100), None);

        // The cap truncates the sample that crosses it, then freezes.
        d.sample(&vec![7u8; DICT_CAP]);
        assert_eq!(d.len(), DICT_CAP);
        d.sample(b"more");
        assert_eq!(d.len(), DICT_CAP, "sampling past the cap must be a no-op");
        assert_eq!(&d.latest()[..9], b"alphabeta");
    }
}
