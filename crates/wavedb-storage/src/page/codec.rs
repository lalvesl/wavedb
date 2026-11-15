//! The stored form of a [`SlotPage`]: its envelope, its body, and the two
//! directions between them and the page in RAM.
//!
//! ```text
//! [ payload_len (u32 LE) ][ to_wire_checked(PageEnvelope) ]  … zero-padded to the run
//!                          └── [ crc32 ][ struct_hash · PagePayload ] ──┘
//! PagePayload::Raw ( to_wire(PageBody) )                               — stored plain
//! PagePayload::Zstd { raw_len, zstd(to_wire(PageBody)) }               — compressed
//! PageBody { keys: [u8], key_width: u32, key_ends: [u32], ends: [u32], data: [u8] }
//! ```
//!
//! The body is **columns**: the keys ascending, back to back — every one
//! `key_width` long when they all are (an anchor's `Id`, 16 bytes), else each
//! ending where `key_ends` says — then where each record ends in `data`, and
//! the records back to back. Opened, it is one buffer the page keeps as it
//! is, viewed in O(1) (RFC 0076 — a list of numbers is not walked), and a
//! record is found by a binary search over the keys.

use wavedb_core::wire::{WaveWire, to_wire_checked, view_wire_checked};

use super::body::check;
use super::{Compression, LEN_PREFIX, SlotPage};
use crate::error::{StorageError, StorageResult};

/// The stored envelope: everything needed to open the page, uncompressed.
#[derive(Debug, Clone, PartialEq, Eq, WaveWire)]
struct PageEnvelope {
    /// The `STRUCT_HASH` every record in this page shares — readable without
    /// decompressing.
    struct_hash: u64,
    /// The body, stored plain or compressed.
    payload: PagePayload,
}

/// How the page body is stored.
#[derive(Debug, Clone, PartialEq, Eq, WaveWire)]
enum PagePayload {
    /// `to_wire(PageBody)`, plain — compression off for this page's type, or
    /// zstd could not shrink the body.
    Raw(Vec<u8>),
    /// `to_wire(PageBody)`, zstd'd — with no dictionary (RFC 0081 §9).
    Zstd {
        /// Decompressed body length (the decompressor's exact capacity).
        raw_len: u32,
        /// The zstd frame.
        bytes: Vec<u8>,
    },
}

impl SlotPage {
    /// Serialise the page, framed `[len][checked envelope]`, first sealing
    /// its changes into its body, which it keeps: the page encoded is the
    /// page held. With `compress` the body is zstd'd — but stored `Raw`
    /// anyway when zstd cannot shrink it (a page never grows for having
    /// been "compressed"). Without `compress` (a hot page type opting out),
    /// zstd never runs.
    ///
    /// # Errors
    /// [`StorageError::Io`] if zstd fails (allocation-class faults only);
    /// [`StorageError::PageTooLarge`] past what the body can index.
    pub fn to_bytes(&mut self, compress: bool) -> StorageResult<Vec<u8>> {
        self.encode(Compression::of(compress))
    }

    /// [`to_bytes`](Self::to_bytes) at the level `compression` names: the
    /// write path's, or the cleanup's.
    ///
    /// # Errors
    /// As [`to_bytes`](Self::to_bytes).
    pub fn encode(
        &mut self,
        compression: Compression,
    ) -> StorageResult<Vec<u8>> {
        self.seal()?;
        let raw = &self.body;
        let payload = if let Some(level) = compression.level() {
            let zstd = zstd::bulk::compress(raw, level)?;
            if zstd.len() < raw.len() {
                PagePayload::Zstd {
                    raw_len: raw.len() as u32,
                    bytes: zstd,
                }
            } else {
                PagePayload::Raw(raw.to_vec())
            }
        } else {
            PagePayload::Raw(raw.to_vec())
        };

        let payload = to_wire_checked(&PageEnvelope {
            struct_hash: self.struct_hash,
            payload,
        });
        let mut out = Vec::with_capacity(LEN_PREFIX + payload.len());
        out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        out.extend_from_slice(&payload);
        Ok(out)
    }

    /// Whether `buf` holds a whole page: its length prefix in range and its
    /// envelope's crc right — what a reconstruction checks of the batch a
    /// crash may have cut (RFC 0072 §7), without decompressing it.
    #[must_use]
    pub fn intact(buf: &[u8]) -> bool {
        envelope_bytes(buf)
            .is_ok_and(|env| view_wire_checked::<PageEnvelope>(env).is_ok())
    }

    /// Parse a page from bytes (typically a whole zero-padded run): length
    /// prefix, crc + envelope view through the checked codec, then — for a
    /// `Zstd` payload — decompress, into the one buffer the page keeps.
    ///
    /// # Errors
    /// [`StorageError::Corrupt`] on a truncated buffer, an out-of-range
    /// length, a crc mismatch, or an undecodable envelope or body.
    pub fn from_bytes(buf: &[u8]) -> StorageResult<Self> {
        let env = envelope_bytes(buf)?;
        #[cfg(feature = "probe")]
        let mut lap = crate::probe::Lap::start();
        let envelope = view_wire_checked::<PageEnvelope>(env)
            .map_err(|_| StorageError::Corrupt("page envelope"))?;
        let body = match envelope.payload {
            PagePayloadRef::Raw(raw) => raw.to_vec(),
            PagePayloadRef::Zstd { raw_len, bytes } => {
                // The probe's parts as they were: no dictionary to find, no
                // decompressor to load it into.
                #[cfg(feature = "probe")]
                {
                    lap.split(0);
                    lap.split(1);
                }
                zstd::bulk::decompress(bytes, raw_len as usize)
                    .map_err(|_| StorageError::Corrupt("page decompress"))?
            }
        };
        #[cfg(feature = "probe")]
        lap.split(2);
        let (cuts, len) = check(&body)?;
        #[cfg(feature = "probe")]
        lap.split(3);
        let page = Self::opened(envelope.struct_hash, body.into(), cuts, len);
        #[cfg(feature = "probe")]
        {
            lap.split(4);
            crate::probe::decode_parts(&lap);
        }
        Ok(page)
    }
}

/// The checked envelope's bytes inside a stored page: past the length
/// prefix, and only as long as it says.
fn envelope_bytes(buf: &[u8]) -> StorageResult<&[u8]> {
    let len_bytes: [u8; LEN_PREFIX] = buf
        .get(..LEN_PREFIX)
        .and_then(|s| s.try_into().ok())
        .ok_or(StorageError::Corrupt("page length truncated"))?;
    let len = u32::from_le_bytes(len_bytes) as usize;
    LEN_PREFIX
        .checked_add(len)
        .and_then(|end| buf.get(LEN_PREFIX..end))
        .ok_or(StorageError::Corrupt("page length out of range"))
}

#[cfg(test)]
mod tests {
    use super::super::body::PageBody;
    use super::{PageEnvelope, PagePayload, SlotPage};

    use crate::error::StorageError;
    use wavedb_core::wire::{to_wire, to_wire_checked};

    /// A stored page around `body`, plain.
    fn stored(body: &PageBody) -> Vec<u8> {
        let env = to_wire_checked(&PageEnvelope {
            struct_hash: 1,
            payload: PagePayload::Raw(to_wire(body)),
        });
        let mut out = (env.len() as u32).to_le_bytes().to_vec();
        out.extend(env);
        out
    }

    fn opened(body: &PageBody) -> Result<SlotPage, StorageError> {
        SlotPage::from_bytes(&stored(body))
    }

    /// Columns that disagree are refused when the page opens, so no read of
    /// it later can land out of range or out of order.
    #[test]
    fn columns_that_disagree_are_a_corrupt_body() {
        // Ids as 16-byte keys, every one as long: the width form.
        let body = |ids: Vec<u128>, ends: Vec<u32>, data: Vec<u8>| PageBody {
            keys: ids.iter().flat_map(|i| i.to_be_bytes()).collect(),
            key_width: 16,
            key_ends: Vec::new(),
            ends,
            data,
        };
        let bad = [
            body(vec![2, 1], vec![1, 2], vec![0, 0]), // ids out of order
            body(vec![1, 1], vec![1, 2], vec![0, 0]), // an id twice
            body(vec![1, 2], vec![1], vec![0, 0]),    // an end missing
            body(vec![1, 2], vec![2, 1], vec![0, 0]), // an end going back
            body(vec![1], vec![3], vec![0, 0]),       // past the data
            body(vec![1], vec![1], vec![0, 0]),       // data left over
        ];
        for b in &bad {
            assert!(
                matches!(opened(b), Err(StorageError::Corrupt("page body"))),
                "{b:?}"
            );
        }
        let good = opened(&body(vec![1, 5], vec![1, 1], vec![9])).unwrap();
        assert_eq!(good.len(), 2);
    }

    /// Keys of their own lengths are cut by their ends, which must cover
    /// the key bytes exactly and keep the keys ascending.
    #[test]
    fn keys_of_many_lengths_are_cut_by_their_ends() {
        let body = |keys: &[u8], key_ends: Vec<u32>| PageBody {
            keys: keys.to_vec(),
            key_width: 0,
            key_ends,
            ends: vec![0, 0],
            data: Vec::new(),
        };
        let good = opened(&body(b"aab", vec![1, 3])).unwrap();
        assert_eq!(
            good.iter().map(|(k, _)| k.to_vec()).collect::<Vec<_>>(),
            [b"a".to_vec(), b"ab".to_vec()]
        );
        for bad in [
            body(b"aab", vec![1, 2]), // a key byte left over
            body(b"aba", vec![2, 3]), // "ab" before "a"
            body(b"aab", vec![2, 1]), // an end going back
        ] {
            assert!(
                matches!(opened(&bad), Err(StorageError::Corrupt("page body"))),
                "{bad:?}"
            );
        }
    }
}
