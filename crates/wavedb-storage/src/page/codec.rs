//! The stored form of a [`SlotPage`]: its envelope, its body, and the two
//! directions between them and the page in RAM.
//!
//! ```text
//! [ payload_len (u32 LE) ][ to_wire_checked(PageEnvelope) ]  … zero-padded to the run
//!                          └── [ crc32 ][ struct_hash · PagePayload ] ──┘
//! PagePayload::Raw ( to_wire(PageBody) )                               — stored plain
//! PagePayload::Zstd { dict_len, raw_len, zstd(to_wire(PageBody)) }     — dictionary-compressed
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
use super::{LEN_PREFIX, LEVEL, SlotPage};
use crate::dictionary::Dictionary;
use crate::error::{StorageError, StorageResult};

/// The stored envelope: everything needed to open the page, uncompressed.
#[derive(Debug, Clone, PartialEq, Eq, WaveWire)]
struct PageEnvelope {
    /// The `STRUCT_HASH` every record in this page shares — readable without
    /// the dictionary.
    struct_hash: u64,
    /// The body, stored plain or dictionary-compressed.
    payload: PagePayload,
}

/// How the page body is stored.
#[derive(Debug, Clone, PartialEq, Eq, WaveWire)]
enum PagePayload {
    /// `to_wire(PageBody)`, plain — compression off for this page's type, or
    /// zstd could not shrink the body.
    Raw(Vec<u8>),
    /// `to_wire(PageBody)` compressed against `dictionary[..dict_len]`.
    Zstd {
        /// The [`Dictionary`] state (prefix length) the body binds.
        dict_len: u32,
        /// Decompressed body length (the decompressor's exact capacity).
        raw_len: u32,
        /// The zstd frame.
        bytes: Vec<u8>,
    },
}

impl SlotPage {
    /// Serialise the page, framed `[len][checked envelope]`, first sealing
    /// its changes into its body, which it keeps: the page encoded is the
    /// page held. With `compress` the body is zstd'd against `dict`'s
    /// **latest** state — but stored `Raw` anyway when zstd cannot shrink
    /// it (a page never grows for having been "compressed"). Without
    /// `compress` (a hot page type opting out), zstd never runs.
    ///
    /// # Errors
    /// [`StorageError::Io`] if zstd fails (allocation-class faults only);
    /// [`StorageError::PageTooLarge`] past what the body can index.
    pub fn to_bytes(
        &mut self,
        dict: &Dictionary,
        compress: bool,
    ) -> StorageResult<Vec<u8>> {
        self.seal()?;
        let raw = &self.body;
        let payload = if compress {
            let zstd =
                zstd::bulk::Compressor::with_dictionary(LEVEL, dict.latest())?
                    .compress(raw)?;
            if zstd.len() < raw.len() {
                PagePayload::Zstd {
                    dict_len: dict.len() as u32,
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
    /// `Zstd` payload — decompress against the dictionary state the page
    /// was written under, into the one buffer the page keeps.
    ///
    /// # Errors
    /// [`StorageError::Corrupt`] on a truncated buffer, an out-of-range
    /// length, a crc mismatch, an undecodable envelope/body, or a `dict_len`
    /// stamping a state `dict` never reached.
    pub fn from_bytes(buf: &[u8], dict: &Dictionary) -> StorageResult<Self> {
        let env = envelope_bytes(buf)?;
        #[cfg(feature = "probe")]
        let mut lap = crate::probe::Lap::start();
        let envelope = view_wire_checked::<PageEnvelope>(env)
            .map_err(|_| StorageError::Corrupt("page envelope"))?;
        let body = match envelope.payload {
            PagePayloadRef::Raw(raw) => raw.to_vec(),
            PagePayloadRef::Zstd {
                dict_len,
                raw_len,
                bytes,
            } => {
                let state = dict.state(dict_len as usize).ok_or(
                    StorageError::Corrupt("page dictionary state unknown"),
                )?;
                #[cfg(feature = "probe")]
                lap.split(0);
                let mut d = zstd::bulk::Decompressor::with_dictionary(state)
                    .map_err(|_| StorageError::Corrupt("page decompress"))?;
                #[cfg(feature = "probe")]
                lap.split(1);
                d.decompress(bytes, raw_len as usize)
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
    use super::{PageBody, PageEnvelope, PagePayload, SlotPage};
    use crate::dictionary::Dictionary;
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
        SlotPage::from_bytes(&stored(body), &Dictionary::new())
    }

    /// Columns that disagree are refused when the page opens, so no read of
    /// it later can land out of range or out of order.
    #[test]
    fn columns_that_disagree_are_a_corrupt_body() {
        let body = |ids: Vec<u128>, ends: Vec<u32>, data: Vec<u8>| PageBody {
            ids,
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
}
