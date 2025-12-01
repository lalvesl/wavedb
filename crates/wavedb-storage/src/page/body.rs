//! A page body's columns ([`super::codec`] holds the envelope around it):
//! the keys, back to back — every one `key_width` long when they all are,
//! else each ending where `key_ends` says — where each record ends, and the
//! records, found once in the page's buffer and read in place.

use wavedb_core::wire::{WaveWire, to_wire, view_wire};

use crate::error::{StorageError, StorageResult};

/// The records, as columns.
#[derive(Debug, Clone, PartialEq, Eq, WaveWire)]
pub(super) struct PageBody {
    /// Every record's key, back to back, strictly ascending.
    pub(super) keys: Vec<u8>,
    /// How long every key is, when they all are; `0` when they are not, and
    /// `key_ends` cuts them.
    pub(super) key_width: u32,
    /// Where key `i` ends in `keys` — empty under a `key_width`.
    pub(super) key_ends: Vec<u32>,
    /// Where record `i` ends in `data`; it starts where record `i - 1`
    /// ended (or at 0). Non-decreasing, the last one `data`'s length.
    pub(super) ends: Vec<u32>,
    /// The records' bytes, back to back in id order.
    pub(super) data: Vec<u8>,
}

/// The width of one end.
const END: usize = size_of::<u32>();

/// Where a body's three columns sit in it, `(start, end)` each, found by
/// viewing the body once when the page takes it: a read then slices the
/// page's buffer and views nothing (a view of the body per read cost more
/// than the binary search it served).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct Cuts {
    keys: (usize, usize),
    key_width: usize,
    key_ends: (usize, usize),
    ends: (usize, usize),
    data: (usize, usize),
}

impl Cuts {
    /// Where `body`'s columns are; `None` unless it views as a body.
    pub fn of(body: &[u8]) -> Option<Self> {
        let r = view_wire::<PageBody>(body).ok()?;
        let at = |column: &[u8]| {
            let start = column.as_ptr().addr() - body.as_ptr().addr();
            (start, start + column.len())
        };
        Some(Self {
            keys: at(r.keys),
            key_width: r.key_width as usize,
            key_ends: at(r.key_ends.bytes()),
            ends: at(r.ends.bytes()),
            data: at(r.data),
        })
    }
}

/// An opened body: its columns over the page's buffer, the numbers read in
/// place.
#[derive(Clone, Copy)]
pub(super) struct Columns<'p> {
    keys: &'p [u8],
    key_width: usize,
    key_ends: &'p [[u8; END]],
    ends: &'p [[u8; END]],
    data: &'p [u8],
    len: usize,
}

impl<'p> Columns<'p> {
    /// The columns of `body` at `cuts` — the body's own cuts, or the default
    /// ones of an empty page. `None` when they do not fit it.
    pub fn at(body: &'p [u8], cuts: Cuts) -> Option<Self> {
        let column = |(start, end)| body.get(start..end);
        let keys = column(cuts.keys)?;
        let (key_ends, []) = column(cuts.key_ends)?.as_chunks::<END>() else {
            return None;
        };
        let (ends, []) = column(cuts.ends)?.as_chunks::<END>() else {
            return None;
        };
        let len = match cuts.key_width {
            0 => key_ends.len(),
            width => keys.len() / width,
        };
        Some(Self {
            keys,
            key_width: cuts.key_width,
            key_ends,
            ends,
            data: column(cuts.data)?,
            len,
        })
    }

    /// Records held.
    pub const fn len(&self) -> usize {
        self.len
    }

    /// Record `i`'s key.
    pub fn key(&self, i: usize) -> Option<&'p [u8]> {
        if i >= self.len {
            return None;
        }
        let (start, end) = match self.key_width {
            0 => {
                let end = |i: usize| {
                    self.key_ends
                        .get(i)
                        .map(|b| u32::from_le_bytes(*b) as usize)
                };
                let start = match i {
                    0 => 0,
                    _ => end(i - 1)?,
                };
                (start, end(i)?)
            }
            width => (i * width, (i + 1) * width),
        };
        self.keys.get(start..end)
    }

    fn end(&self, i: usize) -> Option<usize> {
        self.ends.get(i).map(|b| u32::from_le_bytes(*b) as usize)
    }

    /// Record `i`'s bytes.
    pub fn record(&self, i: usize) -> Option<&'p [u8]> {
        let start = match i {
            0 => 0,
            _ => self.end(i - 1)?,
        };
        self.data.get(start..self.end(i)?)
    }

    /// Where `key` is among the keys: `Ok(i)` at record `i`, `Err(i)` where
    /// it would go.
    pub fn find(&self, key: &[u8]) -> Result<usize, usize> {
        let (mut lo, mut hi) = (0, self.len);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match self.key(mid).unwrap_or_default().cmp(key) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return Ok(mid),
            }
        }
        Err(lo)
    }
}

/// The encoded body of `entries`, which come in ascending key order.
///
/// # Errors
/// [`StorageError::PageTooLarge`] once the keys or the records pass what a
/// `u32` end can say.
pub(super) fn body_of<'e>(
    entries: impl Iterator<Item = (&'e [u8], &'e [u8])>,
    count: usize,
    bytes: usize,
) -> StorageResult<Vec<u8>> {
    let mut body = PageBody {
        keys: Vec::with_capacity(count * 16),
        key_width: 0,
        key_ends: Vec::with_capacity(count),
        ends: Vec::with_capacity(count),
        data: Vec::with_capacity(bytes),
    };
    let end_of = |len: usize| {
        u32::try_from(len).map_err(|_| StorageError::PageTooLarge(len as u64))
    };
    for (key, record) in entries {
        body.keys.extend_from_slice(key);
        body.key_ends.push(end_of(body.keys.len())?);
        body.data.extend_from_slice(record);
        body.ends.push(end_of(body.data.len())?);
    }
    // One width for every key, when there is one: no column of key ends.
    let width = body.key_ends.first().copied().unwrap_or(0);
    let uniform =
        body.key_ends.iter().enumerate().all(|(i, &end)| {
            u64::from(end) == (i as u64 + 1) * u64::from(width)
        });
    if width > 0 && uniform {
        body.key_width = width;
        body.key_ends = Vec::new();
    }
    Ok(to_wire(&body))
}

/// Check a body's columns agree — the keys cut whole, as many ends as
/// keys, the keys strictly ascending, the ends never going back nor past
/// the data — so reading it afterwards cannot go wrong. Returns its cuts
/// and its record count.
pub(super) fn check(body: &[u8]) -> StorageResult<(Cuts, usize)> {
    let bad = || StorageError::Corrupt("page body");
    let cuts = Cuts::of(body).ok_or_else(bad)?;
    let cols = Columns::at(body, cuts).ok_or_else(bad)?;
    let len = cols.len();
    let keys_whole = match cols.key_width {
        0 => {
            let ends = cols.key_ends;
            let last = ends.last().map_or(0, |b| u32::from_le_bytes(*b));
            last as usize == cols.keys.len()
                && ends.windows(2).all(|w| {
                    u32::from_le_bytes(w[0]) <= u32::from_le_bytes(w[1])
                })
        }
        width => cols.key_ends.is_empty() && cols.keys.len() % width == 0,
    };
    let ascending =
        keys_whole && (1..len).all(|i| cols.key(i - 1) < cols.key(i));
    let ordered = (1..len).all(|i| cols.end(i - 1) <= cols.end(i));
    let last = len.checked_sub(1).and_then(|i| cols.end(i)).unwrap_or(0);
    if cols.ends.len() != len
        || !ascending
        || !ordered
        || last != cols.data.len()
    {
        return Err(bad());
    }
    Ok((cuts, len))
}

/// The cuts of a body this crate just encoded.
///
/// # Errors
/// [`StorageError::Corrupt`] should it not view as one — it always does.
pub(super) fn cuts_of(body: &[u8]) -> StorageResult<Cuts> {
    Cuts::of(body).ok_or(StorageError::Corrupt("page body"))
}
