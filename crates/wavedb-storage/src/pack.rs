//! A pack: the small pages of several slots in one run ([RFC 0081] §1, the
//! commit block's sections).
//!
//! A round touching a few lanes writes a page of each, most of them a
//! fraction of a block — a fragment of a lane's appends, a short page — and
//! each took a block of its own. A pack holds them together: each **section**
//! one slot's page image, whole, its own envelope and crc, and every entry
//! naming one of them names the pack's run. A slot has at most one section
//! in a pack, so an entry's slot — its index's — finds its section.
//!
//! On disk: where a page starts with its length, a pack starts with
//! [`TAG`], a length no page can have, then its length and the checked wire
//! of its sections. Every reader of a page run reads it through
//! [`page_image`], which hands a page's bytes on as they are and a pack's
//! section for the slot asking.
//!
//! A run is named by every entry of its sections: the store counts them
//! (`crate::shares`).
//!
//! [RFC 0081]: ../../../rfcs/0081-commit-blocks-slices-and-the-cleaner-PLANNED.md

use std::borrow::Cow;

use wavedb_core::wire::{WaveWire, from_wire_checked, to_wire_checked};

use crate::error::{StorageError, StorageResult};
use crate::page::SlotPage;

/// Where a page's length is, a pack's mark: no page is that long.
pub const TAG: u32 = u32::MAX;

/// Bytes before the checked sections: the tag and their length.
const FRAME: usize = 8;

/// One slot's page in a pack.
#[derive(Debug, Clone, PartialEq, Eq, WaveWire)]
struct Section {
    struct_hash: u64,
    image: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, WaveWire)]
struct Pack {
    sections: Vec<Section>,
}

/// The pack holding `sections` — each a slot's page image, one per slot.
#[must_use]
pub fn encode(sections: Vec<(u64, Vec<u8>)>) -> Vec<u8> {
    let pack = Pack {
        sections: sections
            .into_iter()
            .map(|(struct_hash, image)| Section { struct_hash, image })
            .collect(),
    };
    let wire = to_wire_checked(&pack);
    let mut out = Vec::with_capacity(FRAME + wire.len());
    out.extend_from_slice(&TAG.to_le_bytes());
    out.extend_from_slice(&(wire.len() as u32).to_le_bytes());
    out.extend_from_slice(&wire);
    out
}

/// Bytes [`encode`] makes of sections of `images` bytes in all, `count` of
/// them — to choose what a round packs before packing it.
#[must_use]
pub fn encoded_len(count: usize, images: usize) -> usize {
    let empty = encode(Vec::new()).len();
    let per = encode(vec![(0, Vec::new())]).len() - empty;
    empty + count * per + images
}

/// Whether `image` — a run's bytes — is a pack.
#[must_use]
pub fn is_pack(image: &[u8]) -> bool {
    image.get(..4) == Some(&TAG.to_le_bytes()[..])
}

/// The sections of the pack `image`.
fn sections(image: &[u8]) -> StorageResult<Vec<Section>> {
    let len = image
        .get(4..FRAME)
        .and_then(|s| <[u8; 4]>::try_from(s).ok())
        .map(|b| u32::from_le_bytes(b) as usize)
        .ok_or(StorageError::Corrupt("a pack's length truncated"))?;
    let wire = FRAME
        .checked_add(len)
        .and_then(|end| image.get(FRAME..end))
        .ok_or(StorageError::Corrupt("a pack's length out of range"))?;
    from_wire_checked::<Pack>(wire)
        .map(|p| p.sections)
        .map_err(|_| StorageError::Corrupt("a pack's sections"))
}

/// The page image a run's bytes hold for the slot `struct_hash`: the bytes
/// themselves, or — a pack — its section.
///
/// # Errors
/// [`StorageError::Corrupt`] for a pack that fails its checks, or holds no
/// section of that slot.
pub fn page_image(
    image: &[u8],
    struct_hash: u64,
) -> StorageResult<Cow<'_, [u8]>> {
    if !is_pack(image) {
        return Ok(Cow::Borrowed(image));
    }
    sections(image)?
        .into_iter()
        .find(|s| s.struct_hash == struct_hash)
        .map(|s| Cow::Owned(s.image))
        .ok_or(StorageError::Corrupt("a pack without the slot's section"))
}

/// Whether the run's bytes `image` are whole: a page's envelope, or a
/// pack's and every section's (`SlotPage::intact`).
#[must_use]
pub fn intact(image: &[u8]) -> bool {
    if !is_pack(image) {
        return SlotPage::intact(image);
    }
    sections(image).is_ok_and(|s| s.iter().all(|s| SlotPage::intact(&s.image)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn page(hash: u64, keys: &[u8]) -> Vec<u8> {
        let mut p = SlotPage::new(hash);
        for &k in keys {
            p.upsert(&[k], vec![k; 40]);
        }
        p.to_bytes(false).unwrap()
    }

    #[test]
    fn a_pack_gives_each_slot_its_section() {
        let (a, b) = (page(1, &[1, 2]), page(2, &[3]));
        let mut run = encode(vec![(1, a.clone()), (2, b.clone())]);
        assert_eq!(run.len(), encoded_len(2, a.len() + b.len()));
        // A run reads back zero-padded to its blocks.
        run.resize(4096, 0);
        assert!(is_pack(&run) && intact(&run));
        assert_eq!(page_image(&run, 1).unwrap(), a);
        assert_eq!(page_image(&run, 2).unwrap(), b);
        assert!(matches!(
            page_image(&run, 3),
            Err(StorageError::Corrupt("a pack without the slot's section"))
        ));
        let read = SlotPage::from_bytes(&page_image(&run, 2).unwrap());
        assert_eq!(read.unwrap().len(), 1);
    }

    #[test]
    fn a_page_reads_as_itself() {
        let a = page(1, &[1]);
        assert!(!is_pack(&a));
        assert_eq!(page_image(&a, 99).unwrap(), a);
    }

    #[test]
    fn a_torn_pack_is_not_intact() {
        let run = encode(vec![(1, page(1, &[1])), (2, page(2, &[2]))]);
        let mut torn = run.clone();
        let at = torn.len() - 3;
        torn[at] ^= 0xFF;
        assert!(!intact(&torn));
        assert!(page_image(&torn, 1).is_err());
        assert!(!intact(&run[..run.len() - 1]));
    }
}
