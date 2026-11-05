//! Where a page over target is cut ([RFC 0078] §4) — split from
//! [`crate::shape`], which cuts: at the edge between two collections
//! nearest its middle, when it holds more than one, so a collection that
//! spans pages owns them; else at its middle, or at the quantum boundary
//! nearest it.
//!
//! [RFC 0078]: ../../../rfcs/0078-ordered-lanes-over-a-quantized-page-index-WIP.md

use crate::error::{StorageError, StorageResult};
use crate::ordered::{Cut, CutOutcome, Found};
use crate::page::SlotPage;
use crate::pages::Pages;

/// Cut the page whose first key is `first` and whose keys are `keys`: at
/// the edge between two collections nearest its middle, when it holds more
/// than one ([RFC 0078] §4); else at its middle, or at the quantum boundary
/// nearest it. The index where the right half starts; `None` when the keys
/// cannot part.
///
/// [RFC 0078]: ../../../rfcs/0078-ordered-lanes-over-a-quantized-page-index-WIP.md
pub fn cut_at(
    index: &mut Pages,
    first: &[u8],
    keys: &[Vec<u8>],
) -> StorageResult<Option<usize>> {
    let mid = keys.len() / 2;
    let mut edges: Vec<usize> = (1..keys.len())
        .filter(|&i| {
            let (a, b) = (collection(&keys[i - 1]), collection(&keys[i]));
            a.is_some() && b.is_some() && a != b
        })
        .collect();
    edges.sort_by_key(|&i| i.abs_diff(mid));
    for i in edges {
        if let Some(i) = try_cut(index, first, keys, i)? {
            return Ok(Some(i));
        }
    }
    cut_near(index, first, keys)
}

/// The collection a lane key belongs to: a chain's prefix, the owner's
/// `Id` and the declaration. An anchor, shorter, belongs to none.
pub fn collection(key: &[u8]) -> Option<&[u8]> {
    key.get(..wavedb_core::index::PREFIX)
}

/// Cut the page between `keys[i - 1]` and `keys[i]`, if a quantum
/// boundary — refined to, when the whole page lies in one quantum — falls
/// there.
fn try_cut(
    index: &mut Pages,
    first: &[u8],
    keys: &[Vec<u8>],
    i: usize,
) -> StorageResult<Option<usize>> {
    let Found::Page { at, .. } = index.find(first) else {
        return Err(StorageError::Corrupt("a page to cut lost its entry"));
    };
    let right = index
        .placeholder_at(at, 0)
        .ok_or(StorageError::Corrupt("a page without a range"))?;
    let cut = Cut {
        first: &keys[0],
        left_last: &keys[i - 1],
        right_first: &keys[i],
        last: &keys[keys.len() - 1],
    };
    Ok(matches!(index.cut(at, &cut, right), CutOutcome::Cut(_)).then_some(i))
}

/// [`cut_at`] for a page of one collection: at its middle, or at the
/// quantum boundary nearest it.
fn cut_near(
    index: &mut Pages,
    first: &[u8],
    keys: &[Vec<u8>],
) -> StorageResult<Option<usize>> {
    let Found::Page { at, .. } = index.find(first) else {
        return Err(StorageError::Corrupt("a page to cut lost its entry"));
    };
    let last = &keys[keys.len() - 1];
    let cut = |i: usize| Cut {
        first: &keys[0],
        left_last: &keys[i - 1],
        right_first: &keys[i],
        last,
    };
    let stand_in = |index: &Pages, at| {
        index
            .placeholder_at(at, 0)
            .ok_or(StorageError::Corrupt("a page without a range"))
    };
    let mid = keys.len() / 2;
    let right = stand_in(index, at)?;
    match index.cut(at, &cut(mid), right) {
        CutOutcome::Cut(_) => Ok(Some(mid)),
        CutOutcome::Inseparable => Ok(None),
        CutOutcome::Elsewhere(at) => {
            let q = |k: &[u8]| index.quantum_of(at, k);
            let nearest = (1..keys.len())
                .filter(|&i| q(&keys[i - 1]) < q(&keys[i]))
                .min_by_key(|&i| i.abs_diff(mid));
            let Some(i) = nearest else { return Ok(None) };
            let right = stand_in(index, at)?;
            match index.cut(at, &cut(i), right) {
                CutOutcome::Cut(_) => Ok(Some(i)),
                _ => Ok(None),
            }
        }
    }
}

/// `page` in two: the records before `from`, and the rest.
pub fn halves(page: &SlotPage, from: &[u8]) -> (SlotPage, SlotPage) {
    let hash = page.struct_hash();
    let (mut left, mut right) = (SlotPage::new(hash), SlotPage::new(hash));
    for (key, bytes) in page.iter() {
        let half = if key < from { &mut left } else { &mut right };
        half.upsert(key, bytes.to_vec());
    }
    (left, right)
}
