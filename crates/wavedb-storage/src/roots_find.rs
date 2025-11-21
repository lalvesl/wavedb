//! Where a reconstruction roots ([RFC 0073] §6, [RFC 0072] §6–§7) — split
//! from [`crate::reopen`], which walks from there.
//!
//! 1. **The newest roots block**: the newest data file ends with one, or
//!    with a header naming the newest one written before it — each only
//!    where it says it is, its crc holding. After a crash the end may be
//!    torn: the blocks before it are read back, a chunk at a time, until
//!    one is.
//! 2. **Whether the storage write it ends is whole**: for every data shard
//!    whose root it moved — the others' were checked when the block before
//!    was the newest — the batch that root ends is whole: every header back
//!    to the one ending the batch before present, and every page and
//!    dictionary it placed intact. One write reaches the disk in any order,
//!    so a crash before its `fsync` can keep a header and lose a page; only
//!    the newest storage write can be in that state (the next one is
//!    written once it is durable), so one that is not whole gives way to
//!    the roots block before it, which is durable.
//!
//! [RFC 0072]: ../../../rfcs/0072-the-settle-is-the-durability-point.md
//! [RFC 0073]: ../../../rfcs/0073-data-shards-and-storage-shards.md

use crate::block::{BLOCK_SIZE, BlockDescriptor, Run};
use crate::block_file::BlockFile;
use crate::dictionary::Dictionary;
use crate::error::StorageResult;
use crate::header::{Header, NO_FILE};
use crate::page::SlotPage;
use crate::remote::HEADER_BLOCKS;
use crate::reopen::DiskHeaders;
use crate::roots::{Mark, ROOTS_BLOCKS, Roots};

/// Blocks read at a time while looking for a header behind a torn end.
const SCAN_BLOCKS: u64 = 256;

/// The newest roots block whose storage write is whole for every one of
/// the `per` data shards it names (see the module docs) — `None` when
/// nothing is durable yet.
///
/// # Errors
/// Read faults.
pub fn find_roots(
    headers: &mut DiskHeaders<'_>,
    per: usize,
) -> StorageResult<Option<Roots>> {
    let mut limit = u64::MAX;
    loop {
        let Some(mark) = newest_below(headers.file, limit)? else {
            return Ok(None);
        };
        limit = match &mark {
            Mark::Header(h) => h.at,
            Mark::Roots(r) => r.at,
        };
        let mut at = mark.roots_at();
        loop {
            if at == NO_FILE {
                return Ok(None); // nothing ended before it
            }
            let Some(roots) = read_roots(headers.file, at) else {
                break; // unreadable: look below it
            };
            limit = at;
            let before = read_roots(headers.file, roots.prev);
            let mut durable = true;
            for position in 0..per {
                let moved = before
                    .as_ref()
                    .is_none_or(|b| b.root(position) != roots.root(position));
                if moved && !root_whole(headers, &roots, position)? {
                    durable = false;
                    break;
                }
            }
            if durable {
                return Ok(Some(roots));
            }
            // The write this block ends was cut: the one before is durable.
            at = roots.prev;
        }
    }
}

/// Whether the batch the data shard at `position` roots in is whole — a
/// data shard that never ended one has nothing to check.
fn root_whole(
    headers: &mut DiskHeaders<'_>,
    roots: &Roots,
    position: usize,
) -> StorageResult<bool> {
    let Some(end) = roots.root(position) else {
        return Ok(true);
    };
    let Some(header) = headers
        .read(end)
        .filter(|h| h.end_of_batch && h.shard as usize == position)
    else {
        return Ok(false);
    };
    Ok(whole(headers, &header)?.is_ok())
}

/// The roots block at `at`, if one is there.
pub fn read_roots(file: &BlockFile, at: u64) -> Option<Roots> {
    if at == NO_FILE {
        return None;
    }
    let bytes = file.read_run(Run::new(at, ROOTS_BLOCKS)).ok()?;
    Roots::decode(&bytes).filter(|r| r.at == at)
}

/// Whether the batch `end` ends is whole and every page and dictionary it
/// placed intact — or where to look below for an older one.
fn whole(
    headers: &mut DiskHeaders<'_>,
    end: &Header,
) -> StorageResult<Result<(), u64>> {
    let mut batch = vec![end.clone()];
    let base = loop {
        let Some(last) = batch.last() else {
            return Ok(Err(end.at));
        };
        if last.prev == NO_FILE {
            break 0;
        }
        match headers.read(last.prev) {
            None => return Ok(Err(last.prev)),
            Some(before) if before.end_of_batch => {
                break before.at + HEADER_BLOCKS;
            }
            Some(before) => batch.push(before),
        }
    };
    // What the batch wrote lies between the header before it and its end.
    let placed = |raw: u64| {
        let desc = BlockDescriptor::from_raw(raw);
        (desc.is_allocated() && (base..end.at).contains(&desc.start()))
            .then(|| desc.run())
    };
    for header in &batch {
        for entry in &header.types {
            for &(_, raw) in &entry.changed {
                if let Some(run) = placed(raw)
                    && !SlotPage::intact(&headers.file.read_run(run)?)
                {
                    return Ok(Err(base));
                }
            }
            if let Some(run) = entry.dict.and_then(placed)
                && Dictionary::from_bytes(&headers.file.read_run(run)?).is_err()
            {
                return Ok(Err(base));
            }
        }
    }
    Ok(Ok(()))
}

/// The newest header or roots block wholly below block `limit`, newest file
/// first, read back a chunk at a time.
fn newest_below(file: &BlockFile, limit: u64) -> StorageResult<Option<Mark>> {
    for (start, end) in file.layout().into_iter().rev() {
        let mut hi = end.min(limit);
        while hi > start {
            let lo = hi.saturating_sub(SCAN_BLOCKS).max(start);
            let bytes = file.read_run(Run::new(lo, hi - lo))?;
            for at in (lo..hi).rev() {
                let offset = ((at - lo) as usize) * BLOCK_SIZE;
                if let Some(mark) = Mark::at(&bytes[offset..], at) {
                    return Ok(Some(mark));
                }
            }
            if lo == start {
                break;
            }
            // The next positions are below `lo`; their headers reach past it.
            hi = lo + HEADER_BLOCKS - 1;
        }
    }
    Ok(None)
}
