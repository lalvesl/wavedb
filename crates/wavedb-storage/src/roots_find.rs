//! Where a reconstruction roots ([RFC 0073] §6, [RFC 0072] §6–§7) — split
//! from [`crate::reopen`], which walks from there.
//!
//! 1. **The newest roots block**: the newest data file ends with one, or
//!    with a header naming the newest one written before it — each only
//!    where it says it is, its crc holding. Its last block alone says
//!    which: a roots block, or a header's last block, whose trailer gives
//!    the header's length ([RFC 0079] §2) — one more read when the header
//!    is longer than that block. After a crash the end may be torn: the
//!    blocks before it are read back, a chunk at a time, until one is.
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
//! [RFC 0079]: ../../../rfcs/0079-headers-sized-to-their-change-WIP.md

use crate::block::{BLOCK_SIZE, BlockDescriptor, Run};
use crate::block_file::BlockFile;
use crate::dictionary::Dictionary;
use crate::error::StorageResult;
use crate::header::{Header, NO_FILE};
use crate::header_room::MAX_HEADER_BLOCKS;
use crate::ordered::RangeRecord;
use crate::reopen::DiskHeaders;
use crate::roots::{Mark, ROOTS_BLOCKS, Roots};
use crate::trailer;

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
        let prev = last.prev;
        if !prev.is_some() {
            break 0;
        }
        match headers.read(prev) {
            None => return Ok(Err(prev.at)),
            Some(before) if before.end_of_batch => break prev.run().end(),
            Some(before) => batch.push(before),
        }
    };
    // What the batch wrote lies between the header before it and its end.
    let placed = |start: u64, count: u64| {
        (count > 0 && (base..end.at).contains(&start))
            .then(|| Run::new(start, count))
    };
    let dict_run = |raw: u64| {
        let desc = BlockDescriptor::from_raw(raw);
        placed(desc.start(), desc.count())
    };
    for header in &batch {
        for entry in &header.types {
            let pages = entry.changed.iter().flat_map(RangeRecord::pages);
            for (start, count) in pages {
                if let Some(run) = placed(start, count)
                    && !crate::pack::intact(&headers.file.read_run(run)?)
                {
                    return Ok(Err(base));
                }
            }
            if let Some(run) = entry.dict.and_then(dict_run)
                && Dictionary::from_bytes(&headers.file.read_run(run)?).is_err()
            {
                return Ok(Err(base));
            }
        }
    }
    Ok(Ok(()))
}

/// The newest header or roots block wholly below block `limit`, newest file
/// first: the one ending there, or else read back a chunk at a time.
fn newest_below(file: &BlockFile, limit: u64) -> StorageResult<Option<Mark>> {
    if let Some(mark) = ending(file, limit)? {
        return Ok(Some(mark));
    }
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
            hi = lo + u64::from(MAX_HEADER_BLOCKS) - 1;
        }
    }
    Ok(None)
}

/// The mark ending the newest file below block `limit`, from its last
/// block: a roots block or a one-block header there, or a header that
/// block's trailer says is longer, read whole.
fn ending(file: &BlockFile, limit: u64) -> StorageResult<Option<Mark>> {
    let Some((start, end)) = file
        .layout()
        .into_iter()
        .rev()
        .find(|&(start, end)| start < end.min(limit))
    else {
        return Ok(None);
    };
    let last = end.min(limit) - 1;
    let block = file.read_run(Run::new(last, 1))?;
    if let Some(mark) = Mark::at(&block, last) {
        return Ok(Some(mark));
    }
    let first = trailer::blocks_of(&block)
        .and_then(|blocks| (last + 1).checked_sub(u64::from(blocks)))
        .filter(|&first| first >= start);
    let Some(first) = first else {
        return Ok(None);
    };
    let bytes = file.read_run(Run::new(first, last + 1 - first))?;
    Ok(Mark::at(&bytes, first))
}

#[cfg(test)]
mod tests {
    use super::newest_below;
    use crate::block::{BLOCK_SIZE, Run};
    use crate::block_file::BlockFile;
    use crate::header::{Header, HeaderLink, NO_FILE};
    use crate::roots::{Mark, Roots};
    use crate::trailer;

    /// A header at block `at`, `blocks` long, its trailer closing it.
    fn header(at: u64, blocks: u16) -> Vec<u8> {
        let mut bytes = Header {
            at,
            prev: HeaderLink::NONE,
            horizon: 0,
            shard: 0,
            roots: NO_FILE,
            end_of_batch: true,
            types: Vec::new(),
            remote: Vec::new(),
            chain: None,
        }
        .encode();
        bytes.resize(usize::from(blocks) * BLOCK_SIZE, 0);
        trailer::close(&mut bytes, blocks);
        bytes
    }

    /// A file of a page at block 0 and a three-block header after it.
    fn file(dir: &std::path::Path) -> BlockFile {
        let file = BlockFile::open(dir, [1, 2, 3, 4], false, 0).unwrap();
        file.write_run(0, Run::new(0, 1), &[7; BLOCK_SIZE]).unwrap();
        file.write_run(0, Run::new(1, 3), &header(1, 3)).unwrap();
        file
    }

    fn found(file: &BlockFile) -> Option<u64> {
        match newest_below(file, u64::MAX).unwrap()? {
            Mark::Header(h) => Some(h.at),
            Mark::Roots(r) => Some(r.at),
        }
    }

    /// The newest header is found from the file's last block, its trailer
    /// saying where it starts; a roots block after it is found first.
    #[test]
    fn the_newest_mark_is_found_from_the_last_block() {
        let d = tempfile::tempdir().unwrap();
        let file = file(d.path());
        assert_eq!(found(&file), Some(1), "a header longer than a block");
        let roots = Roots::new(4, NO_FILE, vec![HeaderLink::new(1, 3)]);
        file.write_run(0, Run::new(4, 1), &roots.encode().unwrap())
            .unwrap();
        assert_eq!(found(&file), Some(4), "the roots block ending the write");
    }

    /// A header whose trailer was torn is still found by its envelope while
    /// that holds; one whose envelope was torn is read past, to the one
    /// before it.
    #[test]
    fn a_torn_header_is_read_past() {
        let d = tempfile::tempdir().unwrap();
        let file = BlockFile::open(d.path(), [1, 2, 3, 4], false, 0).unwrap();
        file.write_run(0, Run::new(0, 1), &header(0, 1)).unwrap();
        file.write_run(0, Run::new(1, 3), &header(1, 3)).unwrap();
        assert_eq!(found(&file), Some(1));
        file.write_run(0, Run::new(3, 1), &[0; BLOCK_SIZE]).unwrap();
        assert_eq!(found(&file), Some(1), "its bytes are whole");
        file.write_run(0, Run::new(1, 1), &[0; BLOCK_SIZE]).unwrap();
        assert_eq!(found(&file), Some(0));
    }
}
