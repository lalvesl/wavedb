//! The settle-round probe — what every window a round writes is made of,
//! one line per round, for chasing an issue in a test or a bench. Compiled
//! only under the `probe` feature: a build without it carries none of it.
//!
//! A line goes to the file `WAVEDB_PROBE` names (appended), or to stderr
//! when it names none:
//!
//! ```text
//! round shard=0 blocks=28 | page sh=08c7…fc0e bucket=49 blocks=6 bytes=21794 records=307 heads=08c7…fc0e:307:93182 | … | header blocks=8 | roots blocks=1
//! ```
//!
//! A page's `bytes` is its stored image (compressed), `records` how many it
//! holds, and `heads` the decoded records grouped by their leading 8 bytes —
//! the `STRUCT_HASH` every stored value starts with — as `head:count:bytes`,
//! which tells a type's records from the chain segments, index nodes and
//! Pivots sharing its slot. `scripts/probe_summary.sh` folds a log of them
//! into KiB per round by component.
//!
//! The read path writes `reads` lines beside them — one per 20 000 typed
//! reads, and one per bench phase (`phase=` the phase that ended):
//!
//! ```text
//! reads phase=read_hot n=10000 hit=76 removed=0 miss=9924 page_hit=9008 page_miss=916 ns_per_read=40720 … decode_parts_ns=5425/5644/274244/46027/11552 | cache footprint_KiB=98279 indexed=1961 … heads=… | page_cache KiB=65437 pages=467
//! ```
//!
//! `hit`/`removed`/`miss` are the record cache's answers, `page_hit` /
//! `page_miss` the page cache's; a decode splits into envelope, zstd
//! context, decompression, wire body and map. The record cache's `heads`
//! are `head:live_n/live_KiB:dead_n/dead_KiB` — what its log spends its
//! bytes on, superseded copies included.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::io::Write as _;

use crate::plan::SlotPlan;
use crate::struct_storage::TypeSlot;
use crate::window::Target;

/// The variable naming the file the lines go to.
const DESTINATION: &str = "WAVEDB_PROBE";

/// Record one round of the data shard at `shard`: its `targets`, sized
/// `sizes`, from `plans` over `types`.
pub fn round(
    shard: u32,
    types: &[TypeSlot],
    plans: &[SlotPlan],
    targets: &[Target],
    sizes: &[u64],
) {
    let total: u64 = sizes.iter().sum();
    let mut line = format!("round shard={shard} blocks={total}");
    for (target, &blocks) in targets.iter().zip(sizes) {
        match target {
            Target::Page { plan, bucket } => {
                page(&mut line, types, &plans[*plan], *bucket, blocks);
            }
            Target::Dict { .. } => {
                let _ = write!(line, " | dict blocks={blocks}");
            }
            Target::Header => {
                let _ = write!(line, " | header blocks={blocks}");
            }
            Target::Roots => {
                let _ = write!(line, " | roots blocks={blocks}");
            }
        }
    }
    emit(&line);
}

/// One page of `plan` (see the module docs).
fn page(
    line: &mut String,
    types: &[TypeSlot],
    plan: &SlotPlan,
    bucket: usize,
    blocks: u64,
) {
    let sh = types.get(plan.idx).map_or(0, TypeSlot::struct_hash);
    let bytes = plan.pages.get(&bucket).map_or(0, Vec::len);
    let mut heads: BTreeMap<u64, (usize, usize)> = BTreeMap::new();
    let mut records = 0;
    if let Some(page) = plan.decoded.get(&bucket) {
        for (_, value) in page.iter() {
            records += 1;
            let head = value
                .first_chunk::<8>()
                .map_or(0, |b| u64::from_le_bytes(*b));
            let entry = heads.entry(head).or_default();
            entry.0 += 1;
            entry.1 += value.len();
        }
    }
    let _ = write!(
        line,
        " | page sh={sh:016x} bucket={bucket} blocks={blocks} \
         bytes={bytes} records={records} heads="
    );
    for (i, (head, (count, size))) in heads.iter().enumerate() {
        let sep = if i == 0 { "" } else { "," };
        let _ = write!(line, "{sep}{head:016x}:{count}:{size}");
    }
}

/// Append `line` where [`DESTINATION`] says. A probe that cannot write
/// drops the line: it never fails the round it watches.
fn emit(line: &str) {
    match std::env::var_os(DESTINATION) {
        Some(path) => {
            let file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path);
            if let Ok(mut file) = file {
                let _ = writeln!(file, "{line}");
            }
        }
        None => eprintln!("{line}"),
    }
}

/// The read path's counters since the last `reads` line — one thread uses
/// a store, so a thread-local cell is the whole bookkeeping.
#[derive(Debug, Default, Clone, Copy)]
struct Reads {
    /// Typed reads (`read_now`) finished.
    n: u64,
    /// Record cache: newest bytes, a tombstone, nothing.
    hit: u64,
    removed: u64,
    miss: u64,
    /// Page cache: held decoded, or read and decoded.
    page_hit: u64,
    page_miss: u64,
    /// Over the page misses: stored bytes read, records decoded, and the
    /// time in the read and in the decode.
    page_bytes: u64,
    page_records: u64,
    read_ns: u64,
    decode_ns: u64,
    /// Time in `read_now`, whole.
    total_ns: u64,
    /// A page decode in parts: envelope, zstd context, decompress, wire
    /// body, map — summed over the decodes.
    parts: [u64; 5],
    decodes: u64,
}

thread_local! {
    static READS: std::cell::Cell<Reads> =
        const { std::cell::Cell::new(Reads {
            n: 0, hit: 0, removed: 0, miss: 0, page_hit: 0, page_miss: 0,
            page_bytes: 0, page_records: 0, read_ns: 0, decode_ns: 0,
            total_ns: 0, parts: [0; 5], decodes: 0,
        }) };
}

/// A `reads` line every this many typed reads.
const READS_EVERY: u64 = 20_000;

fn bump(f: impl FnOnce(&mut Reads)) {
    READS.with(|cell| {
        let mut reads = cell.get();
        f(&mut reads);
        cell.set(reads);
    });
}

/// What the record cache answered a typed read.
pub fn record_lookup(lookup: &crate::record_cache::Lookup) {
    use crate::record_cache::Lookup;
    bump(|r| match lookup {
        Lookup::Hit(_) => r.hit += 1,
        Lookup::Removed => r.removed += 1,
        Lookup::Miss => r.miss += 1,
    });
}

/// A page the page cache held.
pub fn page_hit() {
    bump(|r| r.page_hit += 1);
}

/// A page the page cache did not hold: `read` it and `decode` it, timing
/// each, and count it.
///
/// # Errors
/// `read`'s or `decode`'s.
pub fn page_miss(
    read: impl FnOnce() -> crate::error::StorageResult<Vec<u8>>,
    decode: impl FnOnce(&[u8]) -> crate::error::StorageResult<crate::page::SlotPage>,
) -> crate::error::StorageResult<crate::page::SlotPage> {
    let ns = |d: std::time::Duration| {
        u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)
    };
    let start = std::time::Instant::now();
    let image = read()?;
    let read_at = std::time::Instant::now();
    let page = decode(&image)?;
    let decoded = read_at.elapsed();
    bump(|r| {
        r.page_miss += 1;
        r.page_bytes += image.len() as u64;
        r.page_records += page.len() as u64;
        r.read_ns += ns(read_at - start);
        r.decode_ns += ns(decoded);
    });
    Ok(page)
}

/// A typed read finished in `ns`; every [`READS_EVERY`] of them, a `reads`
/// line over the counters and `census` of the caches.
pub fn read_done(ns: u64, census: impl FnOnce() -> String) {
    let mut due = false;
    bump(|r| {
        r.n += 1;
        r.total_ns += ns;
        due = r.n >= READS_EVERY;
    });
    if due {
        flush("", census);
    }
}

/// Close the counters at a phase boundary: a `reads` line over what is
/// left, naming `phase` — the one that just ended.
pub fn mark(phase: &str, census: impl FnOnce() -> String) {
    flush(phase, census);
}

fn flush(phase: &str, census: impl FnOnce() -> String) {
    let r = READS.with(|cell| cell.replace(Reads::default()));
    let per = |ns: u64, n: u64| ns.checked_div(n).unwrap_or(0);
    let mut line = format!(
        "reads{} n={} hit={} removed={} miss={} page_hit={} page_miss={} \
         ns_per_read={} ns_per_page_read={} ns_per_decode={} \
         bytes_per_page={} records_per_page={}",
        if phase.is_empty() {
            String::new()
        } else {
            format!(" phase={phase}")
        },
        r.n,
        r.hit,
        r.removed,
        r.miss,
        r.page_hit,
        r.page_miss,
        per(r.total_ns, r.n),
        per(r.read_ns, r.page_miss),
        per(r.decode_ns, r.page_miss),
        per(r.page_bytes, r.page_miss),
        per(r.page_records, r.page_miss),
    );
    let _ = write!(line, " decode_parts_ns=");
    for (i, part) in r.parts.iter().enumerate() {
        let sep = if i == 0 { "" } else { "/" };
        let _ = write!(line, "{sep}{}", per(*part, r.decodes));
    }
    let _ = write!(line, " | {}", census());
    emit(&line);
}

/// Splits of one page decode: `split(i)` charges the time since the last
/// split to part `i`.
pub struct Lap {
    last: std::time::Instant,
    parts: [u64; 5],
}

impl Lap {
    pub fn start() -> Self {
        Self {
            last: std::time::Instant::now(),
            parts: [0; 5],
        }
    }

    pub fn split(&mut self, part: usize) {
        let now = std::time::Instant::now();
        if let Some(slot) = self.parts.get_mut(part) {
            *slot +=
                u64::try_from((now - self.last).as_nanos()).unwrap_or(u64::MAX);
        }
        self.last = now;
    }
}

/// One page decode's parts (see [`Lap`]).
pub fn decode_parts(lap: &Lap) {
    bump(|r| {
        r.decodes += 1;
        for (sum, part) in r.parts.iter_mut().zip(lap.parts) {
            *sum += part;
        }
    });
}
