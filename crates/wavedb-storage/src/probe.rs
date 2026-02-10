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
//! which tells a type's records from the B+tree nodes, chain segments and
//! Pivots sharing its slot. `scripts/probe_summary.sh` folds a log of them
//! into KiB per round by component.

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
