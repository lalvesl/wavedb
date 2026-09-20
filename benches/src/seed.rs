//! Building and consuming **seed datasets** (RFC 0060 §6).
//!
//! A filled data directory is a pure function of (system, version, rows, seed),
//! which is exactly what a derivation is for. Everything here runs either
//! inside a Nix builder (the fill) or at the start of a run (the materialise);
//! **nothing here is ever timed**, because the insert benchmark *is* the fill
//! and never uses a seed.
//!
//! Two portable forms exist:
//!
//! - `emit_tsv` writes the dataset once, and PostgreSQL/MySQL/MongoDB seeds are
//!   loaded from it by their own bulk tools inside the builder — so those seeds
//!   can be built and version-pinned **before** their Rust adapters exist.
//! - `fill_wavedb` / `fill_sqlite` fill a real store, for the two systems whose
//!   adapters are built.

use std::io::{BufWriter, Write as _};
use std::path::Path;
use std::time::{Duration, Instant};

use futures::executor::block_on;
use wavedb_core::{Id, LocalHandle, U48};
use wavedb_storage::PageStore;

use crate::schema::{Thing, ThingPivotId, thing};

/// The tenant every seeded dataset is written under.
pub const TENANT: u32 = 1;

/// Column order of the TSV form, and of every loader that reads it.
pub const COLUMNS: &str = "id, kind, score, name, tag, body";

/// Write the dataset as tab-separated rows — the one portable form all the
/// server-side loaders (`COPY`, `LOAD DATA`, `mongoimport`) accept.
///
/// Safe without quoting: the generator emits no tabs, newlines or backslashes
/// (`schema::body_text` draws from a fixed word list).
pub fn emit_tsv(out: &Path, rows: u64, seed: u64) -> Result<(), String> {
    let file =
        std::fs::File::create(out).map_err(|e| format!("create tsv: {e}"))?;
    let mut w = BufWriter::new(file);
    for n in 0..rows {
        let t = thing(n, seed);
        writeln!(
            w,
            "{}\t{}\t{}\t{}\t{}\t{}",
            n, t.kind, t.score, t.name, t.tag, t.body
        )
        .map_err(|e| format!("write tsv: {e}"))?;
    }
    w.flush().map_err(|e| format!("flush tsv: {e}"))
}

/// Fill a WaveDB store at `dir`, then write the sidecar the benchmark needs to
/// address it.
///
/// The sidecar is not optional: a NonUnique anchor id is minted from the clock
/// at insert (`key_nanos`), so it cannot be recomputed from the seed the way a
/// SQL primary key can. Without the minted ids a seeded store is unreadable.
pub fn fill_wavedb(dir: &Path, rows: u64, seed: u64) -> Result<(), String> {
    std::fs::create_dir_all(dir).map_err(|e| format!("mkdir: {e}"))?;
    let data = dir.join("data");
    std::fs::create_dir_all(&data).map_err(|e| format!("mkdir: {e}"))?;

    // A fill, not a measurement: the durability window (RFC 0061) turns one
    // `fsync` per row into one per elapsed window. The measured store reopens
    // at the default — `systems::wavedb::open` — so no recorded number is
    // taken against a relaxed engine. See `crate::FILL_WINDOW`.
    let store = PageStore::open_with(
        &data,
        &Thing::storage_entries(),
        wavedb_storage::StoreOptions {
            relax_window: crate::FILL_WINDOW,
            ..Default::default()
        },
    )
    .map_err(|e| format!("open: {e}"))?;
    let db = LocalHandle::new(&store, U48::from(TENANT));
    let pivot = block_on(Thing::create_pivot(&db))
        .map_err(|e| format!("create_pivot: {e}"))?;
    let col = Thing::collection(pivot);

    let mut ids = Vec::with_capacity(rows as usize);
    for n in 0..rows {
        let t = thing(n, seed);
        ids.push(
            block_on(col.insert(&db, &t))
                .map_err(|e| format!("insert {n}: {e}"))?,
        );
    }
    // Leave the store quiesced so the first measured operation does not pay
    // for a settle round the fill postponed.
    store.drain().map_err(|e| format!("drain: {e}"))?;
    store
        .commit_journal()
        .map_err(|e| format!("checkpoint: {e}"))?;
    store
        .commit_journal()
        .map_err(|e| format!("checkpoint: {e}"))?;

    let mut bytes = Vec::with_capacity(ids.len() * 16);
    for id in &ids {
        bytes.extend_from_slice(&id.raw().to_le_bytes());
    }
    std::fs::write(dir.join("ids.bin"), &bytes)
        .map_err(|e| format!("write ids: {e}"))?;
    std::fs::write(dir.join("pivot.bin"), wavedb_core::to_wire(&pivot))
        .map_err(|e| format!("write pivot: {e}"))
}

/// Read back what [`fill_wavedb`] wrote beside the store.
pub fn load_wavedb_sidecar(
    dir: &Path,
) -> Result<(Vec<Id>, ThingPivotId), String> {
    let raw = std::fs::read(dir.join("ids.bin"))
        .map_err(|e| format!("read ids: {e}"))?;
    if raw.len() % 16 != 0 {
        return Err("ids.bin is not a whole number of 128-bit ids".into());
    }
    let ids = raw
        .chunks_exact(16)
        .map(|c| {
            let mut b = [0u8; 16];
            b.copy_from_slice(c);
            Id::from_raw(u128::from_le_bytes(b))
        })
        .collect();
    let pivot_bytes = std::fs::read(dir.join("pivot.bin"))
        .map_err(|e| format!("read pivot: {e}"))?;
    let pivot = wavedb_core::from_wire::<ThingPivotId>(&pivot_bytes)
        .map_err(|e| format!("decode pivot: {e}"))?;
    Ok((ids, pivot))
}

/// Copy a seed out of the (read-only) Nix store into a writable working copy.
///
/// `--reflink=auto` makes this near-free on btrfs/xfs and a full copy
/// elsewhere; either way the cost is returned so it can be reported **outside**
/// the measurement window, which is what stops a slow filesystem from looking
/// like a slow database.
pub fn materialise(from: &Path, to: &Path) -> Result<Duration, String> {
    let start = Instant::now();
    if let Some(parent) = to.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("mkdir: {e}"))?;
    }
    run("cp", &["--reflink=auto", "-r", &s(from), &s(to)])?;
    // Store paths are read-only and databases are not. `go-rwx` is not
    // tidiness: PostgreSQL **refuses to start** on a data directory readable by
    // group or other, and a store copy is world-readable.
    run("chmod", &["-R", "u+w,go-rwx", &s(to)])?;
    Ok(start.elapsed())
}

fn run(cmd: &str, args: &[&str]) -> Result<(), String> {
    let out = std::process::Command::new(cmd)
        .args(args)
        .output()
        .map_err(|e| format!("{cmd}: {e}"))?;
    if out.status.success() {
        return Ok(());
    }
    Err(format!(
        "{cmd} failed: {}",
        String::from_utf8_lossy(&out.stderr).trim()
    ))
}

fn s(p: &Path) -> String {
    p.display().to_string()
}
