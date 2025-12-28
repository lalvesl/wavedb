//! A store's open, timed ([RFC 0079] phase 5).
//!
//! WaveDB replays nothing on open: it finds the newest header and walks back
//! along the headers to rebuild every index. What that costs depends on what
//! the files hold, so a row times it twice: `written`, the store as the
//! workload left it, and `compacted`, after the row's own compaction, which
//! restates every address and so leaves the shortest walk there is.
//!
//! ## Cold, then warm
//!
//! Each point is opened twice. The **cold** open comes after the OS page
//! cache was told to drop every file of the store (`posix_fadvise`,
//! `DONTNEED`: the files are synced by then, so their pages are clean and
//! go), so its reads reach the disk and its time is what a restart costs.
//! The **warm** open follows at once, over the pages the cold one brought
//! in: the reconstruction's CPU alone. Read calls are counted for both;
//! they are what the walk costs in IOps whichever cache serves them.
//!
//! [RFC 0079]: ../../../rfcs/0079-headers-sized-to-their-change-WIP.md

use std::fs::File;
use std::path::Path;
use std::time::Instant;

use rustix::fs::{Advice, fadvise};

use crate::corpus::OpenRecord;
use crate::io_counters::Io;

/// Open the store at `dir` cold and then warm, recording both under
/// `point` into `opens`; returns the warm open's store.
///
/// # Errors
/// The store could not be opened, or its files could not be walked.
pub fn timed<T>(
    point: &str,
    dir: &Path,
    opens: &mut Vec<OpenRecord>,
    open: impl Fn() -> Result<T, String>,
) -> Result<T, String> {
    drop_cache(dir)?;
    let (cold, record) = once(point, "cold", &open)?;
    opens.push(record);
    drop(cold);
    let (warm, record) = once(point, "warm", &open)?;
    opens.push(record);
    Ok(warm)
}

/// One open, measured.
fn once<T>(
    point: &str,
    cache: &str,
    open: impl Fn() -> Result<T, String>,
) -> Result<(T, OpenRecord), String> {
    let before = Io::current();
    let start = Instant::now();
    let store = open()?;
    let wall_ns = start.elapsed().as_nanos() as u64;
    let io = Io::current().since(before);
    Ok((
        store,
        OpenRecord {
            point: point.into(),
            cache: cache.into(),
            wall_ns,
            read_bytes: io.read_bytes,
            rchar: io.rchar,
            read_calls: io.read_calls,
        },
    ))
}

/// Ask the OS to drop every file under `dir` from its page cache.
///
/// # Errors
/// A file or directory could not be read.
fn drop_cache(dir: &Path) -> Result<(), String> {
    let entries = std::fs::read_dir(dir)
        .map_err(|e| format!("{}: {e}", dir.display()))?;
    for entry in entries {
        let path = entry.map_err(|e| format!("{}: {e}", dir.display()))?.path();
        if path.is_dir() {
            drop_cache(&path)?;
        } else if path.is_file() {
            let file = File::open(&path)
                .map_err(|e| format!("{}: {e}", path.display()))?;
            fadvise(&file, 0, None, Advice::DontNeed)
                .map_err(|e| format!("fadvise {}: {e}", path.display()))?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::timed;

    /// Both opens are recorded under the point, cold first, and the warm
    /// one's value is what the caller keeps.
    #[test]
    fn a_point_records_a_cold_then_a_warm_open() {
        let dir = std::env::temp_dir()
            .join(format!("wavedb-bench-opens-{}", std::process::id()));
        std::fs::create_dir_all(dir.join("storage")).expect("mkdir");
        std::fs::write(dir.join("storage").join("f"), [7u8; 8192])
            .expect("write");
        let mut opens = Vec::new();
        let kept = timed("written", &dir, &mut opens, || {
            std::fs::read(dir.join("storage").join("f"))
                .map_err(|e| e.to_string())
        })
        .expect("timed");
        std::fs::remove_dir_all(&dir).expect("cleanup");
        assert_eq!(kept.len(), 8192);
        let seen: Vec<_> = opens
            .iter()
            .map(|o| (o.point.as_str(), o.cache.as_str()))
            .collect();
        assert_eq!(seen, [("written", "cold"), ("written", "warm")]);
        assert!(opens.iter().all(|o| o.rchar >= 8192 && o.read_calls >= 1));
    }
}
