//! The local store behind [`Db::open`](crate::Db::open) — WaveDB caching
//! WaveDB.
//!
//! The cache is not a second storage design: it is the same engine the node
//! runs, cfg-switched per target the way `wavedb-platform` switches its
//! primitives (concrete types, no traits, no `dyn`):
//!
//! - **native** — [`PageStore`](wavedb_storage::PageStore): journal +
//!   data files. `apply` fsyncs the journal before returning, so a mirrored
//!   write is durable at the moment the op resolves — read-your-writes needs
//!   no extra flush. The default location is created automatically under the
//!   platform cache base (`$XDG_CACHE_HOME` / `~/.cache` on unix,
//!   `%LOCALAPPDATA%` on Windows) as `<base>/<app>` — `app` names its own
//!   directory directly under the base (the XDG convention), with no extra
//!   `wavedb/` component in between.
//! - **wasm** — `IdbStore`: IndexedDB as a flat `Id → wire bytes` map
//!   (no pages, no journal — IndexedDB is already a durable transactional
//!   store), database named exactly `app` (the developer chooses the whole
//!   name — no forced prefix, mirroring the native `<app>` leaf).
//!
//! One open native cache per app directory: its files are locked while open,
//! so a second `Db::open` of the same app is `EngineBusy`. A node opens its
//! own directory and could share the process; the deployment shape runs it
//! in its own anyway.

#[cfg(target_arch = "wasm32")]
mod idb;
#[cfg(target_arch = "wasm32")]
mod idb_store;

#[cfg(target_arch = "wasm32")]
pub use idb_store::IdbStore;

/// The concrete local store `Db::open` drives — one type per target, chosen
/// at compile time (the `wavedb-platform` stance: cfg-switch, don't abstract).
#[cfg(not(target_arch = "wasm32"))]
pub type CacheStore = wavedb_storage::PageStore;

/// The concrete local store `Db::open` drives — one type per target, chosen
/// at compile time (the `wavedb-platform` stance: cfg-switch, don't abstract).
#[cfg(target_arch = "wasm32")]
pub type CacheStore = IdbStore;

/// Resolve `<platform cache base>/<app>` — the directory
/// [`Db::open`](crate::Db::open) roots the native cache engine in.
///
/// `app` is the leaf directly under the base (XDG-style: each app owns its
/// own cache dir, no shared `wavedb/` parent). The engine creates the whole
/// path on first open.
///
/// # Errors
/// [`Error::Core`](crate::Error::Core) (`Backend`) when no cache base is
/// discoverable (neither `XDG_CACHE_HOME`/`HOME` on unix nor
/// `LOCALAPPDATA`/`APPDATA` on Windows is set).
#[cfg(not(target_arch = "wasm32"))]
pub fn default_dir(app: &str) -> crate::Result<std::path::PathBuf> {
    use std::path::PathBuf;
    let base = if cfg!(windows) {
        std::env::var_os("LOCALAPPDATA")
            .or_else(|| std::env::var_os("APPDATA"))
            .map(PathBuf::from)
    } else {
        std::env::var_os("XDG_CACHE_HOME")
            .map(PathBuf::from)
            .or_else(|| {
                std::env::var_os("HOME")
                    .map(|home| PathBuf::from(home).join(".cache"))
            })
    };
    let base = base.ok_or_else(|| {
        wavedb_core::Error::Backend(String::from(
            "no cache base: set XDG_CACHE_HOME/HOME (unix) or \
             LOCALAPPDATA/APPDATA (windows), or use Db::open_at",
        ))
    })?;
    Ok(base.join(app))
}

/// Open the native cache engine rooted at `dir`, registering exactly the
/// client registry's declared types (plus the reserved B+tree-node slot).
///
/// # Errors
/// The engine's open/replay faults, converted at the documented seam
/// (`StorageError` → `core::Error::Backend`).
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn open_native(
    registry: &impl wavedb_storage::StorageRegistry,
    dir: &std::path::Path,
) -> crate::Result<CacheStore> {
    wavedb_storage::PageStore::open(dir, &registry.storage_entries())
        .map_err(|e| crate::Error::Core(e.into()))
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    // `default_dir` reads process-global env vars, so the expectations here
    // derive from the same live environment instead of mutating it (tests
    // share the process).
    #[test]
    fn default_dir_is_rooted_under_the_platform_cache_base() {
        let dir = super::default_dir("demo-app").unwrap();
        // `app` is the leaf directly under the base — no `wavedb/` parent.
        assert!(dir.ends_with("demo-app"));
        let base = std::env::var_os("XDG_CACHE_HOME").map_or_else(
            || {
                std::path::PathBuf::from(std::env::var_os("HOME").unwrap())
                    .join(".cache")
            },
            std::path::PathBuf::from,
        );
        assert!(dir.starts_with(base));
    }
}
