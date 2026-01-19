//! The database's `MANIFEST` ([RFC 0071] §2) — its fixed facts, in one
//! record at its root.
//!
//! The magic, the format version, the shard count, the hash seed and the
//! mode: written once, when the database is created, and read on every open;
//! nothing about it ever changes.
//!
//! An object-backed database keeps a copy in its object store, at
//! `<prefix>MANIFEST`, so a database whose disk is gone — or one opened in
//! object mode — still knows every fact. The two copies must agree.
//!
//! The shard count and the seed are part of every record's address: a record
//! lives in shard `seahash(owner) % P` ([`wavedb_core::placement`]) and in the
//! bucket its seeded hash names, so a database opened with another of either
//! would look for every record in the wrong place. A caller's count and mode
//! are only what a **new** database is created with.
//!
//! [RFC 0071]: ../../../rfcs/0071-data-files-by-address.md

use std::fs;
use std::io::Write as _;
use std::path::Path;

use futures::executor::block_on;
use wavedb_platform::s3::{Put, When};
use wavedb_wire::{WaveWire, from_wire_checked, to_wire_checked};

use crate::block_file::random_seed;
use crate::error::{StorageError, StorageResult};
use crate::remote::Remote;

/// The file holding the [`Manifest`], at the database's root.
pub const MANIFEST: &str = "MANIFEST";

/// The bytes every `MANIFEST` opens with.
pub const MAGIC: &[u8; 8] = b"WAVEDBMF";

/// On-disk format version.
///
/// **Pinned at 1 while WaveDB is pre-release**: the format changes freely with
/// no bumps, no history, and no migrations — an older database is simply not
/// supported. Versioning starts mattering at the first release.
pub const FORMAT_VERSION: u32 = 1;

/// The most shards a database may have: a shard's directory is two hex
/// digits.
pub const MAX_SHARDS: usize = 256;

/// A database's fixed facts (see the module docs).
#[derive(Debug, Clone, Copy, PartialEq, Eq, WaveWire)]
pub struct Manifest {
    version: u32,
    shards: u32,
    seed: [u64; 4],
    object_backed: bool,
}

impl Manifest {
    /// The facts of a new database: `shards` shards, a fresh random seed.
    ///
    /// # Errors
    /// [`StorageError::Corrupt`] for a shard count out of `1..=256`.
    pub fn new(shards: usize, object_backed: bool) -> StorageResult<Self> {
        let shards = u32::try_from(checked(shards)?)
            .map_err(|_| StorageError::Corrupt("MANIFEST shard count"))?;
        Ok(Self {
            version: FORMAT_VERSION,
            shards,
            seed: random_seed(),
            object_backed,
        })
    }

    /// The shard count.
    #[must_use]
    pub const fn shards(&self) -> usize {
        self.shards as usize
    }

    /// The hash seed every record is routed with.
    #[must_use]
    pub const fn seed(&self) -> [u64; 4] {
        self.seed
    }

    /// Whether the pages live in an object store (RFC 0070).
    #[must_use]
    pub const fn object_backed(&self) -> bool {
        self.object_backed
    }

    /// The magic, then the checked wire bytes.
    fn encode(&self) -> Vec<u8> {
        let mut bytes = MAGIC.to_vec();
        bytes.extend_from_slice(&to_wire_checked(self));
        bytes
    }

    /// The facts `bytes` hold.
    ///
    /// # Errors
    /// [`StorageError::BadMagic`], [`StorageError::BadVersion`], or
    /// [`StorageError::Corrupt`] for a torn record or a shard count out of
    /// range.
    fn decode(bytes: &[u8]) -> StorageResult<Self> {
        let Some(body) = bytes.strip_prefix(MAGIC.as_slice()) else {
            return Err(StorageError::BadMagic);
        };
        let manifest: Self = from_wire_checked(body)
            .map_err(|_| StorageError::Corrupt("unreadable MANIFEST"))?;
        if manifest.version != FORMAT_VERSION {
            return Err(StorageError::BadVersion(manifest.version));
        }
        checked(manifest.shards())?;
        Ok(manifest)
    }
}

/// The database's facts: the ones its `MANIFEST`s record, or a new one.
///
/// A new database has `shards` shards and the mode the caller opens it with;
/// its `MANIFEST`s are written here, the object store's first, so a racing
/// creator's wins and becomes ours.
///
/// `dir` is the database's root on disk (`None` in object mode); `remote`
/// its object store, if it has one.
///
/// # Errors
/// [`StorageError::ModeMismatch`] for a database created in the other mode;
/// [`StorageError::Corrupt`] for two `MANIFEST`s that disagree; a
/// `MANIFEST`'s own faults (`BadMagic`, `BadVersion`); I/O and object-store
/// faults.
pub fn resolve(
    dir: Option<&Path>,
    remote: Option<&Remote>,
    shards: usize,
) -> StorageResult<Manifest> {
    let object_backed = remote.is_some();
    let on_disk = dir.map(read_local).transpose()?.flatten();
    if let Some(found) = on_disk
        && found.object_backed != object_backed
    {
        return Err(StorageError::ModeMismatch {
            object_backed: found.object_backed,
        });
    }
    let in_store = remote.map(read_remote).transpose()?.flatten();
    if let (Some(a), Some(b)) = (on_disk, in_store)
        && a != b
    {
        return Err(StorageError::Corrupt("the two MANIFESTs disagree"));
    }
    let manifest = match on_disk.or(in_store) {
        Some(found) => found,
        None => Manifest::new(shards, object_backed)?,
    };
    let manifest = match (remote, in_store) {
        (Some(remote), None) => write_remote(remote, manifest)?,
        _ => manifest,
    };
    if let (Some(dir), None) = (dir, on_disk) {
        write_local(dir, &manifest)?;
    }
    Ok(manifest)
}

/// What `dir`'s `MANIFEST` records, or `None` when there is none.
fn read_local(dir: &Path) -> StorageResult<Option<Manifest>> {
    match fs::read(dir.join(MANIFEST)) {
        Ok(bytes) => Manifest::decode(&bytes).map(Some),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Write `dir`'s `MANIFEST`, atomically: a crash part-way leaves a
/// directory that is created again from scratch, never half a record.
fn write_local(dir: &Path, manifest: &Manifest) -> StorageResult<()> {
    fs::create_dir_all(dir)?;
    let tmp = dir.join(format!("{MANIFEST}.tmp"));
    let mut file = fs::File::create(&tmp)?;
    file.write_all(&manifest.encode())?;
    file.sync_all()?;
    fs::rename(&tmp, dir.join(MANIFEST))?;
    fs::File::open(dir)?.sync_all()?;
    Ok(())
}

/// The object store's `MANIFEST` key.
fn remote_key(remote: &Remote) -> String {
    format!("{}{MANIFEST}", remote.prefix())
}

/// What the object store's `MANIFEST` records, if it has one.
fn read_remote(remote: &Remote) -> StorageResult<Option<Manifest>> {
    let bytes = block_on(remote.bucket().get(&remote_key(remote), None))?;
    bytes.map(|b| Manifest::decode(&b)).transpose()
}

/// Write the object store's `MANIFEST` if it has none, returning the one
/// that is there afterwards: ours, or a racing creator's.
fn write_remote(
    remote: &Remote,
    manifest: Manifest,
) -> StorageResult<Manifest> {
    let key = remote_key(remote);
    match block_on(remote.bucket().put(&key, &manifest.encode(), When::Absent))?
    {
        Put::Stored { .. } => Ok(manifest),
        Put::Refused => read_remote(remote)?
            .ok_or(StorageError::Corrupt("the object store's MANIFEST")),
    }
}

const fn checked(count: usize) -> StorageResult<usize> {
    if count == 0 || count > MAX_SHARDS {
        Err(StorageError::Corrupt("shard count out of 1..=256"))
    } else {
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::{MAGIC, MANIFEST, Manifest, resolve};
    use crate::error::StorageError;

    #[test]
    fn a_manifest_roundtrips() {
        let m = Manifest::new(64, true).unwrap();
        assert_eq!(Manifest::decode(&m.encode()).unwrap(), m);
        assert_ne!(m.seed(), [0; 4], "a new database draws a seed");
    }

    #[test]
    fn a_wrong_magic_is_bad_magic() {
        let mut bytes = Manifest::new(1, false).unwrap().encode();
        bytes[0] ^= 0xFF;
        let err = Manifest::decode(&bytes).unwrap_err();
        assert!(matches!(err, StorageError::BadMagic), "{err:?}");
    }

    #[test]
    fn a_torn_body_is_corrupt() {
        let mut bytes = Manifest::new(1, false).unwrap().encode();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        let err = Manifest::decode(&bytes).unwrap_err();
        assert!(matches!(err, StorageError::Corrupt(_)), "{err:?}");
    }

    #[test]
    fn another_version_is_bad_version() {
        let mut m = Manifest::new(1, false).unwrap();
        m.version = 2;
        let mut bytes = MAGIC.to_vec();
        bytes.extend_from_slice(&wavedb_wire::to_wire_checked(&m));
        let err = Manifest::decode(&bytes).unwrap_err();
        assert!(matches!(err, StorageError::BadVersion(2)), "{err:?}");
    }

    /// The facts are the created database's: a reopen keeps its count and
    /// its seed whatever the caller asks for.
    #[test]
    fn a_reopen_keeps_the_created_facts() {
        let dir = tempfile::tempdir().unwrap();
        let created = resolve(Some(dir.path()), None, 4).unwrap();
        let reopened = resolve(Some(dir.path()), None, 64).unwrap();
        assert_eq!(reopened, created);
        assert!(dir.path().join(MANIFEST).exists());
    }
}
