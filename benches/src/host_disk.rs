//! The disk under a directory, as the host key sees it: the physical
//! device's model, and what lies between it and the filesystem.
//!
//! ## Why the host key needs it
//!
//! A durable write is bound by its device's flush, and a run is bound by its
//! write rate: two disks are two machines as far as those numbers go. The
//! fingerprint knew the filesystem and whether the disk spins, so two SSDs
//! under one filesystem — a system NVMe and a dedicated SATA disk, both
//! "xfs, not rotational" — would have filed their rows in one lane.
//!
//! An encryption layer counts too: `dm-crypt` adds its cipher to every IO,
//! and its sector size decides how a 4 KiB write is cut.
//!
//! ## The walk
//!
//! From the device the mount names, through sysfs (`/sys/dev/block/M:m`),
//! down to a disk:
//! - a device-mapper node (`dm/uuid`) is a layer — `crypt` with its logical
//!   block size, `lvm`, or `dm` — and the walk follows its one slave;
//! - a partition is not a layer: the walk goes up to its disk;
//! - a disk ends the walk: its `device/model`, its `queue/rotational`.
//!
//! A mount that is no block device (tmpfs, overlay) has neither model nor
//! layers.

use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

/// What a directory is stored on.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Disk {
    /// The physical device's model (`device/model`), trimmed.
    pub model: Option<String>,
    /// The layers from the filesystem down: `crypt4096`, `lvm`, `dm`, `md`,
    /// `loop`.
    pub layers: Vec<String>,
    /// Whether the physical device spins.
    pub rotational: Option<bool>,
}

/// The disk under the mount whose device is `device` (`/proc/mounts`'s
/// first field), read from `sys`.
#[must_use]
pub fn of(device: &str, sys: &Path) -> Disk {
    let Some(rdev) = Path::new(device)
        .canonicalize()
        .ok()
        .and_then(|d| std::fs::metadata(d).ok())
        .filter(|m| m.rdev() != 0)
        .map(|m| m.rdev())
    else {
        return Disk::default();
    };
    let node =
        sys.join("dev/block")
            .join(format!("{}:{}", major(rdev), minor(rdev)));
    walk(&node)
}

/// Walk from the sysfs node `node` down to its disk (see the module docs).
fn walk(node: &Path) -> Disk {
    let mut disk = Disk::default();
    let Ok(mut at) = node.canonicalize() else {
        return disk;
    };
    // A stack deeper than this is a loop in a broken sysfs, not a disk.
    for _ in 0..8 {
        if let Some(layer) = layer_of(&at) {
            disk.layers.push(layer);
            match slave_of(&at) {
                Some(next) => at = next,
                None => return disk, // a loop device, or a layer of none
            }
        } else if at.join("partition").exists() {
            match at.parent() {
                Some(parent) => at = parent.to_path_buf(),
                None => return disk,
            }
        } else {
            let model = read_trim(at.join("device/model"));
            disk.model = (!model.is_empty()).then_some(model);
            disk.rotational =
                match read_trim(at.join("queue/rotational")).as_str() {
                    "1" => Some(true),
                    "0" => Some(false),
                    _ => None,
                };
            return disk;
        }
    }
    disk
}

/// The layer `node` is, if it is one.
fn layer_of(node: &Path) -> Option<String> {
    if node.join("dm").is_dir() {
        let uuid = read_trim(node.join("dm/uuid"));
        return Some(if uuid.starts_with("CRYPT-") {
            let block = read_trim(node.join("queue/logical_block_size"));
            format!("crypt{block}")
        } else if uuid.starts_with("LVM-") {
            "lvm".into()
        } else {
            "dm".into()
        });
    }
    if node.join("md").is_dir() {
        return Some("md".into());
    }
    node.join("loop").is_dir().then(|| "loop".into())
}

/// The first device `node` is built on, by name order.
fn slave_of(node: &Path) -> Option<PathBuf> {
    let mut slaves: Vec<PathBuf> = std::fs::read_dir(node.join("slaves"))
        .ok()?
        .flatten()
        .map(|e| e.path())
        .collect();
    slaves.sort();
    slaves.first()?.canonicalize().ok()
}

// glibc's major/minor are macros; the encoding is stable enough to inline
// rather than take a `libc` dependency for two shifts.
pub const fn major(dev: u64) -> u64 {
    ((dev >> 8) & 0xfff) | ((dev >> 32) & !0xfff)
}

pub const fn minor(dev: u64) -> u64 {
    (dev & 0xff) | ((dev >> 12) & !0xff)
}

fn read_trim(path: impl AsRef<Path>) -> String {
    std::fs::read_to_string(path)
        .map(|s| s.trim().to_string())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::symlink;
    use std::path::Path;

    use super::{Disk, walk};

    fn write(path: &Path, text: &str) {
        std::fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
        std::fs::write(path, text).expect("write");
    }

    /// A sysfs of an XFS on LUKS2 over a SATA partition, the bench disk's
    /// shape: `dm-1` → `sda1` → `sda`.
    fn fake_sys(root: &Path) {
        let dm = root.join("devices/virtual/block/dm-1");
        write(&dm.join("dm/uuid"), "CRYPT-LUKS2-abc-benchdata\n");
        write(&dm.join("queue/logical_block_size"), "4096\n");
        let sda = root.join("devices/pci/ata1/block/sda");
        write(&sda.join("device/model"), "KINGSTON SA400S3\n");
        write(&sda.join("queue/rotational"), "0\n");
        write(&sda.join("sda1/partition"), "1\n");
        std::fs::create_dir_all(dm.join("slaves")).expect("slaves");
        symlink(sda.join("sda1"), dm.join("slaves/sda1")).expect("slave");
        std::fs::create_dir_all(root.join("dev/block")).expect("dev");
        symlink(&dm, root.join("dev/block/254:1")).expect("dev link");
    }

    #[test]
    fn an_encrypted_partition_is_its_disk_under_a_crypt_layer() {
        let root = std::env::temp_dir()
            .join(format!("wavedb-bench-sysfs-{}", std::process::id()));
        fake_sys(&root);
        let disk = walk(&root.join("dev/block/254:1"));
        std::fs::remove_dir_all(&root).expect("cleanup");
        assert_eq!(
            disk,
            Disk {
                model: Some("KINGSTON SA400S3".into()),
                layers: vec!["crypt4096".into()],
                rotational: Some(false),
            }
        );
    }

    #[test]
    fn a_device_sysfs_does_not_know_is_no_disk() {
        let disk = walk(Path::new("/nonexistent/dev/block/0:0"));
        assert_eq!(disk, Disk::default());
    }
}
