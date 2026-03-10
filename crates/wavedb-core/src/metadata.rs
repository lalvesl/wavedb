//! `Metadata` — the per-record header carried by every stored record: the
//! version chain, authorship, and the access rule.
//!
//! The engine's per-version contract is exactly **who wrote it, when, and
//! under which permission**. The chain exists to review how the store
//! looked at a moment — it is not domain data, and it ends at the type's
//! own `STRUCT_HASH` boundary (a schema migration is a new type with a
//! fresh chain), so a domain fact like "member since" belongs in the
//! record's own fields.
//!
//! Chain links are **authoring instants, not addresses**: an archived
//! version's id is *derived* from the instant its bytes were authored
//! (see `crate::record`'s slot derivation), so a `u64` names a version
//! and no link ever needs repointing after the fact.

use wavedb_wire::{Cursor, WaveWire};

use crate::local_id::LocalId;
use crate::permission::PermissionRef;
use crate::u48::U48;

/// A record's place in its version chain, carrying the authoring instant
/// every archive address derives from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Succession {
    /// This is the live version, authored at the carried instant. When a
    /// save supersedes it, its bytes archive at the slot derived from
    /// exactly this value — the address is agreed the moment the version
    /// is written, never predicted.
    CreatedAt(u64),
    /// Superseded: the successor was authored at the carried instant. Its
    /// archive lives at the derived slot — or, on a miss there, the
    /// successor is still the live record at the shape's anchor.
    Next(u64),
}

impl Succession {
    /// The carried instant: this version's authoring time on a live
    /// record, the successor's on an archive.
    #[must_use]
    pub const fn instant(self) -> u64 {
        match self {
            Self::CreatedAt(t) | Self::Next(t) => t,
        }
    }
}

impl Default for Succession {
    /// A zero-instant live marker — a base for struct-update syntax only;
    /// the write paths always stamp the real instant.
    fn default() -> Self {
        Self::CreatedAt(0)
    }
}

// Hand impl: a fixed 9-byte stack (`tag (1) + instant (8 LE)`). The derive's
// enum form spends a u32 payload length per value; this payload never varies,
// so the length is dead weight on every stored record.
impl WaveWire for Succession {
    const STACK_SIZE: usize = 9;
    fn heap_size(&self) -> usize {
        0
    }
    fn encode_stack(&self, stack: &mut Vec<u8>) {
        let (tag, instant) = match self {
            Self::CreatedAt(t) => (0u8, *t),
            Self::Next(t) => (1u8, *t),
        };
        stack.push(tag);
        stack.extend_from_slice(&instant.to_le_bytes());
    }
    fn encode_heap(&self, _heap: &mut Vec<u8>) {}
    fn decode<'a>(
        stack: &mut Cursor<'a>,
        _heap: &mut Cursor<'a>,
    ) -> wavedb_wire::Result<Self> {
        let tag = stack.take(1)?[0];
        let bytes: [u8; 8] = stack
            .take(8)?
            .try_into()
            .map_err(|_| wavedb_wire::Error::UnexpectedEof)?;
        let instant = u64::from_le_bytes(bytes);
        match tag {
            0 => Ok(Self::CreatedAt(instant)),
            1 => Ok(Self::Next(instant)),
            other => Err(wavedb_wire::Error::InvalidTag(other)),
        }
    }
    wavedb_wire::own_view!();
}

/// Per-record metadata. Injected alongside the record body; serialised through
/// `WaveWire` like everything else.
///
/// `pivot_id` stays an `Option<LocalId>` (`None` = a Unique record; the
/// payload lands in the heap, so `None` costs 1 byte). The chain fields are
/// instants — see the module docs.
#[derive(Debug, Clone, PartialEq, Eq, Default, WaveWire)]
pub struct Metadata {
    /// Authoring instant of the previous version (`None` = first version);
    /// the predecessor's archive address derives from it.
    pub previous: Option<u64>,
    /// This version's place in the chain — live ([`Succession::CreatedAt`])
    /// or superseded ([`Succession::Next`]).
    pub succession: Succession,
    /// Whether this record has left the collection's living set.
    ///
    /// Liveness is a property of the **anchor**, not of a membership index.
    /// RFC 0050 deletes the `current` B+tree, so "is this record live?" is
    /// answered by the same single read that fetches the record — strictly
    /// cheaper than a descent, for the question asked most often.
    ///
    /// A **flag, not the removal instant**, and that is load-bearing rather
    /// than frugal: the instant is minted per store, so a node and a mirror
    /// that both removed the same record would hold anchors differing in those
    /// eight bytes, and every later archive of that version would differ too.
    /// The two converge on a flag. *When* it died is the `dead` log's job,
    /// which is keyed by exactly that instant and is what a catch-up reads.
    ///
    /// It survives an ordinary [`save`], which is what keeps this field and the
    /// index agreeing: writing to a removed anchor does not resurrect it. Only
    /// the path that deliberately makes a non-living anchor live again clears
    /// it (`SavePlan::revives`, a `#[wavedb::key]` upsert or a mirrored
    /// revival).
    ///
    /// [`save`]: crate::Collection::save
    pub removed: bool,
    /// Owning Pivot back-link (`None` = Unique record).
    pub pivot_id: Option<LocalId>,
    /// Who wrote this version.
    pub user: U48,
    /// Which device produced it.
    pub device_created: u64,
    /// Access rule; `None` = tenant-only (the common case).
    pub permission: Option<PermissionRef>,
}

impl Metadata {
    /// Whether this record is still in the collection's living set.
    ///
    /// The anchor answers it, so a caller that already holds the record has the
    /// answer without touching an index — see [`removed`](Self::removed).
    #[must_use]
    pub const fn is_live(&self) -> bool {
        !self.removed
    }
}

// `WaveWire` is derived field-by-field in declaration order: `Option<u64>` (1
// stack byte) + `Succession` (9) + `bool` (1) + `Option<LocalId>` (1) +
// `U48` (6) + `u64` (8) + `Option<PermissionRef>` (1) = 27-byte stack; heap
// grows only for the `Some` fields.

#[cfg(test)]
mod tests {
    use super::{Metadata, Succession};
    use crate::local_id::LocalId;
    use crate::permission::PermissionRef;
    use crate::u48::U48;
    use crate::wire::{WaveWire, from_wire, to_wire};

    fn roundtrip(m: &Metadata) {
        let bytes = to_wire(m);
        assert_eq!(bytes.len(), Metadata::STACK_SIZE + m.heap_size());
        assert_eq!(from_wire::<Metadata>(&bytes).expect("decode"), *m);
    }

    #[test]
    fn default_is_tenant_only_first_version() {
        let m = Metadata::default();
        assert!(m.previous.is_none());
        assert_eq!(m.succession, Succession::CreatedAt(0));
        assert!(m.pivot_id.is_none());
        assert_eq!(m.user, U48::ZERO);
        assert_eq!(m.permission, None);
        roundtrip(&m);
    }

    #[test]
    fn full_roundtrip() {
        roundtrip(&Metadata {
            previous: Some(1_700_000_000_000_000_007),
            succession: Succession::CreatedAt(1_700_000_000_000_000_042),
            pivot_id: Some(LocalId::new(0xABCD, true, 3)),
            user: U48::from(42u32),
            device_created: 0xCAFE,
            removed: false,
            permission: Some(PermissionRef::Tenants(vec![
                U48::from(1u32),
                U48::from(2u32),
            ])),
        });
        roundtrip(&Metadata {
            previous: None,
            succession: Succession::Next(99),
            pivot_id: None,
            user: U48::MAX,
            device_created: 1,
            removed: true,
            permission: Some(PermissionRef::Public),
        });
    }

    #[test]
    fn liveness_reads_off_the_record_itself() {
        // No index consulted: the anchor's own metadata answers it, which is
        // what lets RFC 0050 delete the `current` tree.
        let mut m = Metadata::default();
        assert!(m.is_live());
        m.removed = true;
        assert!(!m.is_live());
        roundtrip(&m);
    }

    #[test]
    fn unique_first_version_is_minimal() {
        // Unique first version: all Option fields None → stack=27, heap=0.
        let m = Metadata::default();
        assert_eq!(Metadata::STACK_SIZE, 27);
        assert_eq!(m.heap_size(), 0);
        assert_eq!(to_wire(&m).len(), 27);
    }

    #[test]
    fn succession_is_nine_fixed_bytes() {
        for s in [Succession::CreatedAt(7), Succession::Next(u64::MAX)] {
            let bytes = to_wire(&s);
            assert_eq!(bytes.len(), 9);
            assert_eq!(from_wire::<Succession>(&bytes), Ok(s));
            assert_eq!(
                s.instant(),
                if s == Succession::CreatedAt(7) {
                    7
                } else {
                    u64::MAX
                }
            );
        }
    }

    #[test]
    fn succession_bad_tag_is_invalid_tag() {
        let mut bytes = to_wire(&Succession::CreatedAt(1));
        bytes[0] = 9;
        assert_eq!(
            from_wire::<Succession>(&bytes),
            Err(wavedb_wire::Error::InvalidTag(9))
        );
    }
}
