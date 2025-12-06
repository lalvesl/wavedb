//! [`Lane`] — which ordered lane of a type a chain is a range of
//! ([RFC 0078] §7).
//!
//! [RFC 0078]: https://github.com/wavedb/wavedb/blob/main/rfcs/0078-ordered-lanes-over-a-quantized-page-index-WIP.md

/// Which ordered lane of a user type a chain lives in.
///
/// One lane per kind of chain per user type, so a page stays homogeneous
/// and the lane's zstd dictionary models one kind of content. Every
/// collection of the type shares its lanes: a collection's chain is the key
/// range under its Pivot, and a declaration's under the Pivot and its
/// index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lane {
    /// **Declared list** chains ([RFC 0051]) — the only entries that carry
    /// records. Value = the record's stored envelope, verbatim.
    ///
    /// [RFC 0051]: https://github.com/wavedb/wavedb/blob/main/rfcs/0051-ordered-record-lists.md
    Records,
    /// The **recency** chain: one entry per living record, keyed by its live
    /// version's instant. No value.
    Recency,
    /// The removal log. No value.
    Dead,
    /// `#[wavedb::pivot(...)]` secondary chains ([RFC 0075]): one
    /// `[field][anchor]` key per record and no value.
    ///
    /// [RFC 0075]: https://github.com/wavedb/wavedb/blob/main/rfcs/0075-secondary-indexes-as-chains.md
    Secondary,
    /// `#[wavedb::fuzzy]` posting chains ([RFC 0075]): one
    /// `[gram][len][anchor]` key per gram of a record's value, and no value.
    ///
    /// [RFC 0075]: https://github.com/wavedb/wavedb/blob/main/rfcs/0075-secondary-indexes-as-chains.md
    Fuzzy,
}

impl Lane {
    /// This lane's reserved hash for the user type `struct_hash`.
    ///
    /// Derived, not declared, so no schema carries it: SeaHash (the pinned
    /// identity hash) over a per-lane tag and the type's own hash. A real
    /// struct hashing to the same value is a 2⁻⁶⁴ event.
    #[must_use]
    pub fn hash(self, struct_hash: u64) -> u64 {
        let tag: &[u8] = match self {
            Self::Records => b"WDB.SEG",
            Self::Recency => b"WDB.REC",
            Self::Dead => b"WDB.DEAD",
            Self::Secondary => b"WDB.SEC",
            Self::Fuzzy => b"WDB.FZY",
        };
        let mut bytes = tag.to_vec();
        bytes.extend_from_slice(&struct_hash.to_le_bytes());
        seahash::hash(&bytes)
    }
}
