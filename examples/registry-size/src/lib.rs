//! The registry-size measuring stick — the M1 risk item ("does the exposure
//! `match` grow the wasm binary per struct?"), measured in M5.
//!
//! Sixty-four `#[wavedb]` structs are always **defined**; how many the
//! `expose_client!` list **names** is feature-selected (1 / `n16` / `n64`).
//! `scripts/registry_size.sh` builds the three widths for
//! wasm32-unknown-unknown and reports the marginal bytes per exposed struct
//! — the delta isolates exactly what one more exposure line costs (the
//! `match` arms plus the retained `WaveWire` decode), because unexposed
//! definitions are dead code the linker drops.
//!
//! The structs cycle four field shapes so structurally identical decode fns
//! don't collapse into one by LLVM function merging (real schemas are
//! heterogeneous). All are Unique — a NonUnique adds its generated
//! Pivot/Chain machinery on top, which is schema code, not registry cost.

use wavedb::prelude::*;

/// Define one measured struct per `name : extra-field-type` pair.
macro_rules! items {
    ($($name:ident : $extra:ty),* $(,)?) => {$(
        #[wavedb]
        #[derive(Debug, Clone, PartialEq, Eq)]
        pub struct $name {
            pub label: String,
            pub value: u64,
            pub extra: $extra,
        }
    )*};
}

items! {
    S00: u8, S01: u32, S02: bool, S03: String,
    S04: u8, S05: u32, S06: bool, S07: String,
    S08: u8, S09: u32, S10: bool, S11: String,
    S12: u8, S13: u32, S14: bool, S15: String,
    S16: u8, S17: u32, S18: bool, S19: String,
    S20: u8, S21: u32, S22: bool, S23: String,
    S24: u8, S25: u32, S26: bool, S27: String,
    S28: u8, S29: u32, S30: bool, S31: String,
    S32: u8, S33: u32, S34: bool, S35: String,
    S36: u8, S37: u32, S38: bool, S39: String,
    S40: u8, S41: u32, S42: bool, S43: String,
    S44: u8, S45: u32, S46: bool, S47: String,
    S48: u8, S49: u32, S50: bool, S51: String,
    S52: u8, S53: u32, S54: bool, S55: String,
    S56: u8, S57: u32, S58: bool, S59: String,
    S60: u8, S61: u32, S62: bool, S63: String,
}

// One registry per feature width — the cfgs are mutually exclusive, so
// exactly one `CLIENT_REGISTRY` exists per build.

#[cfg(not(feature = "n16"))]
wavedb::expose_client! { S00 }

#[cfg(all(feature = "n16", not(feature = "n64")))]
wavedb::expose_client! {
    S00, S01, S02, S03, S04, S05, S06, S07,
    S08, S09, S10, S11, S12, S13, S14, S15,
}

#[cfg(feature = "n64")]
wavedb::expose_client! {
    S00, S01, S02, S03, S04, S05, S06, S07,
    S08, S09, S10, S11, S12, S13, S14, S15,
    S16, S17, S18, S19, S20, S21, S22, S23,
    S24, S25, S26, S27, S28, S29, S30, S31,
    S32, S33, S34, S35, S36, S37, S38, S39,
    S40, S41, S42, S43, S44, S45, S46, S47,
    S48, S49, S50, S51, S52, S53, S54, S55,
    S56, S57, S58, S59, S60, S61, S62, S63,
}

/// The export that anchors the registry into the wasm artifact: both checks
/// take a **runtime** hash, so every `match` arm — and each arm's decode —
/// must survive dead-code elimination, exactly like a real client build.
#[cfg(target_arch = "wasm32")]
mod probe {
    use wasm_bindgen::prelude::*;
    use wavedb_core::expose::Exposure as _;

    /// Bit 0: the registry knows `struct_hash`; bit 1: `payload` decodes as
    /// its declared body.
    #[wasm_bindgen]
    #[must_use]
    pub fn registry_probe(struct_hash: u64, payload: &[u8]) -> u32 {
        let knows = crate::CLIENT_REGISTRY.knows(struct_hash);
        let decodes = crate::CLIENT_REGISTRY
            .decode_check(struct_hash, payload)
            .is_ok();
        u32::from(knows) | (u32::from(decodes) << 1)
    }
}

#[cfg(test)]
mod tests {
    use wavedb_core::expose::Exposure as _;

    #[test]
    fn the_declared_width_is_reachable_and_nothing_else() {
        assert!(super::CLIENT_REGISTRY.knows(super::S00::STRUCT_HASH));
        assert_eq!(
            super::CLIENT_REGISTRY.knows(super::S15::STRUCT_HASH),
            cfg!(feature = "n16"),
        );
        assert_eq!(
            super::CLIENT_REGISTRY.knows(super::S63::STRUCT_HASH),
            cfg!(feature = "n64"),
        );
    }
}
