//! Schema-evolution lookup hooks — `first_try` and `fallback_not_found`.
//!
//! There is **no migration chain, no auto-upgrade walk**. A schema change just
//! yields a new `STRUCT_HASH`; bridging old and new records is the application's
//! job, through two optional async hooks the `#[wavedb]` macro wires up per struct.
//! Both default to `Ok(None)` (no bridging), so a struct that needs neither pays
//! nothing.
//!
//! The hooks are generic over the client handle `Db` because core does not — and
//! must not — name it; the macro resolves `Db` at the call site.

use crate::error::Result;

/// Optional pre-/post-search hooks for bridging records written under a previous
/// `STRUCT_HASH`. Implemented per struct by `#[wavedb]`; both methods default to
/// "no bridging".
pub trait LookupHooks<Db>: Sized {
    /// Runs **before** the storage search. Return `Some(value)` to short-circuit —
    /// e.g. synthesise this type from a record stored under an older
    /// `STRUCT_HASH`. `None` lets the normal lookup proceed.
    async fn first_try(db: &Db) -> Result<Option<Self>> {
        let _ = db;
        Ok(None)
    }

    /// Runs **after** the search misses. The place to fetch, derive a default, or
    /// lift an old record forward. `None` means "genuinely absent".
    async fn fallback_not_found(db: &Db) -> Result<Option<Self>> {
        let _ = db;
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::LookupHooks;
    use crate::error::Result;

    struct DummyDb;

    // A type that takes the default (no-bridging) hooks.
    struct Plain;
    impl LookupHooks<DummyDb> for Plain {}

    // A type that overrides `fallback_not_found` to supply a default.
    #[derive(Debug, PartialEq, Eq)]
    struct WithFallback(u32);
    // A test double, written in the trait's own (async) shape.
    #[allow(clippy::unused_async_trait_impl)]
    impl LookupHooks<DummyDb> for WithFallback {
        async fn fallback_not_found(_db: &DummyDb) -> Result<Option<Self>> {
            Ok(Some(Self(42)))
        }
    }

    #[test]
    fn defaults_return_none() {
        futures::executor::block_on(async {
            assert!(Plain::first_try(&DummyDb).await.unwrap().is_none());
            assert!(
                Plain::fallback_not_found(&DummyDb).await.unwrap().is_none()
            );
        });
    }

    #[test]
    fn override_supplies_default() {
        futures::executor::block_on(async {
            assert!(WithFallback::first_try(&DummyDb).await.unwrap().is_none());
            assert_eq!(
                WithFallback::fallback_not_found(&DummyDb).await.unwrap(),
                Some(WithFallback(42))
            );
        });
    }
}
