//! Leases: owners a `#[server]` body holds across its awaits ([RFC 0067]
//! §5).
//!
//! Each operation a body runs takes its owner's lock for itself, so two of
//! them on one owner are each atomic and nothing spans both. A body that has
//! to read, decide and write as one step **locks** the owners first:
//!
//! ```ignore
//! let held = db.lock(&[Owner::unique::<Profile>(db.tenant())]).await?;
//! let mut p = Profile::get(db).await?.unwrap_or_default();
//! p.visits += 1;
//! p.save(db).await?;     // under the lease: nobody ran in between
//! drop(held);            // released; dropping cannot fail
//! ```
//!
//! Operations on a held owner run under its lease automatically — the
//! handle knows what its body holds — so the body's own operations never
//! queue behind the lock it holds.
//!
//! ## The rules, and why they are enforced rather than advised
//!
//! A body holding an owner and waiting for another is half of a deadlock;
//! another body doing the same in the other order is the other half. So:
//!
//! - **One `lock` takes every owner the body needs**, in one total order
//!   ([`Owner`]'s), whoever asks — two bodies wanting overlapping sets never
//!   hold one each. A second `lock` while one is held is refused.
//! - **While a lock is held, the body stays inside it.** An operation on an
//!   owner it does not hold would wait for that owner's lock — perhaps held
//!   by a body waiting for one of ours — so it is refused instead. The
//!   exception is a collection the body **creates** under the lock: its
//!   Pivot was minted here, nobody else can name it until the body links it
//!   somewhere, so reaching it cannot wait on anyone.
//!
//! Both refusals are [`Error::LockOrder`]: a programming fault in the body,
//! reported to its caller, never a hang.
//!
//! [RFC 0067]: ../../../rfcs/0067-thread-per-core-shards-WIP.md

use std::cell::{Cell, RefCell};

use crate::error::{Error, Result};
use crate::placement::Owner;
use crate::u48::U48;

/// The owners one body holds, and the lease reaching each. One per body
/// invocation: the generated dispatch builds it and lends it to the body's
/// handle, so every copy of that handle sees the same set.
pub struct Leases<L> {
    held: RefCell<Vec<Held<L>>>,
    /// A `lock` is in progress or held — including while its leases are
    /// still being acquired, so two `lock` calls cannot interleave.
    locked: Cell<bool>,
}

struct Held<L> {
    tenant: U48,
    owner: Owner,
    /// `None`: a collection created under the lock, reached plainly.
    lease: Option<L>,
}

/// How an operation reaches its owner, given what the body holds.
pub(crate) enum Reach<L> {
    /// No lock is held: the operation takes its owner's lock for itself.
    Plain,
    /// Under the lease holding its owner.
    Leased(L),
    /// On a collection the body created under its lock.
    Created,
    /// Past what the body holds.
    Refused,
}

impl<L> Default for Leases<L> {
    fn default() -> Self {
        Self::new()
    }
}

impl<L> Leases<L> {
    /// No owner held.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            held: RefCell::new(Vec::new()),
            locked: Cell::new(false),
        }
    }

    /// Start a `lock`.
    ///
    /// # Errors
    /// [`Error::LockOrder`] when one is already held or being taken.
    pub(crate) fn begin(&self) -> Result<Locked<'_, L>> {
        if self.locked.replace(true) {
            return Err(Error::LockOrder(
                "a lock is already held: take every owner in one lock",
            ));
        }
        Ok(Locked { leases: self })
    }

    /// Record `owner`, reached under `tenant`, as held through `lease`.
    pub(crate) fn hold(&self, tenant: U48, owner: Owner, lease: L) {
        self.held.borrow_mut().push(Held {
            tenant,
            owner,
            lease: Some(lease),
        });
    }

    /// Record a collection created under the lock.
    pub(crate) fn created(&self, tenant: U48, owner: Owner) {
        if self.locked.get() {
            self.held.borrow_mut().push(Held {
                tenant,
                owner,
                lease: None,
            });
        }
    }
}

impl<L: Clone> Leases<L> {
    /// How an operation on `owner` under `tenant` runs; `creates` says it
    /// creates that owner.
    pub(crate) fn reach(
        &self,
        tenant: U48,
        owner: Owner,
        creates: bool,
    ) -> Reach<L> {
        if !self.locked.get() {
            return Reach::Plain;
        }
        let held = self.held.borrow();
        match held.iter().find(|h| h.tenant == tenant && h.owner == owner) {
            Some(Held {
                lease: Some(lease), ..
            }) => Reach::Leased(lease.clone()),
            Some(Held { lease: None, .. }) => Reach::Created,
            None if creates => Reach::Created,
            None => Reach::Refused,
        }
    }
}

impl<L> std::fmt::Debug for Leases<L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Leases")
            .field("held", &self.held.borrow().len())
            .field("locked", &self.locked.get())
            .finish()
    }
}

/// The owners one `lock` took. Dropping it releases them all — each lease
/// goes with it — and ends the lock, so the body may take another.
#[must_use = "the owners are released as soon as this drops"]
pub struct Locked<'a, L> {
    leases: &'a Leases<L>,
}

impl<L> Drop for Locked<'_, L> {
    fn drop(&mut self) {
        self.leases.held.borrow_mut().clear();
        self.leases.locked.set(false);
    }
}

impl<L> std::fmt::Debug for Locked<'_, L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Locked")
            .field("leases", self.leases)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::{Leases, Reach};
    use crate::error::Error;
    use crate::placement::Owner;
    use crate::{LocalId, U48};

    const T: U48 = U48::MAX;

    fn pivot(key: u64) -> Owner {
        Owner::Collection(LocalId::new(key, false, 1))
    }

    #[test]
    fn nothing_held_reaches_plainly() {
        let leases = Leases::<u8>::new();
        assert!(matches!(leases.reach(T, pivot(1), false), Reach::Plain));
    }

    #[test]
    fn a_held_owner_is_reached_through_its_lease() {
        let leases = Leases::new();
        let locked = leases.begin().unwrap();
        leases.hold(T, pivot(1), 7u8);
        assert!(matches!(leases.reach(T, pivot(1), false), Reach::Leased(7)));
        drop(locked);
        assert!(matches!(leases.reach(T, pivot(1), false), Reach::Plain));
    }

    /// Reaching past the held owners would wait while holding: refused.
    #[test]
    fn an_owner_outside_the_lock_is_refused() {
        let leases = Leases::new();
        let _locked = leases.begin().unwrap();
        leases.hold(T, pivot(1), 7u8);
        assert!(matches!(leases.reach(T, pivot(2), false), Reach::Refused));
        // The same pivot under another tenant is another owner.
        let other = U48::from(3u32);
        assert!(matches!(
            leases.reach(other, pivot(1), false),
            Reach::Refused
        ));
    }

    /// A collection created under the lock stays reachable after creation.
    #[test]
    fn a_collection_created_under_the_lock_stays_reachable() {
        let leases = Leases::new();
        let _locked = leases.begin().unwrap();
        leases.hold(T, pivot(1), 7u8);
        assert!(matches!(leases.reach(T, pivot(9), true), Reach::Created));
        leases.created(T, pivot(9));
        assert!(matches!(leases.reach(T, pivot(9), false), Reach::Created));
    }

    #[test]
    fn a_second_lock_while_one_is_held_is_refused() {
        let leases = Leases::<u8>::new();
        let locked = leases.begin().unwrap();
        assert!(matches!(leases.begin(), Err(Error::LockOrder(_))));
        drop(locked);
        assert!(leases.begin().is_ok(), "released: a new lock may start");
    }
}
