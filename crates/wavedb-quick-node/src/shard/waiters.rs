//! The waiters behind [`OwnerLocks`](super::OwnerLocks): one thread's table of
//! held owners, each with its **chain** of waiting operations.
//!
//! The owner keeps only the chain's head and tail; each waiter's slot links to
//! the next. Links are slot indices into a fixed array rather than pointers
//! between the waiting futures, which would take `unsafe`. Every size here is
//! fixed at compile time, from the job pool: nothing allocates per wait.

use std::task::Waker;

use heapless::index_map::FnvIndexMap;

use super::OwnerKey;
use crate::__rt::MAX_JOBS;

/// Operations one thread can have waiting for owners at once. Each is a job:
/// at most the whole job pool, plus the one a shard runs in its receive loop
/// when the pool is full.
pub const MAX_WAITERS: usize = MAX_JOBS + 1;

/// Owners one thread can hold at once — no more than it has operations. A
/// power of two because the map is one.
const MAX_HELD: usize = MAX_WAITERS.next_power_of_two();

// Slot indices are `u16`.
const _: () = assert!(MAX_WAITERS <= u16::MAX as usize);

/// A waiter's slot index.
pub(super) type Link = Option<u16>;

/// One waiting operation.
#[derive(Default)]
struct Slot {
    waker: Option<Waker>,
    /// The next waiter on the same owner — or, for a free slot, the next
    /// free one.
    next: Link,
}

/// One held owner's chain of waiters.
#[derive(Default, Clone, Copy)]
struct Chain {
    head: Link,
    tail: Link,
    /// The waiter the lock was handed to, until it wakes and takes it.
    granted: Link,
}

/// What a waiter found when it asked.
pub(super) enum Asked {
    Held,
    Waiting(u16),
    /// No slot free. Cannot happen while every waiter is a job (see
    /// [`MAX_WAITERS`]); the waiter asks again on its next poll.
    Full,
}

pub(super) struct Table {
    /// Boxed: ~50 KB, allocated once with the thread rather than carried by
    /// value in whatever owns the table.
    held: Box<FnvIndexMap<OwnerKey, Chain, MAX_HELD>>,
    slots: Box<[Slot]>,
    /// Head of the free slots' chain.
    free: Link,
}

impl Table {
    pub(super) fn new() -> Self {
        let slots = (0..MAX_WAITERS)
            .map(|i| Slot {
                waker: None,
                next: u16::try_from(i + 1)
                    .ok()
                    .filter(|&n| usize::from(n) < MAX_WAITERS),
            })
            .collect();
        Self {
            held: Box::new(FnvIndexMap::new()),
            slots,
            free: (MAX_WAITERS > 0).then_some(0),
        }
    }

    /// Owners held right now.
    pub(super) fn held(&self) -> usize {
        self.held.len()
    }

    /// A first poll: take the owner if it is free, or join its chain.
    pub(super) fn ask(&mut self, key: OwnerKey, waker: &Waker) -> Asked {
        let Some(chain) = self.held.get_mut(&key) else {
            return match self.held.insert(key, Chain::default()) {
                Ok(_) => Asked::Held,
                Err(_) => Asked::Full,
            };
        };
        let Some(slot) = take(&mut self.slots, &mut self.free, waker) else {
            return Asked::Full;
        };
        match chain.tail.and_then(|t| self.slots.get_mut(usize::from(t))) {
            Some(last) => last.next = Some(slot),
            None => chain.head = Some(slot),
        }
        chain.tail = Some(slot);
        Asked::Waiting(slot)
    }

    /// A later poll of the waiter in `slot`: is the lock ours yet?
    pub(super) fn check(
        &mut self,
        key: OwnerKey,
        slot: u16,
        waker: &Waker,
    ) -> bool {
        let granted = match self.held.get_mut(&key) {
            Some(chain) if chain.granted == Some(slot) => {
                chain.granted = None;
                true
            }
            Some(_) => {
                // Woken spuriously: keep the newest waker.
                if let Some(s) = self.slots.get_mut(usize::from(slot)) {
                    s.waker = Some(waker.clone());
                }
                return false;
            }
            // Cannot happen — a waiter keeps its owner held — but a free
            // owner is ours to take.
            None => self.held.insert(key, Chain::default()).is_ok(),
        };
        give_back(&mut self.slots, &mut self.free, slot);
        granted
    }

    /// The waiter in `slot` gave up. `true` when it had already been handed
    /// the lock, which its caller must then pass on.
    pub(super) fn leave(&mut self, key: OwnerKey, slot: u16) -> bool {
        let handed = self.held.get_mut(&key).is_some_and(|chain| {
            if chain.granted == Some(slot) {
                chain.granted = None;
                return true;
            }
            unlink(chain, &mut self.slots, slot);
            false
        });
        give_back(&mut self.slots, &mut self.free, slot);
        handed
    }

    /// Hand `key` to the head of its chain, or free it when nobody waits.
    /// Returns the waker to wake, outside the table's borrow.
    pub(super) fn release(&mut self, key: OwnerKey) -> Option<Waker> {
        let chain = self.held.get_mut(&key)?;
        let Some(head) = chain.head else {
            self.held.remove(&key);
            return None;
        };
        let slot = self.slots.get_mut(usize::from(head))?;
        chain.head = slot.next.take();
        if chain.head.is_none() {
            chain.tail = None;
        }
        chain.granted = Some(head);
        slot.waker.take()
    }
}

/// Take a free slot for `waker`.
fn take(slots: &mut [Slot], free: &mut Link, waker: &Waker) -> Link {
    let index = (*free)?;
    let slot = slots.get_mut(usize::from(index))?;
    *free = slot.next.take();
    slot.waker = Some(waker.clone());
    Some(index)
}

/// Return `index` to the free chain.
fn give_back(slots: &mut [Slot], free: &mut Link, index: u16) {
    if let Some(slot) = slots.get_mut(usize::from(index)) {
        slot.waker = None;
        slot.next = free.replace(index);
    }
}

/// Remove `index` from `chain`. Walks from the head: only a waiter that gives
/// up pays it, and nothing on the normal path does.
fn unlink(chain: &mut Chain, slots: &mut [Slot], index: u16) {
    let next_of =
        |slots: &[Slot], i: u16| slots.get(usize::from(i)).and_then(|s| s.next);
    let (mut prev, mut cur) = (None, chain.head);
    while let Some(at) = cur {
        if at == index {
            break;
        }
        (prev, cur) = (Some(at), next_of(slots, at));
    }
    if cur != Some(index) {
        return; // not queued
    }
    let next = next_of(slots, index);
    match prev.and_then(|p| slots.get_mut(usize::from(p))) {
        Some(before) => before.next = next,
        None => chain.head = next,
    }
    if chain.tail == Some(index) {
        chain.tail = prev;
    }
}
