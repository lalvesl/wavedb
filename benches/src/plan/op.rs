//! What the generator produces and a consumer executes ([RFC 0065] §3).
//!
//! One value here is **one timed window**: the unit `lat.time` wraps, and the
//! unit a latency percentile is a percentile of. That is why the composed shop
//! phases are single variants rather than several — `order_detail` times a user
//! lookup, an order lookup and an item page *together*, because that is what a
//! customer waits for, and splitting it into three ops would report three
//! numbers nobody waits on.
//!
//! ## Three rules the shape follows
//!
//! **The op carries materialised data, never seed coordinates.** An
//! `Insert` holds a built [`Thing`], not the `(n, seed)` to build one from. If
//! it held the coordinates the consumer would call `thing(n, seed)` itself and
//! the generator thread would be an empty ceremony — generation would be back
//! on the timed thread, serialised with the operation it feeds, which is the
//! arrangement RFC 0065 §2 exists to end.
//!
//! **The op carries the logical key, never a system's id.** `n` is the row
//! number in the dataset. PostgreSQL, MySQL, SQLite and MongoDB use it
//! directly as the primary key / `_id`; WaveDB indexes its minted-anchor table
//! with it (`ids[n]`), because a NonUnique anchor is clock-minted at insert and
//! cannot be recomputed. Putting a system's id in the op would make one
//! generator per system, and then the five rows would no longer be executing
//! the same workload.
//!
//! **Ids that used to be running counters are minted here.** The SQLite shop
//! adapter kept `item_id` mutable inside the phase closure and incremented it
//! per line item; with more than one consumer that is a race. The generator
//! hands out `first_item` and the consumer numbers its own items from it, so
//! the assignment is deterministic and belongs to whoever produced the op.
//!
//! [RFC 0065]: ../../../rfcs/0065-benchmark-suite-ii-the-row-as-the-unit.md

use crate::schema::Thing;
use crate::shop::{ProductRow, ShoppingRow, UserRow};

/// The three-operation workload, one flat type in one collection.
///
/// Every variant names the same `n` the dataset is generated from, so a
/// consumer's whole job is `n → my id → my call`.
pub enum MicroOp {
    /// Append row `n`. The dataset's inserts are sequential, so `n` is also
    /// the order the generator emits them in.
    Insert { n: u64, row: Thing },
    /// Point-read row `n` and assert it came back.
    Read { n: u64 },
    /// Rewrite row `n` **whole** — never a field patch. WaveDB writes whole
    /// records, and a partial update would flatter the others for free.
    Update { n: u64, row: Thing },
}

/// The e-commerce workload, five composed operations.
///
/// Every variant carries `u`, and that is not incidental: `u` is the user, the
/// user is a tenant, and a tenant's `Shopping` collection is one Pivot — so `u`
/// **is** the partition key ([`partition_key`](Self::partition_key)).
pub enum ShopOp {
    /// Create user `u` and whatever collection linkage the system needs.
    Signup { u: u64, row: UserRow },
    /// One order and all of its line items.
    ///
    /// The SQL trio and MongoDB wrap this in one transaction for one commit
    /// barrier; WaveDB has no multi-record transaction, so it pays one batch
    /// per record. Both are correct, the difference is the measurement, and
    /// neither is visible from here — an op says *what*, a driver says *how*.
    Checkout {
        u: u64,
        /// The order's index within this user's history.
        s: u64,
        /// The order's own id, minted by the generator.
        order: u64,
        /// The first line item's id; the consumer numbers the rest upward.
        first_item: u64,
        row: ShoppingRow,
        items: Vec<ProductRow>,
    },
    /// Point-read user `u`'s own record.
    Profile { u: u64 },
    /// Render page `page` of user `u`'s order history — the user, then ten of
    /// their orders. One segment read on WaveDB ([RFC 0051]), an
    /// `ORDER BY … LIMIT 10 OFFSET n` on the other four.
    ///
    /// [RFC 0051]: ../../../rfcs/0051-ordered-record-lists.md
    OrderPage { u: u64, page: usize },
    /// Open user `u`'s first order — the user, the order, then its items.
    OrderDetail { u: u64 },
}

impl MicroOp {
    /// The dataset row this op is about.
    #[must_use]
    pub const fn key(&self) -> u64 {
        match self {
            Self::Insert { n, .. }
            | Self::Read { n }
            | Self::Update { n, .. } => *n,
        }
    }

    /// Which consumer owns this op, out of `consumers`.
    ///
    /// **Always zero, and that is a finding rather than a stub.** The micro
    /// workload lives in exactly one collection (`systems/wavedb.rs` mints one
    /// `Thing::create_pivot`), and a collection is indivisible: its B+tree
    /// nodes and chain segments carry ids of their own and belong to the
    /// Pivot's owner, so two consumers writing disjoint *records* would still
    /// contend on shared *structure* — silent index loss, not a cache miss
    /// (`wavedb-quick-node/src/shard/route.rs` states the rule).
    ///
    /// So `wavedb/multi` is a `shop` row only, and this method is where that
    /// is enforced instead of being a comment in a plan. A run that asks for
    /// more than one consumer on `micro` gets them all fed from one queue,
    /// which the planner refuses earlier — this is the backstop.
    #[must_use]
    pub const fn partition_key(&self) -> u64 {
        0
    }
}

impl ShopOp {
    /// The user this op belongs to — its partition key, and its tenant.
    #[must_use]
    pub const fn partition_key(&self) -> u64 {
        match self {
            Self::Signup { u, .. }
            | Self::Checkout { u, .. }
            | Self::Profile { u }
            | Self::OrderPage { u, .. }
            | Self::OrderDetail { u } => *u,
        }
    }

    /// The phase name this op belongs to, matching
    /// [`systems::shop::PHASES`](crate::systems::shop::PHASES).
    #[must_use]
    pub const fn phase(&self) -> &'static str {
        match self {
            Self::Signup { .. } => "signup",
            Self::Checkout { .. } => "checkout",
            Self::Profile { .. } => "profile",
            Self::OrderPage { .. } => "order_page",
            Self::OrderDetail { .. } => "order_detail",
        }
    }
}

/// Everything that crosses into a consumer thread must be `Send`; nothing a
/// consumer *holds* has to be.
///
/// Structural rather than asserted in prose: both ops are plain data, so a
/// consumer's `Rc`-shaped state (a `ShardStore`, a `Collection` handle, an
/// in-flight non-`Send` future) stays free to be exactly that. This is the
/// same split `wavedb-quick-node/src/shard/msg.rs` makes for the node.
const _: fn() = || {
    const fn assert_send<T: Send>() {}
    assert_send::<MicroOp>();
    assert_send::<ShopOp>();
};

#[cfg(test)]
mod tests {
    use super::{MicroOp, ShopOp};
    use crate::schema::thing;
    use crate::shop::{ShoppingRow, user_row};

    fn shopping() -> ShoppingRow {
        ShoppingRow {
            bought_at: 1,
            discount_cents: 0,
            transport_cents: 0,
        }
    }

    #[test]
    fn a_micro_op_reports_the_row_it_is_about() {
        assert_eq!(MicroOp::Read { n: 42 }.key(), 42);
        assert_eq!(
            MicroOp::Insert {
                n: 7,
                row: thing(7, 1)
            }
            .key(),
            7
        );
        assert_eq!(
            MicroOp::Update {
                n: 9,
                row: thing(9, 1)
            }
            .key(),
            9
        );
    }

    /// The micro workload is one collection, so every op belongs to one
    /// consumer. If this ever varies, `wavedb/multi` has silently become a
    /// micro row and the index is at risk.
    #[test]
    fn every_micro_op_partitions_to_the_same_consumer() {
        for n in 0..1000 {
            assert_eq!(MicroOp::Read { n }.partition_key(), 0);
        }
    }

    #[test]
    fn a_shop_op_partitions_by_its_user() {
        assert_eq!(ShopOp::Profile { u: 17 }.partition_key(), 17);
        assert_eq!(ShopOp::OrderPage { u: 3, page: 1 }.partition_key(), 3);
        assert_eq!(
            ShopOp::Checkout {
                u: 5,
                s: 0,
                order: 100,
                first_item: 500,
                row: shopping(),
                items: Vec::new(),
            }
            .partition_key(),
            5
        );
    }

    /// A user's whole session lands on one consumer — the property that makes
    /// the shard's absence cache sound.
    #[test]
    fn one_user_never_spans_two_consumers() {
        let u = 12;
        let ops = [
            ShopOp::Signup {
                u,
                row: user_row(u, 1),
            },
            ShopOp::Profile { u },
            ShopOp::OrderPage { u, page: 0 },
            ShopOp::OrderDetail { u },
        ];
        for op in &ops {
            assert_eq!(op.partition_key(), u, "{} escaped", op.phase());
        }
    }

    #[test]
    fn phase_names_match_the_report_order() {
        assert_eq!(
            ShopOp::Signup {
                u: 0,
                row: user_row(0, 1)
            }
            .phase(),
            crate::systems::shop::PHASES[0]
        );
        assert_eq!(
            ShopOp::OrderDetail { u: 0 }.phase(),
            crate::systems::shop::PHASES[4]
        );
    }
}
