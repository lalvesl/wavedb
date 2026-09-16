//! WaveDB in the shop workload — **one tenant per user**, and the only row
//! that can run more than one consumer.
//!
//! ## Why this workload partitions and `micro` does not
//!
//! The tenant is not a column here: it is 48 bits of the `Id`, so each user's
//! data is a disjoint region of one key space and `User::get(&db)` needs no
//! key at all. A user is a tenant is one `Shopping` collection, so routing by
//! user routes by **Pivot instance** — [RFC 0064]'s unit of concurrency.
//!
//! That disjointness is a correctness condition, not an optimisation.
//! `ShardStore` memoises absence, which is sound only while a record is
//! reached by exactly one holder; two consumers sharing a tenant would keep
//! serving a `None` after the other's insert filled it. The generator
//! partitions by user and never sends one user's work to two consumers, and
//! [`Factory::route`] is that rule.
//!
//! ## A checkout is six batches, not one
//!
//! The SQL trio wraps a checkout in one transaction and pays one barrier.
//! WaveDB has no multi-record transaction: the line-item Pivot, the order and
//! each item are their own atomic `Store::apply`. That is the data model, and
//! pricing it is the reason this workload exists.
//!
//! [RFC 0064]: ../../../../rfcs/0064-pivot-owned-concurrency-PLANNED.md

use futures::TryStreamExt as _;
use futures::executor::block_on;
use wavedb_core::{LocalHandle, Store, U48};
use wavedb_storage::PageStore;

use crate::harness::{Driver, DriverFactory};
use crate::plan::op::ShopOp;
use crate::shop::{
    PAGE, Product, ProductLists as _, Shopping, ShoppingLists as _, User,
};

use super::SETTLE_AFTER;
use super::engine::ShopEngine;

/// Journal bytes above which a consumer checkpoints, between operations.
const CHECKPOINT_AFTER_BYTES: u64 = 64 << 20;

/// Cache budget the checkpoint evicts down to.
const CACHE_BUDGET_BYTES: usize = 96 << 20;

/// What a consumer needs to build its engine.
pub struct Factory<E: ShopEngine> {
    shared: E::Shared,
    consumers: usize,
}

impl<E: ShopEngine> Factory<E> {
    /// # Errors
    /// The shared engine could not be started.
    pub fn new(store: PageStore, asked: usize) -> Result<Self, String> {
        let consumers = E::consumers(asked);
        Ok(Self {
            shared: E::share(store, consumers)?,
            consumers,
        })
    }

    /// The shared half, so the row can shut the actor down after the last
    /// consumer has closed.
    pub const fn shared(&self) -> &E::Shared {
        &self.shared
    }
}

impl<E: ShopEngine> DriverFactory for Factory<E>
where
    E::Shared: Send + Sync,
{
    type Driver = ShopWavedb<E>;

    fn consumers(&self) -> usize {
        self.consumers
    }

    fn build(&self, shard: usize) -> Result<ShopWavedb<E>, String> {
        Ok(ShopWavedb {
            engine: E::build(&self.shared, shard)?,
            shard,
            consumers: self.consumers,
        })
    }

    /// By user, which is by tenant, which is by Pivot instance.
    ///
    /// Not `shard_of(pivot, count)` — the node's own function — because that
    /// needs the Pivot's `LocalId`, which is minted at preload time and which
    /// a generator refusing to touch the database cannot know. What matters
    /// here is the property `shard_of` exists to provide: **disjointness**. A
    /// user's records all live under that user's tenant, so `u % N` never
    /// hands one record to two holders.
    fn route(&self, op: &ShopOp) -> usize {
        (op.partition_key() % self.consumers.max(1) as u64) as usize
    }
}

pub struct ShopWavedb<E: ShopEngine> {
    engine: E,
    /// Which consumer this is, and how many there are — held only so the
    /// partition rule can be checked rather than trusted.
    shard: usize,
    consumers: usize,
}

impl<E: ShopEngine> Driver for ShopWavedb<E> {
    type Op = ShopOp;

    fn execute(&mut self, op: ShopOp) -> Result<(), String> {
        let u = op.partition_key();
        // The correctness condition, checked rather than trusted.
        //
        // `ShardStore` memoises **absence**, which is sound only while a
        // record is reached by exactly one holder: two consumers sharing a
        // tenant would keep serving a `None` after the other's insert filled
        // it. That is silent wrong data, not a slow read, and it would show
        // up as a plausible number rather than a failure. So the routing rule
        // gets an assertion at the one place it can be observed.
        //
        // `debug_assert` because it is on the timed path: the release row must
        // not pay for it, and the test suite and every development run do.
        debug_assert_eq!(
            u as usize % self.consumers.max(1),
            self.shard,
            "consumer {} was handed user {u}, which belongs to {}",
            self.shard,
            u as usize % self.consumers.max(1)
        );
        let store = self.engine.store();
        let db = tenant(store, u);
        block_on(run(&db, op))
    }

    /// Checkpoint and evict once the journal is large enough to matter — the
    /// embedded bracket's counterpart to a server's background threads, and
    /// outside the timed window for exactly that reason.
    fn after_op(&mut self) -> Result<(), String> {
        if self.engine.journal_len() <= CHECKPOINT_AFTER_BYTES {
            return Ok(());
        }
        self.engine.checkpoint()?;
        self.engine.evict(CACHE_BUDGET_BYTES);
        Ok(())
    }

    /// Settle after the writes and empty the cache, so the three read phases
    /// measure a store nobody has just written to.
    fn between_phases(
        &mut self,
        done: &str,
        _next: &str,
    ) -> Result<(), String> {
        if done != SETTLE_AFTER {
            return Ok(());
        }
        self.engine.checkpoint()?;
        // To zero: the per-type cache is a *write* cache, so everything the
        // write phases just wrote would otherwise still be warm and the read
        // phases would measure RAM. Every other adapter restarts its server
        // here for the same reason.
        self.engine.evict(0);
        Ok(())
    }

    /// Settle, and stop there. The disk actor belongs to the row: with three
    /// consumers over one actor, the first `close` to shut it down would stop
    /// the actor the other two are still using.
    fn close(self) -> Result<(), String> {
        self.engine.checkpoint()
    }
}

async fn run<S: Store>(
    db: &LocalHandle<'_, S>,
    op: ShopOp,
) -> Result<(), String> {
    match op {
        ShopOp::Signup { row, .. } => {
            let shoppings = Shopping::create_pivot(db)
                .await
                .map_err(|e| format!("create shopping pivot: {e}"))?;
            User {
                name: row.name,
                address: row.address,
                city: row.city,
                email: row.email,
                shoppings,
            }
            .save(db)
            .await
            .map(|_| ())
            .map_err(|e| format!("save user: {e}"))
        }
        ShopOp::Checkout { row, items, .. } => {
            let user = User::get(db)
                .await
                .map_err(|e| format!("get user: {e}"))?
                .ok_or("checkout: user is missing")?;
            let pivot = Product::create_pivot(db)
                .await
                .map_err(|e| format!("create product pivot: {e}"))?;
            Shopping::collection(user.shoppings)
                .insert(
                    db,
                    &Shopping {
                        bought_at: row.bought_at,
                        discount_cents: row.discount_cents,
                        transport_cents: row.transport_cents,
                        items: pivot,
                    },
                )
                .await
                .map_err(|e| format!("insert shopping: {e}"))?;
            let products = Product::collection(pivot);
            for item in items {
                products
                    .insert(
                        db,
                        &Product {
                            name: item.name,
                            quantity: item.quantity,
                            unit_cents: item.unit_cents,
                        },
                    )
                    .await
                    .map_err(|e| format!("insert product: {e}"))?;
            }
            Ok(())
        }
        ShopOp::Profile { u } => {
            User::get(db)
                .await
                .map_err(|e| format!("get user: {e}"))?
                .ok_or_else(|| format!("profile: user {u} is missing"))?;
            Ok(())
        }
        ShopOp::OrderPage { u, page } => {
            let user = user_of(db, u).await?;
            let orders: Vec<Shopping> = Shopping::collection(user.shoppings)
                .listed_by_bought_at_at_page(db, page, PAGE)
                .try_collect()
                .await
                .map_err(|e| format!("order page: {e}"))?;
            if orders.is_empty() && page == 0 {
                return Err(format!("order_page: user {u} has no first page"));
            }
            Ok(())
        }
        ShopOp::OrderDetail { u } => {
            let user = user_of(db, u).await?;
            let orders: Vec<Shopping> = Shopping::collection(user.shoppings)
                .listed_by_bought_at_at_page(db, 0, PAGE)
                .try_collect()
                .await
                .map_err(|e| format!("order detail: {e}"))?;
            let order = orders.first().ok_or_else(|| {
                format!("order_detail: user {u} has no order")
            })?;
            let items: Vec<Product> = Product::collection(order.items)
                .listed_by_name_at_page(db, 0, PAGE)
                .try_collect()
                .await
                .map_err(|e| format!("order items: {e}"))?;
            if items.is_empty() {
                return Err(format!("order_detail: user {u}'s order is empty"));
            }
            Ok(())
        }
    }
}

async fn user_of<S: Store>(
    db: &LocalHandle<'_, S>,
    u: u64,
) -> Result<User, String> {
    User::get(db)
        .await
        .map_err(|e| format!("get user: {e}"))?
        .ok_or_else(|| format!("user {u} is missing"))
}

/// The tenant a user's records live under. `u + 1` because tenant 0 is not a
/// tenant, and the row's user numbering starts at zero.
fn tenant<S: Store>(store: &S, u: u64) -> LocalHandle<'_, S> {
    LocalHandle::new(store, U48::from(u32::try_from(u + 1).unwrap_or(1)))
}
