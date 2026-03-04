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
//! That disjointness is a correctness condition, not an optimisation. On
//! `wavedb/multi` each consumer owns one data shard, and user `u` lives in
//! shard `u % N` — the preload put it there — so a user handed to another
//! consumer would find nothing in that consumer's store. The generator
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
use wavedb_core::{CollectionHandle, LocalHandle, NonUniqueStruct, Store, U48};
use wavedb_storage::PageStore;

use crate::harness::{Driver, DriverFactory};
use crate::plan::op::ShopOp;
use crate::shop::{
    PAGE, Product, ProductLists as _, Shopping, ShoppingLists as _, User,
};

use super::SETTLE_AFTER;
use super::engine::ShopEngine;

/// What a consumer needs to build its engine.
pub struct Factory<E: ShopEngine> {
    shared: E::Shared,
    consumers: usize,
    /// Whether every write is made durable before it returns (RFC 0072).
    durable: bool,
}

impl<E: ShopEngine> Factory<E> {
    /// # Errors
    /// The shared engine could not be started.
    pub fn new(
        stores: Vec<PageStore>,
        asked: usize,
        durable: bool,
    ) -> Result<Self, String> {
        let consumers = E::consumers(asked);
        Ok(Self {
            shared: E::share(stores, consumers)?,
            consumers,
            durable,
        })
    }

    /// The shared half: the shards no consumer has taken yet.
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
            durable: self.durable,
            settled: std::time::Instant::now(),
            wrote: false,
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
    /// Whether every write is made durable before it returns.
    durable: bool,
    /// When the store was last settled.
    settled: std::time::Instant,
    /// Whether the last op wrote: only a write can leave work to settle or
    /// records to evict.
    wrote: bool,
}

impl<E: ShopEngine> Driver for ShopWavedb<E> {
    type Op = ShopOp;

    fn execute(&mut self, op: ShopOp) -> Result<(), String> {
        let u = op.partition_key();
        // The correctness condition, checked rather than trusted.
        //
        // User `u` lives in data shard `u % N`, and this consumer owns only
        // its own: handed someone else's user, it would read a `None` from a
        // store that never held them. That is silent wrong data, not a slow
        // read, and it would show up as a plausible number, not a failure. So the routing rule
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
        self.wrote =
            matches!(op, ShopOp::Signup { .. } | ShopOp::Checkout { .. });
        let store = self.engine.store();
        let db = tenant(store, u);
        // A durable row ends each writing op with a `_durable` write (RFC
        // 0072 §12): it returns once its batch is durable, and so is every
        // write before it on this consumer's store — on a store nobody
        // ticks, settled and synced on the spot, inside the timed window.
        block_on(run(&db, op, self.durable))
    }

    /// After a write, never a read — a read adds nothing to settle and
    /// nothing to evict. A durable write has settled already, so it only
    /// evicts; a relaxed one settles (never waiting for the `fsync`, which
    /// a helper takes), then evicts, once the window has
    /// elapsed (a node's tick, outside the timed window) or once what is
    /// not settled has passed the store's pressure line (a node's valve).
    /// Both evict to the store's budget: its data shard's share of the
    /// database's.
    fn after_op(&mut self) -> Result<(), String> {
        if !std::mem::take(&mut self.wrote) {
            return Ok(());
        }
        if !self.durable
            && (self.settled.elapsed() >= crate::RELAXED_WINDOW
                || self.engine.under_pressure())
        {
            self.engine.settle()?;
            self.settled = std::time::Instant::now();
        }
        self.engine.evict_to_budget();
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
        self.engine.drain()?;
        // To zero: the per-type cache is a *write* cache, so everything the
        // write phases just wrote would otherwise still be warm and the read
        // phases would measure RAM. Every other adapter restarts its server
        // here for the same reason.
        self.engine.evict(0);
        Ok(())
    }

    /// Settle, and stop there: the store goes down with this driver.
    fn close(self) -> Result<(), String> {
        self.engine.drain()
    }
}

async fn run<S: Store>(
    db: &LocalHandle<'_, S>,
    op: ShopOp,
    durable: bool,
) -> Result<(), String> {
    match op {
        ShopOp::Signup { row, .. } => {
            let shoppings = Shopping::create_pivot(db)
                .await
                .map_err(|e| format!("create shopping pivot: {e}"))?;
            let user = User {
                name: row.name,
                address: row.address,
                city: row.city,
                email: row.email,
                shoppings,
            };
            if durable {
                user.save_durable(db).await
            } else {
                user.save(db).await
            }
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
            // The op's last write is the durable one.
            let last = items.len();
            let shopping = Shopping {
                bought_at: row.bought_at,
                discount_cents: row.discount_cents,
                transport_cents: row.transport_cents,
                items: pivot,
            };
            let shoppings = Shopping::collection(user.shoppings);
            insert(db, shoppings, &shopping, durable && last == 0)
                .await
                .map_err(|e| format!("insert shopping: {e}"))?;
            let products = Product::collection(pivot);
            for (i, item) in items.into_iter().enumerate() {
                let product = Product {
                    name: item.name,
                    quantity: item.quantity,
                    unit_cents: item.unit_cents,
                };
                insert(db, products, &product, durable && i + 1 == last)
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

/// Insert `value` into `col`, through the `_durable` twin when `durable`.
async fn insert<S: Store, T: NonUniqueStruct>(
    db: &LocalHandle<'_, S>,
    col: CollectionHandle<T>,
    value: &T,
    durable: bool,
) -> wavedb_core::Result<()> {
    if durable {
        col.insert_durable(db, value).await.map(|_| ())
    } else {
        col.insert(db, value).await.map(|_| ())
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
