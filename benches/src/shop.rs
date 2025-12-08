//! The e-commerce workload: schema, and the deterministic generator for it.
//!
//! The micro benchmark (`schema.rs`) measures one operation on one flat type.
//! This one measures what an application actually asks a database for: a
//! **checkout** (one order plus its line items, which is inherently several
//! records) and an **order-history page** (a user, a page of their orders, and
//! one order's items). Those are the units a user waits on, so this workload is
//! reported as *latency of a composed operation* rather than rows per second.
//!
//! Three modelling decisions are load-bearing, and each of them is a claim:
//!
//! - **A user is a tenant.** In WaveDB the tenant is 48 bits *of the `Id`*, so
//!   "one shop, many customers" is what tenancy is for, and this is the first
//!   workload here that has more than one of them (RFC 0060 open question 9).
//!   The other four have no such concept, so they carry a `user_id` column and
//!   an index on it — which is precisely the cost being compared.
//! - **Records hold pivots.** `User` holds its `Shopping` collection's
//!   `PivotId`, and each `Shopping` holds its `Product` collection's. That is
//!   how trees nest, and it is the structural counterpart of the two foreign
//!   keys the SQL schema needs.
//! - **The lists are the read path.** `Shopping` declares a list ordered by
//!   `bought_at` and `Product` one ordered by `name`, so rendering a page of
//!   ten is one skip into the list's key range and the records inline rather
//!   than a read per row. That is the whole point of RFC 0051, and this
//!   workload is where it gets priced — against `ORDER BY … LIMIT 10 OFFSET n`
//!   on the other four.

use wavedb_core::WaveDbStruct;
use wavedb_macros::wavedb;

use crate::schema::Rng;

/// Unique — exactly one live record per tenant, because a user **is** a tenant
/// here. `User::get(&db)` under that tenant is the whole profile lookup: no key
/// to pass, because the identity is in the handle.
#[wavedb]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct User {
    pub name: String,
    pub address: String,
    pub city: String,
    pub email: String,
    pub shoppings: <Shopping as WaveDbStruct>::PivotId,
}

/// NonUnique — the order, and the element that relates a user to what they
/// bought. It carries only what belongs to the order itself; the line items
/// hang off the `PivotId` it holds, which is the SQL `shopping_id` foreign key
/// seen from the other side.
#[wavedb(NonUnique)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Shopping {
    /// The ordering of the order-history page. `IndexKey` encodes integers
    /// big-endian, so the byte order of this list *is* the chronological one.
    #[wavedb::list]
    pub bought_at: u64,
    pub discount_cents: u64,
    pub transport_cents: u64,
    pub items: <Product as WaveDbStruct>::PivotId,
}

/// NonUnique — the line items of **one** order, in their own collection.
#[wavedb(NonUnique)]
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Product {
    #[wavedb::list]
    pub name: String,
    pub quantity: u32,
    pub unit_cents: u64,
}

/// The rows of a rendered page: what every system is asked for.
pub const PAGE: usize = 10;

/// The system-neutral form of the generated data. The stored types above carry
/// pivots that only WaveDB has, so the dataset itself is defined without them
/// and each adapter adds its own linkage.
pub struct UserRow {
    pub name: String,
    pub address: String,
    pub city: String,
    pub email: String,
}

pub struct ShoppingRow {
    pub bought_at: u64,
    pub discount_cents: u64,
    pub transport_cents: u64,
}

pub struct ProductRow {
    pub name: String,
    pub quantity: u32,
    pub unit_cents: u64,
}

/// User `u`'s profile — a pure function of `(u, seed)`, like everything else
/// here, so no adapter ever carries the dataset around.
#[must_use]
pub fn user_row(u: u64, seed: u64) -> UserRow {
    let mut rng = rng_for(seed, 0x5EED_0001, u, 0, 0);
    UserRow {
        name: format!("customer-{u:08}"),
        address: format!(
            "{} street, no {}",
            CITIES[pick(&mut rng, CITIES)],
            rng.below(9999)
        ),
        city: CITIES[pick(&mut rng, CITIES)].into(),
        email: format!("customer-{u:08}@example.test"),
    }
}

/// How many orders user `u` has: uniform in `1..=max`. With the default
/// `max = 20` against a [`PAGE`] of ten, about half the users have a second
/// page, so the pager is exercised rather than merely called.
#[must_use]
pub fn shopping_count(u: u64, seed: u64, max: u64) -> u64 {
    let mut rng = rng_for(seed, 0x5EED_0002, u, 0, 0);
    1 + rng.below(max.max(1))
}

/// Order `s` of user `u`. `bought_at` descends with `s` so the list's order is
/// not the insertion order — a list that agreed with the insertion order would
/// prove nothing about the list.
#[must_use]
pub fn shopping_row(u: u64, s: u64, seed: u64) -> ShoppingRow {
    let mut rng = rng_for(seed, 0x5EED_0003, u, s, 0);
    ShoppingRow {
        // A fixed epoch minus a per-order offset: deterministic, and out of
        // order with respect to `s`.
        bought_at: 1_700_000_000_000 - (rng.below(90 * 86_400_000)),
        discount_cents: rng.below(5_000),
        transport_cents: 500 + rng.below(2_000),
    }
}

/// How many line items order `(u, s)` has: uniform in `1..=max`.
///
/// At the default `max = 5` this never fills a [`PAGE`] of ten, so
/// `order_detail` is always one whole order's items — a clean measure of a
/// page read rather than of a walk.
#[must_use]
pub fn product_count(u: u64, s: u64, seed: u64, max: u64) -> u64 {
    let mut rng = rng_for(seed, 0x5EED_0004, u, s, 0);
    1 + rng.below(max.max(1))
}

/// Line item `p` of order `(u, s)`.
#[must_use]
pub fn product_row(u: u64, s: u64, p: u64, seed: u64) -> ProductRow {
    let mut rng = rng_for(seed, 0x5EED_0005, u, s, p);
    let noun = GOODS[pick(&mut rng, GOODS)];
    ProductRow {
        // Prefixed with `p` so names sort into a stable, non-insertion order
        // inside the declared list.
        name: format!("{noun}-{:04}", rng.below(9999)),
        quantity: 1 + u32::try_from(rng.below(4)).unwrap_or(1),
        unit_cents: 199 + rng.below(50_000),
    }
}

/// The logical payload of the whole dataset: what these records weigh in
/// `WaveWire` before any system stores them.
///
/// Linkage is **excluded** — a `PivotId` on one side and a `user_id`/
/// `shopping_id` column on the other are the same relationship modelled
/// differently, and charging one of them for it would make the ratio a
/// statement about modelling rather than about storage. A `String` costs its
/// `u32` length slot plus its bytes (`docs/wire_format.md`).
#[must_use]
pub fn logical_bytes(users: u64, seed: u64, orders: u64, items: u64) -> u64 {
    let mut total = 0;
    for u in 0..users {
        let r = user_row(u, seed);
        total += str_bytes(&r.name)
            + str_bytes(&r.address)
            + str_bytes(&r.city)
            + str_bytes(&r.email);
        for s in 0..shopping_count(u, seed, orders) {
            total += 8 + 8 + 8; // bought_at, discount, transport
            for p in 0..product_count(u, s, seed, items) {
                let pr = product_row(u, s, p, seed);
                total += str_bytes(&pr.name) + 4 + 8;
            }
        }
    }
    total
}

fn str_bytes(s: &str) -> u64 {
    4 + s.len() as u64
}

/// One generator per (kind, u, s, p) coordinate, so any record can be produced
/// without producing the ones before it — which is what lets a read phase pick
/// a random user's random order without materialising anything.
fn rng_for(seed: u64, kind: u64, u: u64, s: u64, p: u64) -> Rng {
    Rng::new(
        seed ^ kind.wrapping_mul(0x9E37_79B9_7F4A_7C15)
            ^ u.wrapping_mul(0xD6E8_FEB8_6659_FD93)
            ^ s.wrapping_mul(0xA076_1D64_78BD_642F)
            ^ p.wrapping_mul(0x8EBC_6AF0_9C88_C6E3),
    )
}

fn pick(rng: &mut Rng, from: &[&str]) -> usize {
    (rng.below(from.len() as u64)) as usize
}

const CITIES: &[&str] = &[
    "porto", "lisboa", "braga", "coimbra", "faro", "aveiro", "evora", "viseu",
];

const GOODS: &[&str] = &[
    "keyboard", "monitor", "cable", "charger", "mouse", "webcam", "router",
    "ssd", "headset", "dock", "battery", "adapter",
];
