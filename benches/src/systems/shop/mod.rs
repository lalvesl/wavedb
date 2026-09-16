//! The e-commerce workload's adapters (RFC 0060 §3.1).
//!
//! Same five systems, same reporting shape, a different question. The micro
//! workload asks "how fast is one operation on one type"; this one asks "how
//! long does a customer wait", and its phases are therefore **composed**: a
//! checkout is one order *and* its line items, an order page is a user *and* a
//! page of their orders.
//!
//! That composition is where the two workloads disagree about what a database
//! is for, and the disagreement is the measurement:
//!
//! - The SQL trio and MongoDB wrap a checkout in **one transaction**, which is
//!   how an application would write it and costs **one** commit barrier.
//! - WaveDB has no multi-record transaction: every collection op is its own
//!   atomic `Store::apply` batch, so a checkout with five line items is
//!   **seven** batches and seven barriers. That is not a tuning gap, it is the
//!   data model, and pricing it is the reason this workload exists.

#[cfg(feature = "servers")]
pub mod mongodb;
#[cfg(feature = "servers")]
pub mod mysql;
#[cfg(feature = "servers")]
pub mod postgres;
pub mod sqlite;
pub mod wavedb;

use std::path::PathBuf;

/// The e-commerce workload's sizes. Reads dominate deliberately: a shop serves
/// far more page views than checkouts, and a benchmark whose mix says otherwise
/// is measuring a bulk loader.
pub struct ShopCfg {
    /// How many users are preloaded — and, in WaveDB, how many **tenants**.
    pub users: u64,
    /// New users created during the measured window.
    pub signups: u64,
    /// Orders placed during the measured window, each with its line items.
    pub checkouts: u64,
    /// Point reads of a user's own record.
    pub profile_reads: u64,
    /// Order-history pages rendered (a user plus ten of their orders).
    pub page_reads: u64,
    /// Order details opened (one order's line items).
    pub detail_reads: u64,
    /// Most orders a user has; the count is uniform in `1..=max`.
    pub orders_max: u64,
    /// Most line items an order has, likewise uniform.
    pub items_max: u64,
    pub seed: u64,
    pub work_dir: PathBuf,
}

impl ShopCfg {
    /// Total records preloaded, for the bytes-per-record column.
    #[must_use]
    pub fn live_records(&self) -> u64 {
        let mut n = 0;
        for u in 0..self.users {
            n += 1;
            for s in
                0..crate::shop::shopping_count(u, self.seed, self.orders_max)
            {
                n += 1 + crate::shop::product_count(
                    u,
                    s,
                    self.seed,
                    self.items_max,
                );
            }
        }
        n
    }
}

/// The phase names, in report order. Shared so the table can print one row per
/// system without every adapter agreeing by accident.
pub const PHASES: [&str; 5] = [
    "signup",
    "checkout",
    "profile",
    "order_page",
    "order_detail",
];

impl ShopCfg {
    /// Orders the preload creates. The generator needs it to mint checkout
    /// ids: the preload numbers orders `1..=preloaded_orders`, so a measured
    /// checkout starts one past that.
    ///
    /// Computed rather than queried, and that is the point of the split: a
    /// generator that asked the database for `MAX(id)` would be doing database
    /// work on the thread whose whole job is *not* to.
    #[must_use]
    pub fn preloaded_orders(&self) -> u64 {
        (0..self.users)
            .map(|u| crate::shop::shopping_count(u, self.seed, self.orders_max))
            .sum()
    }

    /// Line items the preload creates, numbered the same way.
    #[must_use]
    pub fn preloaded_items(&self) -> u64 {
        (0..self.users)
            .flat_map(|u| {
                (0..crate::shop::shopping_count(u, self.seed, self.orders_max))
                    .map(move |s| {
                        crate::shop::product_count(
                            u,
                            s,
                            self.seed,
                            self.items_max,
                        )
                    })
            })
            .sum()
    }
}
