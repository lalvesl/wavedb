//! The e-commerce workload, as a [`Workload`].
//!
//! ## What the generator mints, and what it may not
//!
//! Every id a measured operation needs is **computed** here — the order's id,
//! its first line item's id, the user. None of it is queried, and that is the
//! split the harness exists for: a generator that asked the database for
//! `MAX(id)` would be doing database work on the thread whose whole job is not
//! to, and it would do it inside the window a consumer is being timed on.
//!
//! It works because the preload is deterministic: it numbers orders
//! `1..=preloaded_orders` and items `1..=preloaded_items` in one fixed walk,
//! so a measured checkout starts one past each
//! ([`ShopCfg::preloaded_orders`]).
//!
//! ## The preload is not a phase
//!
//! It is bulk, untimed, and each system does it in the shape its own bulk
//! loader wants — one transaction with `synchronous = OFF` on SQLite, a
//! relaxed window on WaveDB. The row runs it before the harness starts, so
//! nothing here knows about it beyond the two counts above.
//!
//! ## Every op carries its user, and that is the partition
//!
//! Unlike [`MicroOp`](crate::plan::op::MicroOp), whose `partition_key` is a
//! constant, a shop op names the user it belongs to — and a user is a tenant
//! is one `Shopping` collection, so the key partitions the Pivot instances
//! that are [RFC 0064]'s unit of concurrency. That is why `wavedb/multi` is a
//! `shop` row.
//!
//! [RFC 0064]: ../../../rfcs/0064-pivot-owned-concurrency-PLANNED.md

use crate::plan::op::ShopOp;
use crate::schema::Rng;
use crate::shop::{
    PAGE, product_count, product_row, shopping_count, shopping_row, user_row,
};
use crate::systems::shop::{PHASES, ShopCfg};

use super::Workload;

/// One salt per phase that draws users. Fixed constants rather than anything
/// derived from a phase's name: RFC 0060 seeded these with `seed ^ 1`,
/// `seed ^ 2`, `seed ^ 3`, which worked and made the streams a function of
/// declaration order.
const SALT_CHECKOUT: u64 = 0xC0FF_EE00_C0FF_EE00;
const SALT_PROFILE: u64 = 0x5409_0000_0000_0001;
const SALT_PAGE: u64 = 0x5409_0000_0000_0002;
const SALT_DETAIL: u64 = 0x5409_0000_0000_0003;

/// How many pages of history the `order_page` phase draws from. Two, because
/// `orders_max` defaults to 20 and a page is ten: asking for page 5 would
/// measure an empty result on nearly every user, which is a fast operation
/// and not the one the phase is named for.
const PAGES_DRAWN: u64 = 2;

/// The shop workload over one configuration.
pub struct ShopWorkload {
    cfg: ShopCfg,
    /// Ids the preload used up, so a measured checkout starts past them.
    next_order: u64,
    next_item: u64,
}

impl ShopWorkload {
    #[must_use]
    pub fn new(cfg: ShopCfg) -> Self {
        let next_order = cfg.preloaded_orders() + 1;
        let next_item = cfg.preloaded_items() + 1;
        Self {
            cfg,
            next_order,
            next_item,
        }
    }

    pub const fn cfg(&self) -> &ShopCfg {
        &self.cfg
    }
}

impl Workload for ShopWorkload {
    type Op = ShopOp;

    fn phases(&self) -> &'static [&'static str] {
        &PHASES
    }

    fn len(&self, phase: &str) -> u64 {
        match phase {
            "signup" => self.cfg.signups,
            "checkout" => self.cfg.checkouts,
            "profile" => self.cfg.profile_reads,
            "order_page" => self.cfg.page_reads,
            "order_detail" => self.cfg.detail_reads,
            _ => 0,
        }
    }

    fn generate<E>(&mut self, phase: &str, mut emit: E) -> Result<(), String>
    where
        E: FnMut(ShopOp) -> Result<(), String>,
    {
        let seed = self.cfg.seed;
        let users = self.cfg.users.max(1);
        match phase {
            // New users, numbered past the preloaded ones so a signup can
            // never collide with a user the read phases draw.
            "signup" => {
                for i in 0..self.cfg.signups {
                    let u = self.cfg.users + i;
                    emit(ShopOp::Signup {
                        u,
                        row: user_row(u, seed),
                    })?;
                }
            }
            "checkout" => self.checkouts(&mut emit)?,
            "profile" => {
                let mut rng = Rng::new(seed ^ SALT_PROFILE);
                for _ in 0..self.cfg.profile_reads {
                    emit(ShopOp::Profile {
                        u: rng.below(users),
                    })?;
                }
            }
            "order_page" => {
                let mut rng = Rng::new(seed ^ SALT_PAGE);
                for _ in 0..self.cfg.page_reads {
                    let u = rng.below(users);
                    emit(ShopOp::OrderPage {
                        u,
                        page: rng.below(PAGES_DRAWN) as usize,
                    })?;
                }
            }
            "order_detail" => {
                let mut rng = Rng::new(seed ^ SALT_DETAIL);
                for _ in 0..self.cfg.detail_reads {
                    emit(ShopOp::OrderDetail {
                        u: rng.below(users),
                    })?;
                }
            }
            other => return Err(format!("unknown shop phase {other:?}")),
        }
        Ok(())
    }
}

impl ShopWorkload {
    /// One order and all of its line items, per checkout.
    ///
    /// `s` continues the user's own history rather than restarting it: the
    /// preload gave user `u` `shopping_count(u)` orders, so the new one is
    /// theirs numbered next. It is `+ i` rather than `+ 1` because two
    /// checkouts may draw the same user, and two orders sharing an `s` would
    /// carry identical bytes — the same no-op an update phase hit on MySQL.
    fn checkouts<E>(&mut self, emit: &mut E) -> Result<(), String>
    where
        E: FnMut(ShopOp) -> Result<(), String>,
    {
        let seed = self.cfg.seed;
        let users = self.cfg.users.max(1);
        let mut rng = Rng::new(seed ^ SALT_CHECKOUT);
        for i in 0..self.cfg.checkouts {
            let u = rng.below(users);
            let s = shopping_count(u, seed, self.cfg.orders_max) + i;
            let count = product_count(u, s, seed, self.cfg.items_max);
            let items = (0..count)
                .map(|p| product_row(u, s, p, seed))
                .collect::<Vec<_>>();
            let order = self.next_order;
            let first_item = self.next_item;
            self.next_order += 1;
            self.next_item += count;
            emit(ShopOp::Checkout {
                u,
                s,
                order,
                first_item,
                row: shopping_row(u, s, seed),
                items,
            })?;
        }
        Ok(())
    }
}

/// How many orders a page of history holds — re-exported so a driver spells
/// the same number the generator drew against.
pub const PAGE_SIZE: usize = PAGE;

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::{PAGES_DRAWN, ShopWorkload};
    use crate::harness::Workload;
    use crate::plan::op::ShopOp;
    use crate::systems::shop::{PHASES, ShopCfg};

    fn cfg() -> ShopCfg {
        ShopCfg {
            users: 50,
            signups: 10,
            checkouts: 20,
            profile_reads: 30,
            page_reads: 15,
            detail_reads: 15,
            orders_max: 4,
            items_max: 3,
            seed: 42,
            work_dir: std::path::PathBuf::from("/nonexistent"),
        }
    }

    fn collect(phase: &str) -> Vec<ShopOp> {
        let mut out = Vec::new();
        ShopWorkload::new(cfg())
            .generate(phase, |op| {
                out.push(op);
                Ok(())
            })
            .expect("generate");
        out
    }

    #[test]
    fn each_phase_emits_the_count_it_declared() {
        let w = ShopWorkload::new(cfg());
        for phase in PHASES {
            assert_eq!(collect(phase).len() as u64, w.len(phase), "{phase}");
        }
    }

    #[test]
    fn every_op_lands_in_the_phase_that_generated_it() {
        for phase in PHASES {
            for op in collect(phase) {
                assert_eq!(op.phase(), phase);
            }
        }
    }

    /// The property the whole no-query design rests on: two checkouts never
    /// mint the same order id, and never the same item id either.
    #[test]
    fn minted_ids_are_unique_across_the_checkout_phase() {
        let mut orders = HashSet::new();
        let mut items = HashSet::new();
        for op in collect("checkout") {
            let ShopOp::Checkout {
                order,
                first_item,
                items: rows,
                ..
            } = op
            else {
                panic!("not a checkout");
            };
            assert!(orders.insert(order), "order {order} minted twice");
            for k in 0..rows.len() as u64 {
                assert!(
                    items.insert(first_item + k),
                    "item {} minted twice",
                    first_item + k
                );
            }
        }
    }

    /// And they start past what the preload used, or a checkout would collide
    /// with a record that is already there.
    #[test]
    fn minted_ids_start_past_the_preload() {
        let c = cfg();
        let floor = c.preloaded_orders();
        for op in collect("checkout") {
            if let ShopOp::Checkout { order, .. } = op {
                assert!(order > floor, "{order} <= {floor}");
            }
        }
    }

    /// A signup may not land on a user the read phases draw, or the row would
    /// be reading records it created mid-run.
    #[test]
    fn a_signup_never_collides_with_a_preloaded_user() {
        let c = cfg();
        for op in collect("signup") {
            assert!(op.partition_key() >= c.users);
        }
        for phase in ["profile", "order_page", "order_detail"] {
            for op in collect(phase) {
                assert!(op.partition_key() < c.users, "{phase}");
            }
        }
    }

    /// The partition key is the user, which is what makes `shop` the workload
    /// a sharded row can be measured on at all.
    #[test]
    fn the_partition_key_is_the_user() {
        let ops = collect("profile");
        let keys: HashSet<u64> =
            ops.iter().map(ShopOp::partition_key).collect();
        assert!(keys.len() > 1, "one key would not partition anything");
    }

    #[test]
    fn a_drawn_page_is_inside_the_history_a_user_has() {
        for op in collect("order_page") {
            if let ShopOp::OrderPage { page, .. } = op {
                assert!((page as u64) < PAGES_DRAWN);
            }
        }
    }

    #[test]
    fn a_phase_is_reproducible_from_its_seed() {
        for phase in PHASES {
            let a: Vec<u64> =
                collect(phase).iter().map(ShopOp::partition_key).collect();
            let b: Vec<u64> =
                collect(phase).iter().map(ShopOp::partition_key).collect();
            assert_eq!(a, b, "{phase}");
        }
    }

    #[test]
    fn an_unknown_phase_is_refused() {
        let err = ShopWorkload::new(cfg())
            .generate("vacuum", |_| Ok(()))
            .expect_err("must refuse");
        assert!(err.contains("vacuum"), "{err}");
    }
}
