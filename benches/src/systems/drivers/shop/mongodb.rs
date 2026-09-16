//! MongoDB in the shop workload — the reference peer.
//!
//! Three collections with references, **not** an embedded line-item array.
//! Embedding is what a MongoDB application would usually do, and it would be a
//! different measurement: one document write per checkout instead of several,
//! and no join to render a detail. Keeping the reference model holds the
//! *shape* of the data equal across all five, so the numbers compare storage
//! and access paths rather than modelling choices. The embedded variant is
//! worth measuring on its own; it is not this row.
//!
//! The checkout is a **transaction**, and multi-document transactions are not
//! available on a standalone `mongod` — which is why this row starts a
//! one-node replica set. That is a difference from the `micro` MongoDB row,
//! and it is declared in the stored settings rather than hidden here.

use std::path::PathBuf;
use std::sync::Arc;

use mongodb::IndexModel;
use mongodb::bson::{Document, doc};
use mongodb::sync::{Client, ClientSession, Collection, Database};

use crate::harness::{Driver, DriverFactory};
use crate::plan::op::ShopOp;
use crate::shop::PAGE;
use crate::systems::server::Server;

use super::SETTLE_AFTER;
use super::mongo_server::{connect, restart_server};

/// The shared server handle, the same shape the other two use.
pub type Shared = Arc<std::sync::Mutex<Option<Server>>>;

pub struct Factory {
    pub dir: PathBuf,
    pub port: u16,
    pub journal: bool,
    pub server: Shared,
}

pub struct ShopMongo {
    db: Database,
    session: ClientSession,
    /// Held so the connection outlives the handles taken from it.
    _client: Client,
    dir: PathBuf,
    port: u16,
    journal: bool,
    server: Shared,
}

impl DriverFactory for Factory {
    type Driver = ShopMongo;

    fn consumers(&self) -> usize {
        1
    }

    fn build(&self, _shard: usize) -> Result<ShopMongo, String> {
        let client = connect(self.port, self.journal)?;
        let session = client.start_session().run().map_err(drv)?;
        Ok(ShopMongo {
            db: client.database("shop"),
            session,
            _client: client,
            dir: self.dir.clone(),
            port: self.port,
            journal: self.journal,
            server: Arc::clone(&self.server),
        })
    }

    fn route(&self, _op: &ShopOp) -> usize {
        0
    }

    fn writer(&self) -> crate::metrics::Writer {
        super::server_writer(&self.server)
    }
}

impl Driver for ShopMongo {
    type Op = ShopOp;

    fn execute(&mut self, op: ShopOp) -> Result<(), String> {
        match op {
            ShopOp::Signup { u, row } => users(&self.db)
                .insert_one(doc! {
                    "_id": u as i64,
                    "name": row.name,
                    "address": row.address,
                    "city": row.city,
                    "email": row.email,
                })
                .run()
                .map(|_| ())
                .map_err(drv),
            ShopOp::Checkout {
                u,
                order,
                first_item,
                row,
                items: rows,
                ..
            } => {
                let od = doc! {
                    "_id": order as i64,
                    "user_id": u as i64,
                    "bought_at": row.bought_at as i64,
                    "discount_cents": row.discount_cents as i64,
                    "transport_cents": row.transport_cents as i64,
                };
                let ids: Vec<Document> = rows
                    .iter()
                    .enumerate()
                    .map(|(k, it)| {
                        doc! {
                            "_id": (first_item + k as u64) as i64,
                            "shopping_id": order as i64,
                            "name": it.name.clone(),
                            "quantity": i64::from(it.quantity),
                            "unit_cents": it.unit_cents as i64,
                        }
                    })
                    .collect();
                // One transaction: the order and its line items commit
                // together, one barrier for the whole checkout.
                self.session.start_transaction().run().map_err(drv)?;
                orders(&self.db)
                    .insert_one(&od)
                    .session(&mut self.session)
                    .run()
                    .map_err(drv)?;
                if !ids.is_empty() {
                    items(&self.db)
                        .insert_many(&ids)
                        .session(&mut self.session)
                        .run()
                        .map_err(drv)?;
                }
                self.session.commit_transaction().run().map_err(drv)
            }
            ShopOp::Profile { u } => {
                let got = users(&self.db)
                    .find_one(doc! { "_id": u as i64 })
                    .run()
                    .map_err(drv)?;
                empty(got.is_none(), "profile", u)
            }
            ShopOp::OrderPage { u, page } => self.order_page(u, page),
            ShopOp::OrderDetail { u } => self.order_detail(u),
        }
    }

    /// Restart, which empties the WiredTiger cache — the counterpart of the
    /// WaveDB row's evict and SQLite's reopen.
    fn between_phases(
        &mut self,
        done: &str,
        _next: &str,
    ) -> Result<(), String> {
        if done != SETTLE_AFTER {
            return Ok(());
        }
        restart_server(&self.server, &self.dir, self.port)?;
        let client = connect(self.port, self.journal)?;
        self.session = client.start_session().run().map_err(drv)?;
        self.db = client.database("shop");
        self._client = client;
        Ok(())
    }
}

impl ShopMongo {
    fn order_page(&self, u: u64, page: usize) -> Result<(), String> {
        let found = users(&self.db)
            .find_one(doc! { "_id": u as i64 })
            .run()
            .map_err(drv)?;
        let n = orders(&self.db)
            .find(doc! { "user_id": u as i64 })
            .sort(doc! { "bought_at": 1 })
            .skip((page * PAGE) as u64)
            .limit(PAGE as i64)
            .run()
            .map_err(drv)?
            .count();
        empty(found.is_none() || (n == 0 && page == 0), "order_page", u)
    }

    fn order_detail(&self, u: u64) -> Result<(), String> {
        let found = users(&self.db)
            .find_one(doc! { "_id": u as i64 })
            .run()
            .map_err(drv)?;
        let order = orders(&self.db)
            .find_one(doc! { "user_id": u as i64 })
            .sort(doc! { "bought_at": 1 })
            .run()
            .map_err(drv)?
            .ok_or_else(|| format!("order_detail: user {u} has no order"))?;
        let id = order.get_i64("_id").map_err(|e| format!("order id: {e}"))?;
        let n = items(&self.db)
            .find(doc! { "shopping_id": id })
            .sort(doc! { "name": 1 })
            .limit(PAGE as i64)
            .run()
            .map_err(drv)?
            .count();
        empty(found.is_none() || n == 0, "order_detail", u)
    }
}

pub fn users(db: &Database) -> Collection<Document> {
    db.collection("users")
}

pub fn orders(db: &Database) -> Collection<Document> {
    db.collection("shopping")
}

pub fn items(db: &Database) -> Collection<Document> {
    db.collection("product")
}

/// The two secondary indexes the SQL trio declares in its DDL.
///
/// # Errors
/// The server refused the index build.
pub fn indexes(db: &Database) -> Result<(), String> {
    orders(db)
        .create_index(
            IndexModel::builder()
                .keys(doc! { "user_id": 1, "bought_at": 1 })
                .build(),
        )
        .run()
        .map_err(drv)?;
    items(db)
        .create_index(
            IndexModel::builder()
                .keys(doc! { "shopping_id": 1, "name": 1 })
                .build(),
        )
        .run()
        .map_err(drv)?;
    Ok(())
}

fn empty(missing: bool, what: &str, u: u64) -> Result<(), String> {
    if missing {
        return Err(format!("{what}: user {u} came back empty"));
    }
    Ok(())
}

fn drv(e: mongodb::error::Error) -> String {
    format!("mongodb: {e}")
}
