//! PostgreSQL in the shop workload.
//!
//! Three tables and two indexes where WaveDB has a tenant in the `Id` and a
//! held `PivotId`. The order page is `ORDER BY bought_at LIMIT 10 OFFSET n`
//! against a declared list's page descent, and the profile read is a primary
//! key lookup against `User::get(&db)` needing no key at all.

use std::path::PathBuf;
use std::sync::Arc;

use postgres::{Client, Statement};

use crate::harness::{Driver, DriverFactory};
use crate::plan::op::ShopOp;
use crate::shop::PAGE;
use crate::systems::drivers::postgres::{Shared, connect_at, restart_server};

use super::SETTLE_AFTER;

pub const DDL: &str = "
CREATE TABLE users (
  id      BIGINT PRIMARY KEY,
  name    TEXT NOT NULL,
  address TEXT NOT NULL,
  city    TEXT NOT NULL,
  email   TEXT NOT NULL
);
CREATE TABLE shopping (
  id              BIGINT PRIMARY KEY,
  user_id         BIGINT NOT NULL,
  bought_at       BIGINT NOT NULL,
  discount_cents  BIGINT NOT NULL,
  transport_cents BIGINT NOT NULL
);
CREATE INDEX idx_shopping_user ON shopping(user_id, bought_at);
CREATE TABLE product (
  id          BIGINT PRIMARY KEY,
  shopping_id BIGINT NOT NULL,
  name        TEXT NOT NULL,
  quantity    INTEGER NOT NULL,
  unit_cents  BIGINT NOT NULL
);
CREATE INDEX idx_product_shopping ON product(shopping_id, name);
";

const INS_USER: &str = "INSERT INTO users (id, name, address, city, email) \
                        VALUES ($1, $2, $3, $4, $5)";
const INS_ORDER: &str = "INSERT INTO shopping (id, user_id, bought_at, \
                         discount_cents, transport_cents) \
                         VALUES ($1, $2, $3, $4, $5)";
const INS_ITEM: &str = "INSERT INTO product (id, shopping_id, name, quantity, \
                        unit_cents) VALUES ($1, $2, $3, $4, $5)";
const SEL_USER: &str = "SELECT name FROM users WHERE id = $1";
const SEL_PAGE: &str = "SELECT id FROM shopping WHERE user_id = $1 \
                        ORDER BY bought_at LIMIT $2 OFFSET $3";
const SEL_FIRST: &str = "SELECT id FROM shopping WHERE user_id = $1 \
                         ORDER BY bought_at LIMIT 1";
const SEL_ITEMS: &str = "SELECT name FROM product WHERE shopping_id = $1 \
                         ORDER BY name LIMIT $2";

pub struct Factory {
    pub socket_dir: PathBuf,
    pub sync: &'static str,
    pub server: Shared,
}

pub struct ShopPostgres {
    client: Client,
    stmts: Stmts,
    socket_dir: PathBuf,
    sync: &'static str,
    server: Shared,
}

struct Stmts {
    ins_user: Statement,
    ins_order: Statement,
    ins_item: Statement,
    sel_user: Statement,
    sel_page: Statement,
    sel_first: Statement,
    sel_items: Statement,
}

impl Stmts {
    fn prepare(client: &mut Client) -> Result<Self, String> {
        Ok(Self {
            ins_user: client.prepare(INS_USER).map_err(sql)?,
            ins_order: client.prepare(INS_ORDER).map_err(sql)?,
            ins_item: client.prepare(INS_ITEM).map_err(sql)?,
            sel_user: client.prepare(SEL_USER).map_err(sql)?,
            sel_page: client.prepare(SEL_PAGE).map_err(sql)?,
            sel_first: client.prepare(SEL_FIRST).map_err(sql)?,
            sel_items: client.prepare(SEL_ITEMS).map_err(sql)?,
        })
    }
}

impl DriverFactory for Factory {
    type Driver = ShopPostgres;

    fn consumers(&self) -> usize {
        1
    }

    fn build(&self, _shard: usize) -> Result<ShopPostgres, String> {
        let mut client = connect_at(&self.socket_dir)?;
        Ok(ShopPostgres {
            stmts: Stmts::prepare(&mut client)?,
            client,
            socket_dir: self.socket_dir.clone(),
            sync: self.sync,
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

impl Driver for ShopPostgres {
    type Op = ShopOp;

    fn execute(&mut self, op: ShopOp) -> Result<(), String> {
        match op {
            ShopOp::Signup { u, row } => self
                .client
                .execute(
                    &self.stmts.ins_user,
                    &[
                        &(u as i64),
                        &row.name,
                        &row.address,
                        &row.city,
                        &row.email,
                    ],
                )
                .map(|_| ())
                .map_err(sql),
            ShopOp::Checkout {
                u,
                order,
                first_item,
                row,
                items,
                ..
            } => {
                // One transaction: the order and its line items commit
                // together, one barrier for the whole checkout.
                let mut tx = self.client.transaction().map_err(sql)?;
                tx.execute(
                    &self.stmts.ins_order,
                    &[
                        &(order as i64),
                        &(u as i64),
                        &(row.bought_at as i64),
                        &(row.discount_cents as i64),
                        &(row.transport_cents as i64),
                    ],
                )
                .map_err(sql)?;
                for (k, item) in items.iter().enumerate() {
                    tx.execute(
                        &self.stmts.ins_item,
                        &[
                            &((first_item + k as u64) as i64),
                            &(order as i64),
                            &item.name,
                            &(item.quantity as i32),
                            &(item.unit_cents as i64),
                        ],
                    )
                    .map_err(sql)?;
                }
                tx.commit().map_err(sql)
            }
            ShopOp::Profile { u } => {
                let got = self
                    .client
                    .query_opt(&self.stmts.sel_user, &[&(u as i64)])
                    .map_err(sql)?;
                empty(got.is_none(), "profile", u)
            }
            ShopOp::OrderPage { u, page } => {
                let found = self
                    .client
                    .query_opt(&self.stmts.sel_user, &[&(u as i64)])
                    .map_err(sql)?;
                let rows = self
                    .client
                    .query(
                        &self.stmts.sel_page,
                        &[&(u as i64), &(PAGE as i64), &((page * PAGE) as i64)],
                    )
                    .map_err(sql)?;
                empty(
                    found.is_none() || (rows.is_empty() && page == 0),
                    "order_page",
                    u,
                )
            }
            ShopOp::OrderDetail { u } => self.detail(u),
        }
    }

    /// Restart, which empties `shared_buffers` — the counterpart of the
    /// WaveDB row's evict and SQLite's reopen.
    fn between_phases(
        &mut self,
        done: &str,
        _next: &str,
    ) -> Result<(), String> {
        if done != SETTLE_AFTER {
            return Ok(());
        }
        restart_server(&self.server, &self.socket_dir, self.sync)?;
        let mut client = connect_at(&self.socket_dir)?;
        self.stmts = Stmts::prepare(&mut client)?;
        self.client = client;
        Ok(())
    }
}

impl ShopPostgres {
    fn detail(&mut self, u: u64) -> Result<(), String> {
        let found = self
            .client
            .query_opt(&self.stmts.sel_user, &[&(u as i64)])
            .map_err(sql)?;
        let order: i64 = self
            .client
            .query_opt(&self.stmts.sel_first, &[&(u as i64)])
            .map_err(sql)?
            .map(|r| r.get(0))
            .ok_or_else(|| format!("order_detail: user {u} has no order"))?;
        let items = self
            .client
            .query(&self.stmts.sel_items, &[&order, &(PAGE as i64)])
            .map_err(sql)?;
        empty(found.is_none() || items.is_empty(), "order_detail", u)
    }
}

/// A read that returned nothing is a fast operation and a wrong one.
fn empty(missing: bool, what: &str, u: u64) -> Result<(), String> {
    if missing {
        return Err(format!("{what}: user {u} came back empty"));
    }
    Ok(())
}

fn sql(e: postgres::Error) -> String {
    format!("postgres: {e}")
}
