//! MySQL in the shop workload.
//!
//! Same three tables as the PostgreSQL row, in InnoDB's spelling, and the
//! same checkout-in-one-transaction shape.

use std::path::PathBuf;
use std::sync::Arc;

use mysql::prelude::Queryable as _;
use mysql::{Conn, Statement, params};

use crate::harness::{Driver, DriverFactory};
use crate::plan::op::ShopOp;
use crate::shop::PAGE;
use crate::systems::drivers::mysql::Shared;
use crate::systems::drivers::mysql_server::{connect_at, restart_server};

use super::SETTLE_AFTER;

pub const DDL: [&str; 3] = [
    "CREATE TABLE users (
  id      BIGINT PRIMARY KEY,
  name    VARCHAR(64)  NOT NULL,
  address VARCHAR(128) NOT NULL,
  city    VARCHAR(64)  NOT NULL,
  email   VARCHAR(128) NOT NULL
) ENGINE=InnoDB",
    "CREATE TABLE shopping (
  id              BIGINT PRIMARY KEY,
  user_id         BIGINT NOT NULL,
  bought_at       BIGINT NOT NULL,
  discount_cents  BIGINT NOT NULL,
  transport_cents BIGINT NOT NULL,
  INDEX idx_shopping_user (user_id, bought_at)
) ENGINE=InnoDB",
    "CREATE TABLE product (
  id          BIGINT PRIMARY KEY,
  shopping_id BIGINT NOT NULL,
  name        VARCHAR(64) NOT NULL,
  quantity    INT    NOT NULL,
  unit_cents  BIGINT NOT NULL,
  INDEX idx_product_shopping (shopping_id, name)
) ENGINE=InnoDB",
];

const INS_USER: &str = "INSERT INTO users (id, name, address, city, email) \
                        VALUES (:id, :name, :address, :city, :email)";
const INS_ORDER: &str = "INSERT INTO shopping (id, user_id, bought_at, discount_cents, \
     transport_cents) VALUES (:id, :user_id, :bought_at, :discount, :transport)";
const INS_ITEM: &str = "INSERT INTO product (id, shopping_id, name, quantity, unit_cents) \
     VALUES (:id, :shopping_id, :name, :qty, :unit)";
const SEL_USER: &str = "SELECT name FROM users WHERE id = :id";
const SEL_PAGE: &str = "SELECT id FROM shopping WHERE user_id = :id \
                        ORDER BY bought_at LIMIT :lim OFFSET :off";
const SEL_FIRST: &str = "SELECT id FROM shopping WHERE user_id = :id \
                         ORDER BY bought_at LIMIT 1";
const SEL_ITEMS: &str = "SELECT name FROM product WHERE shopping_id = :id \
                         ORDER BY name LIMIT :lim";

pub struct Factory {
    pub dir: PathBuf,
    pub database: String,
    pub flush: &'static str,
    pub server: Shared,
}

pub struct ShopMysql {
    conn: Conn,
    stmts: Stmts,
    dir: PathBuf,
    database: String,
    flush: &'static str,
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
    fn prepare(conn: &mut Conn) -> Result<Self, String> {
        Ok(Self {
            ins_user: conn.prep(INS_USER).map_err(sql)?,
            ins_order: conn.prep(INS_ORDER).map_err(sql)?,
            ins_item: conn.prep(INS_ITEM).map_err(sql)?,
            sel_user: conn.prep(SEL_USER).map_err(sql)?,
            sel_page: conn.prep(SEL_PAGE).map_err(sql)?,
            sel_first: conn.prep(SEL_FIRST).map_err(sql)?,
            sel_items: conn.prep(SEL_ITEMS).map_err(sql)?,
        })
    }
}

impl DriverFactory for Factory {
    type Driver = ShopMysql;

    fn consumers(&self) -> usize {
        1
    }

    fn build(&self, _shard: usize) -> Result<ShopMysql, String> {
        let mut conn = connect_at(&self.dir, Some(&self.database))?;
        Ok(ShopMysql {
            stmts: Stmts::prepare(&mut conn)?,
            conn,
            dir: self.dir.clone(),
            database: self.database.clone(),
            flush: self.flush,
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

impl Driver for ShopMysql {
    type Op = ShopOp;

    fn execute(&mut self, op: ShopOp) -> Result<(), String> {
        match op {
            ShopOp::Signup { u, row } => self
                .conn
                .exec_drop(
                    &self.stmts.ins_user,
                    params! {
                        "id" => u,
                        "name" => row.name,
                        "address" => row.address,
                        "city" => row.city,
                        "email" => row.email,
                    },
                )
                .map_err(sql),
            ShopOp::Checkout {
                u,
                order,
                first_item,
                row,
                items,
                ..
            } => {
                // One transaction: one barrier for the whole checkout.
                self.conn.query_drop("START TRANSACTION").map_err(sql)?;
                self.conn
                    .exec_drop(
                        &self.stmts.ins_order,
                        params! {
                            "id" => order,
                            "user_id" => u,
                            "bought_at" => row.bought_at,
                            "discount" => row.discount_cents,
                            "transport" => row.transport_cents,
                        },
                    )
                    .map_err(sql)?;
                for (k, item) in items.iter().enumerate() {
                    self.conn
                        .exec_drop(
                            &self.stmts.ins_item,
                            params! {
                                "id" => first_item + k as u64,
                                "shopping_id" => order,
                                "name" => &item.name,
                                "qty" => item.quantity,
                                "unit" => item.unit_cents,
                            },
                        )
                        .map_err(sql)?;
                }
                self.conn.query_drop("COMMIT").map_err(sql)
            }
            ShopOp::Profile { u } => {
                let got: Option<String> = self
                    .conn
                    .exec_first(&self.stmts.sel_user, params! { "id" => u })
                    .map_err(sql)?;
                empty(got.is_none(), "profile", u)
            }
            ShopOp::OrderPage { u, page } => {
                let found: Option<String> = self
                    .conn
                    .exec_first(&self.stmts.sel_user, params! { "id" => u })
                    .map_err(sql)?;
                let rows: Vec<u64> = self
                    .conn
                    .exec(
                        &self.stmts.sel_page,
                        params! {
                            "id" => u,
                            "lim" => PAGE as u64,
                            "off" => (page * PAGE) as u64,
                        },
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

    /// Restart, which empties the InnoDB buffer pool.
    fn between_phases(
        &mut self,
        done: &str,
        _next: &str,
    ) -> Result<(), String> {
        if done != SETTLE_AFTER {
            return Ok(());
        }
        restart_server(&self.server, &self.dir, self.flush)?;
        let mut conn = connect_at(&self.dir, Some(&self.database))?;
        self.stmts = Stmts::prepare(&mut conn)?;
        self.conn = conn;
        Ok(())
    }
}

impl ShopMysql {
    fn detail(&mut self, u: u64) -> Result<(), String> {
        let found: Option<String> = self
            .conn
            .exec_first(&self.stmts.sel_user, params! { "id" => u })
            .map_err(sql)?;
        let order: u64 = self
            .conn
            .exec_first(&self.stmts.sel_first, params! { "id" => u })
            .map_err(sql)?
            .ok_or_else(|| format!("order_detail: user {u} has no order"))?;
        let items: Vec<String> = self
            .conn
            .exec(
                &self.stmts.sel_items,
                params! {
                    "id" => order,
                    "lim" => PAGE as u64,
                },
            )
            .map_err(sql)?;
        empty(found.is_none() || items.is_empty(), "order_detail", u)
    }
}

fn empty(missing: bool, what: &str, u: u64) -> Result<(), String> {
    if missing {
        return Err(format!("{what}: user {u} came back empty"));
    }
    Ok(())
}

fn sql(e: mysql::Error) -> String {
    format!("mysql: {e}")
}
