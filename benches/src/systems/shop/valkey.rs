//! The Valkey shop row's **setup**: the preload, bulk and untimed.
//!
//! Pipelined in chunks — Valkey's own bulk shape — through the same queueing
//! helpers the measured driver uses, so the preloaded records and the ones a
//! checkout writes cannot differ in layout. Ids are numbered exactly as the
//! other preloads number them, so the generator's next order id lands after
//! the last preloaded one on every system.

use std::path::Path;

use super::ShopCfg;
use crate::shop::{product_row, shopping_row, user_row};
use crate::systems::drivers::resp::Resp;
use crate::systems::drivers::shop::valkey::{
    queue_item, queue_order, queue_user,
};
use crate::systems::drivers::valkey_server::sock;

/// Fill the dataset through the server in `dir`.
///
/// # Errors
/// The connection failed or the server refused a command.
pub fn preload(cfg: &ShopCfg, dir: &Path) -> Result<(), String> {
    /// Commands in flight before the replies are read back. Bounded so a
    /// large tier's preload is not also a test of the socket buffers.
    const CHUNK: usize = 1000;
    let mut conn = Resp::connect(&sock(dir))?;
    let mut queued = 0;
    let mut order_id = 0u64;
    let mut item_id = 0u64;
    for u in 0..cfg.users {
        queue_user(&mut conn, u, &user_row(u, cfg.seed))?;
        queued += 1;
        for s in 0..crate::shop::shopping_count(u, cfg.seed, cfg.orders_max) {
            order_id += 1;
            queue_order(&mut conn, order_id, u, &shopping_row(u, s, cfg.seed))?;
            queued += 2;
            for p in
                0..crate::shop::product_count(u, s, cfg.seed, cfg.items_max)
            {
                item_id += 1;
                let row = product_row(u, s, p, cfg.seed);
                queue_item(&mut conn, item_id, order_id, &row)?;
                queued += 2;
            }
        }
        if queued >= CHUNK {
            conn.replies(queued)?;
            queued = 0;
        }
    }
    conn.replies(queued).map(|_| ())
}
