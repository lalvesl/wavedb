//! The browser-demo node: contact-book's registry served on a loopback
//! port, for `scripts/browser_demo.sh` to point the headless-Chrome test
//! at (`crates/wavedb-wasm/tests/live_node.rs`).
//!
//! Binds port 0, prints the resolved address as `LISTENING <addr>` (the
//! script parses it), and serves until killed. The signing secret is the
//! fixed [`contact_book::DEMO_SECRET`] so the browser test can mint its own
//! access token — demo plumbing, exactly like the native e2e test's.

use std::io::Write as _;

use contact_book::{DEMO_SECRET, REGISTRY};
use wavedb_quick_node::Server;

fn main() {
    wavedb_platform::carrier::block_on(async {
        // The store lives for the process only — the demo proves the wire, not
        // node durability (the native e2e covers reopen).
        let dir = tempfile::tempdir().expect("create a temp data dir");
        let bound = Server::new(REGISTRY)
            .secret(DEMO_SECRET)
            .data_dir(dir.path())
            .bind("127.0.0.1:0")
            .await
            .expect("open the engine and bind");
        let addr = bound.local_addr().expect("read the bound address");
        println!("LISTENING {addr}");
        std::io::stdout().flush().expect("flush the address line");
        bound.run().await.expect("serve");
    });
}
