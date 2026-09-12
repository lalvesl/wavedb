//! A minimal JSON writer.
//!
//! Hand-rolled rather than `serde`: the project's stance is that byte layouts
//! are written, not derived, and a results record is a fixed shape that needs
//! no reflection. It also keeps the bench crate's dependency set to the
//! competitor drivers alone.

use std::fmt::Write as _;

/// An indenting object/array writer. Values are emitted in insertion order, so
/// a results file diffs line-by-line against the previous run.
pub struct Json {
    buf: String,
    depth: usize,
    fresh: bool,
}

impl Json {
    #[must_use]
    pub fn new() -> Self {
        Self {
            buf: String::with_capacity(8192),
            depth: 0,
            fresh: true,
        }
    }

    #[must_use]
    pub fn finish(mut self) -> String {
        self.buf.push('\n');
        self.buf
    }

    pub fn obj(&mut self, key: Option<&str>, body: impl FnOnce(&mut Self)) {
        self.open(key, '{');
        body(self);
        self.close('}');
    }

    pub fn arr(&mut self, key: Option<&str>, body: impl FnOnce(&mut Self)) {
        self.open(key, '[');
        body(self);
        self.close(']');
    }

    pub fn str(&mut self, key: &str, value: &str) {
        self.key(Some(key));
        self.buf.push('"');
        escape(&mut self.buf, value);
        self.buf.push('"');
    }

    pub fn num(&mut self, key: &str, value: u64) {
        self.key(Some(key));
        self.buf.push_str(&value.to_string());
    }

    /// Fixed to three decimals: a ratio printed at full float width invites
    /// reading noise as signal.
    pub fn ratio(&mut self, key: &str, value: f64) {
        self.key(Some(key));
        let _ = write!(self.buf, "{value:.3}");
    }

    pub fn boolean(&mut self, key: &str, value: bool) {
        self.key(Some(key));
        self.buf.push_str(if value { "true" } else { "false" });
    }

    /// A bare string element inside an array.
    pub fn elem(&mut self, value: &str) {
        self.key(None);
        self.buf.push('"');
        escape(&mut self.buf, value);
        self.buf.push('"');
    }

    fn open(&mut self, key: Option<&str>, brace: char) {
        self.key(key);
        self.buf.push(brace);
        self.depth += 1;
        self.fresh = true;
    }

    fn close(&mut self, brace: char) {
        self.depth -= 1;
        if !self.fresh {
            self.buf.push('\n');
            self.indent();
        }
        self.buf.push(brace);
        self.fresh = false;
    }

    fn key(&mut self, key: Option<&str>) {
        if self.fresh {
            self.fresh = false;
        } else {
            self.buf.push(',');
        }
        self.buf.push('\n');
        self.indent();
        if let Some(k) = key {
            self.buf.push('"');
            escape(&mut self.buf, k);
            self.buf.push_str("\": ");
        }
    }

    fn indent(&mut self) {
        for _ in 0..self.depth {
            self.buf.push_str("  ");
        }
    }
}

impl Default for Json {
    fn default() -> Self {
        Self::new()
    }
}

fn escape(out: &mut String, s: &str) {
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
}

/// FNV-1a 64, used for the short content hashes in the results record (host
/// fingerprint, `flake.lock`). Not a security hash — an identity for a lane.
#[must_use]
pub fn fnv1a(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}
