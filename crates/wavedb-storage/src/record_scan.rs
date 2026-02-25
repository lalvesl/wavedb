//! Walks over a type's record log ([`crate::record_log`]): the settle
//! round's scan from `s`, and the probe's census of every entry — split from
//! the log for the file budget.

use crate::record_index::Loc;
use crate::record_log::{Entry, TypeLog, parse};

impl TypeLog {
    /// The entries from `s` up to `until`, each with its location and the
    /// position just past it.
    pub fn scan(&self, until: u64) -> Scan<'_> {
        let at = self.order.partition_point(|&n| {
            self.chunk(n).is_none_or(|c| c.end <= self.s)
        });
        let offset = self
            .order
            .get(at)
            .and_then(|&n| self.chunk(n))
            .map_or(0, |c| (self.s.saturating_sub(c.start)) as usize);
        Scan {
            log: self,
            at,
            offset,
            pos: self.s,
            until,
        }
    }

    /// Every entry held, in log order, with its location — the probe's
    /// census of what the log spends its bytes on.
    #[cfg(feature = "probe")]
    pub fn each(&self, mut f: impl FnMut(Loc, Entry<'_>)) {
        for &n in &self.order {
            let Some(chunk) = self.chunk(n) else { continue };
            let mut at = 0;
            while let Some((entry, len)) = parse(&chunk.bytes, at) {
                let loc = Loc {
                    slot: self.slot,
                    offset: at as u16,
                    chunk: n,
                };
                f(loc, entry);
                at += len;
            }
        }
    }
}

/// [`TypeLog::scan`]'s walk.
pub struct Scan<'a> {
    log: &'a TypeLog,
    at: usize,
    offset: usize,
    pos: u64,
    until: u64,
}

impl<'a> Iterator for Scan<'a> {
    type Item = (Loc, Entry<'a>, u64);

    fn next(&mut self) -> Option<Self::Item> {
        while self.pos < self.until {
            let n = *self.log.order.get(self.at)?;
            let chunk = self.log.chunk(n)?;
            let Some((entry, len)) = parse(&chunk.bytes, self.offset) else {
                self.at += 1;
                self.offset = 0;
                continue;
            };
            let loc = Loc {
                slot: self.log.slot,
                offset: self.offset as u16,
                chunk: n,
            };
            self.offset += len;
            self.pos += len as u64;
            return Some((loc, entry, self.pos));
        }
        None
    }
}
