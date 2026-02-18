//! [`IdStreamExt`] — set algebra over `Id` streams (an index's anchors): the
//! no-DSL composite query.
//!
//! `Store`-agnostic, so it runs native or web. Streams from different indexes
//! arrive in different orders, so `intersect`/`except` buffer the argument side
//! into an `Id` set and probe the receiver; `union` merges and dedups.

use std::collections::HashSet;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;

use crate::error::Result;
use crate::id::Id;

/// Set algebra over `Id` streams.
pub trait IdStreamExt: Stream<Item = Result<Id>> + Sized + Unpin {
    /// Ids present in **both** streams (AND).
    fn intersect<B>(self, other: B) -> Intersect<Self, B>
    where
        B: Stream<Item = Result<Id>> + Unpin,
    {
        Intersect {
            left: self,
            right: other,
            buffered: None,
        }
    }

    /// Ids present in **either** stream, deduplicated (OR).
    fn union<B>(self, other: B) -> Union<Self, B>
    where
        B: Stream<Item = Result<Id>> + Unpin,
    {
        Union {
            left: self,
            right: other,
            seen: HashSet::new(),
            left_done: false,
        }
    }

    /// Ids in the receiver but **not** in `other` (NOT / difference).
    fn except<B>(self, other: B) -> Except<Self, B>
    where
        B: Stream<Item = Result<Id>> + Unpin,
    {
        Except {
            left: self,
            right: other,
            buffered: None,
        }
    }
}

impl<T: Stream<Item = Result<Id>> + Sized + Unpin> IdStreamExt for T {}

/// Drive `s` to completion, collecting raw ids into `set`; propagate errors and
/// pending. Returns `Poll::Ready(Ok(()))` once the stream is exhausted.
fn drain_into<S>(
    s: &mut S,
    set: &mut HashSet<u128>,
    cx: &mut Context<'_>,
) -> Poll<Result<()>>
where
    S: Stream<Item = Result<Id>> + Unpin,
{
    loop {
        match Pin::new(&mut *s).poll_next(cx) {
            Poll::Ready(Some(Ok(id))) => {
                set.insert(id.raw());
            }
            Poll::Ready(Some(Err(e))) => return Poll::Ready(Err(e)),
            Poll::Ready(None) => return Poll::Ready(Ok(())),
            Poll::Pending => return Poll::Pending,
        }
    }
}

/// Intersection adapter — see [`IdStreamExt::intersect`].
pub struct Intersect<A, B> {
    left: A,
    right: B,
    buffered: Option<HashSet<u128>>,
}

impl<A, B> Stream for Intersect<A, B>
where
    A: Stream<Item = Result<Id>> + Unpin,
    B: Stream<Item = Result<Id>> + Unpin,
{
    type Item = Result<Id>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.buffered.is_none() {
            let mut set = HashSet::new();
            match drain_into(&mut this.right, &mut set, cx) {
                Poll::Ready(Ok(())) => this.buffered = Some(set),
                Poll::Ready(Err(e)) => return Poll::Ready(Some(Err(e))),
                Poll::Pending => return Poll::Pending,
            }
        }
        let set = this.buffered.as_ref().unwrap();
        loop {
            match Pin::new(&mut this.left).poll_next(cx) {
                Poll::Ready(Some(Ok(id))) => {
                    if set.contains(&id.raw()) {
                        return Poll::Ready(Some(Ok(id)));
                    }
                }
                other => return other,
            }
        }
    }
}

/// Difference adapter — see [`IdStreamExt::except`].
pub struct Except<A, B> {
    left: A,
    right: B,
    buffered: Option<HashSet<u128>>,
}

impl<A, B> Stream for Except<A, B>
where
    A: Stream<Item = Result<Id>> + Unpin,
    B: Stream<Item = Result<Id>> + Unpin,
{
    type Item = Result<Id>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.buffered.is_none() {
            let mut set = HashSet::new();
            match drain_into(&mut this.right, &mut set, cx) {
                Poll::Ready(Ok(())) => this.buffered = Some(set),
                Poll::Ready(Err(e)) => return Poll::Ready(Some(Err(e))),
                Poll::Pending => return Poll::Pending,
            }
        }
        let set = this.buffered.as_ref().unwrap();
        loop {
            match Pin::new(&mut this.left).poll_next(cx) {
                Poll::Ready(Some(Ok(id))) => {
                    if !set.contains(&id.raw()) {
                        return Poll::Ready(Some(Ok(id)));
                    }
                }
                other => return other,
            }
        }
    }
}

/// Union adapter — see [`IdStreamExt::union`]. Emits the left stream (recording
/// each id), then the right stream skipping ids already emitted.
pub struct Union<A, B> {
    left: A,
    right: B,
    seen: HashSet<u128>,
    left_done: bool,
}

impl<A, B> Stream for Union<A, B>
where
    A: Stream<Item = Result<Id>> + Unpin,
    B: Stream<Item = Result<Id>> + Unpin,
{
    type Item = Result<Id>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if !this.left_done {
            loop {
                match Pin::new(&mut this.left).poll_next(cx) {
                    Poll::Ready(Some(Ok(id))) => {
                        if this.seen.insert(id.raw()) {
                            return Poll::Ready(Some(Ok(id)));
                        }
                    }
                    Poll::Ready(Some(Err(e))) => {
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Ready(None) => {
                        this.left_done = true;
                        break;
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }
        }
        loop {
            match Pin::new(&mut this.right).poll_next(cx) {
                Poll::Ready(Some(Ok(id))) => {
                    if this.seen.insert(id.raw()) {
                        return Poll::Ready(Some(Ok(id)));
                    }
                }
                other => return other,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::executor::block_on;
    use futures::{Stream, StreamExt, stream};

    use super::IdStreamExt;
    use crate::error::Result;
    use crate::id::Id;
    use crate::u48::U48;

    fn ids(keys: &[u64]) -> impl Stream<Item = Result<Id>> + Unpin {
        let v: Vec<Result<Id>> = keys
            .iter()
            .map(|&k| Ok(Id::new(k, U48::from(1u32), false, 0)))
            .collect();
        stream::iter(v)
    }

    fn keys_of(out: Vec<Result<Id>>) -> Vec<u64> {
        out.into_iter().map(|r| r.unwrap().key()).collect()
    }

    #[test]
    fn intersect_keeps_common() {
        block_on(async {
            let got = ids(&[1, 2, 3, 4])
                .intersect(ids(&[2, 4, 6]))
                .collect()
                .await;
            assert_eq!(keys_of(got), vec![2, 4]);
        });
    }

    #[test]
    fn union_dedups() {
        block_on(async {
            let got = ids(&[1, 2, 3]).union(ids(&[3, 4, 1])).collect().await;
            assert_eq!(keys_of(got), vec![1, 2, 3, 4]);
        });
    }

    #[test]
    fn except_removes_right() {
        block_on(async {
            let got = ids(&[1, 2, 3, 4]).except(ids(&[2, 4])).collect().await;
            assert_eq!(keys_of(got), vec![1, 3]);
        });
    }

    #[test]
    fn error_propagates() {
        block_on(async {
            let left = stream::iter(vec![
                Ok(Id::default()),
                Err(crate::error::Error::Wire(wavedb_wire::Error::Utf8)),
            ]);
            let right = ids(&[]);
            let got: Vec<Result<Id>> = left.intersect(right).collect().await;
            assert!(got.iter().any(Result::is_err));
        });
    }
}
