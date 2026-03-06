//! Borrowed views over wire bytes (RFC 0076): [`ListRef`], a `Vec`'s view,
//! and [`Wire`], the owned buffer every view borrows from.
//!
//! A view is built in one walk, the walk [`decode`](WaveWire::decode) makes,
//! and keeps references where the decode would copy. Every check happens in
//! that walk, so reading a view afterwards cannot fail.

use core::fmt;
use core::marker::PhantomData;

use crate::{Cursor, Error, Result, WaveWire, from_wire};

/// The view of a `Vec<T>`: its heap region and its element count, every
/// element already checked.
///
/// Elements are self-contained units, each `T`'s stack then its heap, so
/// [`get`](Self::get) is O(1) when no element has heap bytes (a list of
/// numbers, say) and O(i) otherwise. [`iter`](Self::iter) walks them in
/// order.
pub struct ListRef<'a, T> {
    region: &'a [u8],
    len: usize,
    _type: PhantomData<fn() -> T>,
}

impl<'a, T: WaveWire> ListRef<'a, T> {
    /// Check every element of `region` and count them.
    ///
    /// # Errors
    /// [`Error::UnexpectedEof`] when an element runs past the region, or an
    /// element's own view fault.
    pub fn view(region: &'a [u8]) -> Result<Self> {
        let mut rest = region;
        let mut len = 0;
        while !rest.is_empty() {
            let (_, used) = unit::<T>(rest)?;
            rest = &rest[used..];
            len += 1;
        }
        Ok(Self {
            region,
            len,
            _type: PhantomData,
        })
    }

    /// A list of fixed-width numbers: any bytes are valid, so the count is a
    /// division and nothing is walked.
    pub(crate) fn fixed(region: &'a [u8]) -> Result<Self> {
        let width = T::STACK_SIZE;
        if width == 0 || !region.len().is_multiple_of(width) {
            return Err(Error::UnexpectedEof);
        }
        Ok(Self {
            region,
            len: region.len() / width,
            _type: PhantomData,
        })
    }

    /// How many elements the list holds.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.len
    }

    /// Whether the list holds none.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The list's encoded elements, back to back.
    #[must_use]
    pub const fn bytes(&self) -> &'a [u8] {
        self.region
    }

    /// The element at `i`, or `None` past the end.
    #[must_use]
    pub fn get(&self, i: usize) -> Option<T::Ref<'a>> {
        if i >= self.len {
            return None;
        }
        // Every unit is at least its stack; all of them exactly that means
        // none has heap bytes, and the units sit at fixed strides.
        if self.len * T::STACK_SIZE == self.region.len() {
            let at = i * T::STACK_SIZE;
            return unit::<T>(self.region.get(at..)?).ok().map(|(r, _)| r);
        }
        self.iter().nth(i)
    }

    /// The elements in order.
    #[must_use]
    pub const fn iter(&self) -> ListIter<'a, T> {
        ListIter {
            rest: self.region,
            left: self.len,
            _type: PhantomData,
        }
    }

    /// The owned elements back: one allocation for the `Vec`, and whatever
    /// each element owns.
    #[must_use]
    pub fn to_vec(&self) -> Vec<T> {
        self.iter().map(T::from_ref).collect()
    }
}

/// View the unit at the start of `bytes`: `T`'s stack, then as much of the
/// rest as its heap takes. Returns the view and the bytes it spans.
fn unit<T: WaveWire>(bytes: &[u8]) -> Result<(T::Ref<'_>, usize)> {
    if bytes.len() < T::STACK_SIZE {
        return Err(Error::UnexpectedEof);
    }
    let (stack, heap) = bytes.split_at(T::STACK_SIZE);
    let mut sc = Cursor::new(stack);
    let mut hc = Cursor::new(heap);
    let r = T::view(&mut sc, &mut hc)?;
    let used = T::STACK_SIZE + hc.pos();
    // A unit of no bytes can never use up a region that has some: the walk
    // would spin on it (a `Vec<()>` whose region is not empty).
    if used == 0 {
        return Err(Error::UnexpectedEof);
    }
    Ok((r, used))
}

// By hand, not derived: a derive would ask `T: Copy`, and the list holds no `T`.
impl<T> Clone for ListRef<'_, T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for ListRef<'_, T> {}

impl<T: WaveWire> fmt::Debug for ListRef<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

impl<'a, T: WaveWire> IntoIterator for ListRef<'a, T> {
    type Item = T::Ref<'a>;
    type IntoIter = ListIter<'a, T>;
    fn into_iter(self) -> ListIter<'a, T> {
        self.iter()
    }
}

impl<'a, T: WaveWire> IntoIterator for &ListRef<'a, T> {
    type Item = T::Ref<'a>;
    type IntoIter = ListIter<'a, T>;
    fn into_iter(self) -> ListIter<'a, T> {
        self.iter()
    }
}

/// The elements of a [`ListRef`], in order.
pub struct ListIter<'a, T> {
    rest: &'a [u8],
    left: usize,
    _type: PhantomData<fn() -> T>,
}

impl<'a, T: WaveWire> Iterator for ListIter<'a, T> {
    type Item = T::Ref<'a>;

    fn next(&mut self) -> Option<T::Ref<'a>> {
        if self.left == 0 {
            return None;
        }
        // The list's view checked every element, so this cannot fail; were
        // it to, the iteration ends rather than panics.
        let Ok((r, used)) = unit::<T>(self.rest) else {
            self.left = 0;
            return None;
        };
        self.rest = &self.rest[used..];
        self.left -= 1;
        Some(r)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.left, Some(self.left))
    }
}

impl<T: WaveWire> ExactSizeIterator for ListIter<'_, T> {}

/// View a `[stack][heap]` buffer as `T`'s borrowed twin, the counterpart of
/// [`from_wire`]: the same walk and the same checks, nothing copied.
///
/// # Errors
/// Every error [`from_wire`] raises on the same bytes.
pub fn view_wire<T: WaveWire>(bytes: &[u8]) -> Result<T::Ref<'_>> {
    if bytes.len() < T::STACK_SIZE {
        return Err(Error::UnexpectedEof);
    }
    let (stack, heap) = bytes.split_at(T::STACK_SIZE);
    T::view(&mut Cursor::new(stack), &mut Cursor::new(heap))
}

/// An owned buffer holding a `T`'s encoding, which its views borrow from.
///
/// - **One allocation, one drop**, whatever `T` holds: a `String` field is
///   a range of the buffer, not an allocation of its own.
/// - **`Send` and `Sync` whatever `T` is**: it holds bytes, and claims no `T`.
/// - **[`view`](Self::view) checks the bytes on every call** — a view is
///   only handed out over bytes just checked — so view once and keep the
///   view.
///
/// `start` is where `T`'s encoding begins, past any envelope the buffer
/// carries before it (a stored record's head, say).
pub struct Wire<T> {
    bytes: Box<[u8]>,
    start: usize,
    _type: PhantomData<fn() -> T>,
}

impl<T: WaveWire> Wire<T> {
    /// Hold `bytes`, `T`'s encoding starting at `start`.
    #[must_use]
    pub const fn new(bytes: Box<[u8]>, start: usize) -> Self {
        Self {
            bytes,
            start,
            _type: PhantomData,
        }
    }

    /// Encode `value` into a buffer of its own.
    #[must_use]
    pub fn encode(value: &T) -> Self {
        Self::new(crate::to_wire(value).into_boxed_slice(), 0)
    }

    /// The borrowed twin, checked.
    ///
    /// # Errors
    /// Every error [`from_wire`] raises on the same bytes; `start` past the
    /// buffer's end is [`Error::UnexpectedEof`].
    pub fn view(&self) -> Result<T::Ref<'_>> {
        view_wire::<T>(self.body()?)
    }

    /// The owned value, decoded: every copy a view saves.
    ///
    /// # Errors
    /// As [`view`](Self::view).
    pub fn decode(&self) -> Result<T> {
        from_wire(self.body()?)
    }

    /// The whole buffer, envelope included.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// The buffer back, and where `T` starts in it.
    #[must_use]
    pub fn into_parts(self) -> (Box<[u8]>, usize) {
        (self.bytes, self.start)
    }

    fn body(&self) -> Result<&[u8]> {
        self.bytes.get(self.start..).ok_or(Error::UnexpectedEof)
    }
}

impl<T> Clone for Wire<T> {
    fn clone(&self) -> Self {
        Self {
            bytes: self.bytes.clone(),
            start: self.start,
            _type: PhantomData,
        }
    }
}

impl<T> fmt::Debug for Wire<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Wire")
            .field("len", &self.bytes.len())
            .field("start", &self.start)
            .finish()
    }
}
