//! Where a [`BlockFile`](crate::BlockFile)'s bytes live: `data.bin`, or RAM.
//!
//! A store in object mode ([RFC 0070] §9) has no disk: its pages live in the
//! object store, and what it holds besides — the pages not uploaded yet, and
//! the ones it caches — sits in RAM, laid out exactly as `data.bin` would
//! lay them out. So the block layer above is the same in both, and only
//! these few calls tell them apart, by a plain `match`.
//!
//! RAM is held in chunks of [`CHUNK_BYTES`], so growing never copies what is
//! already there and a truncation gives whole chunks back.
//!
//! [RFC 0070]: ../../../rfcs/0070-object-storage-backend-WIP.md

use std::cell::{Cell, RefCell};
use std::fs::File;
use std::io::{Error, ErrorKind, Result};
use std::os::unix::fs::FileExt;

/// Bytes of one RAM chunk (1 MiB).
pub const CHUNK_BYTES: usize = 1 << 20;

/// The bytes behind a block file.
#[derive(Debug)]
pub enum Backing {
    /// `data.bin`.
    File(File),
    /// RAM, for a store with no disk.
    Memory(Memory),
}

/// A file's worth of bytes in RAM.
#[derive(Debug, Default)]
pub struct Memory {
    chunks: RefCell<Vec<Box<[u8]>>>,
    len: Cell<u64>,
}

impl Backing {
    /// The length in bytes.
    pub fn len(&self) -> Result<u64> {
        match self {
            Self::File(file) => Ok(file.metadata()?.len()),
            Self::Memory(memory) => Ok(memory.len.get()),
        }
    }

    /// Fill `buf` from `offset`.
    pub fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        match self {
            Self::File(file) => file.read_exact_at(buf, offset),
            Self::Memory(memory) => memory.read(buf, offset),
        }
    }

    /// Write all of `bytes` at `offset`, within the current length.
    pub fn write_all_at(&self, bytes: &[u8], offset: u64) -> Result<()> {
        match self {
            Self::File(file) => file.write_all_at(bytes, offset),
            Self::Memory(memory) => {
                memory.write(bytes, offset);
                Ok(())
            }
        }
    }

    /// Grow or shrink to exactly `len` bytes; grown bytes read as zero.
    pub fn set_len(&self, len: u64) -> Result<()> {
        match self {
            Self::File(file) => file.set_len(len),
            Self::Memory(memory) => {
                memory.set_len(len);
                Ok(())
            }
        }
    }

    /// Make what was written durable — nothing to do in RAM.
    pub fn sync_all(&self) -> Result<()> {
        match self {
            Self::File(file) => file.sync_all(),
            Self::Memory(_) => Ok(()),
        }
    }
}

impl Memory {
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        if offset + buf.len() as u64 > self.len.get() {
            return Err(Error::new(ErrorKind::UnexpectedEof, "past the end"));
        }
        let chunks = self.chunks.borrow();
        let mut done = 0;
        while done < buf.len() {
            let (chunk, at) = locate(offset + done as u64);
            let n = (CHUNK_BYTES - at).min(buf.len() - done);
            buf[done..done + n].copy_from_slice(&chunks[chunk][at..at + n]);
            done += n;
        }
        Ok(())
    }

    /// The caller grew the length first, so every chunk written exists.
    fn write(&self, bytes: &[u8], offset: u64) {
        let mut chunks = self.chunks.borrow_mut();
        let mut done = 0;
        while done < bytes.len() {
            let (chunk, at) = locate(offset + done as u64);
            let n = (CHUNK_BYTES - at).min(bytes.len() - done);
            if let Some(chunk) = chunks.get_mut(chunk) {
                chunk[at..at + n].copy_from_slice(&bytes[done..done + n]);
            }
            done += n;
        }
    }

    fn set_len(&self, len: u64) {
        let mut chunks = self.chunks.borrow_mut();
        let old = self.len.get();
        if len < old {
            // What is cut off reads as zero if the length grows back.
            let (chunk, at) = locate(len);
            if let Some(tail) = chunks.get_mut(chunk) {
                tail[at..].fill(0);
            }
        }
        let want = (len as usize).div_ceil(CHUNK_BYTES);
        chunks.truncate(want);
        while chunks.len() < want {
            chunks.push(vec![0u8; CHUNK_BYTES].into_boxed_slice());
        }
        self.len.set(len);
    }
}

/// The chunk holding byte `offset`, and where in it.
const fn locate(offset: u64) -> (usize, usize) {
    (
        (offset / CHUNK_BYTES as u64) as usize,
        (offset % CHUNK_BYTES as u64) as usize,
    )
}

#[cfg(test)]
mod tests {
    use super::{Backing, CHUNK_BYTES, Memory};

    fn memory() -> Backing {
        Backing::Memory(Memory::default())
    }

    #[test]
    fn bytes_written_across_chunks_read_back() {
        let m = memory();
        let at = CHUNK_BYTES as u64 - 3;
        m.set_len(at + 10).unwrap();
        m.write_all_at(b"0123456789", at).unwrap();
        let mut back = [0u8; 10];
        m.read_exact_at(&mut back, at).unwrap();
        assert_eq!(&back, b"0123456789");
        assert_eq!(m.len().unwrap(), at + 10);
    }

    #[test]
    fn a_read_past_the_end_fails() {
        let m = memory();
        m.set_len(8).unwrap();
        let mut buf = [0u8; 9];
        assert!(m.read_exact_at(&mut buf, 0).is_err());
    }

    /// A truncation gives whole chunks back, and what it cut reads as zero
    /// once the length grows again — as a file would.
    #[test]
    fn a_truncation_frees_chunks_and_zeroes_the_cut() {
        let m = memory();
        m.set_len(3 * CHUNK_BYTES as u64).unwrap();
        m.write_all_at(&[7; 16], 100).unwrap();
        m.set_len(104).unwrap();
        let Backing::Memory(inner) = &m else {
            unreachable!()
        };
        assert_eq!(inner.chunks.borrow().len(), 1);
        m.set_len(200).unwrap();
        let mut back = [0u8; 16];
        m.read_exact_at(&mut back, 100).unwrap();
        assert_eq!(back[..4], [7; 4]);
        assert_eq!(back[4..], [0; 12]);
    }
}
