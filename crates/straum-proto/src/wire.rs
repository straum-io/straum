//! Primitive encodings of docs/protocol.md section 1.

use crate::DecodeError;

pub(crate) fn put_u8(out: &mut Vec<u8>, v: u8) {
    out.push(v);
}

pub(crate) fn put_u16(out: &mut Vec<u8>, v: u16) {
    out.extend_from_slice(&v.to_le_bytes());
}

pub(crate) fn put_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes());
}

pub(crate) fn put_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_le_bytes());
}

/// Lengths above the field's range are a caller bug the encoder cannot
/// represent; they are truncated to the field's maximum so the frame stays
/// decodable, and the broker's limits keep real values far below it.
pub(crate) fn put_string(out: &mut Vec<u8>, s: &str) {
    let len = s.len().min(u16::MAX as usize);
    put_u16(out, len as u16);
    out.extend_from_slice(&s.as_bytes()[..len]);
}

pub(crate) fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    let len = b.len().min(u32::MAX as usize);
    put_u32(out, len as u32);
    out.extend_from_slice(&b[..len]);
}

pub(crate) fn put_short_bytes(out: &mut Vec<u8>, b: &[u8]) {
    let len = b.len().min(u16::MAX as usize);
    put_u16(out, len as u16);
    out.extend_from_slice(&b[..len]);
}

pub(crate) fn put_count(out: &mut Vec<u8>, n: usize) {
    put_u32(out, n.min(u32::MAX as usize) as u32);
}

/// Reads fields from a body; every read checks the bytes left.
pub(crate) struct Reader<'a> {
    buf: &'a [u8],
}

impl<'a> Reader<'a> {
    pub(crate) fn new(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        if self.buf.len() < n {
            return Err(DecodeError::Truncated);
        }
        let (head, tail) = self.buf.split_at(n);
        self.buf = tail;
        Ok(head)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], DecodeError> {
        let mut a = [0; N];
        a.copy_from_slice(self.take(N)?);
        Ok(a)
    }

    pub(crate) fn u8(&mut self) -> Result<u8, DecodeError> {
        Ok(self.take(1)?[0])
    }

    pub(crate) fn u16(&mut self) -> Result<u16, DecodeError> {
        self.array().map(u16::from_le_bytes)
    }

    pub(crate) fn u32(&mut self) -> Result<u32, DecodeError> {
        self.array().map(u32::from_le_bytes)
    }

    pub(crate) fn u64(&mut self) -> Result<u64, DecodeError> {
        self.array().map(u64::from_le_bytes)
    }

    pub(crate) fn string(&mut self) -> Result<String, DecodeError> {
        let len = self.u16()? as usize;
        let b = self.take(len)?;
        String::from_utf8(b.to_vec()).map_err(|_| DecodeError::InvalidUtf8)
    }

    pub(crate) fn bytes(&mut self) -> Result<Vec<u8>, DecodeError> {
        let len = self.u32()? as usize;
        Ok(self.take(len)?.to_vec())
    }

    pub(crate) fn short_bytes(&mut self) -> Result<Vec<u8>, DecodeError> {
        let len = self.u16()? as usize;
        Ok(self.take(len)?.to_vec())
    }

    /// A count of elements that each take at least `min_size` bytes. A count
    /// the remaining bytes cannot hold is `Truncated` before anything is
    /// allocated.
    pub(crate) fn count(&mut self, min_size: usize) -> Result<usize, DecodeError> {
        let n = self.u32()? as usize;
        if n.saturating_mul(min_size.max(1)) > self.buf.len() {
            return Err(DecodeError::Truncated);
        }
        Ok(n)
    }

    pub(crate) fn finish(self) -> Result<(), DecodeError> {
        if self.buf.is_empty() {
            Ok(())
        } else {
            Err(DecodeError::TrailingBytes)
        }
    }
}
