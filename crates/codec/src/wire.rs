//! The primitive fields every packet is made of (MQTT 5 §1.5), read with
//! bounds checks and written with length checks.
//!
//! Every length here is remote-controlled when reading, so the module refuses
//! indexing, unchecked arithmetic and truncating casts.
#![deny(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::cast_possible_truncation
)]

use bytes::{BufMut, Bytes, BytesMut};

use crate::{DecodeError, EncodeError, PacketId};

/// The largest Variable Byte Integer: four bytes of seven bits (§1.5.5).
pub const MAX_VARINT: u32 = 268_435_455;

/// Reads fields out of one packet body. Slices share the buffer's
/// allocation instead of copying it.
pub struct Reader(pub Bytes);

impl Reader {
    pub fn take(&mut self, count: usize) -> Result<Bytes, DecodeError> {
        if self.0.len() < count {
            return Err(DecodeError::Truncated);
        }
        Ok(self.0.split_to(count))
    }

    pub fn byte(&mut self) -> Result<u8, DecodeError> {
        self.take(1)?.first().copied().ok_or(DecodeError::Truncated)
    }

    pub fn u16(&mut self) -> Result<u16, DecodeError> {
        let bytes: [u8; 2] = self
            .take(2)?
            .as_ref()
            .try_into()
            .map_err(|_| DecodeError::Truncated)?;
        Ok(u16::from_be_bytes(bytes))
    }

    pub fn u32(&mut self) -> Result<u32, DecodeError> {
        let bytes: [u8; 4] = self
            .take(4)?
            .as_ref()
            .try_into()
            .map_err(|_| DecodeError::Truncated)?;
        Ok(u32::from_be_bytes(bytes))
    }

    /// A Variable Byte Integer, at most four bytes (§1.5.5).
    pub fn varint(&mut self) -> Result<u32, DecodeError> {
        let mut value: u32 = 0;
        for shift in [0u32, 7, 14, 21] {
            let byte = self.byte()?;
            value |= u32::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(DecodeError::MalformedVarint)
    }

    /// Length-prefixed binary data.
    pub fn binary(&mut self) -> Result<Bytes, DecodeError> {
        let length = usize::from(self.u16()?);
        self.take(length)
    }

    /// A UTF-8 Encoded String: valid UTF-8, and no U+0000 (§1.5.4).
    pub fn string(&mut self) -> Result<Bytes, DecodeError> {
        let bytes = self.binary()?;
        utf8(&bytes)?;
        Ok(bytes)
    }

    pub fn packet_id(&mut self) -> Result<PacketId, DecodeError> {
        PacketId::new(self.u16()?).ok_or(DecodeError::ZeroPacketId)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn rest(self) -> Bytes {
        self.0
    }

    pub fn finish(self) -> Result<(), DecodeError> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err(DecodeError::TrailingBytes)
        }
    }
}

pub fn utf8(bytes: &[u8]) -> Result<(), DecodeError> {
    if bytes.contains(&0) {
        return Err(DecodeError::NulInString);
    }
    std::str::from_utf8(bytes).map_err(|_| DecodeError::InvalidUtf8)?;
    Ok(())
}

pub fn put_varint(value: u32, out: &mut BytesMut) -> Result<(), EncodeError> {
    if value > MAX_VARINT {
        return Err(EncodeError::TooLarge);
    }
    let mut rest = value;
    loop {
        let byte = u8::try_from(rest & 0x7f).map_err(|_| EncodeError::TooLarge)?;
        rest >>= 7;
        if rest == 0 {
            out.put_u8(byte);
            return Ok(());
        }
        out.put_u8(byte | 0x80);
    }
}

pub fn put_binary(bytes: &[u8], out: &mut BytesMut) -> Result<(), EncodeError> {
    let length = u16::try_from(bytes.len()).map_err(|_| EncodeError::FieldTooLong)?;
    out.put_u16(length);
    out.put_slice(bytes);
    Ok(())
}
