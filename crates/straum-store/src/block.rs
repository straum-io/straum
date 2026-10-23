//! zstd compression of object blocks and the store's dictionary.
//!
//! Level, block unit and dictionary size come from the block survey: one
//! block per group commit at level 1 with a 64 KiB dictionary stayed within
//! 1.5 times the size of 1 MiB blocks on the three Loghub datasets measured.

use std::collections::HashMap;

use crate::codec::NO_DICT;

/// zstd level of every block.
pub const LEVEL: i32 = 1;
/// Largest dictionary trained.
pub const DICT_SIZE: usize = 64 << 10;
/// Most sample bytes given to dictionary training.
pub const DICT_SAMPLE_CAP: usize = 10 << 20;

/// Largest compressed size of `raw_len` raw bytes.
pub fn compress_bound(raw_len: usize) -> usize {
    zstd::compress_bound(raw_len)
}

/// The compressor for new blocks and a decompressor per dictionary.
pub struct Codecs {
    compressor: zstd::bulk::Compressor<'static>,
    /// Dictionary number new blocks use, or `NO_DICT`.
    dict: u64,
    decoders: HashMap<u64, zstd::bulk::Decompressor<'static>>,
}

impl Codecs {
    pub fn new() -> std::io::Result<Self> {
        let mut decoders = HashMap::new();
        decoders.insert(NO_DICT, zstd::bulk::Decompressor::new()?);
        Ok(Self {
            compressor: zstd::bulk::Compressor::new(LEVEL)?,
            dict: NO_DICT,
            decoders,
        })
    }

    /// Makes dictionary `number` known for decompression and, from now on,
    /// the dictionary of new blocks.
    pub fn add_dictionary(&mut self, number: u64, dict: &[u8]) -> std::io::Result<()> {
        self.decoders
            .insert(number, zstd::bulk::Decompressor::with_dictionary(dict)?);
        self.compressor = zstd::bulk::Compressor::with_dictionary(LEVEL, dict)?;
        self.dict = number;
        Ok(())
    }

    /// The dictionary number new blocks are compressed with.
    pub fn dict(&self) -> u64 {
        self.dict
    }

    pub fn compress(&mut self, raw: &[u8]) -> std::io::Result<Vec<u8>> {
        self.compressor.compress(raw)
    }

    /// Decompresses a stored block and checks it against its recorded raw
    /// length and CRC32C. Every failure means the stored bytes are wrong.
    pub fn decompress(
        &mut self,
        dict: u64,
        data: &[u8],
        raw_len: u64,
        crc: u32,
    ) -> Result<Vec<u8>, &'static str> {
        let decoder = self
            .decoders
            .get_mut(&dict)
            .ok_or("block references an unknown dictionary")?;
        let cap = usize::try_from(raw_len).map_err(|_| "block too large for this platform")?;
        let mut raw = Vec::with_capacity(cap);
        decoder
            .decompress_to_buffer(data, &mut raw)
            .map_err(|_| "block does not decompress")?;
        if raw.len() != cap {
            return Err("block decompresses to the wrong length");
        }
        if crc32c::crc32c(&raw) != crc {
            return Err("block fails its CRC");
        }
        Ok(raw)
    }
}

/// Trains a dictionary on `samples`; empty when zstd cannot build one (too
/// few or too uniform samples). Deterministic: the same samples give the
/// same bytes.
pub fn train(samples: &[&[u8]]) -> Vec<u8> {
    zstd::dict::from_samples(samples, DICT_SIZE).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_with_and_without_dictionary() {
        let mut c = Codecs::new().unwrap();
        let raw = b"2026-10-23 sshd[123]: Accepted publickey for root".repeat(10);
        let packed = c.compress(&raw).unwrap();
        let crc = crc32c::crc32c(&raw);
        let len = raw.len() as u64;
        assert_eq!(c.decompress(NO_DICT, &packed, len, crc).unwrap(), raw);

        let lines: Vec<Vec<u8>> = (0..2000u32)
            .map(|i| format!("2026-10-23 12:{:02}:{:02} sshd[{}]: Failed password for user{} from 10.0.{}.{} port {}", i % 60, i % 59, 1000 + i, i % 37, i % 251, i % 199, 40000 + i).into_bytes())
            .collect();
        let samples: Vec<&[u8]> = lines.iter().map(Vec::as_slice).collect();
        let dict = train(&samples);
        assert!(!dict.is_empty() && dict.len() <= DICT_SIZE);
        assert_eq!(train(&samples), dict, "training is deterministic");
        c.add_dictionary(1, &dict).unwrap();
        assert_eq!(c.dict(), 1);
        let packed = c.compress(&raw).unwrap();
        assert_eq!(c.decompress(1, &packed, len, crc).unwrap(), raw);
        assert!(c.decompress(NO_DICT, &packed, len, crc).is_err());
        assert!(c.decompress(2, &packed, len, crc).is_err());
    }

    #[test]
    fn damaged_blocks_are_rejected() {
        let mut c = Codecs::new().unwrap();
        let raw = vec![42u8; 1000];
        let packed = c.compress(&raw).unwrap();
        let crc = crc32c::crc32c(&raw);
        assert_eq!(
            c.decompress(NO_DICT, &packed, 1000, crc ^ 1),
            Err("block fails its CRC")
        );
        assert!(c.decompress(NO_DICT, &packed, 999, crc).is_err());
        assert_eq!(
            c.decompress(NO_DICT, &packed, 1001, crc),
            Err("block decompresses to the wrong length")
        );
        assert!(
            c.decompress(NO_DICT, &packed[..packed.len() - 1], 1000, crc)
                .is_err()
        );
        let mut bad = packed.clone();
        bad[0] ^= 0xff;
        assert!(c.decompress(NO_DICT, &bad, 1000, crc).is_err());
    }
}
