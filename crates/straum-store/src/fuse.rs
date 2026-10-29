//! Binary fuse filter with 8-bit fingerprints (Graf and Lemire, "Binary
//! Fuse Filters: Fast and Smaller Than Xor Filters", 2022), arity 3:
//! about 9 bits per key at a 0.39 % false-positive rate (ADR-0014 §1,
//! "Algorithm selection"). Static: built once from a set of keys, never
//! changed. Keys are uniform 64-bit values (bytes of BLAKE3 ids), so no
//! further mixing is needed before the seed is added.

/// A built filter: its parameters and fingerprints, which `encode` writes
/// and `decode` reads back.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Fuse8 {
    seed: u64,
    segment_length: u32,
    segment_count_length: u32,
    fingerprints: Vec<u8>,
}

fn mix(mut h: u64) -> u64 {
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    h ^ (h >> 33)
}

fn fingerprint(hash: u64) -> u8 {
    (hash ^ (hash >> 32)) as u8
}

fn mulhi(a: u64, b: u64) -> u64 {
    ((u128::from(a) * u128::from(b)) >> 64) as u64
}

impl Fuse8 {
    fn hashes(&self, hash: u64) -> [usize; 3] {
        let mask = u64::from(self.segment_length - 1);
        let h0 = mulhi(hash, u64::from(self.segment_count_length));
        let h1 = h0 + u64::from(self.segment_length);
        let h2 = h1 + u64::from(self.segment_length);
        let h1 = h1 ^ ((hash >> 18) & mask);
        let h2 = h2 ^ (hash & mask);
        [h0 as usize, h1 as usize, h2 as usize]
    }

    /// Whether `key` may be in the set: always for a member, for about
    /// 0.39 % of other keys.
    pub fn contains(&self, key: u64) -> bool {
        if self.fingerprints.is_empty() {
            return false;
        }
        let hash = mix(key.wrapping_add(self.seed));
        let [h0, h1, h2] = self.hashes(hash);
        fingerprint(hash) ^ self.fingerprints[h0] ^ self.fingerprints[h1] ^ self.fingerprints[h2]
            == 0
    }

    /// The filter of no keys: holds nothing, contains nothing.
    pub fn empty() -> Self {
        Self {
            seed: 0,
            segment_length: 1,
            segment_count_length: 0,
            fingerprints: Vec::new(),
        }
    }

    /// Bytes of fingerprints held.
    pub fn bytes(&self) -> usize {
        self.fingerprints.len()
    }

    /// The filter of `keys` (duplicates allowed). Construction is retried
    /// with new seeds and, if needed, a larger array. `None` only past
    /// about 3.8 G distinct keys, where positions no longer fit a u32.
    pub fn build(keys: &[u64]) -> Option<Self> {
        let mut keys = keys.to_vec();
        keys.sort_unstable();
        keys.dedup();
        let size = keys.len();
        if size == 0 {
            return Some(Self::empty());
        }
        let sized = size.max(2) as f64;
        let segment_length: u32 =
            (1u32 << ((sized.ln() / 3.33f64.ln() + 2.25).floor() as u32)).min(1 << 18);
        let size_factor = (0.875 + 0.25 * 1_000_000f64.ln() / sized.ln()).max(1.125);
        let capacity = (sized * size_factor).round() as u64;
        let sl = u64::from(segment_length);
        let mut segment_count = capacity.div_ceil(sl).saturating_sub(2).max(1);
        let mut seed_state = 0x9e37_79b9_7f4a_7c15u64;
        loop {
            if (segment_count + 2).checked_mul(sl)? > u64::from(u32::MAX) {
                return None;
            }
            for _ in 0..100 {
                seed_state = seed_state.wrapping_add(0x9e37_79b9_7f4a_7c15);
                let seed = mix(seed_state);
                if let Some(f) = Self::try_build(&keys, seed, segment_length, segment_count) {
                    return Some(f);
                }
            }
            segment_count += 1;
        }
    }

    fn try_build(keys: &[u64], seed: u64, segment_length: u32, segment_count: u64) -> Option<Self> {
        let size = keys.len();
        let sl = u64::from(segment_length);
        let array_length = ((segment_count + 2) * sl) as usize;
        let segment_count_length = u32::try_from(segment_count * sl).ok()?;
        let mut f = Self {
            seed,
            segment_length,
            segment_count_length,
            fingerprints: vec![0; array_length],
        };
        let mut t2count = vec![0u8; array_length];
        let mut t2hash = vec![0u64; array_length];
        let mut hashes: Vec<u64> = keys.iter().map(|&k| mix(k.wrapping_add(seed))).collect();
        hashes.sort_unstable();
        for w in hashes.windows(2) {
            if w[0] == w[1] {
                return None;
            }
        }
        for &hash in &hashes {
            for (i, &h) in f.hashes(hash).iter().enumerate() {
                t2count[h] = t2count[h].checked_add(4)?;
                t2count[h] ^= i as u8;
                t2hash[h] ^= hash;
            }
        }
        // Peel: positions hit by exactly one key give that key its slot.
        let mut queue: Vec<usize> = (0..array_length)
            .filter(|&i| t2count[i] >> 2 == 1)
            .collect();
        let mut stack: Vec<(u64, u8)> = Vec::with_capacity(size);
        while let Some(index) = queue.pop() {
            if t2count[index] >> 2 != 1 {
                continue;
            }
            let hash = t2hash[index];
            let found = t2count[index] & 3;
            stack.push((hash, found));
            let hs = f.hashes(hash);
            for k in 1..3u8 {
                let j = (found + k) % 3;
                let other = hs[j as usize];
                t2count[other] -= 4;
                t2count[other] ^= j;
                t2hash[other] ^= hash;
                if t2count[other] >> 2 == 1 {
                    queue.push(other);
                }
            }
            t2count[index] = 0;
        }
        if stack.len() != size {
            return None;
        }
        for &(hash, found) in stack.iter().rev() {
            let hs = f.hashes(hash);
            let mut x = fingerprint(hash);
            for k in 1..3u8 {
                x ^= f.fingerprints[hs[((found + k) % 3) as usize]];
            }
            f.fingerprints[hs[found as usize]] = x;
        }
        Some(f)
    }

    /// `seed u64 | segment_length u32 | segment_count_length u32 | len u32 |
    /// fingerprints`.
    pub fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.seed.to_le_bytes());
        out.extend_from_slice(&self.segment_length.to_le_bytes());
        out.extend_from_slice(&self.segment_count_length.to_le_bytes());
        out.extend_from_slice(&(self.fingerprints.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.fingerprints);
    }

    /// A filter `encode` wrote, and the bytes after it; `None` if the
    /// parameters are inconsistent.
    pub fn decode(buf: &[u8]) -> Option<(Self, &[u8])> {
        let head = buf.get(..20)?;
        let seed = u64::from_le_bytes(head[..8].try_into().ok()?);
        let segment_length = u32::from_le_bytes(head[8..12].try_into().ok()?);
        let segment_count_length = u32::from_le_bytes(head[12..16].try_into().ok()?);
        let len = u32::from_le_bytes(head[16..20].try_into().ok()?) as usize;
        let fingerprints = buf.get(20..20 + len)?.to_vec();
        // Every position `hashes` can produce must lie in the array.
        let consistent = segment_length.is_power_of_two()
            && (len == 0
                || (segment_count_length > 0
                    && segment_count_length % segment_length == 0
                    && u64::from(segment_count_length) + 2 * u64::from(segment_length)
                        == len as u64));
        consistent.then(|| {
            (
                Self {
                    seed,
                    segment_length,
                    segment_count_length,
                    fingerprints,
                },
                &buf[20 + len..],
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys(n: u64, salt: u64) -> Vec<u64> {
        (0..n).map(|i| mix(i ^ (salt << 40))).collect()
    }

    #[test]
    fn every_member_is_found_at_every_size() {
        for n in [0, 1, 2, 3, 5, 10, 31, 100, 1000, 4096, 50_000] {
            let k = keys(n, 1);
            let f = Fuse8::build(&k).unwrap();
            assert!(k.iter().all(|&x| f.contains(x)), "{n}");
            let mut enc = Vec::new();
            f.encode(&mut enc);
            enc.extend_from_slice(b"rest");
            let (g, rest) = Fuse8::decode(&enc).unwrap();
            assert_eq!((g, rest), (f, &b"rest"[..]), "{n}");
        }
        assert!(!Fuse8::build(&[]).unwrap().contains(7));
        let dup = Fuse8::build(&[5, 5, 9]).unwrap();
        // A resealed footer whose parameters would index past the array is
        // refused, not trusted.
        let f = Fuse8::build(&keys(1000, 4)).unwrap();
        let mut enc = Vec::new();
        f.encode(&mut enc);
        let scl = u32::from_le_bytes(enc[12..16].try_into().unwrap());
        enc[12..16].copy_from_slice(&(scl + 1).to_le_bytes());
        let len = u32::from_le_bytes(enc[16..20].try_into().unwrap());
        enc[16..20].copy_from_slice(&(len + 1).to_le_bytes());
        enc.push(0);
        assert!(Fuse8::decode(&enc).is_none());
        assert!(dup.contains(5) && dup.contains(9));
    }

    #[test]
    fn false_positives_and_size_match_the_design() {
        let k = keys(100_000, 2);
        let f = Fuse8::build(&k).unwrap();
        let absent = keys(1_000_000, 3);
        let fp = absent.iter().filter(|&&x| f.contains(x)).count();
        // 0.39 % expected.
        assert!(fp < 6_000, "{fp} false positives in 1 000 000");
        let bits_per_key = f.bytes() as f64 * 8.0 / 100_000.0;
        // About 9.4 at this size (size factor 1.175); 9.0 from ~10 M keys.
        assert!(bits_per_key < 10.0, "{bits_per_key} bits per key");
    }
}
