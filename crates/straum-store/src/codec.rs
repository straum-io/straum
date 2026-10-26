//! On-disk encodings: object ids, the WAL file header, WAL frames and
//! records, topic index entries. Every layout here is documented byte for
//! byte in docs/format.md.

use crate::StoreError;

/// BLAKE3 hash of an object's raw bytes.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug)]
pub struct ObjectId(pub [u8; 32]);

impl ObjectId {
    pub fn of(bytes: &[u8]) -> Self {
        Self(*blake3::hash(bytes).as_bytes())
    }
}

/// `magic [8] | version u32 | crc32c u32` at the start of `wal.log`.
pub const WAL_FILE_HEADER: usize = 16;
pub const WAL_MAGIC: [u8; 8] = *b"STRMWAL\0";
pub const WAL_VERSION: u32 = 8;
/// `len u32 | seq u64 | count u32 | mode u8 | crc32c u32`.
pub const FRAME_HEADER: usize = 21;
/// Header bytes covered by the frame CRC (everything before it).
const FRAME_CRC_AT: usize = 17;

/// How the batch in a frame was made durable, recorded so recovery knows
/// whether a frame can legitimately be durable before its object bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameMode {
    /// Object bytes were durable before the frame was written. Also used for
    /// release and compaction frames, which introduce no object bytes.
    Sequential = 1,
    /// Segment bytes and the frame were synced concurrently: a crash can
    /// leave the frame durable without its segment bytes.
    ParallelSync = 2,
    /// Inline objects are inside the frame; segment objects were durable
    /// before the frame was written (Sequential order).
    InlineSmall = 3,
}

impl FrameMode {
    fn from_byte(b: u8) -> Option<Self> {
        match b {
            1 => Some(Self::Sequential),
            2 => Some(Self::ParallelSync),
            3 => Some(Self::InlineSmall),
            _ => None,
        }
    }
}
/// Topic names are 1..=MAX_TOPIC bytes; the hex file name then stays under
/// NAME_MAX (255) on every supported filesystem.
pub const MAX_TOPIC: usize = 100;
/// Segment id meaning "no new segment": compaction dropped every object.
pub const NO_SEGMENT: u64 = u64::MAX;

const PUT_OBJECT: u8 = 1;
const PUBLISH: u8 = 2;
const RELEASE: u8 = 3;
const COMPACT: u8 = 4;
const PUT_INLINE: u8 = 5;
const PUT_TOPIC: u8 = 6;

pub fn wal_file_header() -> [u8; WAL_FILE_HEADER] {
    let mut h = [0; WAL_FILE_HEADER];
    h[..8].copy_from_slice(&WAL_MAGIC);
    h[8..12].copy_from_slice(&WAL_VERSION.to_le_bytes());
    let crc = crc32c::crc32c(&h[..12]);
    h[12..].copy_from_slice(&crc.to_le_bytes());
    h
}

/// True if `buf` starts with this version's intact file header.
pub fn is_current_wal(buf: &[u8]) -> bool {
    buf.get(..WAL_FILE_HEADER) == Some(&wal_file_header()[..])
}

/// Longest unsigned LEB128 encoding of a u64.
pub const MAX_VARINT: usize = 10;

/// Appends `v` as an unsigned LEB128 varint: 7 bits per byte, low bits
/// first, high bit set on every byte but the last.
pub fn put_uvarint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push(v as u8 | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// Signed values as varints: 0, -1, 1, -2, … become 0, 1, 2, 3, ….
pub fn zigzag(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

pub fn unzigzag(v: u64) -> i64 {
    (v >> 1) as i64 ^ -((v & 1) as i64)
}

/// Decodes one varint from the start of `buf`; returns it and its length.
/// Rejects a truncated varint, one longer than 10 bytes or overflowing u64,
/// and an overlong (non-minimal) encoding: each value has exactly one.
pub fn get_uvarint(buf: &[u8]) -> Result<(u64, usize), &'static str> {
    let mut v = 0u64;
    for (i, &b) in buf.iter().enumerate().take(MAX_VARINT) {
        if i == MAX_VARINT - 1 && b > 1 {
            return Err("varint overflows u64");
        }
        v |= u64::from(b & 0x7f) << (7 * i);
        if b & 0x80 == 0 {
            if b == 0 && i > 0 {
                return Err("overlong varint");
            }
            return Ok((v, i + 1));
        }
    }
    if buf.len() >= MAX_VARINT {
        Err("varint longer than 10 bytes")
    } else {
        Err("truncated varint")
    }
}

/// Record header: `len varint | type u8`. At most this many bytes.
pub const RECORD_HEADER_MAX: usize = MAX_VARINT + 1;

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Record<'a> {
    /// A new object: its full BLAKE3 id, the local object number every
    /// later reference uses, and where its bytes are.
    PutObject {
        id: ObjectId,
        number: u64,
        segment: u64,
        offset: u64,
        len: u32,
    },
    /// A topic number assigned to a name, once, in the frame of the topic's
    /// first Publish. Publish and Release refer to the number.
    PutTopic {
        number: u64,
        name: &'a [u8],
    },
    Publish {
        /// Topic number from the topic's PutTopic.
        topic: u64,
        seq: u64,
        /// Object number from the object's PutObject or PutInline.
        number: u64,
        timestamp: u64,
        /// The publication's key and headers (`encode_attrs`); empty when
        /// it has neither. `attrs_at` is where they start within the frame
        /// body; set by `decode_records`, ignored by `encode`.
        attrs: &'a [u8],
        attrs_at: usize,
    },
    Release {
        topic: u64,
        seq: u64,
    },
    /// Segment `old` was rewritten into `new`; `kept` lists every surviving
    /// object with its offset in `new`. Objects of `old` not listed are gone.
    Compact {
        old: u64,
        new: u64,
        kept: Vec<(ObjectId, u64)>,
    },
    /// An object stored in the WAL itself (`WriteMode::InlineSmall`).
    /// `data_at` is where `data` starts within the frame body; set by
    /// `decode_records`, ignored by `encode`.
    PutInline {
        id: ObjectId,
        number: u64,
        data: &'a [u8],
        data_at: usize,
    },
}

/// Upper bounds of encoded record sizes, header included, used to bound a
/// frame before any I/O. Variable parts: + name length (PutTopic), + 40 per
/// kept object (Compact), + object length (PutInline).
pub const RECORD_PUT_OBJECT: usize = RECORD_HEADER_MAX + 32 + 4 * MAX_VARINT;
pub const RECORD_PUT_TOPIC_BASE: usize = RECORD_HEADER_MAX + MAX_VARINT;
/// Without its attributes (`attrs_len`).
pub const RECORD_PUBLISH: usize = RECORD_HEADER_MAX + 4 * MAX_VARINT;
pub const RECORD_RELEASE: usize = RECORD_HEADER_MAX + 2 * MAX_VARINT;
pub const RECORD_COMPACT_BASE: usize = RECORD_HEADER_MAX + 20;
pub const RECORD_PUT_INLINE_BASE: usize = RECORD_HEADER_MAX + 32 + MAX_VARINT;

/// Delta state of one frame. The first Publish's object number is relative
/// to the next object number when the frame starts (known from earlier
/// frames), its timestamp to zero; every later Publish is relative to the
/// previous one (zigzag varints). A PutObject or PutInline number is
/// relative to the next expected object number, which starts at the frame's
/// next object number and becomes the previous new object's number + 1
/// (numbers are assigned in order, so the delta is normally 0).
#[derive(Clone, Copy, Debug)]
pub struct FrameCtx {
    number: u64,
    timestamp: u64,
    next_object: u64,
}

impl FrameCtx {
    pub fn new(next_object: u64) -> Self {
        Self {
            number: next_object,
            timestamp: 0,
            next_object,
        }
    }

    fn put_number(&mut self, p: &mut Vec<u8>, number: u64) {
        put_uvarint(p, number.wrapping_sub(self.next_object));
        self.next_object = number.wrapping_add(1);
    }

    fn get_number(&mut self, r: &mut Reader<'_>) -> Result<u64, &'static str> {
        let number = self.next_object.wrapping_add(r.uvarint()?);
        self.next_object = number.wrapping_add(1);
        Ok(number)
    }
}

impl Record<'_> {
    /// Upper bound of the encoded size, header included.
    #[cfg(test)]
    fn max_size(&self) -> usize {
        match self {
            Record::PutObject { .. } => RECORD_PUT_OBJECT,
            Record::PutTopic { name, .. } => RECORD_PUT_TOPIC_BASE + name.len(),
            Record::Publish { attrs, .. } => RECORD_PUBLISH + attrs.len(),
            Record::Release { .. } => RECORD_RELEASE,
            Record::Compact { kept, .. } => RECORD_COMPACT_BASE + 40 * kept.len(),
            Record::PutInline { data, .. } => RECORD_PUT_INLINE_BASE + data.len(),
        }
    }

    /// Appends `len varint | type u8 | payload` to `out`.
    pub fn encode(&self, out: &mut Vec<u8>, ctx: &mut FrameCtx) -> Result<(), StoreError> {
        let mut p = Vec::new();
        let kind = match self {
            Record::PutObject {
                id,
                number,
                segment,
                offset,
                len,
            } => {
                p.extend_from_slice(&id.0);
                ctx.put_number(&mut p, *number);
                put_uvarint(&mut p, *segment);
                put_uvarint(&mut p, *offset);
                put_uvarint(&mut p, u64::from(*len));
                PUT_OBJECT
            }
            Record::PutTopic { number, name } => {
                if name.is_empty() || name.len() > MAX_TOPIC {
                    return Err(StoreError::InvalidTopic);
                }
                put_uvarint(&mut p, *number);
                p.extend_from_slice(name);
                PUT_TOPIC
            }
            Record::Publish {
                topic,
                seq,
                number,
                timestamp,
                attrs,
                ..
            } => {
                put_uvarint(&mut p, *topic);
                put_uvarint(&mut p, *seq);
                put_uvarint(&mut p, zigzag(number.wrapping_sub(ctx.number) as i64));
                put_uvarint(&mut p, zigzag(timestamp.wrapping_sub(ctx.timestamp) as i64));
                p.extend_from_slice(attrs);
                ctx.number = *number;
                ctx.timestamp = *timestamp;
                PUBLISH
            }
            Record::Release { topic, seq } => {
                put_uvarint(&mut p, *topic);
                put_uvarint(&mut p, *seq);
                RELEASE
            }
            Record::Compact { old, new, kept } => {
                let count = u32::try_from(kept.len()).map_err(|_| StoreError::FrameTooLarge)?;
                p.extend_from_slice(&old.to_le_bytes());
                p.extend_from_slice(&new.to_le_bytes());
                p.extend_from_slice(&count.to_le_bytes());
                for (id, offset) in kept {
                    p.extend_from_slice(&id.0);
                    p.extend_from_slice(&offset.to_le_bytes());
                }
                COMPACT
            }
            Record::PutInline {
                id, number, data, ..
            } => {
                p.extend_from_slice(&id.0);
                ctx.put_number(&mut p, *number);
                p.extend_from_slice(data);
                PUT_INLINE
            }
        };
        u32::try_from(p.len()).map_err(|_| StoreError::FrameTooLarge)?;
        put_uvarint(out, p.len() as u64);
        out.push(kind);
        out.extend_from_slice(&p);
        Ok(())
    }

    /// Offset of the payload of an encoded record whose payload is `len`
    /// bytes, from the start of its header.
    #[cfg(test)]
    fn header_len(len: usize) -> usize {
        let mut n = 1;
        let mut v = len as u64;
        while v >= 0x80 {
            n += 1;
            v >>= 7;
        }
        n + 1
    }
}

/// Builds one frame in a buffer: `begin`, then records, then `finish`.
pub struct FrameBuilder {
    start: usize,
    count: u32,
    ctx: FrameCtx,
}

impl FrameBuilder {
    /// `next_object`: the next object number when this frame starts.
    pub fn begin(out: &mut Vec<u8>, next_object: u64) -> Self {
        let start = out.len();
        out.extend_from_slice(&[0; FRAME_HEADER]);
        Self {
            start,
            count: 0,
            ctx: FrameCtx::new(next_object),
        }
    }

    pub fn record(&mut self, out: &mut Vec<u8>, rec: &Record<'_>) -> Result<(), StoreError> {
        rec.encode(out, &mut self.ctx)?;
        self.count += 1;
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Fills in the header: body length, frame sequence number, record count,
    /// write mode and one CRC32C over header fields and body.
    pub fn finish(self, out: &mut [u8], seq: u64, mode: FrameMode) -> Result<(), StoreError> {
        let body = out.len() - self.start - FRAME_HEADER;
        let body = u32::try_from(body).map_err(|_| StoreError::FrameTooLarge)?;
        let h = self.start;
        out[h..h + 4].copy_from_slice(&body.to_le_bytes());
        out[h + 4..h + 12].copy_from_slice(&seq.to_le_bytes());
        out[h + 12..h + 16].copy_from_slice(&self.count.to_le_bytes());
        out[h + 16] = mode as u8;
        let crc = frame_crc(&out[h..h + FRAME_CRC_AT], &out[h + FRAME_HEADER..]);
        out[h + FRAME_CRC_AT..h + FRAME_HEADER].copy_from_slice(&crc.to_le_bytes());
        Ok(())
    }
}

fn frame_crc(header: &[u8], body: &[u8]) -> u32 {
    crc32c::crc32c_append(crc32c::crc32c(header), body)
}

#[derive(Debug, PartialEq, Eq)]
pub struct Frame<'a> {
    pub seq: u64,
    pub count: u32,
    /// Raw write-mode byte; `decode_records` rejects unknown values.
    pub mode: u8,
    pub body: &'a [u8],
}

impl Frame<'_> {
    /// Bytes the frame occupies in the file.
    pub fn size(&self) -> usize {
        FRAME_HEADER + self.body.len()
    }
}

/// Decodes the frame at the start of `buf`; `None` if the header is short,
/// the body runs past the end of `buf`, or the CRC does not match.
pub fn decode_frame(buf: &[u8]) -> Option<Frame<'_>> {
    let header = buf.get(..FRAME_HEADER)?;
    let len = u32_at(header, 0) as usize;
    let body = buf.get(FRAME_HEADER..FRAME_HEADER.checked_add(len)?)?;
    if frame_crc(&header[..FRAME_CRC_AT], body) != u32_at(header, FRAME_CRC_AT) {
        return None;
    }
    Some(Frame {
        seq: u64_at(header, 4),
        count: u32_at(header, 12),
        mode: header[16],
        body,
    })
}

/// The first offset after `from` where a frame with a valid CRC starts.
/// Records carry no checksum of their own, so intact records inside a torn
/// frame are never mistaken for one.
pub fn next_valid_frame(buf: &[u8], from: usize) -> Option<usize> {
    (from + 1..buf.len()).find(|&at| decode_frame(&buf[at..]).is_some())
}

/// The write mode of a frame whose CRC matched.
pub fn frame_mode(frame: &Frame<'_>) -> Result<FrameMode, &'static str> {
    FrameMode::from_byte(frame.mode).ok_or("unknown frame write mode")
}

/// Decodes the records of a frame whose CRC matched. Any malformation here
/// was written that way: corruption, not a torn write. `next_object` is the
/// next object number when the frame starts (the Publish delta base).
pub fn decode_records<'a>(
    frame: &Frame<'a>,
    next_object: u64,
) -> Result<Vec<Record<'a>>, &'static str> {
    let mut rest = frame.body;
    // Bounded by the body: every record takes at least 2 bytes.
    if frame.count as usize > rest.len() / 2 {
        return Err("record count exceeds frame body");
    }
    let mut ctx = FrameCtx::new(next_object);
    let mut records = Vec::with_capacity(frame.count as usize);
    for _ in 0..frame.count {
        let (len, n) = get_uvarint(rest)?;
        let kind = *rest.get(n).ok_or("record header past frame end")?;
        let start = n + 1;
        let payload = usize::try_from(len)
            .ok()
            .and_then(|len| rest.get(start..start.checked_add(len)?))
            .ok_or("record payload past frame end")?;
        let payload_at = frame.body.len() - rest.len() + start;
        records.push(decode_record(kind, payload, payload_at, &mut ctx)?);
        rest = &rest[start + payload.len()..];
    }
    if !rest.is_empty() {
        return Err("trailing bytes after the last record of a frame");
    }
    Ok(records)
}

fn decode_record<'a>(
    kind: u8,
    payload: &'a [u8],
    payload_at: usize,
    ctx: &mut FrameCtx,
) -> Result<Record<'a>, &'static str> {
    let mut r = Reader(payload);
    let rec = match kind {
        PUT_OBJECT => Record::PutObject {
            id: r.id()?,
            number: ctx.get_number(&mut r)?,
            segment: r.uvarint()?,
            offset: r.uvarint()?,
            len: u32::try_from(r.uvarint()?).map_err(|_| "object length over u32")?,
        },
        PUT_TOPIC => {
            let number = r.uvarint()?;
            let name = std::mem::take(&mut r.0);
            if name.is_empty() || name.len() > MAX_TOPIC {
                return Err("topic length out of range");
            }
            Record::PutTopic { number, name }
        }
        PUBLISH => {
            let topic = r.uvarint()?;
            let seq = r.uvarint()?;
            let number = ctx.number.wrapping_add(unzigzag(r.uvarint()?) as u64);
            let timestamp = ctx.timestamp.wrapping_add(unzigzag(r.uvarint()?) as u64);
            ctx.number = number;
            ctx.timestamp = timestamp;
            let attrs_at = payload_at + payload.len() - r.0.len();
            let attrs = std::mem::take(&mut r.0);
            decode_attrs(attrs)?;
            Record::Publish {
                topic,
                seq,
                number,
                timestamp,
                attrs,
                attrs_at,
            }
        }
        RELEASE => Record::Release {
            topic: r.uvarint()?,
            seq: r.uvarint()?,
        },
        COMPACT => {
            let old = r.u64()?;
            let new = r.u64()?;
            let count = r.u32()? as usize;
            // Bounded by the payload length: 40 bytes per entry.
            if count > r.0.len() / 40 {
                return Err("compact entry count exceeds payload");
            }
            let mut kept = Vec::with_capacity(count);
            for _ in 0..count {
                kept.push((r.id()?, r.u64()?));
            }
            Record::Compact { old, new, kept }
        }
        PUT_INLINE => {
            let id = r.id()?;
            let number = ctx.get_number(&mut r)?;
            let data_at = payload_at + payload.len() - r.0.len();
            Record::PutInline {
                id,
                number,
                data: std::mem::take(&mut r.0),
                data_at,
            }
        }
        _ => return Err("unknown record type"),
    };
    if !r.0.is_empty() {
        return Err("trailing bytes in record");
    }
    Ok(rec)
}

/// A publication's key and headers, as stored after a Publish record's
/// timestamp: `key` (varint length + 1, 0 = no key) and its bytes, `count`
/// varint, then per header `name` and `value`, each a varint length and the
/// bytes. A publication with neither stores nothing at all.
pub fn encode_attrs(out: &mut Vec<u8>, key: Option<&[u8]>, headers: &[(&[u8], &[u8])]) {
    if key.is_none() && headers.is_empty() {
        return;
    }
    match key {
        Some(k) => {
            put_uvarint(out, k.len() as u64 + 1);
            out.extend_from_slice(k);
        }
        None => out.push(0),
    }
    put_uvarint(out, headers.len() as u64);
    for (name, value) in headers {
        put_uvarint(out, name.len() as u64);
        out.extend_from_slice(name);
        put_uvarint(out, value.len() as u64);
        out.extend_from_slice(value);
    }
}

/// Exact length of `encode_attrs`'s output.
pub fn attrs_len(key: Option<&[u8]>, headers: &[(&[u8], &[u8])]) -> usize {
    if key.is_none() && headers.is_empty() {
        return 0;
    }
    let field = |len: usize| varint_len(len as u64) + len;
    let key = match key {
        Some(k) => varint_len(k.len() as u64 + 1) + k.len(),
        None => 1,
    };
    key + varint_len(headers.len() as u64)
        + headers
            .iter()
            .map(|(n, v)| field(n.len()) + field(v.len()))
            .sum::<usize>()
}

fn varint_len(mut v: u64) -> usize {
    let mut n = 1;
    while v >= 0x80 {
        n += 1;
        v >>= 7;
    }
    n
}

/// A publication's key and headers.
pub type Attrs = (Option<Vec<u8>>, Vec<(Vec<u8>, Vec<u8>)>);

/// Decodes `encode_attrs`'s output; empty input is no key and no headers.
pub fn decode_attrs(buf: &[u8]) -> Result<Attrs, &'static str> {
    if buf.is_empty() {
        return Ok((None, Vec::new()));
    }
    let mut r = Reader(buf);
    let key = match r.uvarint()? {
        0 => None,
        n => Some(
            r.take(usize::try_from(n - 1).map_err(|_| "key too long")?)?
                .to_vec(),
        ),
    };
    let count = r.uvarint()?;
    // Every header takes at least 2 bytes.
    if count > r.0.len() as u64 / 2 {
        return Err("header count exceeds attributes");
    }
    let mut headers = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let n = usize::try_from(r.uvarint()?).map_err(|_| "header name too long")?;
        let name = r.take(n)?.to_vec();
        let n = usize::try_from(r.uvarint()?).map_err(|_| "header value too long")?;
        headers.push((name, r.take(n)?.to_vec()));
    }
    if key.is_none() && headers.is_empty() {
        return Err("empty attributes are stored as nothing");
    }
    if !r.0.is_empty() {
        return Err("trailing bytes in attributes");
    }
    Ok((key, headers))
}

struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], &'static str> {
        if self.0.len() < n {
            return Err("record payload too short");
        }
        let (head, rest) = self.0.split_at(n);
        self.0 = rest;
        Ok(head)
    }
    fn u32(&mut self) -> Result<u32, &'static str> {
        Ok(u32_at(self.take(4)?, 0))
    }
    fn u64(&mut self) -> Result<u64, &'static str> {
        Ok(u64_at(self.take(8)?, 0))
    }
    fn id(&mut self) -> Result<ObjectId, &'static str> {
        Ok(id_at(self.take(32)?, 0))
    }
    fn uvarint(&mut self) -> Result<u64, &'static str> {
        let (v, n) = get_uvarint(self.0)?;
        self.0 = &self.0[n..];
        Ok(v)
    }
}

/// Entries per topic index block. Entry `seq` is entry `seq % 64` of block
/// `seq / 64`; the index starts at seq 0 and is never cut, so topic_seq is
/// implicit.
pub const INDEX_BLOCK: u64 = 64;

/// Encodes topic index entries in order. In a block, the first entry is
/// `number varint | timestamp varint`; each later one is `zigzag(number −
/// previous number) varint | (timestamp − previous timestamp) varint`
/// (timestamps do not decrease within a topic). After its 64th entry a
/// block ends with `crc32c u32` over the block number (u64 LE) and the
/// block's entry bytes. The encoding is deterministic: the same entries
/// always give the same bytes.
#[derive(Clone, Copy, Debug, Default)]
pub struct IndexWriter {
    seq: u64,
    prev_number: u64,
    prev_timestamp: u64,
    crc: u32,
}

impl IndexWriter {
    /// Appends the entry with topic_seq `self.seq()`. Returns true if it
    /// starts a block.
    pub fn push(&mut self, out: &mut Vec<u8>, number: u64, timestamp: u64) -> bool {
        let first = self.seq.is_multiple_of(INDEX_BLOCK);
        let start = out.len();
        if first {
            self.crc = crc32c::crc32c(&(self.seq / INDEX_BLOCK).to_le_bytes());
            put_uvarint(out, number);
            put_uvarint(out, timestamp);
        } else {
            put_uvarint(out, zigzag(number.wrapping_sub(self.prev_number) as i64));
            put_uvarint(out, timestamp.wrapping_sub(self.prev_timestamp));
        }
        self.crc = crc32c::crc32c_append(self.crc, &out[start..]);
        self.prev_number = number;
        self.prev_timestamp = timestamp;
        self.seq += 1;
        if self.seq.is_multiple_of(INDEX_BLOCK) {
            out.extend_from_slice(&self.crc.to_le_bytes());
        }
        first
    }
}

/// A topic index block decoded from the start of `buf`.
#[derive(Debug, PartialEq, Eq)]
pub struct IndexBlock {
    /// `(object number, timestamp)`, up to 64.
    pub entries: Vec<(u64, u64)>,
    /// Byte offset just after each entry, from the block start.
    pub ends: Vec<usize>,
    /// 64 entries followed by a CRC that matches.
    pub sealed: bool,
}

/// Decodes block `block` from the start of `buf`: entries until 64, the end
/// of `buf`, or a malformed varint (a torn or unwritten tail).
pub fn decode_index_block(buf: &[u8], block: u64) -> IndexBlock {
    let mut b = IndexBlock {
        entries: Vec::new(),
        ends: Vec::new(),
        sealed: false,
    };
    let mut pos = 0;
    let (mut number, mut timestamp) = (0u64, 0u64);
    while (b.entries.len() as u64) < INDEX_BLOCK {
        let Ok((x, n)) = get_uvarint(&buf[pos..]) else {
            return b;
        };
        let Ok((y, m)) = get_uvarint(&buf[pos + n..]) else {
            return b;
        };
        if b.entries.is_empty() {
            (number, timestamp) = (x, y);
        } else {
            number = number.wrapping_add(unzigzag(x) as u64);
            timestamp = timestamp.wrapping_add(y);
        }
        pos += n + m;
        b.entries.push((number, timestamp));
        b.ends.push(pos);
    }
    if let Some(stored) = buf.get(pos..pos + 4) {
        let crc = crc32c::crc32c_append(crc32c::crc32c(&block.to_le_bytes()), &buf[..pos]);
        b.sealed = crc == u32_at(stored, 0);
    }
    b
}

// Callers pass slices whose length was checked; out-of-range reads cannot occur.
fn u32_at(b: &[u8], at: usize) -> u32 {
    let mut a = [0; 4];
    a.copy_from_slice(&b[at..at + 4]);
    u32::from_le_bytes(a)
}
fn u64_at(b: &[u8], at: usize) -> u64 {
    let mut a = [0; 8];
    a.copy_from_slice(&b[at..at + 8]);
    u64::from_le_bytes(a)
}
fn id_at(b: &[u8], at: usize) -> ObjectId {
    let mut a = [0; 32];
    a.copy_from_slice(&b[at..at + 32]);
    ObjectId(a)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn samples() -> Vec<Record<'static>> {
        let id = ObjectId::of(b"x");
        vec![
            Record::PutObject {
                id,
                number: 77,
                segment: 3,
                offset: 1 << 40,
                len: 7,
            },
            Record::PutTopic {
                number: 4,
                name: b"orders",
            },
            Record::Publish {
                topic: 4,
                seq: 42,
                number: 77,
                timestamp: 1_700_000_000_000,
                attrs: &[],
                attrs_at: 0,
            },
            // A deduplicated object far back, and a timestamp going down
            // (another topic): negative deltas.
            Record::Publish {
                topic: 0,
                seq: 1 << 40,
                number: 3,
                timestamp: 1_699_999_999_000,
                attrs: &[],
                attrs_at: 0,
            },
            Record::Release { topic: 4, seq: 0 },
            Record::Compact {
                old: 1,
                new: 9,
                kept: vec![(id, 0), (ObjectId::of(b"y"), 1)],
            },
            Record::Compact {
                old: 2,
                new: NO_SEGMENT,
                kept: vec![],
            },
        ]
    }

    fn frame_of(records: &[Record<'_>], seq: u64, next_object: u64) -> Vec<u8> {
        let mut out = Vec::new();
        let mut f = FrameBuilder::begin(&mut out, next_object);
        for r in records {
            f.record(&mut out, r).unwrap();
        }
        f.finish(&mut out, seq, FrameMode::ParallelSync).unwrap();
        out
    }

    #[test]
    fn object_id_is_blake3() {
        // BLAKE3 test vector for the empty input.
        assert_eq!(
            ObjectId::of(b"").0[..4],
            [0xaf, 0x13, 0x49, 0xb9],
            "BLAKE3(\"\") starts af1349b9"
        );
        assert_eq!(ObjectId::of(b"abc"), ObjectId::of(b"abc"));
        assert_ne!(ObjectId::of(b"abc"), ObjectId::of(b"abd"));
    }

    #[test]
    fn varint_boundaries_round_trip() {
        for (v, len) in [
            (0u64, 1),
            (1, 1),
            (127, 1),
            (128, 2),
            (16_383, 2),
            (16_384, 3),
            (1 << 63, 10),
            (u64::MAX, 10),
        ] {
            let mut buf = Vec::new();
            put_uvarint(&mut buf, v);
            assert_eq!(buf.len(), len, "{v}");
            assert_eq!(get_uvarint(&buf), Ok((v, len)), "{v}");
        }
        assert_eq!(
            &{
                let mut b = Vec::new();
                put_uvarint(&mut b, 300);
                b
            }[..],
            &[0xac, 0x02]
        );
        for v in [0i64, -1, 1, -2, i64::MAX, i64::MIN] {
            assert_eq!(unzigzag(zigzag(v)), v);
        }
        assert_eq!((zigzag(0), zigzag(-1), zigzag(1)), (0, 1, 2));
    }

    #[test]
    fn malformed_varints_are_rejected() {
        assert_eq!(get_uvarint(&[]), Err("truncated varint"));
        assert_eq!(get_uvarint(&[0x80]), Err("truncated varint"));
        assert_eq!(get_uvarint(&[0xff, 0xff]), Err("truncated varint"));
        assert_eq!(
            get_uvarint(&[0x80, 0x00]),
            Err("overlong varint"),
            "0 in two bytes"
        );
        assert_eq!(get_uvarint(&[0xff, 0x80, 0x00]), Err("overlong varint"));
        let mut eleven = vec![0x80; 10];
        eleven.push(0x01);
        assert_eq!(get_uvarint(&eleven), Err("varint overflows u64"));
        let mut ten_big = vec![0xff; 9];
        ten_big.push(0x02); // bit 64 set
        assert_eq!(get_uvarint(&ten_big), Err("varint overflows u64"));
    }

    #[test]
    fn wal_file_header_is_recognised() {
        let h = wal_file_header();
        assert_eq!(&h[..8], b"STRMWAL\0");
        assert_eq!(&h[8..12], &WAL_VERSION.to_le_bytes());
        assert!(is_current_wal(&h));
        for version in (1..WAL_VERSION).chain([WAL_VERSION + 1]) {
            // An older or a future version, with a valid header CRC.
            let mut other = h;
            other[8..12].copy_from_slice(&version.to_le_bytes());
            let crc = crc32c::crc32c(&other[..12]);
            other[12..].copy_from_slice(&crc.to_le_bytes());
            assert!(!is_current_wal(&other), "version {version}");
        }
        assert!(!is_current_wal(&h[..15]));
    }

    #[test]
    fn frames_round_trip_within_size_bounds() {
        let records = samples();
        for next_object in [0, 77, u64::MAX] {
            let buf = frame_of(&records, 7, next_object);
            let bound: usize = records.iter().map(Record::max_size).sum();
            assert!(buf.len() <= FRAME_HEADER + bound, "size bounds hold");
            let frame = decode_frame(&buf).unwrap();
            assert_eq!((frame.seq, frame.count, frame.size()), (7, 7, buf.len()));
            assert_eq!(frame_mode(&frame), Ok(FrameMode::ParallelSync));
            assert_eq!(
                positionless(decode_records(&frame, next_object).unwrap()),
                records
            );
        }
    }

    /// Clears the positions the decoder fills in (`data_at`, `attrs_at`), so
    /// decoded records compare equal to the ones encoded.
    fn positionless(mut recs: Vec<Record<'_>>) -> Vec<Record<'_>> {
        for r in &mut recs {
            match r {
                Record::PutInline { data_at, .. } => *data_at = 0,
                Record::Publish { attrs_at, .. } => *attrs_at = 0,
                _ => {}
            }
        }
        recs
    }

    #[test]
    fn maximum_values_round_trip() {
        let records = vec![
            Record::PutTopic {
                number: u64::MAX,
                name: &[0xff; MAX_TOPIC],
            },
            Record::Publish {
                topic: u64::MAX,
                seq: u64::MAX,
                number: u64::MAX,
                timestamp: u64::MAX,
                attrs: &[],
                attrs_at: 0,
            },
            Record::Publish {
                topic: 0,
                seq: 0,
                number: 0,
                timestamp: 0,
                attrs: &[],
                attrs_at: 0,
            },
            Record::Release {
                topic: u64::MAX,
                seq: u64::MAX,
            },
            Record::PutObject {
                id: ObjectId([0xff; 32]),
                number: u64::MAX,
                segment: u64::MAX,
                offset: u64::MAX,
                len: u32::MAX,
            },
            Record::PutInline {
                id: ObjectId([0; 32]),
                number: 0,
                data: &[0xab; 300],
                data_at: 0,
            },
        ];
        let buf = frame_of(&records, u64::MAX, 0);
        let frame = decode_frame(&buf).unwrap();
        let decoded = decode_records(&frame, 0).unwrap();
        assert_eq!(positionless(decoded), records);
    }

    #[test]
    fn frame_layout_matches_format_doc() {
        let records = [
            Record::PutTopic {
                number: 0,
                name: b"ab",
            },
            Record::Publish {
                topic: 0,
                seq: 1,
                number: 300,
                timestamp: 2,
                attrs: &[],
                attrs_at: 0,
            },
            Record::Publish {
                topic: 0,
                seq: 2,
                number: 301,
                timestamp: 2,
                attrs: &[],
                attrs_at: 0,
            },
        ];
        // next object number 300 when the frame starts
        let buf = frame_of(&records, 9, 300);
        assert_eq!(
            &buf[0..4],
            &(buf.len() as u32 - 21).to_le_bytes(),
            "body length"
        );
        assert_eq!(&buf[4..12], &9u64.to_le_bytes(), "frame seq");
        assert_eq!(&buf[12..16], &3u32.to_le_bytes(), "record count");
        assert_eq!(buf[16], 2, "write mode ParallelSync");
        assert_eq!(
            &buf[17..21],
            &crc32c::crc32c_append(crc32c::crc32c(&buf[..17]), &buf[21..]).to_le_bytes()
        );
        let body = &buf[21..];
        #[rustfmt::skip]
        let expected: &[u8] = &[
            3, PUT_TOPIC, 0, b'a', b'b',   // len 3: topic 0, "ab"
            4, PUBLISH, 0, 1, 0, 4,        // topic 0, seq 1, Δnumber 0, timestamp 2 (zigzag 4)
            4, PUBLISH, 0, 2, 2, 0,        // topic 0, seq 2, Δnumber +1 (zigzag 2), Δtimestamp 0
        ];
        assert_eq!(body, expected);
        assert_eq!(buf.len(), 21 + 17);
    }

    #[test]
    fn crc_rejects_any_flipped_bit_and_any_prefix() {
        let buf = frame_of(&samples(), 3, 0);
        for byte in 0..buf.len() {
            for bit in 0..8 {
                let mut bad = buf.clone();
                bad[byte] ^= 1 << bit;
                assert_eq!(decode_frame(&bad), None, "byte {byte} bit {bit}");
            }
        }
        for cut in 0..buf.len() {
            assert_eq!(decode_frame(&buf[..cut]), None, "cut {cut}");
        }
    }

    /// Re-checksums a hand-edited frame so only the records are wrong.
    fn reseal(mut buf: Vec<u8>) -> Vec<u8> {
        let len = (buf.len() - FRAME_HEADER) as u32;
        buf[0..4].copy_from_slice(&len.to_le_bytes());
        let crc = frame_crc(&buf[..17], &buf[21..]);
        buf[17..21].copy_from_slice(&crc.to_le_bytes());
        buf
    }

    #[test]
    fn valid_crc_with_bad_records_is_corruption() {
        let release = Record::Release { topic: 0, seq: 0 };
        // Count says 2, body holds 1 record.
        let mut buf = frame_of(std::slice::from_ref(&release), 0, 0);
        buf[12..16].copy_from_slice(&2u32.to_le_bytes());
        let buf = reseal(buf);
        assert_eq!(
            decode_records(&decode_frame(&buf).unwrap(), 0),
            Err("truncated varint")
        );
        // Unknown write mode.
        let mut buf = frame_of(std::slice::from_ref(&release), 0, 0);
        buf[16] = 9;
        let buf = reseal(buf);
        assert_eq!(
            frame_mode(&decode_frame(&buf).unwrap()),
            Err("unknown frame write mode")
        );
        // Overlong varint in a record payload: Release topic 0 as 0x80 0x00.
        let mut buf = frame_of(&[], 0, 0);
        buf[12..16].copy_from_slice(&1u32.to_le_bytes());
        buf.extend_from_slice(&[3, RELEASE, 0x80, 0x00, 0x00]);
        let buf = reseal(buf);
        assert_eq!(
            decode_records(&decode_frame(&buf).unwrap(), 0),
            Err("overlong varint")
        );
        // Overlong record length.
        let mut buf = frame_of(&[], 0, 0);
        buf[12..16].copy_from_slice(&1u32.to_le_bytes());
        buf.extend_from_slice(&[0x82, 0x00, RELEASE, 0x00, 0x00]);
        let buf = reseal(buf);
        assert_eq!(
            decode_records(&decode_frame(&buf).unwrap(), 0),
            Err("overlong varint")
        );
        // Truncated varint at the end of a Publish payload.
        let mut buf = frame_of(&[], 0, 0);
        buf[12..16].copy_from_slice(&1u32.to_le_bytes());
        buf.extend_from_slice(&[4, PUBLISH, 0, 0, 0, 0x80]);
        let buf = reseal(buf);
        assert_eq!(
            decode_records(&decode_frame(&buf).unwrap(), 0),
            Err("truncated varint")
        );
    }

    #[test]
    fn put_inline_round_trip_reports_data_position() {
        let id = ObjectId::of(b"hello");
        let put = |data_at| Record::PutInline {
            id,
            number: 5,
            data: b"hello",
            data_at,
        };
        let release = Record::Release { topic: 0, seq: 0 };
        let buf = frame_of(&[release.clone(), put(0)], 0, 5);
        let frame = decode_frame(&buf).unwrap();
        let records = decode_records(&frame, 5).unwrap();
        // Release (header 2 + payload 2), then PutInline header (2), id (32)
        // and object number delta 0 (1 byte).
        let at = 4 + Record::header_len(38) + 33;
        assert_eq!(records, vec![release, put(at)]);
        assert_eq!(&frame.body[at..at + 5], b"hello");
    }

    #[test]
    fn invalid_topic_is_rejected_on_encode() {
        let mut buf = Vec::new();
        let long = [b'a'; MAX_TOPIC + 1];
        for name in [&b""[..], &long[..]] {
            let rec = Record::PutTopic { number: 0, name };
            let mut ctx = FrameCtx::new(0);
            assert!(matches!(
                rec.encode(&mut buf, &mut ctx),
                Err(StoreError::InvalidTopic)
            ));
        }
    }

    fn index_of(entries: &[(u64, u64)]) -> (Vec<u8>, Vec<usize>) {
        let mut w = IndexWriter::default();
        let mut out = Vec::new();
        let mut starts = Vec::new();
        for &(n, t) in entries {
            let at = out.len();
            if w.push(&mut out, n, t) {
                starts.push(at);
            }
        }
        (out, starts)
    }

    #[test]
    fn index_blocks_round_trip_and_seal() {
        // New objects in order, a deduplicated reference back, a time step.
        let entries: Vec<(u64, u64)> = (0..150u64)
            .map(|i| {
                let n = if i % 10 == 9 { 3 } else { 1000 + i };
                (n, 1_700_000_000_000 + i / 7)
            })
            .collect();
        let (bytes, starts) = index_of(&entries);
        assert_eq!(starts.len(), 3);
        for (b, &start) in starts.iter().enumerate() {
            let block = decode_index_block(&bytes[start..], b as u64);
            let want = &entries[b * 64..((b + 1) * 64).min(entries.len())];
            assert_eq!(block.entries, want, "block {b}");
            assert_eq!(block.sealed, want.len() == 64, "block {b}");
        }
        // About 2 bytes per entry: 1 + 1 for consecutive numbers and equal
        // or +1 timestamps; the first entry of a block is absolute.
        assert!(bytes.len() < 150 * 3, "{} bytes", bytes.len());
    }

    #[test]
    fn index_block_crc_covers_position_and_bytes() {
        let entries: Vec<(u64, u64)> = (0..128).map(|i| (i, 5)).collect();
        let (bytes, starts) = index_of(&entries);
        let block0 = &bytes[..starts[1]];
        assert!(decode_index_block(block0, 0).sealed);
        // The same bytes at block 1's position do not seal.
        assert!(!decode_index_block(block0, 1).sealed);
        let mut bad = block0.to_vec();
        bad[5] ^= 1;
        assert!(!decode_index_block(&bad, 0).sealed);
        // A torn block: entries up to the cut, not sealed.
        let torn = decode_index_block(&bytes[..10], 0);
        assert!(!torn.sealed);
        assert!(torn.entries.len() < 64);
        assert_eq!(torn.entries, entries[..torn.entries.len()]);
    }

    #[test]
    fn index_handles_extreme_values() {
        let entries = [
            (u64::MAX, u64::MAX),
            (0, u64::MAX),
            (u64::MAX, 0),
            (1 << 63, 1),
        ];
        let (bytes, _) = index_of(&entries);
        assert_eq!(decode_index_block(&bytes, 0).entries, entries);
    }

    #[test]
    fn attrs_round_trip_and_cost_nothing_when_empty() {
        let mut out = Vec::new();
        encode_attrs(&mut out, None, &[]);
        assert!(out.is_empty(), "no key and no headers store nothing");
        assert_eq!(attrs_len(None, &[]), 0);
        assert_eq!(decode_attrs(&[]), Ok((None, Vec::new())));
        let big = vec![7u8; 300];
        let headers: [(&[u8], &[u8]); 2] = [(b"a", b""), (b"", &big)];
        for (key, headers) in [
            (Some(&b""[..]), &headers[..0]),
            (Some(&b"user-42"[..]), &headers[..]),
            (None, &headers[..1]),
            (Some(&big[..]), &headers[..]),
        ] {
            out.clear();
            encode_attrs(&mut out, key, headers);
            assert_eq!(out.len(), attrs_len(key, headers));
            let (k, h) = decode_attrs(&out).unwrap();
            assert_eq!(k.as_deref(), key);
            let h: Vec<(&[u8], &[u8])> = h.iter().map(|(n, v)| (&n[..], &v[..])).collect();
            assert_eq!(h, headers);
        }
        // An empty key costs 2 bytes: key length 0 + 1, no headers.
        assert_eq!(attrs_len(Some(b""), &[]), 2);
    }

    #[test]
    fn malformed_attrs_are_rejected() {
        for (bad, why) in [
            (&[0u8, 0][..], "empty attributes are stored as nothing"),
            (&[3, b'a'][..], "truncated"),
            (&[0, 5, 0][..], "header count exceeds attributes"),
            (&[1, 0, 9][..], "trailing bytes in attributes"),
        ] {
            assert!(decode_attrs(bad).is_err(), "{why}: {bad:?}");
        }
    }

    #[test]
    fn publish_attributes_are_located_in_the_frame() {
        let mut attrs = Vec::new();
        encode_attrs(&mut attrs, Some(b"k"), &[(b"h", b"v")]);
        let mut buf = Vec::new();
        let mut frame = FrameBuilder::begin(&mut buf, 0);
        let rec = Record::Publish {
            topic: 0,
            seq: 0,
            number: 0,
            timestamp: 9,
            attrs: &attrs,
            attrs_at: 0,
        };
        frame.record(&mut buf, &rec).unwrap();
        frame.finish(&mut buf, 0, FrameMode::Sequential).unwrap();
        let f = decode_frame(&buf).unwrap();
        let recs = decode_records(&f, 0).unwrap();
        let Record::Publish {
            attrs: got,
            attrs_at,
            ..
        } = recs[0]
        else {
            panic!("not a Publish")
        };
        assert_eq!(got, &attrs[..]);
        assert_eq!(&f.body[attrs_at..attrs_at + attrs.len()], &attrs[..]);
    }
}
