//! On-disk encodings: object ids, the WAL file header, WAL frames and
//! records, raw object blocks, topic index entries. Every layout here is
//! documented byte for byte in docs/format.md.

use crate::StoreError;

/// BLAKE3 hash of an object's raw bytes.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug)]
pub struct ObjectId(pub [u8; 32]);

impl ObjectId {
    pub fn of(bytes: &[u8]) -> Self {
        Self(*blake3::hash(bytes).as_bytes())
    }
}

/// `magic [8] | version u32 | dict_train_bytes u64 | crc32c u32` at the
/// start of `wal.log`.
pub const WAL_FILE_HEADER: usize = 24;
pub const WAL_MAGIC: [u8; 8] = *b"STRMWAL\0";
pub const WAL_VERSION: u32 = 12;
/// Header bytes covered by the header CRC (everything before it).
const WAL_HEADER_CRC_AT: usize = 20;
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
/// Topic names are 1..=MAX_TOPIC bytes.
pub const MAX_TOPIC: usize = 100;
/// Segment id meaning "no new segment": compaction dropped every block.
pub const NO_SEGMENT: u64 = u64::MAX;
/// Dictionary number of a block compressed without a dictionary.
pub const NO_DICT: u64 = 0;

// Types 1 (PutObject with a segment offset), 4 (Compact of objects) and 5
// (PutInline) belong to WAL versions before 8 and are not read.
const PUBLISH: u8 = 2;
const RELEASE: u8 = 3;
const PUT_TOPIC: u8 = 6;
const PUT_BLOCK: u8 = 7;
const PUT_INLINE_BLOCK: u8 = 8;
const PUT_OBJECT: u8 = 9;
const PUT_DICT: u8 = 10;
const COMPACT: u8 = 11;

/// The file header of a new WAL. `dict_train_bytes` is the dictionary
/// training threshold for the WAL's lifetime (`Config::dict_train_bytes` at
/// creation).
pub fn wal_file_header(dict_train_bytes: u64) -> [u8; WAL_FILE_HEADER] {
    let mut h = [0; WAL_FILE_HEADER];
    h[..8].copy_from_slice(&WAL_MAGIC);
    h[8..12].copy_from_slice(&WAL_VERSION.to_le_bytes());
    h[12..20].copy_from_slice(&dict_train_bytes.to_le_bytes());
    let crc = crc32c::crc32c(&h[..WAL_HEADER_CRC_AT]);
    h[WAL_HEADER_CRC_AT..].copy_from_slice(&crc.to_le_bytes());
    h
}

/// The `dict_train_bytes` of `buf`'s file header if `buf` starts with this
/// version's intact header; `None` for anything else.
pub fn decode_wal_header(buf: &[u8]) -> Option<u64> {
    let h: &[u8; WAL_FILE_HEADER] = buf.get(..WAL_FILE_HEADER)?.try_into().ok()?;
    let dict_train_bytes = u64::from_le_bytes(h[12..20].try_into().ok()?);
    (*h == wal_file_header(dict_train_bytes)).then_some(dict_train_bytes)
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

/// A block written by compaction: the live objects of partly dead blocks,
/// recompressed together.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Rewritten {
    pub block: u64,
    pub offset: u64,
    pub len: u64,
    pub raw_len: u64,
    pub dict: u64,
    pub crc: u32,
    /// Object numbers in block index order.
    pub objects: Vec<u64>,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Record<'a> {
    /// A compressed block of new objects in a segment. `len` is the stored
    /// (compressed) length, `raw_len` and `crc` (CRC32C) describe the raw
    /// block, `dict` is the dictionary number or `NO_DICT`.
    PutBlock {
        number: u64,
        segment: u64,
        offset: u64,
        len: u64,
        raw_len: u64,
        dict: u64,
        crc: u32,
    },
    /// The same, stored in the WAL frame itself (`WriteMode::InlineSmall`).
    /// `data_at` is where `data` starts within the frame body; set by
    /// `decode_records`, ignored by `encode`.
    PutInlineBlock {
        number: u64,
        raw_len: u64,
        dict: u64,
        crc: u32,
        data: &'a [u8],
        data_at: usize,
    },
    /// A new object: its full BLAKE3 id, the local object number every
    /// later reference uses, and its place: object `index` of `block`.
    PutObject {
        id: ObjectId,
        number: u64,
        block: u64,
        index: u64,
    },
    /// A compression dictionary, numbered from 1. Empty: training was tried
    /// and produced no dictionary; it is not tried again.
    PutDict {
        number: u64,
        data: &'a [u8],
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
    /// Segment `old` was rewritten into `new`. `kept` lists every block
    /// copied unchanged, with its offset in `new`; `rewritten` are the blocks
    /// holding, in order, the live objects of the other blocks of `old`
    /// (several when they exceed `Config::max_block_raw_bytes`). Every other
    /// block and object of `old` is gone.
    Compact {
        old: u64,
        new: u64,
        kept: Vec<(u64, u64)>,
        rewritten: Vec<Rewritten>,
    },
}

/// Upper bounds of encoded record sizes, header included, used to bound a
/// frame before any I/O. Variable parts: + name length (PutTopic), + data
/// length (PutInlineBlock, PutDict), + `RECORD_COMPACT_KEPT` per kept block
/// and `RECORD_COMPACT_REWRITTEN` per rewritten block + `MAX_VARINT` per
/// object of the rewritten blocks (Compact).
pub const RECORD_PUT_OBJECT: usize = RECORD_HEADER_MAX + 32 + 3 * MAX_VARINT;
pub const RECORD_PUT_TOPIC_BASE: usize = RECORD_HEADER_MAX + MAX_VARINT;
/// Without its attributes (`attrs_len`).
pub const RECORD_PUBLISH: usize = RECORD_HEADER_MAX + 4 * MAX_VARINT;
pub const RECORD_RELEASE: usize = RECORD_HEADER_MAX + 2 * MAX_VARINT;
pub const RECORD_PUT_BLOCK: usize = RECORD_HEADER_MAX + 6 * MAX_VARINT + 4;
pub const RECORD_PUT_INLINE_BLOCK_BASE: usize = RECORD_HEADER_MAX + 3 * MAX_VARINT + 4;
pub const RECORD_PUT_DICT_BASE: usize = RECORD_HEADER_MAX + MAX_VARINT;
pub const RECORD_COMPACT_BASE: usize = RECORD_HEADER_MAX + 4 * MAX_VARINT;
pub const RECORD_COMPACT_KEPT: usize = 2 * MAX_VARINT;
pub const RECORD_COMPACT_REWRITTEN: usize = 6 * MAX_VARINT + 4;

/// Delta state of one frame. The first Publish's object number is relative
/// to the next object number when the frame starts (known from earlier
/// frames), its timestamp to zero; every later Publish is relative to the
/// previous one (zigzag varints). A PutObject number is relative to the next
/// expected object number, which starts at the frame's next object number
/// and becomes the previous new object's number + 1 (numbers are assigned in
/// order, so the delta is normally 0). A PutBlock or PutInlineBlock number
/// is relative to the next expected block number in the same way. A
/// PutObject's block is a zigzag delta from the previous PutBlock,
/// PutInlineBlock or PutObject block of the frame (0 at the start), its
/// index a delta from the index after the previous PutObject of the same
/// block (from 0 for another block).
#[derive(Clone, Copy, Debug)]
pub struct FrameCtx {
    number: u64,
    timestamp: u64,
    next_object: u64,
    next_block: u64,
    block: u64,
    next_index: u64,
}

impl FrameCtx {
    pub fn new(next_object: u64, next_block: u64) -> Self {
        Self {
            number: next_object,
            timestamp: 0,
            next_object,
            next_block,
            block: 0,
            next_index: 0,
        }
    }

    fn put_block_number(&mut self, p: &mut Vec<u8>, number: u64) {
        put_uvarint(p, number.wrapping_sub(self.next_block));
        self.set_block(number);
    }

    fn get_block_number(&mut self, r: &mut Reader<'_>) -> Result<u64, &'static str> {
        let number = self.next_block.wrapping_add(r.uvarint()?);
        self.set_block(number);
        Ok(number)
    }

    fn set_block(&mut self, number: u64) {
        self.next_block = number.wrapping_add(1);
        self.block = number;
        self.next_index = 0;
    }

    fn index_base(&self, block: u64) -> u64 {
        if block == self.block {
            self.next_index
        } else {
            0
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
            Record::PutBlock { .. } => RECORD_PUT_BLOCK,
            Record::PutInlineBlock { data, .. } => RECORD_PUT_INLINE_BLOCK_BASE + data.len(),
            Record::PutObject { .. } => RECORD_PUT_OBJECT,
            Record::PutDict { data, .. } => RECORD_PUT_DICT_BASE + data.len(),
            Record::PutTopic { name, .. } => RECORD_PUT_TOPIC_BASE + name.len(),
            Record::Publish { attrs, .. } => RECORD_PUBLISH + attrs.len(),
            Record::Release { .. } => RECORD_RELEASE,
            Record::Compact {
                kept, rewritten, ..
            } => {
                RECORD_COMPACT_BASE
                    + RECORD_COMPACT_KEPT * kept.len()
                    + rewritten
                        .iter()
                        .map(|r| RECORD_COMPACT_REWRITTEN + MAX_VARINT * r.objects.len())
                        .sum::<usize>()
            }
        }
    }

    /// Appends `len varint | type u8 | payload` to `out`.
    pub fn encode(&self, out: &mut Vec<u8>, ctx: &mut FrameCtx) -> Result<(), StoreError> {
        let mut p = Vec::new();
        let kind = match self {
            Record::PutBlock {
                number,
                segment,
                offset,
                len,
                raw_len,
                dict,
                crc,
            } => {
                ctx.put_block_number(&mut p, *number);
                put_uvarint(&mut p, *segment);
                put_uvarint(&mut p, *offset);
                put_uvarint(&mut p, *len);
                put_uvarint(&mut p, *raw_len);
                put_uvarint(&mut p, *dict);
                p.extend_from_slice(&crc.to_le_bytes());
                PUT_BLOCK
            }
            Record::PutInlineBlock {
                number,
                raw_len,
                dict,
                crc,
                data,
                ..
            } => {
                ctx.put_block_number(&mut p, *number);
                put_uvarint(&mut p, *raw_len);
                put_uvarint(&mut p, *dict);
                p.extend_from_slice(&crc.to_le_bytes());
                p.extend_from_slice(data);
                PUT_INLINE_BLOCK
            }
            Record::PutObject {
                id,
                number,
                block,
                index,
            } => {
                p.extend_from_slice(&id.0);
                ctx.put_number(&mut p, *number);
                put_uvarint(&mut p, zigzag(block.wrapping_sub(ctx.block) as i64));
                put_uvarint(&mut p, index.wrapping_sub(ctx.index_base(*block)));
                ctx.block = *block;
                ctx.next_index = index.wrapping_add(1);
                PUT_OBJECT
            }
            Record::PutDict { number, data } => {
                put_uvarint(&mut p, *number);
                p.extend_from_slice(data);
                PUT_DICT
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
            Record::Compact {
                old,
                new,
                kept,
                rewritten,
            } => {
                put_uvarint(&mut p, *old);
                put_uvarint(&mut p, *new);
                put_uvarint(&mut p, kept.len() as u64);
                for (block, offset) in kept {
                    put_uvarint(&mut p, *block);
                    put_uvarint(&mut p, *offset);
                }
                put_uvarint(&mut p, rewritten.len() as u64);
                for r in rewritten {
                    for v in [r.block, r.offset, r.len, r.raw_len, r.dict] {
                        put_uvarint(&mut p, v);
                    }
                    p.extend_from_slice(&r.crc.to_le_bytes());
                    put_uvarint(&mut p, r.objects.len() as u64);
                    for &number in &r.objects {
                        put_uvarint(&mut p, number);
                    }
                }
                COMPACT
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
    /// `next_object`, `next_block`: the next object and block numbers when
    /// this frame starts.
    pub fn begin(out: &mut Vec<u8>, next_object: u64, next_block: u64) -> Self {
        let start = out.len();
        out.extend_from_slice(&[0; FRAME_HEADER]);
        Self {
            start,
            count: 0,
            ctx: FrameCtx::new(next_object, next_block),
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
/// was written that way: corruption, not a torn write. `next_object` and
/// `next_block` are the next object and block numbers when the frame starts
/// (delta bases).
pub fn decode_records<'a>(
    frame: &Frame<'a>,
    next_object: u64,
    next_block: u64,
) -> Result<Vec<Record<'a>>, &'static str> {
    let mut rest = frame.body;
    // Bounded by the body: every record takes at least 2 bytes.
    if frame.count as usize > rest.len() / 2 {
        return Err("record count exceeds frame body");
    }
    let mut ctx = FrameCtx::new(next_object, next_block);
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
        PUT_BLOCK => Record::PutBlock {
            number: ctx.get_block_number(&mut r)?,
            segment: r.uvarint()?,
            offset: r.uvarint()?,
            len: r.uvarint()?,
            raw_len: r.uvarint()?,
            dict: r.uvarint()?,
            crc: r.u32()?,
        },
        PUT_INLINE_BLOCK => {
            let number = ctx.get_block_number(&mut r)?;
            let raw_len = r.uvarint()?;
            let dict = r.uvarint()?;
            let crc = r.u32()?;
            let data_at = payload_at + payload.len() - r.0.len();
            Record::PutInlineBlock {
                number,
                raw_len,
                dict,
                crc,
                data: std::mem::take(&mut r.0),
                data_at,
            }
        }
        PUT_OBJECT => {
            let id = r.id()?;
            let number = ctx.get_number(&mut r)?;
            let block = ctx.block.wrapping_add(unzigzag(r.uvarint()?) as u64);
            let index = ctx.index_base(block).wrapping_add(r.uvarint()?);
            ctx.block = block;
            ctx.next_index = index.wrapping_add(1);
            Record::PutObject {
                id,
                number,
                block,
                index,
            }
        }
        PUT_DICT => Record::PutDict {
            number: r.uvarint()?,
            data: std::mem::take(&mut r.0),
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
            let old = r.uvarint()?;
            let new = r.uvarint()?;
            // Bounded by the payload length: at least 2 bytes per entry.
            let count = usize::try_from(r.uvarint()?)
                .ok()
                .filter(|&c| c <= r.0.len() / 2)
                .ok_or("compact block count exceeds payload")?;
            let mut kept = Vec::with_capacity(count);
            for _ in 0..count {
                kept.push((r.uvarint()?, r.uvarint()?));
            }
            // At least 10 bytes per rewritten block: five varints, the CRC,
            // the object count.
            let blocks = usize::try_from(r.uvarint()?)
                .ok()
                .filter(|&c| c <= r.0.len() / 10)
                .ok_or("compact rewritten-block count exceeds payload")?;
            let mut rewritten = Vec::with_capacity(blocks);
            for _ in 0..blocks {
                {
                    let block = r.uvarint()?;
                    let offset = r.uvarint()?;
                    let len = r.uvarint()?;
                    let raw_len = r.uvarint()?;
                    let dict = r.uvarint()?;
                    let crc = r.u32()?;
                    // At least 1 byte per object number.
                    let count = usize::try_from(r.uvarint()?)
                        .ok()
                        .filter(|&c| c <= r.0.len())
                        .ok_or("compact object count exceeds payload")?;
                    let mut objects = Vec::with_capacity(count);
                    for _ in 0..count {
                        objects.push(r.uvarint()?);
                    }
                    rewritten.push(Rewritten {
                        block,
                        offset,
                        len,
                        raw_len,
                        dict,
                        crc,
                        objects,
                    });
                }
            }
            Record::Compact {
                old,
                new,
                kept,
                rewritten,
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
    fn id(&mut self) -> Result<ObjectId, &'static str> {
        Ok(id_at(self.take(32)?, 0))
    }
    fn uvarint(&mut self) -> Result<u64, &'static str> {
        let (v, n) = get_uvarint(self.0)?;
        self.0 = &self.0[n..];
        Ok(v)
    }
}

/// Appends one object to a raw block: `len varint | bytes`.
pub fn push_block_object(raw: &mut Vec<u8>, bytes: &[u8]) {
    put_uvarint(raw, bytes.len() as u64);
    raw.extend_from_slice(bytes);
}

/// Bytes `push_block_object` appends for an object of `bytes`.
pub fn block_entry_len(bytes: &[u8]) -> usize {
    let mut n = 1;
    let mut v = bytes.len() as u64;
    while v >= 0x80 {
        n += 1;
        v >>= 7;
    }
    n + bytes.len()
}

/// Byte ranges of the objects of a raw block, in index order. The block
/// must be exactly a sequence of `len varint | bytes`.
pub fn split_block(raw: &[u8]) -> Result<Vec<std::ops::Range<usize>>, &'static str> {
    let mut spans = Vec::new();
    let mut pos = 0;
    while pos < raw.len() {
        let (len, n) = get_uvarint(&raw[pos..])?;
        let start = pos + n;
        let end = usize::try_from(len)
            .ok()
            .and_then(|len| start.checked_add(len))
            .filter(|&end| end <= raw.len())
            .ok_or("object runs past the end of its block")?;
        spans.push(start..end);
        pos = end;
    }
    Ok(spans)
}

/// Entries per topic index block. Entry `seq` is entry `seq % 64` of block
/// `seq / 64`; topic_seq is implicit.
pub const INDEX_BLOCK: u64 = 64;

/// Encodes topic index entries in order. In a block, the first entry is
/// `number varint | timestamp varint`; each later one is `zigzag(number −
/// previous number) varint | (timestamp − previous timestamp) varint`
/// (timestamps do not decrease within a topic). A full block is stored as
/// the body of an index record, whose CRC covers it. The encoding is
/// deterministic: the same entries always give the same bytes.
#[derive(Clone, Copy, Debug, Default)]
pub struct IndexWriter {
    seq: u64,
    prev_number: u64,
    prev_timestamp: u64,
}

impl IndexWriter {
    /// A writer whose next entry gets topic_seq `seq` and follows the entry
    /// `(prev_number, prev_timestamp)`.
    pub fn resume(seq: u64, prev_number: u64, prev_timestamp: u64) -> Self {
        Self {
            seq,
            prev_number,
            prev_timestamp,
        }
    }

    /// The topic_seq the next entry gets.
    pub fn seq(&self) -> u64 {
        self.seq
    }

    /// The last entry's timestamp (0 before the first).
    pub fn last_timestamp(&self) -> u64 {
        self.prev_timestamp
    }

    /// Appends the entry with topic_seq `self.seq()`. Returns true if it
    /// completes a block.
    pub fn push(&mut self, out: &mut Vec<u8>, number: u64, timestamp: u64) -> bool {
        if self.seq.is_multiple_of(INDEX_BLOCK) {
            put_uvarint(out, number);
            put_uvarint(out, timestamp);
        } else {
            put_uvarint(out, zigzag(number.wrapping_sub(self.prev_number) as i64));
            put_uvarint(out, timestamp.wrapping_sub(self.prev_timestamp));
        }
        self.prev_number = number;
        self.prev_timestamp = timestamp;
        self.seq += 1;
        self.seq.is_multiple_of(INDEX_BLOCK)
    }
}

/// The entries of one block (full, or the incomplete last one), encoded by
/// `IndexWriter` from a block start. `None` unless `buf` is a whole number
/// of well-formed entries, at most 64.
pub fn decode_index_entries(buf: &[u8]) -> Option<Vec<(u64, u64)>> {
    let mut entries = Vec::with_capacity(INDEX_BLOCK as usize);
    let mut pos = 0;
    let (mut number, mut timestamp) = (0u64, 0u64);
    while pos < buf.len() {
        if entries.len() as u64 == INDEX_BLOCK {
            return None;
        }
        let (x, n) = get_uvarint(&buf[pos..]).ok()?;
        let (y, m) = get_uvarint(&buf[pos + n..]).ok()?;
        if entries.is_empty() {
            (number, timestamp) = (x, y);
        } else {
            number = number.wrapping_add(unzigzag(x) as u64);
            timestamp = timestamp.wrapping_add(y);
        }
        pos += n + m;
        entries.push((number, timestamp));
    }
    Some(entries)
}

/// `magic [8] | version u32 | created_at u64 | crc32c u32` at the start of
/// an index segment `idx-<id>.dat`.
pub const INDEX_HEADER: usize = 24;
pub const INDEX_MAGIC: [u8; 8] = *b"STRMIDX\0";

/// The file header of a new index segment. `created_at`: the WAL frame
/// sequence number the store's next frame had when the segment was created.
pub fn index_header(created_at: u64) -> [u8; INDEX_HEADER] {
    let mut h = [0; INDEX_HEADER];
    h[..8].copy_from_slice(&INDEX_MAGIC);
    h[8..12].copy_from_slice(&WAL_VERSION.to_le_bytes());
    h[12..20].copy_from_slice(&created_at.to_le_bytes());
    let crc = crc32c::crc32c(&h[..20]);
    h[20..].copy_from_slice(&crc.to_le_bytes());
    h
}

/// `created_at` of an intact index segment header of this version.
pub fn decode_index_header(buf: &[u8]) -> Option<u64> {
    let h: &[u8; INDEX_HEADER] = buf.get(..INDEX_HEADER)?.try_into().ok()?;
    let created_at = u64::from_le_bytes(h[12..20].try_into().ok()?);
    (*h == index_header(created_at)).then_some(created_at)
}

/// Index record kinds.
pub const INDEX_RECORD_BLOCK: u8 = 1;
pub const INDEX_RECORD_SEEK_PAGE: u8 = 2;

/// Where an index record lies: `segment u32 | len u32 | offset u64`, 16
/// bytes in a seek page.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IndexLoc {
    pub segment: u32,
    /// The whole record, header and CRC included.
    pub len: u32,
    pub offset: u64,
}

pub const INDEX_LOC: usize = 16;

impl IndexLoc {
    fn put(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.segment.to_le_bytes());
        out.extend_from_slice(&self.len.to_le_bytes());
        out.extend_from_slice(&self.offset.to_le_bytes());
    }

    fn at(b: &[u8]) -> Self {
        Self {
            segment: u32_at(b, 0),
            len: u32_at(b, 4),
            offset: u64_at(b, 8),
        }
    }
}

/// An index record: `kind u8 | len varint | topic varint | number varint |
/// stamp varint | body (len bytes) | crc32c u32` over everything before the
/// CRC.
#[derive(Debug, PartialEq, Eq)]
pub struct IndexRecord<'a> {
    pub kind: u8,
    /// Topic number.
    pub topic: u64,
    /// Block number, or the seek page's number within its level.
    pub number: u64,
    /// The WAL frame sequence number after the frames the record describes.
    pub stamp: u64,
    pub body: &'a [u8],
    /// Bytes the whole record takes.
    pub size: usize,
}

/// Appends an index record; returns its size in bytes.
pub fn put_index_record(
    out: &mut Vec<u8>,
    kind: u8,
    topic: u64,
    number: u64,
    stamp: u64,
    body: &[u8],
) -> usize {
    let start = out.len();
    out.push(kind);
    put_uvarint(out, body.len() as u64);
    put_uvarint(out, topic);
    put_uvarint(out, number);
    put_uvarint(out, stamp);
    out.extend_from_slice(body);
    let crc = crc32c::crc32c(&out[start..]);
    out.extend_from_slice(&crc.to_le_bytes());
    out.len() - start
}

/// The index record at the start of `buf`; `None` if it is short, malformed
/// or fails its CRC (a torn, unwritten or damaged record).
pub fn decode_index_record(buf: &[u8]) -> Option<IndexRecord<'_>> {
    let kind = *buf.first()?;
    let mut pos = 1;
    let mut field = || {
        let (v, n) = get_uvarint(buf.get(pos..)?).ok()?;
        pos += n;
        Some(v)
    };
    let (len, topic, number, stamp) = (field()?, field()?, field()?, field()?);
    let end = pos.checked_add(usize::try_from(len).ok()?)?;
    let stored = buf.get(end..end.checked_add(4)?)?;
    if crc32c::crc32c(&buf[..end]) != u32_at(stored, 0) {
        return None;
    }
    Some(IndexRecord {
        kind,
        topic,
        number,
        stamp,
        body: &buf[pos..end],
        size: end + 4,
    })
}

/// A seek page's body: `level u8 | locations, 16 bytes each`.
pub fn seek_page_body(level: u8, locs: &[IndexLoc]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + locs.len() * INDEX_LOC);
    out.push(level);
    for loc in locs {
        loc.put(&mut out);
    }
    out
}

/// The level and the number of locations of a seek page body; `None` if
/// malformed.
pub fn seek_page_shape(body: &[u8]) -> Option<(u8, usize)> {
    let (&level, locs) = body.split_first()?;
    locs.len()
        .is_multiple_of(INDEX_LOC)
        .then_some((level, locs.len() / INDEX_LOC))
}

/// Location `i` of a seek page body whose shape was checked.
pub fn seek_page_loc(body: &[u8], i: usize) -> Option<IndexLoc> {
    let at = 1 + i.checked_mul(INDEX_LOC)?;
    body.get(at..at + INDEX_LOC).map(IndexLoc::at)
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
            Record::PutDict {
                number: 1,
                data: b"dictionary bytes",
            },
            Record::PutBlock {
                number: 12,
                segment: 3,
                offset: 1 << 40,
                len: 700,
                raw_len: 4000,
                dict: 1,
                crc: 0xdead_beef,
            },
            Record::PutObject {
                id,
                number: 77,
                block: 12,
                index: 0,
            },
            Record::PutObject {
                id: ObjectId::of(b"z"),
                number: 78,
                block: 12,
                index: 1,
            },
            // An object of another block (not written so, but encodable).
            Record::PutObject {
                id,
                number: 79,
                block: 3,
                index: 9,
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
                kept: vec![(4, 0), (5, 1000)],
                rewritten: vec![Rewritten {
                    block: 13,
                    offset: 2000,
                    len: 50,
                    raw_len: 300,
                    dict: 0,
                    crc: 7,
                    objects: vec![2, 300, 1 << 33],
                }],
            },
            Record::Compact {
                old: 2,
                new: NO_SEGMENT,
                kept: vec![],
                rewritten: vec![],
            },
        ]
    }

    fn frame_of(records: &[Record<'_>], seq: u64, next_object: u64) -> Vec<u8> {
        let mut out = Vec::new();
        let mut f = FrameBuilder::begin(&mut out, next_object, 12);
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
        let h = wal_file_header(0x0102_0304_0506_0708);
        assert_eq!(&h[..8], b"STRMWAL\0");
        assert_eq!(&h[8..12], &WAL_VERSION.to_le_bytes());
        assert_eq!(&h[12..20], &0x0102_0304_0506_0708u64.to_le_bytes());
        assert_eq!(decode_wal_header(&h), Some(0x0102_0304_0506_0708));
        for version in (1..WAL_VERSION).chain([WAL_VERSION + 1]) {
            // An older or a future version, with a valid header CRC.
            let mut other = h;
            other[8..12].copy_from_slice(&version.to_le_bytes());
            let crc = crc32c::crc32c(&other[..WAL_HEADER_CRC_AT]);
            other[WAL_HEADER_CRC_AT..].copy_from_slice(&crc.to_le_bytes());
            assert_eq!(decode_wal_header(&other), None, "version {version}");
        }
        for i in 0..WAL_FILE_HEADER {
            let mut flipped = h;
            flipped[i] ^= 1;
            assert_eq!(decode_wal_header(&flipped), None, "byte {i} flipped");
        }
        assert_eq!(decode_wal_header(&h[..WAL_FILE_HEADER - 1]), None);
    }

    #[test]
    fn frames_round_trip_within_size_bounds() {
        let records = samples();
        for next_object in [0, 77, u64::MAX] {
            let buf = frame_of(&records, 7, next_object);
            let bound: usize = records.iter().map(Record::max_size).sum();
            assert!(buf.len() <= FRAME_HEADER + bound, "size bounds hold");
            let frame = decode_frame(&buf).unwrap();
            assert_eq!((frame.seq, frame.count, frame.size()), (7, 11, buf.len()));
            assert_eq!(frame_mode(&frame), Ok(FrameMode::ParallelSync));
            assert_eq!(
                positionless(decode_records(&frame, next_object, 12).unwrap()),
                records
            );
        }
    }

    /// Clears the positions the decoder fills in (`data_at`, `attrs_at`), so
    /// decoded records compare equal to the ones encoded.
    fn positionless(mut recs: Vec<Record<'_>>) -> Vec<Record<'_>> {
        for r in &mut recs {
            match r {
                Record::PutInlineBlock { data_at, .. } => *data_at = 0,
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
            Record::PutBlock {
                number: u64::MAX,
                segment: u64::MAX,
                offset: u64::MAX,
                len: u64::MAX,
                raw_len: u64::MAX,
                dict: u64::MAX,
                crc: u32::MAX,
            },
            Record::PutObject {
                id: ObjectId([0xff; 32]),
                number: u64::MAX,
                block: 0,
                index: u64::MAX,
            },
            Record::PutInlineBlock {
                number: 0,
                raw_len: 0,
                dict: 0,
                crc: 0,
                data: &[0xab; 300],
                data_at: 0,
            },
            Record::Compact {
                old: u64::MAX,
                new: u64::MAX,
                kept: vec![(u64::MAX, u64::MAX)],
                rewritten: vec![Rewritten {
                    block: u64::MAX,
                    offset: u64::MAX,
                    len: u64::MAX,
                    raw_len: u64::MAX,
                    dict: u64::MAX,
                    crc: u32::MAX,
                    objects: vec![u64::MAX],
                }],
            },
        ];
        let buf = frame_of(&records, u64::MAX, 0);
        let frame = decode_frame(&buf).unwrap();
        let decoded = decode_records(&frame, 0, 12).unwrap();
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
            decode_records(&decode_frame(&buf).unwrap(), 0, 0),
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
            decode_records(&decode_frame(&buf).unwrap(), 0, 0),
            Err("overlong varint")
        );
        // Overlong record length.
        let mut buf = frame_of(&[], 0, 0);
        buf[12..16].copy_from_slice(&1u32.to_le_bytes());
        buf.extend_from_slice(&[0x82, 0x00, RELEASE, 0x00, 0x00]);
        let buf = reseal(buf);
        assert_eq!(
            decode_records(&decode_frame(&buf).unwrap(), 0, 0),
            Err("overlong varint")
        );
        // Truncated varint at the end of a Publish payload.
        let mut buf = frame_of(&[], 0, 0);
        buf[12..16].copy_from_slice(&1u32.to_le_bytes());
        buf.extend_from_slice(&[4, PUBLISH, 0, 0, 0, 0x80]);
        let buf = reseal(buf);
        assert_eq!(
            decode_records(&decode_frame(&buf).unwrap(), 0, 0),
            Err("truncated varint")
        );
    }

    #[test]
    fn put_inline_block_round_trip_reports_data_position() {
        let put = |data_at| Record::PutInlineBlock {
            number: 12,
            raw_len: 6,
            dict: 0,
            crc: 1,
            data: b"hello",
            data_at,
        };
        let release = Record::Release { topic: 0, seq: 0 };
        let buf = frame_of(&[release.clone(), put(0)], 0, 5);
        let frame = decode_frame(&buf).unwrap();
        let records = decode_records(&frame, 5, 12).unwrap();
        // Release (header 2 + payload 2), then the PutInlineBlock header (2),
        // block number delta 0, raw length and dictionary (1 byte each) and
        // the CRC (4).
        let at = 4 + Record::header_len(12) + 7;
        assert_eq!(records, vec![release, put(at)]);
        assert_eq!(&frame.body[at..at + 5], b"hello");
    }

    /// A batch frame: one block, its objects in index order, all deltas 0.
    #[test]
    fn batch_records_take_one_byte_per_delta() {
        let id = ObjectId::of(b"a");
        let records = [
            Record::PutBlock {
                number: 40,
                segment: 2,
                offset: 100,
                len: 30,
                raw_len: 90,
                dict: 1,
                crc: 9,
            },
            Record::PutObject {
                id,
                number: 300,
                block: 40,
                index: 0,
            },
            Record::PutObject {
                id,
                number: 301,
                block: 40,
                index: 1,
            },
        ];
        let mut out = Vec::new();
        let mut f = FrameBuilder::begin(&mut out, 300, 40);
        for r in &records {
            f.record(&mut out, r).unwrap();
        }
        f.finish(&mut out, 0, FrameMode::Sequential).unwrap();
        // PutObject: header 2, id 32, number, block and index deltas 1 each.
        let body = &out[FRAME_HEADER..];
        let block_record = 2 + 1 + 1 + 1 + 1 + 1 + 1 + 4;
        assert_eq!(body.len(), block_record + 2 * 37);
        let frame = decode_frame(&out).unwrap();
        assert_eq!(decode_records(&frame, 300, 40).unwrap(), records);
    }

    #[test]
    fn raw_blocks_split_into_objects() {
        let mut raw = Vec::new();
        for obj in [&b""[..], b"x", &[7; 200]] {
            push_block_object(&mut raw, obj);
        }
        let spans = split_block(&raw).unwrap();
        assert_eq!(spans.len(), 3);
        assert_eq!(&raw[spans[1].clone()], b"x");
        assert_eq!(raw[spans[2].clone()], [7; 200]);
        assert_eq!(split_block(&[]).unwrap(), vec![]);
        assert_eq!(
            split_block(&raw[..raw.len() - 1]),
            Err("object runs past the end of its block")
        );
        assert_eq!(split_block(&[0x80]), Err("truncated varint"));
    }

    #[test]
    fn invalid_topic_is_rejected_on_encode() {
        let mut buf = Vec::new();
        let long = [b'a'; MAX_TOPIC + 1];
        for name in [&b""[..], &long[..]] {
            let rec = Record::PutTopic { number: 0, name };
            let mut ctx = FrameCtx::new(0, 0);
            assert!(matches!(
                rec.encode(&mut buf, &mut ctx),
                Err(StoreError::InvalidTopic)
            ));
        }
    }

    fn index_of(entries: &[(u64, u64)]) -> Vec<Vec<u8>> {
        let mut w = IndexWriter::default();
        let mut blocks = vec![Vec::new()];
        for &(n, t) in entries {
            if w.push(blocks.last_mut().unwrap(), n, t) {
                blocks.push(Vec::new());
            }
        }
        blocks
    }

    #[test]
    fn index_blocks_round_trip() {
        // New objects in order, a deduplicated reference back, a time step.
        let entries: Vec<(u64, u64)> = (0..150u64)
            .map(|i| {
                let n = if i % 10 == 9 { 3 } else { 1000 + i };
                (n, 1_700_000_000_000 + i / 7)
            })
            .collect();
        let blocks = index_of(&entries);
        assert_eq!(blocks.len(), 3);
        for (b, bytes) in blocks.iter().enumerate() {
            let want = &entries[b * 64..((b + 1) * 64).min(entries.len())];
            assert_eq!(decode_index_entries(bytes).unwrap(), want, "block {b}");
        }
        // About 2 bytes per entry: 1 + 1 for consecutive numbers and equal
        // or +1 timestamps; the first entry of a block is absolute.
        let total: usize = blocks.iter().map(Vec::len).sum();
        assert!(total < 150 * 3, "{total} bytes");
        // A cut inside an entry, or more than 64 entries, is not a block.
        assert_eq!(
            decode_index_entries(&blocks[0][..blocks[0].len() - 1]),
            None
        );
        assert_eq!(
            decode_index_entries(&[blocks[0].clone(), vec![0, 0]].concat()),
            None
        );
        // The encoding resumes from the writer state.
        let mut w = IndexWriter::resume(64, entries[63].0, entries[63].1);
        let mut out = Vec::new();
        for &(n, t) in &entries[64..128] {
            w.push(&mut out, n, t);
        }
        assert_eq!(out, blocks[1]);
        assert_eq!((w.seq(), w.last_timestamp()), (128, entries[127].1));
    }

    #[test]
    fn index_record_crc_covers_topic_number_stamp_and_body() {
        let mut buf = Vec::new();
        let size = put_index_record(&mut buf, INDEX_RECORD_BLOCK, 7, 300, 42, b"entries");
        assert_eq!(size, buf.len());
        let rec = decode_index_record(&buf).unwrap();
        assert_eq!(
            (
                rec.kind, rec.topic, rec.number, rec.stamp, rec.body, rec.size
            ),
            (INDEX_RECORD_BLOCK, 7, 300, 42, &b"entries"[..], size)
        );
        // Every byte is covered: any flip fails, and so does a cut.
        for i in 0..buf.len() {
            let mut bad = buf.clone();
            bad[i] ^= 0x01;
            assert_ne!(decode_index_record(&bad).as_ref(), Some(&rec), "byte {i}");
        }
        assert_eq!(decode_index_record(&buf[..size - 1]), None);
        assert_eq!(decode_index_record(&[0; 32]), None);
    }

    #[test]
    fn seek_pages_and_headers_round_trip() {
        let locs: Vec<IndexLoc> = (0..3)
            .map(|i| IndexLoc {
                segment: i,
                len: 100 + i,
                offset: u64::MAX - u64::from(i),
            })
            .collect();
        let body = seek_page_body(2, &locs);
        assert_eq!(body.len(), 1 + 3 * INDEX_LOC);
        assert_eq!(seek_page_shape(&body), Some((2, 3)));
        for (i, loc) in locs.iter().enumerate() {
            assert_eq!(seek_page_loc(&body, i), Some(*loc));
        }
        assert_eq!(seek_page_loc(&body, 3), None);
        assert_eq!(seek_page_shape(&body[..5]), None);
        let h = index_header(77);
        assert_eq!(decode_index_header(&h), Some(77));
        let mut bad = h;
        bad[13] ^= 1;
        assert_eq!(decode_index_header(&bad), None);
    }

    #[test]
    fn index_handles_extreme_values() {
        let entries = [
            (u64::MAX, u64::MAX),
            (0, u64::MAX),
            (u64::MAX, 0),
            (1 << 63, 1),
        ];
        let blocks = index_of(&entries);
        assert_eq!(decode_index_entries(&blocks[0]).unwrap(), entries);
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
        let mut frame = FrameBuilder::begin(&mut buf, 0, 0);
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
        let recs = decode_records(&f, 0, 0).unwrap();
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
