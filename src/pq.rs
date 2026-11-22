//! A small Parquet writer for the user_daily table: one row group, one data
//! page (v1) per column, every column dictionary-encoded with bit-packed
//! indexes, no compression or statistics. It does only what this one schema
//! needs, without the general writer's per-value machinery.

use std::fs::File;
use std::io::Write;
use std::path::Path;

use rustc_hash::FxHashMap;

use crate::{Ctx, UserRow, id};

// ---------------------------------------------------------------- thrift compact

struct Thrift {
    buf: Vec<u8>,
    last: Vec<i16>,
}

const T_I32: u8 = 5;
const T_I64: u8 = 6;
const T_BINARY: u8 = 8;
const T_LIST: u8 = 9;
const T_STRUCT: u8 = 12;

impl Thrift {
    fn new() -> Self {
        Thrift { buf: Vec::new(), last: vec![0] }
    }
    fn varint(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.buf.push(v as u8 | 0x80);
            v >>= 7;
        }
        self.buf.push(v as u8);
    }
    fn zigzag(&mut self, v: i64) {
        self.varint(((v << 1) ^ (v >> 63)) as u64);
    }
    fn field(&mut self, id: i16, ty: u8) {
        let last = self.last.last_mut().unwrap();
        let d = id - *last;
        *last = id;
        if (1..=15).contains(&d) {
            self.buf.push((d as u8) << 4 | ty);
        } else {
            self.buf.push(ty);
            self.zigzag(id as i64);
        }
    }
    fn i32(&mut self, id: i16, v: i32) {
        self.field(id, T_I32);
        self.zigzag(v as i64);
    }
    fn i64(&mut self, id: i16, v: i64) {
        self.field(id, T_I64);
        self.zigzag(v);
    }
    fn str(&mut self, id: i16, s: &[u8]) {
        self.field(id, T_BINARY);
        self.varint(s.len() as u64);
        self.buf.extend_from_slice(s);
    }
    fn begin(&mut self, id: i16) {
        self.field(id, T_STRUCT);
        self.last.push(0);
    }
    /// A struct as a list element.
    fn begin_elem(&mut self) {
        self.last.push(0);
    }
    fn end(&mut self) {
        self.buf.push(0);
        self.last.pop();
    }
    fn list(&mut self, id: i16, elem: u8, n: usize) {
        self.field(id, T_LIST);
        if n < 15 {
            self.buf.push((n as u8) << 4 | elem);
        } else {
            self.buf.push(0xF0 | elem);
            self.varint(n as u64);
        }
    }
    fn list_i32(&mut self, v: i32) {
        self.zigzag(v as i64);
    }
    fn list_str(&mut self, s: &[u8]) {
        self.varint(s.len() as u64);
        self.buf.extend_from_slice(s);
    }
}

// ---------------------------------------------------------------- encodings

const PLAIN: i32 = 0;
const RLE: i32 = 3;
const RLE_DICTIONARY: i32 = 8;

/// `vals` (each below 2^bw) as bit-packed runs of the RLE/bit-packing hybrid.
fn bitpack(out: &mut Vec<u8>, vals: &[u32], bw: u32) {
    const MAX_GROUPS: usize = 63;
    for run in vals.chunks(MAX_GROUPS * 8) {
        let groups = run.len().div_ceil(8);
        let mut h = (groups as u64) << 1 | 1;
        while h >= 0x80 {
            out.push(h as u8 | 0x80);
            h >>= 7;
        }
        out.push(h as u8);
        let (mut acc, mut nbits) = (0u64, 0u32);
        for i in 0..groups * 8 {
            let v = run.get(i).copied().unwrap_or(0) as u64;
            acc |= v << nbits;
            nbits += bw;
            while nbits >= 8 {
                out.push(acc as u8);
                acc >>= 8;
                nbits -= 8;
            }
        }
        debug_assert_eq!(nbits, 0);
    }
}

fn bit_width(n: usize) -> u32 {
    // Indexes 0..n; at least 1 bit.
    (usize::BITS - n.saturating_sub(1).leading_zeros()).max(1)
}

struct Chunk {
    /// Column bytes: dictionary page then data page.
    bytes: Vec<u8>,
    dict_len: usize,
    num_values: i64,
}

fn page_header(ty: i32, size: usize, num_values: usize, dict: bool, encoding: i32) -> Vec<u8> {
    let mut t = Thrift::new();
    t.i32(1, ty);
    t.i32(2, size as i32);
    t.i32(3, size as i32);
    if dict {
        t.begin(7);
        t.i32(1, num_values as i32);
        t.i32(2, encoding);
        t.end();
    } else {
        t.begin(5);
        t.i32(1, num_values as i32);
        t.i32(2, encoding);
        t.i32(3, RLE);
        t.i32(4, RLE);
        t.end();
    }
    t.buf.push(0);
    t.buf
}

/// A dictionary-encoded column: plain dictionary values, `idx` per present
/// value, and `def` levels (1 = present) for an optional column.
fn column(dict_plain: &[u8], ndict: usize, idx: &[u32], def: Option<&[u32]>, num_values: usize) -> Chunk {
    let mut bytes = page_header(2, dict_plain.len(), ndict, true, PLAIN);
    bytes.extend_from_slice(dict_plain);
    let dict_len = bytes.len();
    let mut data = Vec::with_capacity(idx.len() * 2 + 16);
    if let Some(def) = def {
        let mut lv = Vec::new();
        bitpack(&mut lv, def, 1);
        data.extend_from_slice(&(lv.len() as u32).to_le_bytes());
        data.extend_from_slice(&lv);
    }
    let bw = bit_width(ndict);
    data.push(bw as u8);
    bitpack(&mut data, idx, bw);
    bytes.extend_from_slice(&page_header(0, data.len(), num_values, false, RLE_DICTIONARY));
    bytes.extend_from_slice(&data);
    Chunk { bytes, dict_len, num_values: num_values as i64 }
}

/// Dictionary of i64 values in first-seen order, and each row's index.
fn dict_i64(vals: impl Iterator<Item = i64>, n: usize) -> (Vec<u8>, usize, Vec<u32>) {
    let mut map: FxHashMap<i64, u32> = FxHashMap::default();
    let mut plain = Vec::new();
    let mut idx = Vec::with_capacity(n);
    for v in vals {
        let next = map.len() as u32;
        let i = *map.entry(v).or_insert_with(|| {
            plain.extend_from_slice(&v.to_le_bytes());
            next
        });
        idx.push(i);
    }
    (plain, map.len(), idx)
}

// ---------------------------------------------------------------- user_daily

const PHYS_INT32: i32 = 1;
const PHYS_INT64: i32 = 2;
const PHYS_BYTE_ARRAY: i32 = 6;
const CONV_UTF8: i32 = 0;
const CONV_DATE: i32 = 6;

/// Write user_daily rows. Each person's rows must be consecutive (they are:
/// rows come person by person), so the user_id dictionary needs no lookups.
pub fn write_users(ctx: &Ctx, path: &Path, users: &[&UserRow]) {
    let n = users.len();
    // user_id: a new dictionary entry whenever the person changes.
    let mut udict = Vec::with_capacity(n * 8);
    let mut uidx = Vec::with_capacity(n);
    let mut nu = 0usize;
    let mut prev: Option<u64> = None;
    let mut text = Vec::with_capacity(64);
    for u in users {
        if prev != Some(u.person) {
            text.clear();
            id::text_into(u.person, &mut text);
            udict.extend_from_slice(&(text.len() as u32).to_le_bytes());
            udict.extend_from_slice(&text);
            nu += 1;
            prev = Some(u.person);
        }
        uidx.push(nu as u32 - 1);
    }
    // day: every day of the window.
    let mut ddict = Vec::with_capacity(ctx.ndays * 4);
    for d in 0..ctx.ndays {
        ddict.extend_from_slice(&((ctx.day_base + d as i64) as i32).to_le_bytes());
    }
    let didx: Vec<u32> = users.iter().map(|u| (u.day as i64 - ctx.day_base) as u32).collect();
    let (sd, sn, si) = dict_i64(users.iter().map(|u| u.sessions as i64), n);
    let (ed, en, ei) = dict_i64(users.iter().map(|u| u.events as i64), n);
    let (rd, rn, ri) = dict_i64(users.iter().map(|u| u.revenue as i64), n);
    // top_category: every category, then indexes of the present ones.
    let mut tdict = Vec::new();
    for c in &ctx.cat_names {
        tdict.extend_from_slice(&(c.len() as u32).to_le_bytes());
        tdict.extend_from_slice(c);
    }
    let tdef: Vec<u32> = users.iter().map(|u| u.top.is_some() as u32).collect();
    let tidx: Vec<u32> = users.iter().filter_map(|u| u.top.map(|c| c as u32)).collect();

    let cols = [
        ("user_id", PHYS_BYTE_ARRAY, Some(CONV_UTF8), false, column(&udict, nu, &uidx, None, n)),
        ("day", PHYS_INT32, Some(CONV_DATE), false, column(&ddict, ctx.ndays, &didx, None, n)),
        ("sessions", PHYS_INT64, None, false, column(&sd, sn, &si, None, n)),
        ("events", PHYS_INT64, None, false, column(&ed, en, &ei, None, n)),
        ("revenue_cents", PHYS_INT64, None, false, column(&rd, rn, &ri, None, n)),
        ("top_category", PHYS_BYTE_ARRAY, Some(CONV_UTF8), true, column(&tdict, ctx.cat_names.len(), &tidx, Some(&tdef), n)),
    ];

    let mut out = Vec::with_capacity(cols.iter().map(|c| c.4.bytes.len()).sum::<usize>() + 4096);
    out.extend_from_slice(b"PAR1");
    let mut offsets = Vec::new();
    for c in &cols {
        offsets.push(out.len() as i64);
        out.extend_from_slice(&c.4.bytes);
    }

    let mut t = Thrift::new();
    t.i32(1, 1);
    // Schema: root, then the six columns.
    t.list(2, T_STRUCT, cols.len() + 1);
    t.begin_elem();
    t.str(4, b"user_daily");
    t.i32(5, cols.len() as i32);
    t.end();
    for (name, phys, conv, optional, _) in &cols {
        t.begin_elem();
        t.i32(1, *phys);
        t.i32(3, *optional as i32);
        t.str(4, name.as_bytes());
        if let Some(cv) = conv {
            t.i32(6, *cv);
            // logicalType: STRING (1) or DATE (6), each an empty struct.
            t.begin(10);
            t.begin(if *cv == CONV_DATE { 6 } else { 1 });
            t.end();
            t.end();
        }
        t.end();
    }
    t.i64(3, n as i64);
    t.list(4, T_STRUCT, 1);
    t.begin_elem();
    t.list(1, T_STRUCT, cols.len());
    let mut total = 0i64;
    for (k, (name, phys, _, _, ch)) in cols.iter().enumerate() {
        let size = ch.bytes.len() as i64;
        total += size;
        t.begin_elem();
        t.i64(2, offsets[k]);
        t.begin(3);
        t.i32(1, *phys);
        t.list(2, T_I32, 3);
        t.list_i32(PLAIN);
        t.list_i32(RLE);
        t.list_i32(RLE_DICTIONARY);
        t.list(3, T_BINARY, 1);
        t.list_str(name.as_bytes());
        t.i32(4, 0);
        t.i64(5, ch.num_values);
        t.i64(6, size);
        t.i64(7, size);
        t.i64(9, offsets[k] + ch.dict_len as i64);
        t.i64(11, offsets[k]);
        t.end();
        t.end();
    }
    t.i64(2, total);
    t.i64(3, n as i64);
    t.end();
    t.str(6, b"shopping-sessions");
    t.buf.push(0);
    out.extend_from_slice(&t.buf);
    out.extend_from_slice(&(t.buf.len() as u32).to_le_bytes());
    out.extend_from_slice(b"PAR1");
    File::create(path).unwrap().write_all(&out).unwrap();
}
