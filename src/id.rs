//! IDs as `u64` keys.
//!
//! A canonical ID (a lowercase letter, then 1 to 11 lowercase base-36 digits
//! with no leading zero) is packed exactly: `letter << 57 | value`. That covers
//! every ID the generator writes on purpose (`u3k9x2`, `e1a2b3`, `k9z`, `p123`).
//! Anything else (`U1`, ` u1`, `c_12`, long strings) becomes a 63-bit hash with
//! the top bit set; its text is kept in a side table when it may be needed.
//! Keys are equal exactly when the strings are (up to 63-bit hash collisions
//! among the rare non-canonical IDs). Keys do not sort like the strings: use
//! [`cmp`] for byte order.

use std::sync::{Mutex, OnceLock};

use rustc_hash::FxHashMap;

pub type Id = u64;
const HASHED: u64 = 1 << 63;

static TEXT: OnceLock<Mutex<FxHashMap<Id, Vec<u8>>>> = OnceLock::new();

fn table() -> &'static Mutex<FxHashMap<Id, Vec<u8>>> {
    TEXT.get_or_init(|| Mutex::new(FxHashMap::default()))
}

/// Base-36 digit value of a lowercase digit or letter, or 255.
static B36: [u8; 256] = {
    let mut t = [255u8; 256];
    let mut i = 0;
    while i < 10 {
        t[b'0' as usize + i] = i as u8;
        i += 1;
    }
    let mut i = 0;
    while i < 26 {
        t[b'a' as usize + i] = 10 + i as u8;
        i += 1;
    }
    t
};

#[inline(always)]
fn canonical(s: &[u8]) -> Option<u64> {
    let n = s.len();
    if !(2..=12).contains(&n) || !s[0].is_ascii_lowercase() || (s[1] == b'0' && n > 2) {
        return None;
    }
    let mut v: u64 = 0;
    for &c in &s[1..] {
        let d = B36[c as usize];
        if d == 255 {
            return None;
        }
        v = v * 36 + d as u64;
    }
    Some(((s[0] - b'a') as u64) << 57 | v)
}

#[inline(always)]
fn hashed(s: &[u8]) -> Id {
    xxhash_rust::xxh3::xxh3_64(s) | HASHED
}

/// Key for an ID whose text is never needed (equality only).
#[inline(always)]
pub fn key(s: &[u8]) -> Id {
    canonical(s).unwrap_or_else(|| hashed(s))
}

/// Key for an ID whose text or byte order may be needed later.
#[inline(always)]
pub fn key_kept(s: &[u8]) -> Id {
    match canonical(s) {
        Some(k) => k,
        None => {
            let k = hashed(s);
            table().lock().unwrap().entry(k).or_insert_with(|| s.to_vec());
            k
        }
    }
}

/// Write the text of a key made by `key_kept` into `out`.
pub fn text_into(id: Id, out: &mut Vec<u8>) {
    if id & HASHED != 0 {
        out.extend_from_slice(table().lock().unwrap().get(&id).expect("id text"));
        return;
    }
    out.push(b'a' + (id >> 57) as u8);
    let mut v = id & ((1 << 57) - 1);
    let mut buf = [0u8; 12];
    let mut at = buf.len();
    loop {
        at -= 1;
        buf[at] = b"0123456789abcdefghijklmnopqrstuvwxyz"[(v % 36) as usize];
        v /= 36;
        if v == 0 {
            break;
        }
    }
    out.extend_from_slice(&buf[at..]);
}

pub fn text(id: Id) -> Vec<u8> {
    let mut v = Vec::with_capacity(12);
    text_into(id, &mut v);
    v
}

/// Byte-wise order of the texts of two keys made by `key_kept`.
#[inline]
pub fn cmp(a: Id, b: Id) -> std::cmp::Ordering {
    if a == b {
        return std::cmp::Ordering::Equal;
    }
    text(a).cmp(&text(b))
}

/// A well-mixed hash of a key, for partitioning.
#[inline(always)]
pub fn mix(id: Id) -> u64 {
    let x = id.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    x ^ (x >> 29)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_and_order() {
        let ids: Vec<&[u8]> = vec![
            b"", b"U1", b"a", b"a0", b"a00", b"ab", b"u1", b"u1 ", b"\tx", b"u10", b"u9", b"uzzzzzzzzzzz",
            b"uzzzzzzzzzzzz", b"c_12", b"k0", b"u01", b"e1a2b3c4d5", b"P123", b"p123",
        ];
        for a in &ids {
            assert_eq!(text(key_kept(a)), a.to_vec(), "{:?}", String::from_utf8_lossy(a));
            for b in &ids {
                assert_eq!(cmp(key_kept(a), key_kept(b)), a.cmp(b));
                assert_eq!(key(a) == key(b), a == b);
            }
        }
    }
}
