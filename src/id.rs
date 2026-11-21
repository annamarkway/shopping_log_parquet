//! IDs as `u128` keys. A string of at most 15 bytes is packed exactly:
//! bytes left-aligned big-endian, length in the low byte, so comparing keys
//! compares the strings byte-wise. A longer string becomes a 120-bit hash
//! tagged 0xFF in the low byte; its text is kept in a side table when needed.

use std::sync::{Mutex, OnceLock};

use rustc_hash::FxHashMap;

pub type Id = u128;
const LONG: u8 = 0xFF;

static LONG_TEXT: OnceLock<Mutex<FxHashMap<Id, Vec<u8>>>> = OnceLock::new();

fn table() -> &'static Mutex<FxHashMap<Id, Vec<u8>>> {
    LONG_TEXT.get_or_init(|| Mutex::new(FxHashMap::default()))
}

#[inline(always)]
pub fn is_long(id: Id) -> bool {
    id as u8 == LONG
}

#[inline(always)]
fn pack_short(s: &[u8]) -> Id {
    let mut buf = [0u8; 16];
    buf[..s.len()].copy_from_slice(s);
    buf[15] = s.len() as u8;
    u128::from_be_bytes(buf)
}

#[inline(always)]
fn hash_long(s: &[u8]) -> Id {
    let h = xxhash_rust::xxh3::xxh3_128(s);
    (h & !0xFF) | LONG as u128
}

/// Key for an ID whose text is never needed again (equality only).
#[inline(always)]
pub fn key(s: &[u8]) -> Id {
    if s.len() <= 15 { pack_short(s) } else { hash_long(s) }
}

/// Key for an ID whose text or byte order may be needed later.
#[inline(always)]
pub fn key_kept(s: &[u8]) -> Id {
    if s.len() <= 15 {
        pack_short(s)
    } else {
        let id = hash_long(s);
        table().lock().unwrap().entry(id).or_insert_with(|| s.to_vec());
        id
    }
}

/// The text of an ID made by `key_kept`.
pub fn text(id: Id) -> Vec<u8> {
    if is_long(id) {
        table().lock().unwrap().get(&id).cloned().expect("long id text")
    } else {
        let buf = id.to_be_bytes();
        buf[..buf[15] as usize].to_vec()
    }
}

/// Byte-wise order of two IDs made by `key_kept`.
#[inline(always)]
pub fn cmp(a: Id, b: Id) -> std::cmp::Ordering {
    if !is_long(a) && !is_long(b) {
        a.cmp(&b)
    } else {
        text(a).cmp(&text(b))
    }
}

/// A well-mixed 64-bit hash of a key, for partitioning.
#[inline(always)]
pub fn mix(id: Id) -> u64 {
    let x = (id as u64) ^ ((id >> 64) as u64).rotate_left(29);
    let x = x.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    x ^ (x >> 32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn order() {
        let ids: Vec<&[u8]> = vec![b"", b"U1", b"a", b"ab", b"ab\0", b"abc", b"u1", b"u1 ", b"\tx"];
        for a in &ids {
            for b in &ids {
                assert_eq!(cmp(key_kept(a), key_kept(b)), a.cmp(b));
            }
            assert_eq!(text(key_kept(a)), a.to_vec());
        }
        let long = b"abcdefghijklmnopqrstuvwxyz".as_slice();
        let k = key_kept(long);
        assert_eq!(text(k), long.to_vec());
        assert_eq!(cmp(k, key_kept(b"abd")), std::cmp::Ordering::Less);
    }
}
