//! IDs as `u64` keys, exact for every short ASCII ID.
//!
//! - Up to 8 ASCII bytes: `len << 56 | packed`, where `packed` is the 7-bit
//!   characters squeezed into 56 bits (bit 62 clear).
//! - 9 ASCII bytes starting with `a`-`z` (the generator's `u…`, `e…`, `k…`):
//!   `1 << 62 | letter << 56 | packed(rest)`.
//! - Anything else: a 63-bit hash with the top bit set; its text is kept in a
//!   side table when it may be needed.
//! Keys are equal exactly when the strings are (up to 63-bit hash collisions
//! among the rare other IDs). Keys do not sort like the strings: use [`cmp`].

use std::cell::UnsafeCell;
use std::sync::{Arc, Mutex, OnceLock};

use rustc_hash::FxHashMap;

pub type Id = u64;
const HASHED: u64 = 1 << 63;

/// Hashed key -> its text, as address | length << 48 into a never-freed arena.
type Texts = FxHashMap<Id, u64>;

/// One thread's texts. Only its own thread writes them, and only while
/// parsing; other threads read them only after every `key_kept` call is done
/// (see [`hashed_text`]), so no lock is needed.
struct Local {
    map: UnsafeCell<Texts>,
    arena: UnsafeCell<(*mut u8, usize)>, // free space in the current arena chunk
}
unsafe impl Sync for Local {}
unsafe impl Send for Local {}

/// Every thread's table, for lookups of other threads' texts.
static TABLES: Mutex<Vec<Arc<Local>>> = Mutex::new(Vec::new());
static FROZEN: OnceLock<Vec<Arc<Local>>> = OnceLock::new();

thread_local! {
    static LOCAL: Arc<Local> = {
        let t = Arc::new(Local { map: UnsafeCell::new(Texts::default()), arena: UnsafeCell::new((std::ptr::null_mut(), 0)) });
        TABLES.lock().unwrap().push(t.clone());
        t
    };
}

#[inline(always)]
fn unpack_text(v: u64) -> &'static [u8] {
    unsafe { std::slice::from_raw_parts((v & ((1 << 48) - 1)) as *const u8, (v >> 48) as usize) }
}

/// Keep the text of a hashed key in this thread's table (copied once, on
/// the thread's first sight of it).
#[inline(always)]
fn keep_text(k: Id, s: &[u8]) {
    LOCAL.with(|t| {
        let map = unsafe { &mut *t.map.get() };
        if !map.contains_key(&k) {
            let v = unsafe { copy_to_arena(&mut *t.arena.get(), s) };
            map.insert(k, v);
        }
    });
}

#[cold]
#[inline(never)]
fn copy_to_arena(arena: &mut (*mut u8, usize), s: &[u8]) -> u64 {
    let n = s.len();
    assert!(n < 1 << 16, "ID too long");
    if arena.1 < n {
        let size = (1 << 20).max(n);
        arena.0 = Box::leak(vec![0u8; size].into_boxed_slice()).as_mut_ptr();
        arena.1 = size;
    }
    let p = arena.0;
    unsafe {
        std::ptr::copy_nonoverlapping(s.as_ptr(), p, n);
        arena.0 = p.add(n);
    }
    arena.1 -= n;
    assert!((p as u64) < 1 << 48);
    p as u64 | (n as u64) << 48
}

/// The text of a hashed key: this thread's own table first (so a thread can
/// read back what it kept while others still parse), then every table. The
/// second only happens once every `key_kept` call is done.
fn hashed_text(k: Id, f: impl FnOnce(&[u8])) {
    let own = LOCAL.with(|t| unsafe { (*t.map.get()).get(&k).copied() });
    let v = own.unwrap_or_else(|| {
        let tables = FROZEN.get_or_init(|| TABLES.lock().unwrap().clone());
        tables.iter().find_map(|t| unsafe { (*t.map.get()).get(&k).copied() }).expect("id text")
    });
    f(unpack_text(v))
}

const TAG9: u64 = 1 << 62;
const HIGH: u64 = 0x8080_8080_8080_8080;

/// Eight 7-bit characters (first in the lowest byte) into 56 bits.
#[inline(always)]
fn pack56(x: u64) -> u64 {
    let y = (x & 0x007F_007F_007F_007F) | (x & 0x7F00_7F00_7F00_7F00) >> 1;
    let y = (y & 0x0000_3FFF_0000_3FFF) | (y & 0x3FFF_0000_3FFF_0000) >> 2;
    (y & 0x0000_0000_0FFF_FFFF) | (y & 0x0FFF_FFFF_0000_0000) >> 4
}

/// Inverse of [`pack56`].
#[inline(always)]
fn unpack56(y: u64) -> u64 {
    let y = (y & 0x0000_0000_0FFF_FFFF) | (y & 0x00FF_FFFF_F000_0000) << 4;
    let y = (y & 0x0000_3FFF_0000_3FFF) | (y & 0x0FFF_C000_0FFF_C000) << 2;
    (y & 0x007F_007F_007F_007F) | (y & 0x3F80_3F80_3F80_3F80) << 1
}

#[inline(always)]
fn canonical(s: &[u8]) -> Option<u64> {
    let n = s.len();
    if n <= 8 {
        let mut b = [0u8; 8];
        b[..n].copy_from_slice(s);
        let x = u64::from_le_bytes(b);
        if x & HIGH != 0 {
            return None;
        }
        Some((n as u64) << 56 | pack56(x))
    } else if n == 9 && s[0].is_ascii_lowercase() {
        let x = u64::from_le_bytes(s[1..9].try_into().unwrap());
        if x & HIGH != 0 {
            return None;
        }
        Some(TAG9 | ((s[0] - b'a') as u64) << 56 | pack56(x))
    } else {
        None
    }
}

#[inline(always)]
fn hashed(s: &[u8]) -> Id {
    xxhash_rust::xxh3::xxh3_64(s) | HASHED
}

/// True for a key made by hashing (text not in the key).
#[inline(always)]
pub fn is_hashed(id: Id) -> bool {
    id & HASHED != 0
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
            keep_text(k, s);
            k
        }
    }
}

/// [`key`] (or [`key_kept`] if `KEPT`) reading whole words.
///
/// # Safety
/// 8 bytes must be readable from `s.as_ptr()` and from `s.as_ptr() + 1`.
#[inline(always)]
pub unsafe fn key_wide<const KEPT: bool>(s: &[u8]) -> Id {
    let n = s.len();
    let p = s.as_ptr();
    if n <= 8 {
        let x = unsafe { (p as *const u64).read_unaligned() };
        let x = if n == 8 { x } else { x & ((1u64 << (n * 8)) - 1) };
        if x & HIGH == 0 {
            return (n as u64) << 56 | pack56(x);
        }
    } else if n == 9 {
        let c = unsafe { *p };
        let x = unsafe { (p.add(1) as *const u64).read_unaligned() };
        if c.is_ascii_lowercase() && x & HIGH == 0 {
            return TAG9 | ((c - b'a') as u64) << 56 | pack56(x);
        }
    }
    if KEPT { key_kept(s) } else { key(s) }
}

/// Text of a non-hashed key into `buf`, returning its length.
#[inline(always)]
fn short_text(id: Id, buf: &mut [u8; 9]) -> usize {
    let raw = unpack56(id & ((1 << 56) - 1)).to_le_bytes();
    if id & TAG9 != 0 {
        buf[0] = b'a' + (id >> 56 & 0x1F) as u8;
        buf[1..9].copy_from_slice(&raw);
        9
    } else {
        let n = (id >> 56 & 0xF) as usize;
        buf[..8].copy_from_slice(&raw);
        n
    }
}

/// Write the text of a key made by `key_kept` into `out`.
pub fn text_into(id: Id, out: &mut Vec<u8>) {
    if id & HASHED != 0 {
        hashed_text(id, |t| out.extend_from_slice(t));
        return;
    }
    let mut buf = [0u8; 9];
    let n = short_text(id, &mut buf);
    out.extend_from_slice(&buf[..n]);
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
    if a & HASHED == 0 && b & HASHED == 0 {
        let (mut x, mut y) = ([0u8; 9], [0u8; 9]);
        let (nx, ny) = (short_text(a, &mut x), short_text(b, &mut y));
        return x[..nx].cmp(&y[..ny]);
    }
    // The first text goes to a reused buffer: a lookup holds its table's
    // lock while it runs `f`, so lookups must not nest.
    thread_local! {
        static BUF: std::cell::RefCell<Vec<u8>> = const { std::cell::RefCell::new(Vec::new()) };
    }
    BUF.with(|buf| {
        let mut buf = buf.borrow_mut();
        buf.clear();
        with_text(a, |ta| buf.extend_from_slice(ta));
        let mut r = std::cmp::Ordering::Equal;
        with_text(b, |tb| r = buf.as_slice().cmp(tb));
        r
    })
}

/// Call `f` with the text of a key made by `key_kept`, without copying it.
fn with_text(id: Id, f: impl FnOnce(&[u8])) {
    if id & HASHED != 0 {
        hashed_text(id, f);
    } else {
        let mut buf = [0u8; 9];
        let n = short_text(id, &mut buf);
        f(&buf[..n]);
    }
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
            b"uabcdefgh", b"Uabcdefgh", b"\x7f\x7f\x7f\x7f\x7f\x7f\x7f\x7f", b"\x7f\x7f\x7f\x7f\x7f\x7f\x7f",
            b"zzzzzzzzz", b"abcdefgh", b"bcdefgh", b"e\x00", b"e", b"u\x7f\x00\x01\x02\x03\x04\x05\x06",
            "u\u{e9}".as_bytes(), b"u1\r", b" u1",
        ];
        for a in &ids {
            assert_eq!(text(key_kept(a)), a.to_vec(), "{:?}", String::from_utf8_lossy(a));
            let mut padded = a.to_vec();
            padded.extend_from_slice(&[0xAA; 16]);
            let wide = unsafe { key_wide::<true>(&padded[..a.len()]) };
            assert_eq!(wide, key_kept(a));
            assert_eq!(unsafe { key_wide::<false>(&padded[..a.len()]) }, key(a));
            for b in &ids {
                assert_eq!(cmp(key_kept(a), key_kept(b)), a.cmp(b));
                assert_eq!(key(a) == key(b), a == b);
            }
        }
    }
}
