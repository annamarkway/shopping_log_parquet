//! A flat open-addressing map from `Id` to `Id`, built once and then read by
//! many threads, with batched lookups that prefetch ahead to hide DRAM latency.

use crate::id::{self, Id};

const EMPTY: Id = u64::MAX;

pub struct IdMap {
    slots: Vec<(Id, Id)>,
    mask: usize,
    /// One bit per hash of the keys (a 1-hash Bloom filter, small enough for
    /// the cache): a clear bit means "not in the map" without touching `slots`.
    filter: Vec<u64>,
    fmask: usize,
}

const FILTER_BITS: usize = 1 << 23;
const USE_FILTER: bool = option_env!("SS_NOFILTER").is_none();

#[inline(always)]
pub fn prefetch<T>(p: *const T) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        core::arch::x86_64::_mm_prefetch(p as *const i8, core::arch::x86_64::_MM_HINT_T0);
    }
    #[cfg(target_arch = "aarch64")]
    unsafe {
        core::arch::asm!("prfm pldl1keep, [{0}]", in(reg) p, options(nostack, readonly, preserves_flags));
    }
}

impl IdMap {
    pub fn with_capacity(n: usize) -> Self {
        let size = (n * 2).next_power_of_two().max(16);
        let fbits = (n * 8).next_power_of_two().clamp(1 << 12, FILTER_BITS);
        IdMap { slots: vec![(EMPTY, 0); size], mask: size - 1, filter: vec![0; fbits / 64], fmask: fbits - 1 }
    }

    #[inline(always)]
    fn fbit(&self, k: Id) -> usize {
        (id::mix(k) >> 36) as usize & self.fmask
    }

    #[inline(always)]
    fn may_have(&self, k: Id) -> bool {
        if !USE_FILTER {
            return true;
        }
        let b = self.fbit(k);
        unsafe { *self.filter.get_unchecked(b >> 6) >> (b & 63) & 1 != 0 }
    }

    #[inline(always)]
    fn home(&self, k: Id) -> usize {
        (id::mix(k) >> 7) as usize & self.mask
    }

    pub fn insert(&mut self, k: Id, v: Id) {
        let b = self.fbit(k);
        self.filter[b >> 6] |= 1 << (b & 63);
        let mut i = self.home(k);
        loop {
            let s = &mut self.slots[i];
            if s.0 == EMPTY || s.0 == k {
                *s = (k, v);
                return;
            }
            i = (i + 1) & self.mask;
        }
    }

    #[inline(always)]
    pub fn get(&self, k: Id) -> Option<Id> {
        if !self.may_have(k) {
            return None;
        }
        let mut i = self.home(k);
        loop {
            let s = unsafe { self.slots.get_unchecked(i) };
            if s.0 == k {
                return Some(s.1);
            }
            if s.0 == EMPTY {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }

    /// Map every `*key(item)` through the map in place (unmapped keys stay),
    /// prefetching ahead in two stages: the filter word far ahead, then the
    /// slot (only if the filter lets the key through) nearer.
    pub fn resolve_all<T>(&self, items: &mut [T], key: impl Fn(&mut T) -> &mut Id) {
        const FAR: usize = 32;
        const NEAR: usize = 12;
        let n = items.len();
        for i in 0..n.min(FAR) {
            let k = *key(&mut items[i]);
            if USE_FILTER {
                prefetch(&self.filter[self.fbit(k) >> 6]);
            }
        }
        for i in 0..n.min(NEAR) {
            let k = *key(&mut items[i]);
            if self.may_have(k) {
                prefetch(&self.slots[self.home(k)]);
            }
        }
        for i in 0..n {
            if USE_FILTER && i + FAR < n {
                let k = *key(&mut items[i + FAR]);
                prefetch(&self.filter[self.fbit(k) >> 6]);
            }
            if i + NEAR < n {
                let k = *key(&mut items[i + NEAR]);
                if self.may_have(k) {
                    prefetch(&self.slots[self.home(k)]);
                }
            }
            let k = key(&mut items[i]);
            if let Some(v) = self.get(*k) {
                *k = v;
            }
        }
    }

    pub fn len_hint(&self) -> usize {
        self.slots.len() / 2
    }

    #[cfg(test)]
    pub fn test_roundtrip() {
        let mut m = IdMap::with_capacity(1000);
        for k in 0..1000u64 {
            m.insert(k * 7919, k);
        }
        for k in 0..20000u64 {
            let want = (k % 7919 == 0 && k / 7919 < 1000).then_some(k / 7919);
            assert_eq!(m.get(k), want);
        }
        let mut v: Vec<u64> = (0..20000).collect();
        m.resolve_all(&mut v, |x| x);
        for (k, &x) in v.iter().enumerate() {
            let k = k as u64;
            assert_eq!(x, if k % 7919 == 0 && k / 7919 < 1000 { k / 7919 } else { k });
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn roundtrip() {
        super::IdMap::test_roundtrip();
    }
}
