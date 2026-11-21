//! A flat open-addressing map from `Id` to `Id`, built once and then read by
//! many threads, with batched lookups that prefetch ahead to hide DRAM latency.

use crate::id::{self, Id};

const EMPTY: Id = u64::MAX;

pub struct IdMap {
    slots: Vec<(Id, Id)>,
    mask: usize,
}

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
        IdMap { slots: vec![(EMPTY, 0); size], mask: size - 1 }
    }

    #[inline(always)]
    fn home(&self, k: Id) -> usize {
        (id::mix(k) >> 7) as usize & self.mask
    }

    pub fn insert(&mut self, k: Id, v: Id) {
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
    /// prefetching a few items ahead.
    pub fn resolve_all<T>(&self, items: &mut [T], key: impl Fn(&mut T) -> &mut Id) {
        const AHEAD: usize = 16;
        let n = items.len();
        for i in 0..n.min(AHEAD) {
            let k = *key(&mut items[i]);
            prefetch(&self.slots[self.home(k)]);
        }
        for i in 0..n {
            if i + AHEAD < n {
                let k = *key(&mut items[i + AHEAD]);
                prefetch(&self.slots[self.home(k)]);
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
}
