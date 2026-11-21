//! The CSV dialect of rules.md: `\n` ends a line, `,` always separates fields,
//! quotes are allowed only around a whole field, and an empty value is null.

/// Strip the quotes of one raw field, or `None` if a quote is misplaced.
#[inline(always)]
fn unquote(raw: &[u8]) -> Option<&[u8]> {
    match memchr::memchr(b'"', raw) {
        None => Some(raw),
        Some(0) => {
            let n = raw.len();
            if n < 2 || raw[n - 1] != b'"' {
                return None;
            }
            let inner = &raw[1..n - 1];
            if memchr::memchr(b'"', inner).is_some() {
                None
            } else {
                Some(inner)
            }
        }
        Some(_) => None,
    }
}

/// Split one line (no trailing `\n`) into exactly `N` unquoted fields.
/// `None` means the line is malformed and must be dropped.
#[inline]
pub fn split<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    if line.is_empty() {
        return None;
    }
    let mut out: [&[u8]; N] = [&[]; N];
    let mut idx = 0;
    let mut start = 0;
    let mut commas = memchr::memchr_iter(b',', line);
    loop {
        let end = commas.next();
        if idx >= N {
            return None;
        }
        let stop = end.unwrap_or(line.len());
        out[idx] = unquote(&line[start..stop])?;
        idx += 1;
        match end {
            Some(e) => start = e + 1,
            None => break,
        }
    }
    if idx != N {
        return None;
    }
    Some(out)
}

/// Call `f` on every line of `data`, skipping the first line (the header)
/// if `skip_header`. Empty lines are malformed in every table, so they are
/// skipped here.
#[inline]
pub fn for_each_line<'a>(data: &'a [u8], skip_header: bool, mut f: impl FnMut(&'a [u8])) {
    let mut pos = 0;
    if skip_header {
        match memchr::memchr(b'\n', data) {
            Some(e) => pos = e + 1,
            None => return,
        }
    }
    while pos < data.len() {
        let end = memchr::memchr(b'\n', &data[pos..]).map_or(data.len(), |e| pos + e);
        if end > pos {
            f(&data[pos..end]);
        }
        pos = end + 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dialect() {
        assert_eq!(split::<2>(b"a,b"), Some([&b"a"[..], b"b"]));
        assert_eq!(split::<2>(b"\"a\",\"\""), Some([&b"a"[..], b""]));
        assert_eq!(split::<2>(b"a,b,c"), None);
        assert_eq!(split::<2>(b"a"), None);
        assert_eq!(split::<2>(b""), None);
        assert_eq!(split::<2>(b"a,b\""), None);
        assert_eq!(split::<2>(b"a,\""), None);
        assert_eq!(split::<2>(b"a\"x\",b"), None);
        assert_eq!(split::<2>(b"\"a\"x,b"), None);
        assert_eq!(split::<3>(b"a,b,"), Some([&b"a"[..], b"b", b""]));
        assert_eq!(split::<2>(b" a ,\" b\""), Some([&b" a "[..], b" b"]));
    }
}

// ---------------------------------------------------------------- SIMD scan

/// Bit masks of `\n`, `,` and `"` for 64 bytes.
#[derive(Clone, Copy)]
struct Masks {
    nl: u64,
    comma: u64,
    quote: u64,
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline(always)]
fn classify(block: &[u8; 64]) -> Masks {
    use core::arch::x86_64::*;
    unsafe {
        let a = _mm256_loadu_si256(block.as_ptr() as *const __m256i);
        let b = _mm256_loadu_si256(block.as_ptr().add(32) as *const __m256i);
        let m = |c: u8| {
            let p = _mm256_set1_epi8(c as i8);
            let lo = _mm256_movemask_epi8(_mm256_cmpeq_epi8(a, p)) as u32 as u64;
            let hi = _mm256_movemask_epi8(_mm256_cmpeq_epi8(b, p)) as u32 as u64;
            lo | hi << 32
        };
        Masks { nl: m(b'\n'), comma: m(b','), quote: m(b'"') }
    }
}

#[cfg(all(target_arch = "x86_64", not(target_feature = "avx2")))]
#[inline(always)]
fn classify(block: &[u8; 64]) -> Masks {
    use core::arch::x86_64::*;
    unsafe {
        let v: [__m128i; 4] =
            core::array::from_fn(|i| _mm_loadu_si128(block.as_ptr().add(16 * i) as *const __m128i));
        let m = |c: u8| {
            let p = _mm_set1_epi8(c as i8);
            let mut r = 0u64;
            for (i, x) in v.iter().enumerate() {
                r |= (_mm_movemask_epi8(_mm_cmpeq_epi8(*x, p)) as u32 as u64) << (16 * i);
            }
            r
        };
        Masks { nl: m(b'\n'), comma: m(b','), quote: m(b'"') }
    }
}

#[cfg(target_arch = "aarch64")]
#[inline(always)]
fn classify(block: &[u8; 64]) -> Masks {
    use core::arch::aarch64::*;
    unsafe {
        let v0 = vld1q_u8(block.as_ptr());
        let v1 = vld1q_u8(block.as_ptr().add(16));
        let v2 = vld1q_u8(block.as_ptr().add(32));
        let v3 = vld1q_u8(block.as_ptr().add(48));
        let bits: [u8; 16] = [1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128];
        let bits = vld1q_u8(bits.as_ptr());
        let m = |c: u8| {
            let p = vdupq_n_u8(c);
            let m0 = vandq_u8(vceqq_u8(v0, p), bits);
            let m1 = vandq_u8(vceqq_u8(v1, p), bits);
            let m2 = vandq_u8(vceqq_u8(v2, p), bits);
            let m3 = vandq_u8(vceqq_u8(v3, p), bits);
            let s = vpaddq_u8(vpaddq_u8(m0, m1), vpaddq_u8(m2, m3));
            let s = vpaddq_u8(s, s);
            vgetq_lane_u64(vreinterpretq_u64_u8(s), 0)
        };
        Masks { nl: m(b'\n'), comma: m(b','), quote: m(b'"') }
    }
}

#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
#[inline(always)]
fn classify(block: &[u8; 64]) -> Masks {
    let (mut nl, mut comma, mut quote) = (0u64, 0u64, 0u64);
    for (i, &c) in block.iter().enumerate() {
        nl |= ((c == b'\n') as u64) << i;
        comma |= ((c == b',') as u64) << i;
        quote |= ((c == b'"') as u64) << i;
    }
    Masks { nl, comma, quote }
}

/// Call `f` with the `N` unquoted fields of every well-formed line of `data`
/// (after the header line if `skip_header`). Malformed and empty lines are
/// skipped. Same dialect as [`split`], found with 64-byte SIMD masks.
#[inline]
pub fn scan<'a, const N: usize>(data: &'a [u8], skip_header: bool, mut f: impl FnMut([&'a [u8]; N])) {
    let mut start = 0;
    if skip_header {
        match memchr::memchr(b'\n', data) {
            Some(e) => start = e + 1,
            None => return,
        }
    }
    let data = &data[start..];
    let len = data.len();

    let mut fields: [&[u8]; N] = [&[]; N];
    let mut idx = 0usize; // fields seen in this line
    let mut bad = false;
    let mut line_start = 0usize;
    let mut field_start = 0usize;
    let mut qacc = 0u32; // quotes of the current field in earlier blocks

    // Finish the field [field_start, end) with `q` quotes in it.
    macro_rules! field {
        ($end:expr, $q:expr) => {{
            let end = $end;
            if !bad {
                if idx >= N {
                    bad = true;
                } else {
                    let q = $q;
                    let s = field_start;
                    if q == 0 {
                        fields[idx] = unsafe { data.get_unchecked(s..end) };
                    } else if q == 2 && end - s >= 2 && data[s] == b'"' && data[end - 1] == b'"' {
                        fields[idx] = unsafe { data.get_unchecked(s + 1..end - 1) };
                    } else {
                        bad = true;
                    }
                    idx += 1;
                }
            }
        }};
    }

    let mut base = 0usize;
    let mut tail = [0u8; 64];
    while base < len {
        let block: &[u8; 64] = if base + 64 <= len {
            unsafe { &*(data.as_ptr().add(base) as *const [u8; 64]) }
        } else {
            tail[..len - base].copy_from_slice(&data[base..]);
            &tail
        };
        let m = classify(block);
        let mut seps = m.nl | m.comma;
        let mut prev = 0u32; // bit index where the current field's quotes start counting
        while seps != 0 {
            let bit = seps.trailing_zeros();
            let below = if bit == 0 { 0 } else { u64::MAX >> (64 - bit) };
            let from = if prev >= 64 { 0 } else { u64::MAX << prev };
            let q = qacc + (m.quote & below & from).count_ones();
            let abs = base + bit as usize;
            field!(abs, q);
            qacc = 0;
            if m.nl >> bit & 1 == 1 {
                if abs > line_start && !bad && idx == N {
                    f(fields);
                }
                idx = 0;
                bad = false;
                line_start = abs + 1;
            }
            field_start = abs + 1;
            prev = bit + 1;
            seps &= seps - 1;
        }
        let from = if prev >= 64 { 0 } else { u64::MAX << prev };
        qacc += (m.quote & from).count_ones();
        base += 64;
    }
    // A last line without its `\n`.
    if line_start < len {
        field!(len, qacc);
        if !bad && idx == N {
            f(fields);
        }
    }
}

#[cfg(test)]
mod scan_tests {
    use super::*;

    fn by_split<const N: usize>(data: &[u8]) -> Vec<[Vec<u8>; N]> {
        let mut v = Vec::new();
        for_each_line(data, false, |l| {
            if let Some(f) = split::<N>(l) {
                v.push(f.map(|x| x.to_vec()));
            }
        });
        v
    }
    fn by_scan<const N: usize>(data: &[u8]) -> Vec<[Vec<u8>; N]> {
        let mut v = Vec::new();
        scan::<N>(data, false, |f| v.push(f.map(|x| x.to_vec())));
        v
    }

    #[test]
    fn scan_matches_split() {
        // Random lines over a small alphabet, many lengths, so fields and
        // quotes straddle the 64-byte blocks in every way.
        let alphabet = b"ab,\"\n\r x";
        let mut seed = 0x1234_5678_9abc_def0u64;
        for round in 0..3000 {
            let n = (round % 300) + 1;
            let mut data = Vec::with_capacity(n);
            for _ in 0..n {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                data.push(alphabet[(seed % alphabet.len() as u64) as usize]);
            }
            assert_eq!(by_scan::<3>(&data), by_split::<3>(&data), "{:?}", String::from_utf8_lossy(&data));
            assert_eq!(by_scan::<2>(&data), by_split::<2>(&data));
        }
        let mut long = Vec::new();
        for i in 0..200 {
            long.extend_from_slice(format!("\"id{i}\",x{i}{}\n", "y".repeat(i % 70)).as_bytes());
        }
        assert_eq!(by_scan::<2>(&long), by_split::<2>(&long));
        assert_eq!(by_scan::<2>(&long).len(), 200);
    }
}
