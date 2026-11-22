//! Timestamps: F1 `YYYY-MM-DDTHH:MM:SSZ`, F2 `YYYY-MM-DDTHH:MM:SS+HH:MM`,
//! F3 epoch milliseconds (ASCII digits only). Anything else is bad.

pub const DAY: i64 = 86_400_000;

pub const fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let yoe = year - era * 400;
    let doy = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468
}

/// Days since the epoch of the first of each month, years TABLE_Y0 .. +TABLE_YEARS,
/// plus the first of the month after the last (so every month's length is a
/// difference of neighbours).
const TABLE_Y0: i64 = 1900;
const TABLE_YEARS: usize = 400;
static MONTH_START: [i32; TABLE_YEARS * 12 + 1] = {
    let mut t = [0i32; TABLE_YEARS * 12 + 1];
    let mut i = 0;
    while i <= TABLE_YEARS * 12 {
        t[i] = days_from_civil(TABLE_Y0 + (i / 12) as i64, (i % 12) as i64 + 1, 1) as i32;
        i += 1;
    }
    t
};

#[inline(always)]
fn days(year: i64, month: i64, day: i64) -> i64 {
    let y = year - TABLE_Y0;
    if (0..TABLE_YEARS as i64).contains(&y) {
        MONTH_START[(y * 12 + month - 1) as usize] as i64 + day - 1
    } else {
        days_from_civil(year, month, day)
    }
}

/// Eight ASCII digits (first digit in the lowest byte) to their value.
#[inline(always)]
fn swar8(x: u64) -> Option<u64> {
    if x & 0xF0F0_F0F0_F0F0_F0F0 != 0x3030_3030_3030_3030 {
        return None;
    }
    let d = x - 0x3030_3030_3030_3030;
    if d.wrapping_add(0x0606_0606_0606_0606) & 0xF0F0_F0F0_F0F0_F0F0 != 0 {
        return None;
    }
    let d = d * 10 + (d >> 8);
    let d = ((d & 0x0000_00FF_0000_00FF).wrapping_mul(100 + (1_000_000 << 32))
        + ((d >> 16) & 0x0000_00FF_0000_00FF).wrapping_mul(1 + (10_000 << 32)))
        >> 32;
    Some(d)
}

/// All-digit string to a value; `None` on a non-digit or i64 overflow.
#[inline(always)]
fn digits(s: &[u8]) -> Option<i64> {
    let mut v: u64 = 0;
    let mut rest = s;
    if s.len() <= 19 {
        while rest.len() >= 8 {
            v = v * 100_000_000 + swar8(u64::from_le_bytes(rest[..8].try_into().unwrap()))?;
            rest = &rest[8..];
        }
        for &c in rest {
            if !c.is_ascii_digit() {
                return None;
            }
            v = v * 10 + (c - b'0') as u64;
        }
    } else {
        for &c in s {
            if !c.is_ascii_digit() {
                return None;
            }
            v = v.checked_mul(10)?.checked_add((c - b'0') as u64)?;
        }
    }
    if v > i64::MAX as u64 { None } else { Some(v as i64) }
}

#[inline(always)]
fn leap(y: i64) -> bool {
    y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)
}

#[inline(always)]
fn days_in_month(y: i64, m: i64) -> i64 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => {
            if leap(y) {
                29
            } else {
                28
            }
        }
    }
}

#[inline(always)]
fn digit(b: u8) -> Option<i64> {
    if b.is_ascii_digit() {
        Some((b - b'0') as i64)
    } else {
        None
    }
}

#[inline(always)]
fn d2(s: &[u8], i: usize) -> Option<i64> {
    Some(digit(s[i])? * 10 + digit(s[i + 1])?)
}

/// Per-byte values of 8 ASCII digits, or `None` if any byte is not one.
#[inline(always)]
fn digit_bytes(x: u64) -> Option<u64> {
    if x & 0xF0F0_F0F0_F0F0_F0F0 != 0x3030_3030_3030_3030 {
        return None;
    }
    let d = x - 0x3030_3030_3030_3030;
    if d.wrapping_add(0x0606_0606_0606_0606) & 0xF0F0_F0F0_F0F0_F0F0 != 0 {
        return None;
    }
    Some(d)
}

const ZEROS: u64 = 0x3030_3030_3030_3030;

/// The `YYYY-MM-DDTHH:MM:SS` start of an F1 or F2 value (`s.len() >= 19`), in
/// ms. `None` means "not sure": the caller falls back to the full parser
/// (which also covers years outside the table).
#[inline(always)]
fn core19(s: &[u8]) -> Option<i64> {
    #[cfg(all(target_arch = "x86_64", target_feature = "ssse3", target_feature = "sse4.1"))]
    {
        core19_sse(s)
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "ssse3", target_feature = "sse4.1")))]
    {
        core19_swar(s)
    }
}

/// [`core19`] in one 16-byte register: gather the 14 digits, check them and
/// the 5 separators at once, then combine digit pairs with multiply-adds.
#[cfg(all(target_arch = "x86_64", target_feature = "ssse3", target_feature = "sse4.1"))]
#[inline(always)]
fn core19_sse(s: &[u8]) -> Option<i64> {
    use core::arch::x86_64::*;
    assert!(s.len() >= 19);
    unsafe {
        let a = _mm_loadu_si128(s.as_ptr() as *const __m128i); // bytes 0..16
        let b = _mm_loadu_si128(s.as_ptr().add(3) as *const __m128i); // bytes 3..19
        // Separators: a[4] '-', a[7] '-', a[10] 'T', a[13] ':', b[13] = s[16] ':'.
        let sep_a = _mm_setr_epi8(0, 0, 0, 0, b'-' as i8, 0, 0, b'-' as i8, 0, 0, b'T' as i8, 0, 0, b':' as i8, 0, 0);
        let sep_b = _mm_setr_epi8(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, b':' as i8, 0, 0);
        let ma = _mm_movemask_epi8(_mm_cmpeq_epi8(a, sep_a)) as u32;
        let mb = _mm_movemask_epi8(_mm_cmpeq_epi8(b, sep_b)) as u32;
        // Digits in order: Y Y Y Y M M D D H H M M (from a) S S (from b), then 2 zeros.
        let da = _mm_shuffle_epi8(a, _mm_setr_epi8(0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, -1, -1, -1, -1));
        let db = _mm_shuffle_epi8(b, _mm_setr_epi8(-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 14, 15, -1, -1));
        let zero_pad = _mm_setr_epi8(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, b'0' as i8, b'0' as i8);
        let d = _mm_sub_epi8(_mm_or_si128(_mm_or_si128(da, db), zero_pad), _mm_set1_epi8(b'0' as i8));
        // Every digit byte at most 9 (as unsigned: anything below '0' wrapped high).
        let digits_ok = _mm_movemask_epi8(_mm_cmpeq_epi8(_mm_min_epu8(d, _mm_set1_epi8(9)), d)) as u32;
        // Separator bytes a: 4, 7, 10, 13 (0x2490); b: 13 (0x2000).
        if (ma & 0x2490 != 0x2490) | (mb & 0x2000 == 0) | (digits_ok != 0xFFFF) {
            return None;
        }
        // Pairs: YY, YY, MM, DD, HH, MI, SS, 0 as u16.
        let p = _mm_maddubs_epi16(d, _mm_set1_epi16(0x010A));
        // Range checks per lane: month 1..12, day 1..31, hour..23, minute..59, second..59.
        let hi = _mm_setr_epi16(99, 99, 12, 31, 23, 59, 59, 0);
        let lo = _mm_setr_epi16(0, 0, 1, 1, 0, 0, 0, 0);
        let bad = _mm_or_si128(_mm_cmpgt_epi16(p, hi), _mm_cmplt_epi16(p, lo));
        if _mm_movemask_epi8(bad) != 0 {
            return None;
        }
        let year = (_mm_extract_epi16(p, 0) * 100 + _mm_extract_epi16(p, 1)) as usize;
        let month = _mm_extract_epi16(p, 2) as usize;
        let day = _mm_extract_epi16(p, 3) as i64;
        // hour * 3600 + minute * 60 in lane 2, second in lane 3 (as i32).
        let hms = _mm_madd_epi16(p, _mm_setr_epi16(0, 0, 0, 0, 3600, 60, 1, 0));
        let secs = (_mm_extract_epi32(hms, 2) + _mm_extract_epi32(hms, 3)) as i64;
        let y = year.wrapping_sub(TABLE_Y0 as usize);
        if y >= TABLE_YEARS {
            return None;
        }
        let idx = y * 12 + month - 1;
        let m0 = *MONTH_START.get_unchecked(idx) as i64;
        let dim = *MONTH_START.get_unchecked(idx + 1) as i64 - m0;
        if day > dim {
            return None;
        }
        Some(((m0 + day - 1) * 86_400 + secs) * 1_000)
    }
}

/// [`core19`] with word-at-a-time checks.
#[cfg_attr(all(target_arch = "x86_64", target_feature = "ssse3", target_feature = "sse4.1"), allow(dead_code))]
#[inline(always)]
fn core19_swar(s: &[u8]) -> Option<i64> {
    let w = |i: usize| u64::from_le_bytes(s[i..i + 8].try_into().unwrap());
    let (a, b, c) = (w(0), w(8), w(11)); // "YYYY-MM-", "DDT.....", "HH:MM:SS"
    const AM: u64 = 0xFF00_00FF_0000_0000; // bytes 4 and 7
    const AV: u64 = 0x2D00_002D_0000_0000; // '-'
    const CM: u64 = 0x0000_FF00_00FF_0000; // bytes 2 and 5
    const CV: u64 = 0x0000_3A00_003A_0000; // ':'
    if (a & AM != AV) | (c & CM != CV) | ((b >> 16) & 0xFF != b'T' as u64) {
        return None;
    }
    let da = digit_bytes(a & !AM | ZEROS & AM)?;
    let db = digit_bytes(b & 0xFFFF | ZEROS & !0xFFFF)?;
    let dc = digit_bytes(c & !CM | ZEROS & CM)?;
    // Byte i of t is 10 * digit i + digit i+1.
    let pair = |d: u64| d * 10 + (d >> 8);
    let (ta, tb, tc) = (pair(da), pair(db), pair(dc));
    let year = ((ta & 0xFF) * 100 + (ta >> 16 & 0xFF)) as usize;
    let month = (ta >> 40 & 0xFF) as usize;
    let day = (tb & 0xFF) as i64;
    let hour = (tc & 0xFF) as i64;
    let minute = (tc >> 24 & 0xFF) as i64;
    let second = (tc >> 48 & 0xFF) as i64;
    let idx = year.wrapping_sub(TABLE_Y0 as usize).wrapping_mul(12).wrapping_add(month.wrapping_sub(1));
    if (month.wrapping_sub(1) >= 12) | (year.wrapping_sub(TABLE_Y0 as usize) >= TABLE_YEARS) {
        return None;
    }
    let m0 = MONTH_START[idx] as i64;
    let dim = MONTH_START[idx + 1] as i64 - m0;
    if ((day - 1) as u64 >= dim as u64) | (hour > 23) | (minute > 59) | (second > 59) {
        return None;
    }
    Some(((m0 + day - 1) * 86_400 + hour * 3_600 + minute * 60 + second) * 1_000)
}

/// F1 `YYYY-MM-DDTHH:MM:SSZ`; `None` means "use the full parser".
#[inline(always)]
fn f1_fast(s: &[u8]) -> Option<i64> {
    if s.len() != 20 || s[19] != b'Z' {
        return None;
    }
    core19(s)
}

/// F2 `YYYY-MM-DDTHH:MM:SS+HH:MM`; `None` means "use the full parser".
#[inline(always)]
fn f2_fast(s: &[u8]) -> Option<i64> {
    if s.len() != 25 || s[22] != b':' {
        return None;
    }
    let sign = match s[19] {
        b'+' => 1,
        b'-' => -1,
        _ => return None,
    };
    let v = core19(s)?;
    let oh = d2(s, 20)?;
    let om = d2(s, 23)?;
    if oh > 23 || om > 59 {
        return None;
    }
    Some(v - sign * (oh * 60 + om) * 60_000)
}

/// Parse a timestamp to epoch ms. `None` for any malformed value, and for an
/// F3 number too large for i64 (it is outside every window anyway).
#[inline(always)]
pub fn parse(s: &[u8]) -> Option<i64> {
    match s.len() {
        20 => {
            if let Some(v) = f1_fast(s) {
                return Some(v);
            }
        }
        13 => {
            // F3 with 13 digits (every in-window epoch-ms value from 2001 to 2286):
            // digits 0..5 as "000" + 5 digits, digits 5..13 as 8.
            let x = u64::from_le_bytes(s[0..8].try_into().unwrap());
            let y = u64::from_le_bytes(s[5..13].try_into().unwrap());
            if let (Some(hi), Some(lo)) = (swar8(x << 24 | 0x30_3030), swar8(y)) {
                return Some((hi * 100_000_000 + lo) as i64);
            }
        }
        25 => {
            if let Some(v) = f2_fast(s) {
                return Some(v);
            }
        }
        _ => {}
    }
    parse_full(s)
}

#[inline(never)]
fn parse_full(s: &[u8]) -> Option<i64> {
    let n = s.len();
    if n == 0 {
        return None;
    }
    if n != 20 && n != 25 || s[0].is_ascii_digit() && s[4].is_ascii_digit() {
        // F3 candidate (an F1/F2 always has '-' at index 4).
        return digits(s);
    }
    if s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let year = d2(s, 0)? * 100 + d2(s, 2)?;
    let month = d2(s, 5)?;
    let day = d2(s, 8)?;
    let hour = d2(s, 11)?;
    let minute = d2(s, 14)?;
    let second = d2(s, 17)?;
    if !(1..=12).contains(&month)
        || day < 1
        || day > days_in_month(year, month)
        || hour > 23
        || minute > 59
        || second > 59
    {
        return None;
    }
    let offset_ms = if n == 20 {
        if s[19] != b'Z' {
            return None;
        }
        0
    } else {
        let sign = match s[19] {
            b'+' => 1,
            b'-' => -1,
            _ => return None,
        };
        if s[22] != b':' {
            return None;
        }
        let oh = d2(s, 20)?;
        let om = d2(s, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        sign * (oh * 60 + om) * 60_000
    };
    let days = days(year, month, day);
    Some(((days * 86_400 + hour * 3_600 + minute * 60 + second) * 1_000) - offset_ms)
}

#[cfg(test)]
mod tests {
    use super::parse;

    fn format_f1(ms: i64) -> String {
        let days = ms.div_euclid(86_400_000);
        let secs = ms.rem_euclid(86_400_000) / 1000;
        // civil_from_days
        let z = days + 719_468;
        let era = z.div_euclid(146_097);
        let doe = z - era * 146_097;
        let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let d = doy - (153 * mp + 2) / 5 + 1;
        let m = if mp < 10 { mp + 3 } else { mp - 9 };
        let y = yoe + era * 400 + (m <= 2) as i64;
        format!("{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z", secs / 3600, secs / 60 % 60, secs % 60)
    }

    #[test]
    fn forms() {
        let f1 = parse(b"2026-10-03T23:58:10Z").unwrap();
        assert_eq!(f1, 1_791_071_890_000);
        assert_eq!(parse(b"2026-10-03T19:58:10-04:00"), Some(f1));
        assert_eq!(parse(b"1791071890000"), Some(f1));
        assert_eq!(parse(b"0001791071890000"), Some(f1));
        assert_eq!(parse(b"2026-10-04T01:30:00+02:00"), parse(b"2026-10-03T23:30:00Z"));
        assert_eq!(parse(b"2026-10-03T23:58:10-00:00"), Some(f1));
        for bad in [
            &b""[..],
            b"2026-10-03T23:58:10.000Z",
            b"2026-10-03T23:58:10z",
            b"2026-10-03t23:58:10Z",
            b"2026-10-03 23:58:10Z",
            b"2026-10-03T23:58:10",
            b"1791071890000.0",
            b"1.79e12",
            b"-5",
            b"2026-13-03T23:58:10Z",
            b"2026-02-30T23:58:10Z",
            b"2027-02-29T23:58:10Z",
            b"2026-10-03T24:58:10Z",
            b"2026-10-03T23:58:60Z",
            b"2026-10-03T23:58:10+24:00",
            b"2026-10-03T23:58:10+05:60",
            b"2026-10-03T23:58:10+0200",
            b"999999999999999999999",
            "٢٥٠".as_bytes(),
        ] {
            assert_eq!(parse(bad), None, "{:?}", String::from_utf8_lossy(bad));
        }
        assert!(parse(b"2028-02-29T00:00:00Z").is_some());
        // Table and formula agree, digits agree with a plain parse.
        for y in [1600, 1899, 1900, 1970, 2000, 2028, 2100, 2299, 2300, 9999] {
            for m in 1..=12 {
                assert_eq!(super::days(y, m, 1), super::days_from_civil(y, m, 1));
            }
        }
        for v in [0u64, 7, 12345678, 99999999, 1791071890000, 9223372036854775807] {
            assert_eq!(super::digits(v.to_string().as_bytes()), Some(v as i64));
        }
        assert_eq!(super::digits(b"9223372036854775808"), None);
        for v in [1000000000000u64, 1791071890000, 9999999999999, 1234567890123] {
            assert_eq!(parse(v.to_string().as_bytes()), Some(v as i64));
        }
        for bad in [&b"179107189000x"[..], b"17910718900:0", b"1791071/90000", b"x791071890000", b"1791071890 00"] {
            assert_eq!(parse(bad), None, "{:?}", String::from_utf8_lossy(bad));
        }
        let mut seed = 99u64;
        let alphabet = b"0123456789-T:Zz ";
        for i in 0..200_000 {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            // Start from a valid F1 and corrupt one or two bytes.
            let base_ms = 1_600_000_000_000i64 + (seed % 3_000_000_000_000) as i64 / 1000 * 1000;
            let mut t = format_f1(base_ms).into_bytes();
            if i % 3 != 0 {
                let at = (seed >> 20) as usize % 20;
                t[at] = alphabet[(seed >> 40) as usize % alphabet.len()];
            }
            if i % 7 == 0 {
                let at = (seed >> 30) as usize % 20;
                t[at] = alphabet[(seed >> 50) as usize % alphabet.len()];
            }
            assert_eq!(parse(&t), super::parse_full(&t), "{}", String::from_utf8_lossy(&t));
            assert_eq!(super::core19(&t), super::core19_swar(&t), "{}", String::from_utf8_lossy(&t));
            // The same with an offset, corrupted too.
            let mut t2 = t.clone();
            t2.truncate(19);
            t2.extend_from_slice(if i % 2 == 0 { b"+05:30" } else { b"-11:00" });
            if i % 5 == 0 {
                let at = 19 + (seed >> 33) as usize % 6;
                t2[at] = alphabet[(seed >> 44) as usize % alphabet.len()];
            }
            assert_eq!(parse(&t2), super::parse_full(&t2), "{}", String::from_utf8_lossy(&t2));
            if super::f1_fast(&t).is_some() {
                assert_eq!(super::f1_fast(&t), super::parse_full(&t));
            }
        }
        assert_eq!(super::digits(b"1234567/"), None);
        assert_eq!(super::digits(b"123456:8"), None);
        assert_eq!(super::digits(b"12345678901\x00"), None);
        assert_eq!(parse(b"86400000"), Some(86_400_000));
        // 20 digits: too long for i64, so outside every window.
        assert_eq!(parse(b"20261003235810123456"), None);
        assert_eq!(parse(b"00000000000001791071890000"), Some(f1));
    }
}
