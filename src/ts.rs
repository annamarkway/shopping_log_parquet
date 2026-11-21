//! Timestamps: F1 `YYYY-MM-DDTHH:MM:SSZ`, F2 `YYYY-MM-DDTHH:MM:SS+HH:MM`,
//! F3 epoch milliseconds (ASCII digits only). Anything else is bad.

pub const DAY: i64 = 86_400_000;

pub fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let yoe = year - era * 400;
    let doy = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468
}

/// Days since the epoch of the first of each month, years TABLE_Y0 .. +TABLE_YEARS.
const TABLE_Y0: i64 = 1900;
const TABLE_YEARS: usize = 400;
static MONTH_START: std::sync::LazyLock<Vec<i32>> = std::sync::LazyLock::new(|| {
    let mut t = Vec::with_capacity(TABLE_YEARS * 12);
    for y in 0..TABLE_YEARS as i64 {
        for m in 1..=12 {
            t.push(days_from_civil(TABLE_Y0 + y, m, 1) as i32);
        }
    }
    t
});

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

/// F1 `YYYY-MM-DDTHH:MM:SSZ` with word-at-a-time checks. `None` means "not a
/// valid F1": the caller falls back to the full parser.
#[inline(always)]
fn f1_fast(s: &[u8]) -> Option<i64> {
    if s.len() != 20 || s[19] != b'Z' {
        return None;
    }
    let w = |i: usize| u64::from_le_bytes(s[i..i + 8].try_into().unwrap());
    let (a, b, c) = (w(0), w(8), w(11)); // "YYYY-MM-", "DDT.....", "HH:MM:SS"
    const AM: u64 = 0xFF00_00FF_0000_0000; // bytes 4 and 7
    const AV: u64 = 0x2D00_002D_0000_0000; // '-'
    const CM: u64 = 0x0000_FF00_00FF_0000; // bytes 2 and 5
    const CV: u64 = 0x0000_3A00_003A_0000; // ':'
    if a & AM != AV || c & CM != CV || (b >> 16) & 0xFF != b'T' as u64 {
        return None;
    }
    let da = digit_bytes(a & !AM | ZEROS & AM)?;
    let db = digit_bytes(b & 0xFFFF | ZEROS & !0xFFFF)?;
    let dc = digit_bytes(c & !CM | ZEROS & CM)?;
    // Byte i of t is 10 * digit i + digit i+1.
    let pair = |d: u64| d * 10 + (d >> 8);
    let (ta, tb, tc) = (pair(da), pair(db), pair(dc));
    let year = ((ta & 0xFF) * 100 + (ta >> 16 & 0xFF)) as i64;
    let month = (ta >> 40 & 0xFF) as i64;
    let day = (tb & 0xFF) as i64;
    let hour = (tc & 0xFF) as i64;
    let minute = (tc >> 24 & 0xFF) as i64;
    let second = (tc >> 48 & 0xFF) as i64;
    if !(1..=12).contains(&month)
        || day < 1
        || day > days_in_month(year, month)
        || hour > 23
        || minute > 59
        || second > 59
    {
        return None;
    }
    Some((days(year, month, day) * 86_400 + hour * 3_600 + minute * 60 + second) * 1_000)
}

/// Parse a timestamp to epoch ms. `None` for any malformed value, and for an
/// F3 number too large for i64 (it is outside every window anyway).
#[inline(always)]
pub fn parse(s: &[u8]) -> Option<i64> {
    if let Some(v) = f1_fast(s) {
        return Some(v);
    }
    if s.len() == 13 {
        // F3 with 13 digits (every in-window epoch-ms value from 2001 to 2286):
        // digits 0..5 as "000" + 5 digits, digits 5..13 as 8.
        let x = u64::from_le_bytes(s[0..8].try_into().unwrap());
        let y = u64::from_le_bytes(s[5..13].try_into().unwrap());
        if let (Some(hi), Some(lo)) = (swar8(x << 24 | 0x30_3030), swar8(y)) {
            return Some((hi * 100_000_000 + lo) as i64);
        }
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
            assert_eq!(super::f1_fast(&t).or_else(|| super::parse_full(&t)), super::parse_full(&t), "{}", String::from_utf8_lossy(&t));
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
