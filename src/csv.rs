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
