//! Shopping Sessions pipeline: rules.md, end to end.
//!
//! 1. Lookups and links (union-find; each person named by its smallest ID).
//! 2. Events: parse every chunk in parallel into records partitioned by
//!    event_id; per partition, keep the earliest-ingested copy, keep it only if
//!    valid, and repartition the survivors by person.
//! 3. Clicks: parse, keep the valid ones, partition by person.
//! 4. Per person partition: sort by (person, ts), cut sessions, credit and
//!    assist from the person's clicks, aggregate both tables.

mod csv;
mod id;
mod idmap;
mod output;
mod pq;
mod ts;

use std::fs::File;
use std::path::{Path, PathBuf};
use std::time::Instant;

use memmap2::{Mmap, MmapOptions};
use rayon::prelude::*;
use rustc_hash::FxHashMap;

use id::Id;
use idmap::IdMap;
use ts::DAY;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const SESSION_GAP: u32 = 1_800_000;
const CLICK_REACH: i64 = 7 * DAY;
/// Partitions by event_id (dedup) and by person (sessions).
const fn env_or(v: Option<&str>, d: usize) -> usize {
    match v {
        None => d,
        Some(s) => {
            let b = s.as_bytes();
            let mut n = 0;
            let mut i = 0;
            while i < b.len() {
                n = n * 10 + (b[i] - b'0') as usize;
                i += 1;
            }
            n
        }
    }
}
const P: usize = env_or(option_env!("SS_P"), 512);
const Q: usize = env_or(option_env!("SS_Q"), 128);
const CHUNK: usize = env_or(option_env!("SS_CHUNK_KB"), 2048) << 10;
/// `ts` of an event that failed validation.
const INVALID: u32 = u32::MAX;

static T0: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();

// Measurement (SS_PROF): thread-time per step, summed over all tasks.
static PROF: [std::sync::atomic::AtomicU64; 20] = [const { std::sync::atomic::AtomicU64::new(0) }; 20];
const PROF_NAMES: [&str; 17] = [
    "parse: scan+decode", "parse: partition copy", "dedup: gather", "dedup: hash+keep", "dedup: survivors",
    "dedup: person lookup", "dedup: person copy", "freeze texts (+sess gather)", "sess: group hash",
    "sess: bucket events", "sess: sort events", "sess: clicks gather+bucket", "sess: session loop",
    "clicks: parse", "clicks: lookup+copy", "links (own thread)", "output",
];
#[inline(always)]
fn prof(i: usize, t: Instant) -> Instant {
    let now = Instant::now();
    PROF[i].fetch_add((now - t).as_nanos() as u64, std::sync::atomic::Ordering::Relaxed);
    now
}

macro_rules! log {
    ($($arg:tt)*) => {
        eprintln!("[{:>7.1} ms] {}", T0.get().unwrap().elapsed().as_secs_f64() * 1e3, format!($($arg)*))
    };
}

pub struct Ctx {
    pub ws: i64,
    pub we: i64,
    /// Every stored time is `ts - base` as u32, `base = ws - 7 days`.
    pub base: i64,
    pub day_base: i64,
    pub ndays: usize,
    /// Category of `p<n>` (decimal, no leading zero) at index n; 255 if none.
    pub product_dense: Vec<u8>,
    /// Categories of products with any other ID.
    pub products: FxHashMap<Id, u8>,
    pub unknown_cat: u8,
    /// Category names sorted byte-wise, so a smaller index is a smaller name.
    pub cat_names: Vec<Vec<u8>>,
    pub campaigns: FxHashMap<Id, u16>,
    /// Index of campaign `c_<n>` (decimal, no leading zero) at n; u16::MAX if none.
    pub camp_dense: Vec<u16>,
    /// Index `ncamp - 1` is `organic`.
    pub camp_ids: Vec<Vec<u8>>,
    pub camp_channels: Vec<Vec<u8>>,
}

/// One event copy as parsed: 32 bytes.
#[derive(Clone, Copy)]
struct Ev {
    eid: Id,
    ingest: i64,
    /// The user's ID key (resolved to the person after dedup).
    person: Id,
    /// `ts - base`, or INVALID.
    ts: u32,
    /// price (0 unless a purchase) | category << 24
    meta: u32,
}

/// A surviving event: 16 bytes.
#[derive(Clone, Copy)]
pub struct Rec {
    person: Id,
    ts: u32,
    meta: u32,
}

/// A valid click: 24 bytes.
#[derive(Clone, Copy)]
struct Click {
    person: Id,
    cid: Id,
    /// For a hashed click_id: its text in the (still mapped) input, as
    /// address | length << 48; 0 if its text was kept by `id::key_kept`.
    ctext: u64,
    ts: u32,
    camp: u16,
}

/// The text of a click's click_id (for the rare same-time tie).
fn cid_text(c: &Click) -> std::borrow::Cow<'static, [u8]> {
    if id::is_hashed(c.cid) && c.ctext != 0 {
        let (p, n) = ((c.ctext & ((1 << 48) - 1)) as *const u8, (c.ctext >> 48) as usize);
        // The input maps live until the process exits.
        std::borrow::Cow::Borrowed(unsafe { std::slice::from_raw_parts(p, n) })
    } else {
        std::borrow::Cow::Owned(id::text(c.cid))
    }
}

pub struct UserRow {
    pub person: Id,
    pub day: i32,
    pub sessions: u64,
    pub events: u64,
    pub revenue: u64,
    pub top: Option<u8>,
}

/// sessions, purchasing_sessions, revenue_cents, assisted_sessions
pub type CampAgg = [u64; 4];

/// Items grouped by partition: partition `p` is `data[off[p]..off[p + 1]]`.
struct Parted<T> {
    data: Vec<T>,
    off: Vec<u32>,
}

impl<T: Copy> Parted<T> {
    fn new(items: &[T], n: usize, part: impl Fn(&T) -> usize) -> Self {
        let mut off = vec![0u32; n + 1];
        for it in items {
            off[part(it) + 1] += 1;
        }
        Self::scatter(items, off, part)
    }
    /// With `off[p + 1]` already counting the items of partition `p`.
    fn scatter(items: &[T], mut off: Vec<u32>, part: impl Fn(&T) -> usize) -> Self {
        let n = off.len() - 1;
        for p in 0..n {
            off[p + 1] += off[p];
        }
        let mut at: Vec<u32> = off[..n].to_vec();
        let mut data: Vec<T> = Vec::with_capacity(items.len());
        let ptr = data.as_mut_ptr();
        for it in items {
            let p = part(it);
            unsafe { ptr.add(at[p] as usize).write(*it) };
            at[p] += 1;
        }
        unsafe { data.set_len(items.len()) };
        Parted { data, off }
    }
    #[inline]
    fn part(&self, p: usize) -> &[T] {
        &self.data[self.off[p] as usize..self.off[p + 1] as usize]
    }
}

fn gather<T: Copy>(parts: &[Parted<T>], p: usize) -> Vec<T> {
    let mut v = Vec::with_capacity(parts.iter().map(|x| x.part(p).len()).sum());
    for x in parts {
        v.extend_from_slice(x.part(p));
    }
    v
}

#[inline(always)]
fn person_part(person: Id) -> usize {
    (id::mix(person) >> 40) as usize % Q
}

fn main() {
    T0.get_or_init(Instant::now);
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into()));
    let out = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into()));
    if let Ok(n) = std::env::var("NCPU").map(|v| v.parse::<usize>().unwrap_or(0)) {
        if n > 0 {
            rayon::ThreadPoolBuilder::new().num_threads(n).build_global().ok();
        }
    }

    if let Ok(gb) = std::env::var("ALLOC_GB").map(|v| v.parse::<usize>().unwrap()) {
        // Teardown experiment: touch `gb` GB in 64 MB vectors, then exit
        // either holding them (default) or after freeing them in parallel.
        let v: Vec<Vec<u8>> = (0..gb * 16).into_par_iter().map(|_| vec![1u8; 64 << 20]).collect();
        log!("allocated {} GB", gb);
        if std::env::var_os("FREE_PAR").is_some() {
            v.into_par_iter().for_each(drop);
            log!("freed");
        } else {
            std::mem::forget(v);
        }
        return;
    }
    let event_maps = map_dir(&data.join("events"));
    let click_maps = map_dir(&data.join("clicks"));
    log!("mapped");
    let ctx = load_ctx(&data);
    log!("lookups");
    if let Ok(mode) = std::env::var("BENCH") {
        let cs = chunks(&event_maps);
        for _ in 0..3 {
            let t = Instant::now();
            let n: usize = match mode.as_str() {
                "lines" => cs.par_iter().map(|c| { let mut n = 0; csv::for_each_line(c.bytes, c.first, |_| n += 1); n }).sum(),
                "split" => cs.par_iter().map(|c| { let mut n = 0; csv::for_each_line(c.bytes, c.first, |l| if csv::split::<7>(l).is_some() { n += 1 }); n }).sum(),
                "fast" => cs.par_iter().map(|c| { let mut n = 0; csv::scan_fast::<7>(c.bytes, c.first, |_, _| n += 1); n }).sum(),
                "pfast" => cs.par_iter().map(|c| { let mut n = 0u64; csv::scan_fast::<7>(c.bytes, c.first, |f, sl| if let Some(e) = parse_event(&ctx, f, sl) { n = n.wrapping_add(e.eid ^ e.person ^ e.ts as u64) }); n as usize }).sum(),
                "f_ing" => cs.par_iter().map(|c| { let mut n = 0i64; csv::scan_fast::<7>(c.bytes, c.first, |f, _| n = n.wrapping_add(ts::parse(f[3]).unwrap_or(1))); n as usize }).sum(),
                "f_ingp" => cs.par_iter().map(|c| { let mut n = 0i64; csv::scan_fast::<7>(c.bytes, c.first, |f, _| n = n.wrapping_add(probe_ts_parse(f[3]))); n as usize }).sum(),
                "f_ts" => cs.par_iter().map(|c| { let mut n = 0i64; csv::scan_fast::<7>(c.bytes, c.first, |f, _| n = n.wrapping_add(ts::parse(f[2]).unwrap_or(1))); n as usize }).sum(),
                "f_eid" => cs.par_iter().map(|c| { let mut n = 0u64; csv::scan_fast::<7>(c.bytes, c.first, |f, sl| n = n.wrapping_add(if sl { unsafe { id::key_wide::<false>(f[0]) } } else { 0 })); n as usize }).sum(),
                "f_user" => cs.par_iter().map(|c| { let mut n = 0u64; csv::scan_fast::<7>(c.bytes, c.first, |f, sl| n = n.wrapping_add(if sl { unsafe { id::key_wide::<true>(f[1]) } } else { 0 })); n as usize }).sum(),
                "f_kind" => cs.par_iter().map(|c| { let mut n = 0u64; csv::scan_fast::<7>(c.bytes, c.first, |f, _| n += match f[4] { b"view" | b"cart" => 1, b"purchase" => 2, _ => 3 }); n as usize }).sum(),
                "f_len" => cs.par_iter().map(|c| { let mut n = 0usize; csv::scan_fast::<7>(c.bytes, c.first, |f, _| n += f.iter().map(|x| x.len()).sum::<usize>()); n }).sum(),
                "scan" => cs.par_iter().map(|c| { let mut n = 0; csv::scan::<7>(c.bytes, c.first, |_| n += 1); n }).sum(),
                "parse" => cs.par_iter().map(|c| { let mut n = 0u64; csv::scan::<7>(c.bytes, c.first, |f| if let Some(e) = parse_event(&ctx, f, false) { n = n.wrapping_add(e.eid ^ e.person ^ e.ts as u64) }); n as usize }).sum(),
                "keys" => cs.par_iter().map(|c| { let mut n = 0u64; csv::scan::<7>(c.bytes, c.first, |f| { n = n.wrapping_add(id::key(f[0]) ^ id::key(f[1])) }); n as usize }).sum(),
                "tsonly" => cs.par_iter().map(|c| { let mut n = 0i64; csv::scan::<7>(c.bytes, c.first, |f| { n = n.wrapping_add(ts::parse(f[2]).unwrap_or(0) ^ ts::parse(f[3]).unwrap_or(0)) }); n as usize }).sum(),
                "store" => cs.par_iter().map(|c| { let mut v: Vec<Ev> = Vec::with_capacity(c.bytes.len() / 64); csv::scan::<7>(c.bytes, c.first, |f| if let Some(e) = parse_event(&ctx, f, false) { v.push(e) }); v.len() }).sum(),
                "full" => parse_events(&ctx, &event_maps).iter().map(|p| p.data.len()).sum(),
                "ts" => cs.par_iter().map(|c| { let mut n = 0; csv::for_each_line(c.bytes, c.first, |l| if let Some(f) = csv::split::<7>(l) { if ts::parse(f[2]).is_some() { n += 1 } if ts::parse(f[3]).is_some() { n += 1 } }); n }).sum(),
                _ => cs.par_iter().map(|c| c.bytes.iter().map(|&b| b as usize).sum::<usize>()).sum(),
            };
            eprintln!("bench {mode}: {n} in {:?}", t.elapsed());
        }
        return;
    }

    // Parsing needs no links, so the (mostly single-threaded) link work runs
    // alongside it; persons are resolved afterwards.
    // A dedicated thread, not the pool: pool tasks would queue behind every
    // event chunk.
    let (links, (parsed, raw_clicks)) = std::thread::scope(|sc| {
        let h = sc.spawn(|| {
            let t = Instant::now();
            let l = load_links(&data);
            prof(15, t);
            log!("links: {} slots", l.len_hint());
            l
        });
        let r = rayon::join(
            || parse_events(&ctx, &event_maps),
            || chunks(&click_maps).par_iter().map(|c| parse_clicks(&ctx, c)).collect::<Vec<_>>(),
        );
        (h.join().unwrap(), r)
    });
    log!("events and clicks parsed");
    // Every ID text is in by now: index them alongside the dedup.
    let ((recs, ()), clicks) = rayon::join(
        || {
            rayon::join(
                || dedup_events(&links, parsed),
                || {
                    let t = Instant::now();
                    id::freeze_texts();
                    prof(7, t);
                },
            )
        },
        || {
            raw_clicks
                .into_par_iter()
                .map(|mut v| {
                    let t = Instant::now();
                    links.resolve_all(&mut v, |k| &mut k.person);
                    let r = Parted::new(&v, Q, |k| person_part(k.person));
                    prof(14, t);
                    r
                })
                .collect::<Vec<_>>()
        },
    );
    log!("events and clicks partitioned by person");

    // One accumulator set per fold (about one per thread), reused across partitions.
    let states: Vec<(SessState, Vec<Vec<UserRow>>)> = (0..Q)
        .into_par_iter()
        .fold(
            || (SessState::new(&ctx), Vec::new()),
            |(mut st, mut us), q| {
                us.push(sessions_for_partition(&ctx, &recs, &clicks, q, &mut st));
                (st, us)
            },
        )
        .collect();
    log!("sessions");

    let ncamp = ctx.camp_ids.len();
    let nd = ctx.ndays;
    let mut camp = vec![[0u64; 4]; ncamp * nd];
    let mut users: Vec<Vec<UserRow>> = Vec::with_capacity(Q);
    for (st, us) in states {
        for (x, y) in camp.iter_mut().zip(&st.camp) {
            for k in 0..3 {
                x[k] += y[k];
            }
        }
        for di in 0..nd {
            for c in 0..ncamp {
                camp[c * nd + di][3] += st.asst[di * ncamp + c] as u64;
            }
        }
        users.extend(us);
    }
    let t = Instant::now();
    output::write_all(&ctx, &out, &users, &camp);
    prof(16, t);
    log!("done: {} user rows", users.iter().map(|u| u.len()).sum::<usize>());
    if std::env::var_os("SS_DEBUG").is_some() {
        if let Ok(t) = std::fs::read_to_string("/proc/self/smaps_rollup") {
            for l in t.lines().filter(|l| l.starts_with("Rss") || l.starts_with("AnonHuge") || l.starts_with("Anonymous")) {
                eprintln!("{l}");
            }
        }
    }
    // Skip freeing everything: the process is about to exit.
    if std::env::var_os("SS_PROF").is_some() {
        for (i, n) in PROF_NAMES.iter().enumerate() {
            let v = PROF[i].load(std::sync::atomic::Ordering::Relaxed);
            eprintln!("prof {n:<28} {:>8.1} thread-ms", v as f64 / 1e6);
        }
    }
    std::mem::forget((recs, clicks, users, event_maps, click_maps, ctx));
}

// ---------------------------------------------------------------- input files

fn list_csv(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.is_file() && p.extension().is_some_and(|x| x == "csv"))
            .collect(),
        Err(_) => Vec::new(),
    };
    v.sort();
    v
}

fn map_file(path: &Path) -> Option<Mmap> {
    let f = File::open(path).ok()?;
    if f.metadata().ok()?.len() == 0 {
        return None;
    }
    let mut o = MmapOptions::new();
    if std::env::var_os("POPULATE").is_some() {
        o.populate();
    }
    unsafe { o.map(&f).ok() }
}

fn map_dir(dir: &Path) -> Vec<Mmap> {
    list_csv(dir).par_iter().filter_map(|p| map_file(p)).collect()
}

/// A piece of one file that starts at a line start and ends after a `\n` (or
/// at EOF). The first piece of each file holds its header.
struct Chunk<'a> {
    bytes: &'a [u8],
    first: bool,
}

fn chunks(maps: &[Mmap]) -> Vec<Chunk<'_>> {
    let mut out = Vec::new();
    for m in maps {
        let data: &[u8] = m;
        let mut start = 0;
        while start < data.len() {
            let mut end = (start + CHUNK).min(data.len());
            if end < data.len() {
                end = memchr::memchr(b'\n', &data[end..]).map_or(data.len(), |e| end + e + 1);
            }
            out.push(Chunk { bytes: &data[start..end], first: start == 0 });
            start = end;
        }
    }
    out
}

// ---------------------------------------------------------------- lookups

fn read_small(path: &Path) -> Vec<u8> {
    std::fs::read(path).unwrap_or_default()
}

fn manifest_field(text: &str, name: &str) -> i64 {
    let at = text.find(&format!("\"{name}\"")).expect("manifest field");
    let rest = &text[at + name.len() + 2..];
    let open = rest.find('"').unwrap() + 1;
    let close = rest[open..].find('"').unwrap() + open;
    ts::parse(rest[open..close].as_bytes()).expect("manifest timestamp")
}

fn load_ctx(data: &Path) -> Ctx {
    let manifest = String::from_utf8(read_small(&data.join("manifest.json"))).unwrap();
    let ws = manifest_field(&manifest, "window_start");
    let we = manifest_field(&manifest, "window_end");
    let base = ws - CLICK_REACH;
    assert!(we - base < INVALID as i64, "window too long");
    let day_base = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - day_base + 1) as usize;

    let (product_dense, products, unknown_cat, cat_names, campaigns, camp_ids, camp_channels) = load_lookups(data);
    let mut camp_dense = Vec::new();
    for (i, c) in camp_ids.iter().enumerate().take(camp_ids.len() - 1) {
        if let Some(n) = dense_campaign(c) {
            if camp_dense.len() <= n {
                camp_dense.resize(n + 1, u16::MAX);
            }
            camp_dense[n] = i as u16;
        }
    }
    Ctx {
        ws,
        we,
        base,
        day_base,
        ndays,
        product_dense,
        products,
        unknown_cat,
        cat_names,
        campaigns,
        camp_dense,
        camp_ids,
        camp_channels,
    }
}

#[allow(clippy::type_complexity)]
fn load_lookups(
    data: &Path,
) -> (Vec<u8>, FxHashMap<Id, u8>, u8, Vec<Vec<u8>>, FxHashMap<Id, u16>, Vec<Vec<u8>>, Vec<Vec<u8>>) {
    let raw = read_small(&data.join("products.csv"));
    let mut pairs: Vec<(&[u8], &[u8])> = Vec::new();
    csv::for_each_line(&raw, true, |line| {
        if let Some([p, c]) = csv::split::<2>(line) {
            pairs.push((p, c));
        }
    });
    let mut cat_names: Vec<Vec<u8>> = pairs.iter().map(|(_, c)| c.to_vec()).collect();
    cat_names.push(b"unknown".to_vec());
    cat_names.sort();
    cat_names.dedup();
    assert!(cat_names.len() < 255, "too many categories");
    let cat_index: FxHashMap<&[u8], u8> =
        cat_names.iter().enumerate().map(|(i, c)| (c.as_slice(), i as u8)).collect();
    let unknown_cat = cat_index[b"unknown".as_slice()];
    let mut product_dense = Vec::new();
    let mut products = FxHashMap::default();
    for (p, c) in &pairs {
        let cat = cat_index[c];
        match dense_product(p) {
            Some(n) => {
                if product_dense.len() <= n {
                    product_dense.resize(n + 1, 255u8);
                }
                product_dense[n] = cat;
            }
            None => {
                products.insert(id::key(p), cat);
            }
        }
    }

    let raw = read_small(&data.join("campaigns.csv"));
    let mut camp_ids = Vec::new();
    let mut camp_channels = Vec::new();
    let mut campaigns = FxHashMap::default();
    csv::for_each_line(&raw, true, |line| {
        if let Some([c, ch]) = csv::split::<2>(line) {
            campaigns.insert(id::key(c), camp_ids.len() as u16);
            camp_ids.push(c.to_vec());
            camp_channels.push(ch.to_vec());
        }
    });
    camp_ids.push(b"organic".to_vec());
    camp_channels.push(b"organic".to_vec());
    (product_dense, products, unknown_cat, cat_names, campaigns, camp_ids, camp_channels)
}

/// `n` for a product ID `p<n>`, n decimal without a leading zero, below 10^7.
#[inline(always)]
fn dense_product(s: &[u8]) -> Option<usize> {
    if !(2..=8).contains(&s.len()) || s[0] != b'p' || s[1] == b'0' {
        return None;
    }
    let mut n = 0usize;
    for &c in &s[1..] {
        if !c.is_ascii_digit() {
            return None;
        }
        n = n * 10 + (c - b'0') as usize;
    }
    Some(n)
}

/// `n` for a campaign ID `c_<n>`, n decimal without a leading zero, below 10^6.
#[inline(always)]
fn dense_campaign(s: &[u8]) -> Option<usize> {
    if !(3..=8).contains(&s.len()) || s[0] != b'c' || s[1] != b'_' || s[2] == b'0' {
        return None;
    }
    let mut n = 0usize;
    for &c in &s[2..] {
        if !c.is_ascii_digit() {
            return None;
        }
        n = n * 10 + (c - b'0') as usize;
    }
    Some(n)
}

/// The campaign index of a campaign_id, if it is a known campaign.
#[inline(always)]
fn campaign_of(ctx: &Ctx, s: &[u8]) -> Option<u16> {
    match dense_campaign(s) {
        Some(n) => ctx.camp_dense.get(n).copied().filter(|&c| c != u16::MAX),
        None => ctx.campaigns.get(&id::key(s)).copied(),
    }
}

#[inline(always)]
fn product_cat(ctx: &Ctx, s: &[u8]) -> u8 {
    match dense_product(s) {
        Some(n) => match ctx.product_dense.get(n) {
            Some(&c) if c != 255 => c,
            _ => ctx.unknown_cat,
        },
        None => ctx.products.get(&id::key(s)).copied().unwrap_or(ctx.unknown_cat),
    }
}

fn find(parent: &mut [u32], mut x: u32) -> u32 {
    let mut root = x;
    while parent[root as usize] != root {
        root = parent[root as usize];
    }
    while parent[x as usize] != root {
        let next = parent[x as usize];
        parent[x as usize] = root;
        x = next;
    }
    root
}

/// Every linked ID mapped to its person's name (the byte-wise smallest ID).
fn load_links(data: &Path) -> IdMap {
    let maps = map_dir(&data.join("links"));
    let parts: Vec<Vec<(Id, Id)>> = chunks(&maps)
        .iter()
        .map(|c| {
            let mut v = Vec::with_capacity(c.bytes.len() / 20);
            csv::scan_fast::<2>(c.bytes, c.first, |[a, b], slack| {
                if !a.is_empty() && !b.is_empty() {
                    v.push(if slack {
                        unsafe { (id::key_wide::<true>(a), id::key_wide::<true>(b)) }
                    } else {
                        (id::key_kept(a), id::key_kept(b))
                    });
                }
            });
            v
        })
        .collect();
    log!("links parsed: {} lines", parts.iter().map(|p| p.len()).sum::<usize>());

    let mut index: FxHashMap<Id, u32> = FxHashMap::default();
    index.reserve(parts.iter().map(|p| p.len()).sum::<usize>() * 5 / 4);
    let mut ids: Vec<Id> = Vec::new();
    let mut parent: Vec<u32> = Vec::new();
    let mut intern = |x: Id, ids: &mut Vec<Id>, parent: &mut Vec<u32>| -> u32 {
        *index.entry(x).or_insert_with(|| {
            ids.push(x);
            parent.push(parent.len() as u32);
            (ids.len() - 1) as u32
        })
    };
    for part in &parts {
        for &(a, b) in part {
            let ia = intern(a, &mut ids, &mut parent);
            let ib = intern(b, &mut ids, &mut parent);
            let ra = find(&mut parent, ia);
            let rb = find(&mut parent, ib);
            if ra != rb {
                parent[ra as usize] = rb;
            }
        }
    }
    let n = ids.len();
    log!("links unioned: {n} ids");
    let mut name: Vec<Option<Id>> = vec![None; n];
    for i in 0..n {
        let r = find(&mut parent, i as u32) as usize;
        name[r] = Some(match name[r] {
            None => ids[i],
            Some(cur) => {
                if id::cmp(ids[i], cur).is_lt() {
                    ids[i]
                } else {
                    cur
                }
            }
        });
    }
    let mut pairs: Vec<(Id, Id)> = Vec::with_capacity(n);
    for i in 0..n {
        let r = find(&mut parent, i as u32) as usize;
        let nm = name[r].unwrap();
        if nm != ids[i] {
            pairs.push((ids[i], nm));
        }
    }
    let mut links = IdMap::with_capacity(pairs.len());
    for &(k, v) in &pairs {
        links.insert(k, v);
    }
    links
}


// ---------------------------------------------------------------- events

#[inline(always)]
fn parse_price(s: &[u8]) -> Option<u32> {
    if s.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &c in s {
        if !c.is_ascii_digit() {
            return None;
        }
        v = v * 10 + (c - b'0') as u64;
        if v > 10_000_000 {
            return None; // too large, whatever follows
        }
    }
    if v >= 1 { Some(v as u32) } else { None }
}

#[inline(always)]
fn parse_event(ctx: &Ctx, [eid, user, tsf, ingest, kind, product, price]: [&[u8]; 7], slack: bool) -> Option<Ev> {
    if eid.is_empty() {
        return None;
    }
    let ingest = ts::parse(ingest).unwrap_or(i64::MAX);
    // With slack, words can be read past any field of the line.
    let eid = if slack { unsafe { id::key_wide::<false>(eid) } } else { id::key(eid) };
    let mut ev = Ev { eid, ingest, person: 0, ts: INVALID, meta: 0 };
    'valid: {
        if user.is_empty() || product.is_empty() {
            break 'valid;
        }
        let Some(t) = ts::parse(tsf) else { break 'valid };
        if t < ctx.ws || t >= ctx.we || ingest == i64::MAX || ingest - t > DAY {
            break 'valid;
        }
        let meta = match kind {
            b"view" | b"cart" => 0,
            b"purchase" => match purchase_meta(ctx, price, product) {
                Some(m) => m,
                None => break 'valid,
            },
            _ => break 'valid,
        };
        // The input maps live until the process exits, so the text is only logged.
        ev.person = unsafe { if slack { id::key_wide_logged(user) } else { id::key_logged(user) } };
        ev.ts = (t - ctx.base) as u32;
        ev.meta = meta;
    }
    Some(ev)
}

/// price | category << 24 of a purchase, or `None` if its price is bad.
/// Out of line: purchases are few, and keeping this here frees registers.
#[cold]
#[inline(never)]
fn purchase_meta(ctx: &Ctx, price: &[u8], product: &[u8]) -> Option<u32> {
    let price = parse_price(price)?;
    // The category only matters for purchases (top_category).
    Some(price | (product_cat(ctx, product) as u32) << 24)
}

#[inline(always)]
fn eid_part(eid: Id) -> usize {
    (id::mix(eid) >> 40) as usize % P
}

/// Every event copy, per chunk, partitioned by event_id.
fn parse_events(ctx: &Ctx, maps: &[Mmap]) -> Vec<Parted<Ev>> {
    let parsed: Vec<Parted<Ev>> = chunks(maps)
        .par_iter()
        .map(|c| {
            let t = Instant::now();
            let mut v: Vec<Ev> = Vec::with_capacity(c.bytes.len() / 64);
            // Partition sizes counted as the events come.
            let mut off = vec![0u32; P + 1];
            csv::scan_fast::<7>(c.bytes, c.first, |f, slack| {
                if let Some(ev) = parse_event(ctx, f, slack) {
                    off[eid_part(ev.eid) + 1] += 1;
                    v.push(ev);
                }
            });
            let t = prof(0, t);
            let r = Parted::scatter(&v, off, |e| eid_part(e.eid));
            prof(1, t);
            r
        })
        .collect();
    log!("events parsed");
    parsed
}

/// Surviving events, as one person-partitioned set per event_id partition.
fn dedup_events(links: &IdMap, parsed: Vec<Parted<Ev>>) -> Vec<Parted<Rec>> {
    let out = (0..P)
        .into_par_iter()
        .map(|p| {
            let t = Instant::now();
            let v = gather(&parsed, p);
            let t = prof(2, t);
            // Open addressing on event_id: each slot holds the index of the
            // earliest-ingested copy seen so far (equal ingest_ts copies are identical).
            let size = (v.len() * 2).next_power_of_two().max(16);
            let mask = size - 1;
            let mut table = vec![u32::MAX; size];
            // keep[i]: copy i is the earliest-ingested of its event_id so far.
            let mut keep = vec![0u8; v.len()];
            for (i, e) in v.iter().enumerate() {
                let mut h = (e.eid.wrapping_mul(0xD6E8_FEB8_6659_FD93) >> 32) as usize & mask;
                loop {
                    let t = table[h];
                    if t == u32::MAX {
                        table[h] = i as u32;
                        keep[i] = 1;
                        break;
                    }
                    let o = unsafe { v.get_unchecked(t as usize) };
                    if o.eid == e.eid {
                        if e.ingest < o.ingest {
                            table[h] = i as u32;
                            keep[t as usize] = 0;
                            keep[i] = 1;
                        }
                        break;
                    }
                    h = (h + 1) & mask;
                }
            }
            let t = prof(3, t);
            let mut recs: Vec<Rec> = Vec::with_capacity(v.len());
            for (e, &k) in v.iter().zip(&keep) {
                if k != 0 && e.ts != INVALID {
                    recs.push(Rec { person: e.person, ts: e.ts, meta: e.meta });
                }
            }
            if p < 4 && std::env::var_os("SS_DEBUG").is_some() {
                let hit = recs.iter().filter(|r| links.get(r.person).is_some()).count();
                eprintln!("dedup {p}: {} copies, {} kept, {hit} linked, table {size}", v.len(), recs.len());
            }
            let t = prof(4, t);
            links.resolve_all(&mut recs, |r| &mut r.person);
            let t = prof(5, t);
            let r = Parted::new(&recs, Q, |r| person_part(r.person));
            prof(6, t);
            r
        })
        .collect();
    // Free in parallel so later stages reuse these pages instead of fresh, zeroed ones.
    parsed.into_par_iter().for_each(drop);
    out
}

// ---------------------------------------------------------------- clicks

#[inline(always)]
fn parse_click(ctx: &Ctx, [cid, user, camp, tsf]: [&[u8]; 4], slack: bool) -> Option<Click> {
    if cid.is_empty() || user.is_empty() {
        return None;
    }
    let t = ts::parse(tsf)?;
    if t < ctx.base || t >= ctx.we {
        return None;
    }
    // With slack, words can be read past any field of the line. A click's
    // user_id text is never needed (persons are named from events and links).
    let camp = campaign_of(ctx, camp)?;
    let (person, mut ckey) = if slack {
        unsafe { (id::key_wide::<false>(user), id::key_wide::<false>(cid)) }
    } else {
        (id::key(user), id::key(cid))
    };
    let mut ctext = 0u64;
    if id::is_hashed(ckey) {
        let addr = cid.as_ptr() as u64;
        if addr < 1 << 48 && cid.len() < 1 << 16 {
            ctext = addr | (cid.len() as u64) << 48;
        } else {
            ckey = id::key_kept(cid);
        }
    }
    Some(Click { person, cid: ckey, ctext, ts: (t - ctx.base) as u32, camp })
}

/// Valid clicks of one chunk (duplicates included), by user ID.
fn parse_clicks(ctx: &Ctx, c: &Chunk) -> Vec<Click> {
    let t = Instant::now();
    let mut v: Vec<Click> = Vec::with_capacity(c.bytes.len() / 40);
    csv::scan_fast::<4>(c.bytes, c.first, |f, slack| {
        if let Some(k) = parse_click(ctx, f, slack) {
            v.push(k);
        }
    });
    prof(13, t);
    v
}

// ---------------------------------------------------------------- sessions

/// Persons of one partition, numbered in order of first appearance.
struct Groups {
    keys: Vec<Id>,
    gid: Vec<u32>,
    mask: usize,
    persons: Vec<Id>,
}

impl Groups {
    /// For `n` items; persons usually have many each, so the table starts
    /// small (to stay in cache) and grows.
    fn new(n: usize) -> Self {
        let size = (n / 8).next_power_of_two().max(64);
        Groups { keys: vec![u64::MAX; size], gid: vec![0; size], mask: size - 1, persons: Vec::new() }
    }
    #[cold]
    fn grow(&mut self) {
        let size = self.keys.len() * 2;
        self.keys = vec![u64::MAX; size];
        self.gid = vec![0; size];
        self.mask = size - 1;
        for (g, &k) in self.persons.iter().enumerate() {
            let h = self.slot(k);
            self.keys[h] = k;
            self.gid[h] = g as u32;
        }
    }
    #[inline(always)]
    fn slot(&self, k: Id) -> usize {
        let mut h = (k.wrapping_mul(0xD6E8_FEB8_6659_FD93) >> 32) as usize & self.mask;
        loop {
            let x = self.keys[h];
            if x == k || x == u64::MAX {
                return h;
            }
            h = (h + 1) & self.mask;
        }
    }
    #[inline(always)]
    fn insert(&mut self, k: Id) -> u32 {
        let h = self.slot(k);
        if self.keys[h] != k {
            let g = self.persons.len() as u32;
            self.keys[h] = k;
            self.gid[h] = g;
            self.persons.push(k);
            if self.persons.len() * 2 > self.keys.len() {
                self.grow();
            }
            return g;
        }
        self.gid[h]
    }
    #[inline(always)]
    fn get(&self, k: Id) -> Option<u32> {
        let h = self.slot(k);
        (self.keys[h] == k).then(|| self.gid[h])
    }
}

/// Stable-free bucket scatter: items grouped by `gid`, with group offsets.
fn bucket<T: Copy, U: Copy>(items: &[T], gids: &[u32], ng: usize, map: impl Fn(&T) -> U) -> (Vec<U>, Vec<u32>) {
    let mut off = vec![0u32; ng + 1];
    for &g in gids {
        if g != u32::MAX {
            off[g as usize + 1] += 1;
        }
    }
    for g in 0..ng {
        off[g + 1] += off[g];
    }
    let total = off[ng] as usize;
    let mut at: Vec<u32> = off[..ng].to_vec();
    let mut out: Vec<U> = Vec::with_capacity(total);
    let ptr = out.as_mut_ptr();
    for (it, &g) in items.iter().zip(gids) {
        if g != u32::MAX {
            unsafe { ptr.add(at[g as usize] as usize).write(map(it)) };
            at[g as usize] += 1;
        }
    }
    unsafe { out.set_len(total) };
    (out, off)
}

/// Campaign accumulators and scratch shared by the partitions one fold handles.
struct SessState {
    /// [campaign][day]: sessions, purchasing sessions, revenue (assists in `asst`).
    camp: Vec<CampAgg>,
    /// Assisted sessions as [day][campaign]: one session's increments share a row.
    asst: Vec<u32>,
    /// Clicks per campaign in the current 7-day window, and the campaigns with
    /// at least one (`active`, with each one's index in it in `at`).
    cnt: Vec<u32>,
    at: Vec<u32>,
    active: Vec<u16>,
    catrev: Vec<u64>,
}

impl SessState {
    fn new(ctx: &Ctx) -> Self {
        let (ncamp, nd) = (ctx.camp_ids.len(), ctx.ndays);
        SessState {
            camp: vec![[0u64; 4]; ncamp * nd],
            asst: vec![0u32; nd * ncamp],
            cnt: vec![0u32; ncamp],
            at: vec![0u32; ncamp],
            active: Vec::new(),
            catrev: vec![0u64; ctx.cat_names.len()],
        }
    }
}

fn sessions_for_partition(
    ctx: &Ctx,
    recs: &[Parted<Rec>],
    clicks: &[Parted<Click>],
    q: usize,
    st: &mut SessState,
) -> Vec<UserRow> {
    // Events grouped by person, each group sorted by ts. An event is packed as
    // ts << 32 | meta, so sorting the u64s sorts by ts.
    let t = Instant::now();
    let raw = gather(recs, q);
    let t = prof(7, t);
    let mut groups = Groups::new(raw.len());
    let gids: Vec<u32> = raw.iter().map(|e| groups.insert(e.person)).collect();
    let ng = groups.persons.len();
    let t = prof(8, t);
    if q < 2 && std::env::var_os("SS_DEBUG").is_some() {
        eprintln!("partition {q}: {} events, {ng} persons, table {}", raw.len(), groups.keys.len());
    }
    let (mut ev, eoff) = bucket(&raw, &gids, ng, |e| (e.ts as u64) << 32 | e.meta as u64);
    drop(raw);
    let t = prof(9, t);
    for g in 0..ng {
        ev[eoff[g] as usize..eoff[g + 1] as usize].sort_unstable();
    }
    let t = prof(10, t);

    // Clicks of the same persons (others can credit nothing), each group by
    // time, then click_id descending: the last click at or before a session
    // start is the latest, with the smallest click_id on a tie.
    let raw = gather(clicks, q);
    let cg: Vec<u32> = raw.iter().map(|k| groups.get(k.person).unwrap_or(u32::MAX)).collect();
    let (mut ck, coff) = bucket(&raw, &cg, ng, |k| *k);
    drop(raw);
    let t = prof(11, t);

    let ncamp = ctx.camp_ids.len();
    let nd = ctx.ndays;
    let organic = ncamp - 1;
    let reach = CLICK_REACH as u32;
    let SessState { camp, asst, cnt, at, active, catrev } = st;
    let mut users = Vec::new();

    for g in 0..ng {
        let person = groups.persons[g];
        let pe = &ev[eoff[g] as usize..eoff[g + 1] as usize];
        let pc = &mut ck[coff[g] as usize..coff[g + 1] as usize];
        pc.sort_unstable_by(|a, b| {
            a.ts.cmp(&b.ts).then_with(|| {
                if id::is_hashed(a.cid) || id::is_hashed(b.cid) {
                    cid_text(b).cmp(&cid_text(a))
                } else {
                    id::cmp(b.cid, a.cid)
                }
            })
        });
        // Duplicates (same click_id) are now adjacent; drop them in place.
        let mut m = 0;
        for j in 0..pc.len() {
            if m == 0 || pc[m - 1].cid != pc[j].cid {
                pc[m] = pc[j];
                m += 1;
            }
        }
        let pc = &pc[..m];

        let mut day = i64::MIN;
        let (mut d_sessions, mut d_events, mut d_revenue) = (0u64, 0u64, 0u64);
        let flush = |day: i64, s: u64, e: u64, r: u64, catrev: &mut [u64], users: &mut Vec<UserRow>| {
            if s == 0 {
                return;
            }
            let mut top: Option<u8> = None;
            // Category revenue is only ever added with a purchase.
            if r > 0 {
                let mut best = 0u64;
                for (c, v) in catrev.iter_mut().enumerate() {
                    if *v > best {
                        best = *v;
                        top = Some(c as u8);
                    }
                    *v = 0;
                }
            }
            users.push(UserRow { person, day: day as i32, sessions: s, events: e, revenue: r, top });
        };

        let (mut l, mut r) = (0usize, 0usize);
        let mut k = 0;
        while k < pe.len() {
            let start = (pe[k] >> 32) as u32;
            let mut last = start;
            let (mut n, mut rev, mut bought) = (0u64, 0u64, false);
            let sday = (ctx.base + start as i64).div_euclid(DAY);
            if sday != day {
                flush(day, d_sessions, d_events, d_revenue, catrev, &mut users);
                day = sday;
                d_sessions = 0;
                d_events = 0;
                d_revenue = 0;
            }
            while k < pe.len() {
                let t = (pe[k] >> 32) as u32;
                if t - last > SESSION_GAP {
                    break;
                }
                last = t;
                n += 1;
                let meta = pe[k] as u32;
                let price = (meta & 0xFF_FFFF) as u64;
                if price > 0 {
                    bought = true;
                    rev += price;
                    catrev[(meta >> 24) as usize] += price;
                }
                k += 1;
            }
            d_sessions += 1;
            d_events += n;
            d_revenue += rev;

            // Attribution and assists: clicks in [start - 7 days, start].
            while r < pc.len() && pc[r].ts <= start {
                let c = pc[r].camp as usize;
                if cnt[c] == 0 {
                    at[c] = active.len() as u32;
                    active.push(c as u16);
                }
                cnt[c] += 1;
                r += 1;
            }
            while l < r && pc[l].ts < start - reach {
                let c = pc[l].camp as usize;
                cnt[c] -= 1;
                if cnt[c] == 0 {
                    let i = at[c] as usize;
                    let last = *active.last().unwrap();
                    active[i] = last;
                    at[last as usize] = i as u32;
                    active.pop();
                }
                l += 1;
            }
            let di = (sday - ctx.day_base) as usize;
            let credited = if l < r { pc[r - 1].camp as usize } else { organic };
            let a = &mut camp[credited * nd + di];
            a[0] += 1;
            a[1] += bought as u64;
            a[2] += rev;
            let row = &mut asst[di * ncamp..(di + 1) * ncamp];
            for &c in active.iter() {
                unsafe { *row.get_unchecked_mut(c as usize) += 1 };
            }
        }
        flush(day, d_sessions, d_events, d_revenue, catrev, &mut users);
        for &c in active.iter() {
            cnt[c as usize] = 0;
        }
        active.clear();
    }
    prof(12, t);
    users
}

#[unsafe(no_mangle)]
#[inline(never)]
pub fn probe_ts_parse(s: &[u8]) -> i64 {
    ts::parse(s).unwrap_or(1)
}
