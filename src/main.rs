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
    ts: u32,
    camp: u16,
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
                "scan" => cs.par_iter().map(|c| { let mut n = 0; csv::scan::<7>(c.bytes, c.first, |_| n += 1); n }).sum(),
                "parse" => cs.par_iter().map(|c| { let mut n = 0u64; csv::scan::<7>(c.bytes, c.first, |f| if let Some(e) = parse_event(&ctx, f) { n = n.wrapping_add(e.eid ^ e.person ^ e.ts as u64) }); n as usize }).sum(),
                "keys" => cs.par_iter().map(|c| { let mut n = 0u64; csv::scan::<7>(c.bytes, c.first, |f| { n = n.wrapping_add(id::key(f[0]) ^ id::key(f[1])) }); n as usize }).sum(),
                "tsonly" => cs.par_iter().map(|c| { let mut n = 0i64; csv::scan::<7>(c.bytes, c.first, |f| { n = n.wrapping_add(ts::parse(f[2]).unwrap_or(0) ^ ts::parse(f[3]).unwrap_or(0)) }); n as usize }).sum(),
                "store" => cs.par_iter().map(|c| { let mut v: Vec<Ev> = Vec::with_capacity(c.bytes.len() / 64); csv::scan::<7>(c.bytes, c.first, |f| if let Some(e) = parse_event(&ctx, f) { v.push(e) }); v.len() }).sum(),
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
            let l = load_links(&data);
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
    let (recs, clicks) = rayon::join(
        || dedup_events(&links, parsed),
        || {
            raw_clicks
                .into_par_iter()
                .map(|mut v| {
                    links.resolve_all(&mut v, |k| &mut k.person);
                    Parted::new(&v, Q, |k| person_part(k.person))
                })
                .collect::<Vec<_>>()
        },
    );
    log!("events and clicks partitioned by person");

    let results: Vec<(Vec<UserRow>, Vec<CampAgg>)> =
        (0..Q).into_par_iter().map(|q| sessions_for_partition(&ctx, &recs, &clicks, q)).collect();
    log!("sessions");

    let (users, camps): (Vec<Vec<UserRow>>, Vec<Vec<CampAgg>>) = results.into_iter().unzip();
    let cells = ctx.camp_ids.len() * ctx.ndays;
    let camp = camps
        .into_par_iter()
        .reduce(
            || vec![[0u64; 4]; cells],
            |mut a, b| {
                for (x, y) in a.iter_mut().zip(&b) {
                    for k in 0..4 {
                        x[k] += y[k];
                    }
                }
                a
            },
        );
    output::write_all(&ctx, &out, &users, &camp);
    log!("done: {} user rows", users.iter().map(|u| u.len()).sum::<usize>());
    if std::env::var_os("SS_DEBUG").is_some() {
        if let Ok(t) = std::fs::read_to_string("/proc/self/smaps_rollup") {
            for l in t.lines().filter(|l| l.starts_with("Rss") || l.starts_with("AnonHuge") || l.starts_with("Anonymous")) {
                eprintln!("{l}");
            }
        }
    }
    // Skip freeing everything: the process is about to exit.
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
            let mut v = Vec::new();
            csv::scan::<2>(c.bytes, c.first, |[a, b]| {
                if !a.is_empty() && !b.is_empty() {
                    v.push((id::key_kept(a), id::key_kept(b)));
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
    let mut links = IdMap::with_capacity(n);
    for i in 0..n {
        let r = find(&mut parent, i as u32) as usize;
        let nm = name[r].unwrap();
        if nm != ids[i] {
            links.insert(ids[i], nm);
        }
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
fn parse_event(ctx: &Ctx, [eid, user, tsf, ingest, kind, product, price]: [&[u8]; 7]) -> Option<Ev> {
    if eid.is_empty() {
        return None;
    }
    let ingest = ts::parse(ingest).unwrap_or(i64::MAX);
    let mut ev = Ev { eid: id::key(eid), ingest, person: 0, ts: INVALID, meta: 0 };
    (|| {
        if user.is_empty() || product.is_empty() {
            return None;
        }
        let t = ts::parse(tsf)?;
        if t < ctx.ws || t >= ctx.we || ingest == i64::MAX || ingest - t > DAY {
            return None;
        }
        let price = match kind {
            b"view" | b"cart" => 0,
            b"purchase" => parse_price(price)?,
            _ => return None,
        };
        // The category only matters for purchases (top_category).
        let cat = if price > 0 { product_cat(ctx, product) } else { 0 };
        ev.person = id::key_kept(user);
        ev.ts = (t - ctx.base) as u32;
        ev.meta = price | (cat as u32) << 24;
        Some(())
    })();
    Some(ev)
}

/// Every event copy, per chunk, partitioned by event_id.
fn parse_events(ctx: &Ctx, maps: &[Mmap]) -> Vec<Parted<Ev>> {
    let parsed: Vec<Parted<Ev>> = chunks(maps)
        .par_iter()
        .map(|c| {
            let mut v: Vec<Ev> = Vec::with_capacity(c.bytes.len() / 64);
            csv::scan::<7>(c.bytes, c.first, |f| {
                if let Some(ev) = parse_event(ctx, f) {
                    v.push(ev);
                }
            });
            Parted::new(&v, P, |e| (id::mix(e.eid) >> 40) as usize % P)
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
            let v = gather(&parsed, p);
            // Open addressing on event_id: each slot holds the index of the
            // earliest-ingested copy seen so far (equal ingest_ts copies are identical).
            let size = (v.len() * 2).next_power_of_two().max(16);
            let mask = size - 1;
            let mut table = vec![u32::MAX; size];
            for (i, e) in v.iter().enumerate() {
                let mut h = (e.eid.wrapping_mul(0xD6E8_FEB8_6659_FD93) >> 32) as usize & mask;
                loop {
                    let t = table[h];
                    if t == u32::MAX {
                        table[h] = i as u32;
                        break;
                    }
                    let o = unsafe { v.get_unchecked(t as usize) };
                    if o.eid == e.eid {
                        if e.ingest < o.ingest {
                            table[h] = i as u32;
                        }
                        break;
                    }
                    h = (h + 1) & mask;
                }
            }
            // Mark the kept copies, then read the copies in order (the table
            // order is random, so reading through it misses the cache).
            let mut keep = vec![0u8; v.len()];
            for &k in &table {
                if k != u32::MAX {
                    unsafe { *keep.get_unchecked_mut(k as usize) = 1 };
                }
            }
            let mut recs: Vec<Rec> = Vec::with_capacity(v.len());
            for (e, &k) in v.iter().zip(&keep) {
                if k != 0 && e.ts != INVALID {
                    recs.push(Rec { person: e.person, ts: e.ts, meta: e.meta });
                }
            }
            links.resolve_all(&mut recs, |r| &mut r.person);
            Parted::new(&recs, Q, |r| person_part(r.person))
        })
        .collect();
    // Free in parallel so later stages reuse these pages instead of fresh, zeroed ones.
    parsed.into_par_iter().for_each(drop);
    out
}

// ---------------------------------------------------------------- clicks

#[inline(always)]
fn parse_click(ctx: &Ctx, [cid, user, camp, tsf]: [&[u8]; 4]) -> Option<Click> {
    if cid.is_empty() || user.is_empty() {
        return None;
    }
    let t = ts::parse(tsf)?;
    if t < ctx.base || t >= ctx.we {
        return None;
    }
    let camp = *ctx.campaigns.get(&id::key(camp))?;
    Some(Click { person: id::key_kept(user), cid: id::key_kept(cid), ts: (t - ctx.base) as u32, camp })
}

/// Valid clicks of one chunk (duplicates included), by user ID.
fn parse_clicks(ctx: &Ctx, c: &Chunk) -> Vec<Click> {
    let mut v: Vec<Click> = Vec::with_capacity(c.bytes.len() / 40);
    csv::scan::<4>(c.bytes, c.first, |f| {
        if let Some(k) = parse_click(ctx, f) {
            v.push(k);
        }
    });
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
    fn new(n: usize) -> Self {
        let size = (n * 2).next_power_of_two().max(16);
        Groups { keys: vec![u64::MAX; size], gid: vec![0; size], mask: size - 1, persons: Vec::new() }
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
            self.keys[h] = k;
            self.gid[h] = self.persons.len() as u32;
            self.persons.push(k);
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

fn sessions_for_partition(
    ctx: &Ctx,
    recs: &[Parted<Rec>],
    clicks: &[Parted<Click>],
    q: usize,
) -> (Vec<UserRow>, Vec<CampAgg>) {
    // Events grouped by person, each group sorted by ts. An event is packed as
    // ts << 32 | meta, so sorting the u64s sorts by ts.
    let raw = gather(recs, q);
    let mut groups = Groups::new(raw.len());
    let gids: Vec<u32> = raw.iter().map(|e| groups.insert(e.person)).collect();
    let ng = groups.persons.len();
    let (mut ev, eoff) = bucket(&raw, &gids, ng, |e| (e.ts as u64) << 32 | e.meta as u64);
    drop(raw);
    for g in 0..ng {
        ev[eoff[g] as usize..eoff[g + 1] as usize].sort_unstable();
    }

    // Clicks of the same persons (others can credit nothing), each group by
    // time, then click_id descending: the last click at or before a session
    // start is the latest, with the smallest click_id on a tie.
    let raw = gather(clicks, q);
    let cg: Vec<u32> = raw.iter().map(|k| groups.get(k.person).unwrap_or(u32::MAX)).collect();
    let (mut ck, coff) = bucket(&raw, &cg, ng, |k| *k);
    drop(raw);

    let ncamp = ctx.camp_ids.len();
    let nd = ctx.ndays;
    let organic = ncamp - 1;
    let reach = CLICK_REACH as u32;
    let mut camp = vec![[0u64; 4]; ncamp * nd];
    let mut users = Vec::new();
    // Clicks per campaign in the current 7-day window, and the campaigns with
    // at least one (`active`, with each one's index in it in `at`).
    let mut cnt = vec![0u32; ncamp];
    let mut at = vec![0u32; ncamp];
    let mut active: Vec<u16> = Vec::new();
    // Assisted sessions as [day][campaign]: one session's increments share a row.
    let mut asst = vec![0u32; nd * ncamp];
    let mut catrev = vec![0u64; ctx.cat_names.len()];

    for g in 0..ng {
        let person = groups.persons[g];
        let pe = &ev[eoff[g] as usize..eoff[g + 1] as usize];
        let pc = &mut ck[coff[g] as usize..coff[g + 1] as usize];
        pc.sort_unstable_by(|a, b| a.ts.cmp(&b.ts).then_with(|| id::cmp(b.cid, a.cid)));
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
            let mut best = 0u64;
            for (c, v) in catrev.iter_mut().enumerate() {
                if *v > best {
                    best = *v;
                    top = Some(c as u8);
                }
                *v = 0;
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
                flush(day, d_sessions, d_events, d_revenue, &mut catrev, &mut users);
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
            for &c in &active {
                unsafe { *row.get_unchecked_mut(c as usize) += 1 };
            }
        }
        flush(day, d_sessions, d_events, d_revenue, &mut catrev, &mut users);
        for &c in &active {
            cnt[c as usize] = 0;
        }
        active.clear();
    }
    for di in 0..nd {
        for c in 0..ncamp {
            camp[c * nd + di][3] += asst[di * ncamp + c] as u64;
        }
    }
    (users, camp)
}
