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
const P: usize = 512;
const Q: usize = 512;
const CHUNK: usize = 2 << 20;
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
    pub links: IdMap,
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

    let event_maps = map_dir(&data.join("events"));
    if let Ok(mode) = std::env::var("BENCH") {
        let cs = chunks(&event_maps);
        for _ in 0..3 {
            let t = Instant::now();
            let n: usize = match mode.as_str() {
                "lines" => cs.par_iter().map(|c| { let mut n = 0; csv::for_each_line(c.bytes, c.first, |_| n += 1); n }).sum(),
                "split" => cs.par_iter().map(|c| { let mut n = 0; csv::for_each_line(c.bytes, c.first, |l| if csv::split::<7>(l).is_some() { n += 1 }); n }).sum(),
                "scan" => cs.par_iter().map(|c| { let mut n = 0; csv::scan::<7>(c.bytes, c.first, |_| n += 1); n }).sum(),
                "ts" => cs.par_iter().map(|c| { let mut n = 0; csv::for_each_line(c.bytes, c.first, |l| if let Some(f) = csv::split::<7>(l) { if ts::parse(f[2]).is_some() { n += 1 } if ts::parse(f[3]).is_some() { n += 1 } }); n }).sum(),
                _ => cs.par_iter().map(|c| c.bytes.iter().map(|&b| b as usize).sum::<usize>()).sum(),
            };
            eprintln!("bench {mode}: {n} in {:?}", t.elapsed());
        }
        return;
    }
    let click_maps = map_dir(&data.join("clicks"));
    log!("mapped");
    let ctx = load_ctx(&data);
    log!("lookups and links: {} link slots", ctx.links.len_hint());

    let (recs, clicks) = rayon::join(
        || events_by_person(&ctx, &event_maps),
        || {
            chunks(&click_maps)
                .par_iter()
                .map(|c| parse_clicks(&ctx, c))
                .collect::<Vec<_>>()
        },
    );
    log!("events and clicks partitioned by person");

    let results: Vec<(Vec<UserRow>, Vec<CampAgg>)> =
        (0..Q).into_par_iter().map(|q| sessions_for_partition(&ctx, &recs, &clicks, q)).collect();
    log!("sessions");

    let ncamp = ctx.camp_ids.len();
    let mut camp = vec![[0u64; 4]; ncamp * ctx.ndays];
    let mut users: Vec<UserRow> = Vec::with_capacity(results.iter().map(|r| r.0.len()).sum());
    for (u, c) in results {
        users.extend(u);
        for (acc, v) in camp.iter_mut().zip(c) {
            for k in 0..4 {
                acc[k] += v[k];
            }
        }
    }
    output::write_all(&ctx, &out, &users, &camp);
    log!("done: {} user rows", users.len());
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
    unsafe { MmapOptions::new().populate().map(&f).ok() }
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

    let (links, (product_dense, products, unknown_cat, cat_names, campaigns, camp_ids, camp_channels)) =
        rayon::join(|| load_links(data), || load_lookups(data));
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
        links,
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
        .par_iter()
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
        let cat = product_cat(ctx, product);
        ev.person = id::key_kept(user);
        ev.ts = (t - ctx.base) as u32;
        ev.meta = price | (cat as u32) << 24;
        Some(())
    })();
    Some(ev)
}

/// Surviving events, as one person-partitioned set per event_id partition.
fn events_by_person(ctx: &Ctx, maps: &[Mmap]) -> Vec<Parted<Rec>> {
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

    let out = (0..P)
        .into_par_iter()
        .map(|p| {
            let mut v = gather(&parsed, p);
            v.sort_unstable_by_key(|e| (e.eid, e.ingest));
            let mut recs: Vec<Rec> = Vec::with_capacity(v.len());
            let mut i = 0;
            while i < v.len() {
                let e = v[i];
                i += 1;
                while i < v.len() && v[i].eid == e.eid {
                    i += 1;
                }
                if e.ts != INVALID {
                    recs.push(Rec { person: e.person, ts: e.ts, meta: e.meta });
                }
            }
            ctx.links.resolve_all(&mut recs, |r| &mut r.person);
            Parted::new(&recs, Q, |r| person_part(r.person))
        })
        .collect();
    std::mem::forget(parsed);
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

/// Valid clicks of one chunk (duplicates included), partitioned by person.
fn parse_clicks(ctx: &Ctx, c: &Chunk) -> Parted<Click> {
    let mut v: Vec<Click> = Vec::with_capacity(c.bytes.len() / 40);
    csv::scan::<4>(c.bytes, c.first, |f| {
        if let Some(k) = parse_click(ctx, f) {
            v.push(k);
        }
    });
    ctx.links.resolve_all(&mut v, |k| &mut k.person);
    Parted::new(&v, Q, |k| person_part(k.person))
}

// ---------------------------------------------------------------- sessions

fn sessions_for_partition(
    ctx: &Ctx,
    recs: &[Parted<Rec>],
    clicks: &[Parted<Click>],
    q: usize,
) -> (Vec<UserRow>, Vec<CampAgg>) {
    let mut ev = gather(recs, q);
    ev.sort_unstable_by_key(|e| (e.person, e.ts));

    let mut ck = gather(clicks, q);
    // Person, then time, then click_id descending: the last click at or before
    // a session start is the latest, with the smallest click_id on a tie.
    ck.sort_unstable_by(|a, b| {
        a.person.cmp(&b.person).then(a.ts.cmp(&b.ts)).then_with(|| id::cmp(b.cid, a.cid))
    });
    ck.dedup_by(|b, a| a.person == b.person && a.cid == b.cid);

    let ncamp = ctx.camp_ids.len();
    let nd = ctx.ndays;
    let organic = ncamp - 1;
    let reach = CLICK_REACH as u32;
    let mut camp = vec![[0u64; 4]; ncamp * nd];
    let mut users = Vec::new();
    let mut stamp = vec![0u32; ncamp];
    let mut gen_ = 0u32;
    let mut catrev = vec![0u64; ctx.cat_names.len()];

    let mut ci = 0; // first click of the current person
    let mut i = 0;
    while i < ev.len() {
        let person = ev[i].person;
        let mut end = i + 1;
        while end < ev.len() && ev[end].person == person {
            end += 1;
        }
        while ci < ck.len() && ck[ci].person < person {
            ci += 1;
        }
        let mut cend = ci;
        while cend < ck.len() && ck[cend].person == person {
            cend += 1;
        }
        let pc = &ck[ci..cend];

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
        let mut k = i;
        while k < end {
            let start = ev[k].ts;
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
            while k < end && ev[k].ts - last <= SESSION_GAP {
                let e = ev[k];
                last = e.ts;
                n += 1;
                let price = (e.meta & 0xFF_FFFF) as u64;
                if price > 0 {
                    bought = true;
                    rev += price;
                    catrev[(e.meta >> 24) as usize] += price;
                }
                k += 1;
            }
            d_sessions += 1;
            d_events += n;
            d_revenue += rev;

            // Attribution and assists: clicks in [start - 7 days, start].
            while r < pc.len() && pc[r].ts <= start {
                r += 1;
            }
            while l < r && pc[l].ts < start - reach {
                l += 1;
            }
            let di = (sday - ctx.day_base) as usize;
            let credited = if l < r { pc[r - 1].camp as usize } else { organic };
            let a = &mut camp[credited * nd + di];
            a[0] += 1;
            a[1] += bought as u64;
            a[2] += rev;
            gen_ += 1;
            for c in &pc[l..r] {
                let c = c.camp as usize;
                if stamp[c] != gen_ {
                    stamp[c] = gen_;
                    camp[c * nd + di][3] += 1;
                }
            }
        }
        flush(day, d_sessions, d_events, d_revenue, &mut catrev, &mut users);
        ci = cend;
        i = end;
    }
    (users, camp)
}
