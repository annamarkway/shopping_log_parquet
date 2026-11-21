//! Shopping Sessions pipeline: rules.md, end to end.

mod csv;
mod id;
mod output;
mod ts;

use std::fs::File;
use std::path::{Path, PathBuf};
use std::time::Instant;

use memmap2::Mmap;
use rayon::prelude::*;
use rustc_hash::FxHashMap;

use id::Id;
use ts::DAY;

const SESSION_GAP: i64 = 1_800_000;
const CLICK_REACH: i64 = 7 * DAY;
/// Partitions by event_id (dedup) and by person (sessions).
const P: usize = 256;
const Q: usize = 256;
const CHUNK: usize = 4 << 20;

pub struct Ctx {
    pub ws: i64,
    pub we: i64,
    pub day_base: i64,
    pub ndays: usize,
    pub products: FxHashMap<Id, u8>,
    pub unknown_cat: u8,
    /// Category names sorted byte-wise, so a smaller index is a smaller name.
    pub cat_names: Vec<Vec<u8>>,
    pub campaigns: FxHashMap<Id, u16>,
    /// Index `ncamp` is `organic`.
    pub camp_ids: Vec<Vec<u8>>,
    pub camp_channels: Vec<Vec<u8>>,
    pub links: FxHashMap<Id, Id>,
}

impl Ctx {
    fn organic(&self) -> u16 {
        (self.camp_ids.len() - 1) as u16
    }
}

#[derive(Clone, Copy)]
struct Ev {
    eid: Id,
    person: Id,
    ingest: i64,
    ts: i64,
    price: u32,
    cat: u8,
    /// 0 invalid, 1 view or cart, 2 purchase.
    kind: u8,
}

#[derive(Clone, Copy)]
pub struct Rec {
    person: Id,
    ts: i64,
    price: u32,
    cat: u8,
    purchase: bool,
}

#[derive(Clone, Copy)]
struct Click {
    person: Id,
    cid: Id,
    ts: i64,
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

fn main() {
    let t0 = Instant::now();
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into()));
    let out = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into()));
    if let Ok(n) = std::env::var("NCPU").map(|v| v.parse::<usize>().unwrap_or(0)) {
        if n > 0 {
            rayon::ThreadPoolBuilder::new().num_threads(n).build_global().ok();
        }
    }

    let ctx = load_ctx(&data);
    eprintln!("[{:?}] lookups and links: {} linked ids", t0.elapsed(), ctx.links.len());

    let event_maps = map_dir(&data.join("events"));
    let click_maps = map_dir(&data.join("clicks"));

    let (recs, clicks) = rayon::join(
        || events_by_person(&ctx, &event_maps),
        || clicks_by_person(&ctx, &click_maps),
    );
    eprintln!("[{:?}] parsed, deduped, partitioned", t0.elapsed());

    let results: Vec<(Vec<UserRow>, Vec<CampAgg>)> = (0..Q)
        .into_par_iter()
        .map(|q| sessions_for_partition(&ctx, &recs, &clicks, q))
        .collect();
    drop(recs);
    drop(clicks);
    eprintln!("[{:?}] sessions", t0.elapsed());

    let ncamp = ctx.camp_ids.len();
    let mut camp = vec![[0u64; 4]; ncamp * ctx.ndays];
    let mut users: Vec<UserRow> = Vec::new();
    for (u, c) in results {
        users.extend(u);
        for (acc, v) in camp.iter_mut().zip(c) {
            for k in 0..4 {
                acc[k] += v[k];
            }
        }
    }
    output::write_all(&ctx, &out, &users, &camp);
    eprintln!("[{:?}] done: {} user rows", t0.elapsed(), users.len());
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
    unsafe { Mmap::map(&f).ok() }
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
    let day_base = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - day_base + 1) as usize;

    // Products and categories.
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
    let products = pairs.iter().map(|(p, c)| (id::key(p), cat_index[c])).collect();

    // Campaigns.
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

    let links = load_links(data);
    Ctx {
        ws,
        we,
        day_base,
        ndays,
        products,
        unknown_cat,
        cat_names,
        campaigns,
        camp_ids,
        camp_channels,
        links,
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
fn load_links(data: &Path) -> FxHashMap<Id, Id> {
    let maps = map_dir(&data.join("links"));
    let parts: Vec<Vec<(Id, Id)>> = chunks(&maps)
        .par_iter()
        .map(|c| {
            let mut v = Vec::new();
            csv::for_each_line(c.bytes, c.first, |line| {
                if let Some([a, b]) = csv::split::<2>(line) {
                    if !a.is_empty() && !b.is_empty() {
                        v.push((id::key_kept(a), id::key_kept(b)));
                    }
                }
            });
            v
        })
        .collect();

    let mut index: FxHashMap<Id, u32> = FxHashMap::default();
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
    let mut links = FxHashMap::default();
    links.reserve(n);
    for i in 0..n {
        let r = find(&mut parent, i as u32) as usize;
        links.insert(ids[i], name[r].unwrap());
    }
    links
}

#[inline(always)]
fn person_of(ctx: &Ctx, user: Id) -> Id {
    ctx.links.get(&user).copied().unwrap_or(user)
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
            // Keep scanning would only grow it; but a later non-digit must
            // still not matter: any value over the limit is bad either way.
            return None;
        }
    }
    if v >= 1 { Some(v as u32) } else { None }
}

fn parse_event(ctx: &Ctx, line: &[u8]) -> Option<Ev> {
    let [eid, user, tsf, ingest, kind, product, price] = csv::split::<7>(line)?;
    if eid.is_empty() {
        return None;
    }
    let eid = id::key(eid);
    let ingest = ts::parse(ingest).unwrap_or(i64::MAX);
    let mut ev = Ev { eid, person: 0, ingest, ts: 0, price: 0, cat: 0, kind: 0 };
    let valid = (|| {
        if user.is_empty() || product.is_empty() {
            return None;
        }
        let t = ts::parse(tsf)?;
        if t < ctx.ws || t >= ctx.we {
            return None;
        }
        let k = match kind {
            b"view" | b"cart" => 1,
            b"purchase" => 2,
            _ => return None,
        };
        if k == 2 {
            ev.price = parse_price(price)?;
        }
        if ingest == i64::MAX || ingest - t > DAY {
            return None;
        }
        ev.ts = t;
        ev.kind = k;
        ev.cat = ctx.products.get(&id::key(product)).copied().unwrap_or(ctx.unknown_cat);
        ev.person = person_of(ctx, id::key_kept(user));
        Some(())
    })();
    if valid.is_none() {
        ev.kind = 0;
    }
    Some(ev)
}

/// Valid, deduplicated events, as `[eid partition][person partition]`.
fn events_by_person(ctx: &Ctx, maps: &[Mmap]) -> Vec<Vec<Vec<Rec>>> {
    let parsed: Vec<Vec<Vec<Ev>>> = chunks(maps)
        .par_iter()
        .map(|c| {
            let mut parts: Vec<Vec<Ev>> = (0..P).map(|_| Vec::new()).collect();
            csv::for_each_line(c.bytes, c.first, |line| {
                if let Some(ev) = parse_event(ctx, line) {
                    parts[(id::mix(ev.eid) as usize) % P].push(ev);
                }
            });
            parts
        })
        .collect();

    (0..P)
        .into_par_iter()
        .map(|p| {
            let mut v: Vec<Ev> = Vec::with_capacity(parsed.iter().map(|c| c[p].len()).sum());
            for c in &parsed {
                v.extend_from_slice(&c[p]);
            }
            v.sort_unstable_by_key(|e| (e.eid, e.ingest));
            let mut out: Vec<Vec<Rec>> = (0..Q).map(|_| Vec::new()).collect();
            let mut i = 0;
            while i < v.len() {
                let e = v[i];
                let mut j = i + 1;
                while j < v.len() && v[j].eid == e.eid {
                    j += 1;
                }
                if e.kind != 0 {
                    out[(id::mix(e.person) >> 32) as usize % Q].push(Rec {
                        person: e.person,
                        ts: e.ts,
                        price: e.price,
                        cat: e.cat,
                        purchase: e.kind == 2,
                    });
                }
                i = j;
            }
            out
        })
        .collect()
}

// ---------------------------------------------------------------- clicks

fn parse_click(ctx: &Ctx, line: &[u8]) -> Option<Click> {
    let [cid, user, camp, tsf] = csv::split::<4>(line)?;
    if cid.is_empty() || user.is_empty() {
        return None;
    }
    let t = ts::parse(tsf)?;
    if t < ctx.ws - CLICK_REACH || t >= ctx.we {
        return None;
    }
    let camp = *ctx.campaigns.get(&id::key(camp))?;
    Some(Click {
        person: person_of(ctx, id::key_kept(user)),
        cid: id::key_kept(cid),
        ts: t,
        camp,
    })
}

/// Valid clicks (duplicates included), as `[chunk][person partition]`.
fn clicks_by_person(ctx: &Ctx, maps: &[Mmap]) -> Vec<Vec<Vec<Click>>> {
    chunks(maps)
        .par_iter()
        .map(|c| {
            let mut parts: Vec<Vec<Click>> = (0..Q).map(|_| Vec::new()).collect();
            csv::for_each_line(c.bytes, c.first, |line| {
                if let Some(k) = parse_click(ctx, line) {
                    parts[(id::mix(k.person) >> 32) as usize % Q].push(k);
                }
            });
            parts
        })
        .collect()
}

// ---------------------------------------------------------------- sessions

fn sessions_for_partition(
    ctx: &Ctx,
    recs: &[Vec<Vec<Rec>>],
    clicks: &[Vec<Vec<Click>>],
    q: usize,
) -> (Vec<UserRow>, Vec<CampAgg>) {
    let mut ev: Vec<Rec> = Vec::with_capacity(recs.iter().map(|r| r[q].len()).sum());
    for r in recs {
        ev.extend_from_slice(&r[q]);
    }
    ev.sort_unstable_by_key(|e| (e.person, e.ts));

    let mut ck: Vec<Click> = Vec::with_capacity(clicks.iter().map(|c| c[q].len()).sum());
    for c in clicks {
        ck.extend_from_slice(&c[q]);
    }
    // Person, then time, then click_id descending: the last click at or before
    // a session start is the latest, with the smallest click_id on a tie.
    ck.sort_unstable_by(|a, b| {
        a.person.cmp(&b.person).then(a.ts.cmp(&b.ts)).then_with(|| id::cmp(b.cid, a.cid))
    });
    ck.dedup_by(|b, a| a.person == b.person && a.cid == b.cid);

    let ncamp = ctx.camp_ids.len();
    let organic = ctx.organic() as usize;
    let mut camp = vec![[0u64; 4]; ncamp * ctx.ndays];
    let mut users = Vec::new();
    let mut stamp = vec![0u32; ncamp];
    let mut gen_ = 0u32;
    let ncat = ctx.cat_names.len();
    let mut catrev = vec![0u64; ncat];

    let mut ci = 0; // first click of the current person
    let mut i = 0;
    while i < ev.len() {
        let person = ev[i].person;
        let mut end = i;
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

        // Day accumulator.
        let mut day = i64::MIN;
        let (mut d_sessions, mut d_events, mut d_revenue) = (0u64, 0u64, 0u64);
        let mut flush = |day: i64, s: u64, e: u64, r: u64, catrev: &mut [u64], users: &mut Vec<UserRow>| {
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
            let sday = start.div_euclid(DAY);
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
                if e.purchase {
                    bought = true;
                    rev += e.price as u64;
                    catrev[e.cat as usize] += e.price as u64;
                }
                k += 1;
            }
            d_sessions += 1;
            d_events += n;
            d_revenue += rev;

            // Attribution and assists.
            while r < pc.len() && pc[r].ts <= start {
                r += 1;
            }
            while l < r && pc[l].ts < start - CLICK_REACH {
                l += 1;
            }
            let di = (sday - ctx.day_base) as usize;
            let credited = if l < r { pc[r - 1].camp as usize } else { organic };
            let a = &mut camp[credited * ctx.ndays + di];
            a[0] += 1;
            a[1] += bought as u64;
            a[2] += rev;
            gen_ += 1;
            for c in &pc[l..r] {
                let c = c.camp as usize;
                if stamp[c] != gen_ {
                    stamp[c] = gen_;
                    camp[c * ctx.ndays + di][3] += 1;
                }
            }
        }
        flush(day, d_sessions, d_events, d_revenue, &mut catrev, &mut users);
        ci = cend;
        i = end;
    }
    (users, camp)
}
