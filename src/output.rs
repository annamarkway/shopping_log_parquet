//! Writes the two tables as Parquet.

use std::fs::{self, File};
use std::path::Path;
use std::sync::Arc;

use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;

use crate::{CampAgg, Ctx, UserRow, id};

fn props() -> Arc<WriterProperties> {
    Arc::new(WriterProperties::builder().set_dictionary_enabled(false).build())
}

struct Writer<'a> {
    rg: parquet::file::writer::SerializedRowGroupWriter<'a, File>,
}

impl Writer<'_> {
    fn bytes(&mut self, v: &[ByteArray], def: Option<&[i16]>) {
        let mut c = self.rg.next_column().unwrap().unwrap();
        c.typed::<ByteArrayType>().write_batch(v, def, None).unwrap();
        c.close().unwrap();
    }
    fn i32(&mut self, v: &[i32]) {
        let mut c = self.rg.next_column().unwrap().unwrap();
        c.typed::<Int32Type>().write_batch(v, None, None).unwrap();
        c.close().unwrap();
    }
    fn i64(&mut self, v: &[i64]) {
        let mut c = self.rg.next_column().unwrap().unwrap();
        c.typed::<Int64Type>().write_batch(v, None, None).unwrap();
        c.close().unwrap();
    }
}

pub fn write_all(ctx: &Ctx, out: &Path, users: &[Vec<UserRow>], camp: &[CampAgg]) {
    let ud = out.join("user_daily");
    let cd = out.join("campaign_daily");
    fs::create_dir_all(&ud).unwrap();
    fs::create_dir_all(&cd).unwrap();
    use rayon::prelude::*;
    // Row order and file count do not matter: one file per group of partitions.
    let pieces = 16;
    let per = users.len().div_ceil(pieces).max(1);
    rayon::join(
        || {
            users.par_chunks(per).enumerate().for_each(|(i, group)| {
                let rows: Vec<&UserRow> = group.iter().flatten().collect();
                write_users(ctx, &ud.join(format!("part-{i}.parquet")), &rows)
            });
            if users.is_empty() {
                write_users(ctx, &ud.join("part-0.parquet"), &[]);
            }
        },
        || write_camps(ctx, &cd.join("part-0.parquet"), camp),
    );
}

fn write_users(ctx: &Ctx, path: &Path, users: &[&UserRow]) {
    let schema = Arc::new(
        parse_message_type(
            "message user_daily {
                REQUIRED BYTE_ARRAY user_id (UTF8);
                REQUIRED INT32 day (DATE);
                REQUIRED INT64 sessions;
                REQUIRED INT64 events;
                REQUIRED INT64 revenue_cents;
                OPTIONAL BYTE_ARRAY top_category (UTF8);
            }",
        )
        .unwrap(),
    );
    let names: Vec<ByteArray> = users.iter().map(|u| ByteArray::from(id::text(u.person))).collect();
    let days: Vec<i32> = users.iter().map(|u| u.day).collect();
    let sessions: Vec<i64> = users.iter().map(|u| u.sessions as i64).collect();
    let events: Vec<i64> = users.iter().map(|u| u.events as i64).collect();
    let revenue: Vec<i64> = users.iter().map(|u| u.revenue as i64).collect();
    let cat_bytes: Vec<ByteArray> = ctx.cat_names.iter().map(|c| ByteArray::from(c.clone())).collect();
    let tops: Vec<ByteArray> = users.iter().filter_map(|u| u.top.map(|c| cat_bytes[c as usize].clone())).collect();
    let def: Vec<i16> = users.iter().map(|u| u.top.is_some() as i16).collect();

    let file = File::create(path).unwrap();
    let mut w = SerializedFileWriter::new(file, schema, props()).unwrap();
    {
    let mut g = Writer { rg: w.next_row_group().unwrap() };
    g.bytes(&names, None);
    g.i32(&days);
    g.i64(&sessions);
    g.i64(&events);
    g.i64(&revenue);
    g.bytes(&tops, Some(&def));
    g.rg.close().unwrap();
    }
    w.close().unwrap();
}

fn write_camps(ctx: &Ctx, path: &Path, camp: &[CampAgg]) {
    let schema = Arc::new(
        parse_message_type(
            "message campaign_daily {
                REQUIRED BYTE_ARRAY campaign_id (UTF8);
                REQUIRED BYTE_ARRAY channel (UTF8);
                REQUIRED INT32 day (DATE);
                REQUIRED INT64 sessions;
                REQUIRED INT64 purchasing_sessions;
                REQUIRED INT64 revenue_cents;
                REQUIRED INT64 assisted_sessions;
            }",
        )
        .unwrap(),
    );
    let (mut ids, mut chans, mut days) = (Vec::new(), Vec::new(), Vec::new());
    let mut cols: [Vec<i64>; 4] = Default::default();
    for (c, (cid, ch)) in ctx.camp_ids.iter().zip(&ctx.camp_channels).enumerate() {
        for d in 0..ctx.ndays {
            let a = camp[c * ctx.ndays + d];
            if a[0] == 0 && a[3] == 0 {
                continue;
            }
            ids.push(ByteArray::from(cid.clone()));
            chans.push(ByteArray::from(ch.clone()));
            days.push((ctx.day_base + d as i64) as i32);
            for k in 0..4 {
                cols[k].push(a[k] as i64);
            }
        }
    }
    let file = File::create(path).unwrap();
    let mut w = SerializedFileWriter::new(file, schema, props()).unwrap();
    {
    let mut g = Writer { rg: w.next_row_group().unwrap() };
    g.bytes(&ids, None);
    g.bytes(&chans, None);
    g.i32(&days);
    for c in &cols {
        g.i64(c);
    }
    g.rg.close().unwrap();
    }
    w.close().unwrap();
}
