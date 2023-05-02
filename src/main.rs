#![allow(unused_imports, unused_must_use)]

use std::fs::File;
use chrono::{NaiveDate,NaiveDateTime};
use polars::prelude::*;
use rand::Rng;

fn main() {
    let mut rng = rand::thread_rng();

    // let vlr = Series::new("VLR", &[1, 2, 3]);

    // let mut df = DataFrame::new(vec![vlr]).unwrap();

    const _SIZE: usize = 500;

    let file = File::create("table.parquet").expect("error on create file");
    let mut tmp_vlr = vec![];
    let dt_start = NaiveDate::from_ymd_opt(2023, 01, 01)
        .unwrap()
        .and_hms_opt(01, 01, 01)
        .unwrap();

    let dt_end = dt_start.checked_add_days( NaiveDate: 90 ).unwrap()
    let mut tmp_ts = vec![];
    let mut tmp_dt = vec![];

    for _i in 1..=500 {
        let vlr: f64 = rng.gen_range(100. ..10_000.);
        let ts = rng.gen_range(1675212761.70698..1682988761.70698);
        let dt = NaiveDateTime::from_timestamp(ts, 0)

        &tmp_vlr.push((vlr * 100.).round() / 100.0);
        &tmp_ts.push(ts);


    }

    // let mut i_array = [0u64; SIZE];
    // let mut f_array = [0f64; SIZE];

    // rng.fill(&mut i_array[..]);
    // rng.fill(&mut f_array[..]);

    // let s1 = Series::new("ivlr", &tmp_vlr  ) ;
    // let s2 = Series::new("fvlr", &tmp_vlr ) ;
    // let vlr = s1 + s2;

    let mut df = df!("vlr" => &tmp_vlr,
                                "ts"  => &tmp_ts )
    .unwrap();
    ParquetWriter::new(file).finish(&mut df).unwrap();
}

// struct Transaction {
//     crt: u64,
//     ec: u64,
//     ec_nome: String,
//     dttime_aut: String,
//     score: u64,
//     pos: String,
//     acquirer: u64,
//     bandeira: String,
//     name: String,
//     c1: String,
//     c2: String,
//     c3: String,
//     c4: u64,
//     c5: u64,
//     c6: u64,
//     c7: u64,
//     c8: u64,
//     c9: u64,
//     c10: u64,
//     c11: u64,
//     c12: u64,
// }

// impl Transaction {
//     fn new(&self) -> Transaction {
//         Transaction {
//             crt: (),
//             ec: (),
//             ec_nome: (),
//             dttime_aut: (),
//             score: (),
//             pos: (),
//             acquirer: (),
//             bandeira: (),
//             name: (),
//             c1: (),
//             c2: (),
//             c3: (),
//             c4: (),
//             c5: (),
//             c6: (),
//             c7: (),
//             c8: (),
//             c9: (),
//             c10: (),
//             c11: (),
//             c12: (),
//         }
//     }
// }