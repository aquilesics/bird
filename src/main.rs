#![allow(unused_imports, unused_must_use, dead_code)]

use chrono::{Days, NaiveDate, NaiveDateTime};
use polars::{prelude::*, lazy::dsl::{as_struct, GetOutput}, export::{arrow::io::parquet::read::ParquetError, regex::Error}};
use rand::Rng;
use std::fs::File;

const POS: [&str; 5] = ["05", "07", "10", "81", "01"];
const MCC: [&str; 9] = [
    "3212", "1233", "1122", "3331", "2342", "3494", "2340", "9902", "3034",
];

fn main() {

    // #TODO !!
    // let df = LazyFrame::scan_parquet("table.parquet", Default::default());

    // let out = df
    //     .unwrap()
    //     .sort("dttime_ts",Default::default())
    //     .groupby_stable([col("crt")])
    //     .agg( [ 
    //         as_struct(&[col("crt")]).apply(|s|test, GetOutput::from_type(DataType::Utf8))
    //          ]);

    // println!("{}", out.collect().unwrap());
}

struct Rule {
    qtd: u64,
    ts: f64,
    vlr: f64,
}


impl Rule {
    fn r1(self, _df: &DataFrame) -> String {
        "teste".to_string()
    }
}

// # TODO
// fn test() -> Result<String,String> {
//     Ok:<T,E>("sd".to_string());
//     Err("error".to_string())

// }
fn export() {
    let mut df = fake_aut();
    let file = File::create("table.parquet").expect("error on create file");
    ParquetWriter::new(file).finish(&mut df).unwrap();

    let file = File::create("table.csv").expect("error on create file");
    CsvWriter::new(file).finish(&mut df).unwrap();
}

fn fake_aut<'a>() -> DataFrame {
    let mut rng = rand::thread_rng();

    const SIZE_DF: usize = 20_000_000;

    let mut tmp_vlr = vec![];
    let dt_start = NaiveDate::from_ymd_opt(2023, 01, 01)
        .unwrap()
        .and_hms_opt(01, 01, 01)
        .unwrap();
    let dt_end = dt_start.checked_add_days(Days::new(90)).unwrap();
    let dt_start_ts: i64 = dt_start.timestamp();
    let dt_end_ts: i64 = dt_end.timestamp();
    let mut tmp_ts: Vec<f64> = vec![];
    let mut tmp_dt: Vec<NaiveDateTime> = vec![];
    let mut tmp_pos: Vec<&str> = vec![];
    let mut tmp_mcc: Vec<&str> = vec![];
    let mut tmp_ec: Vec<String> = vec![];
    let mut tmp_ec_id: Vec<String> = vec![];
    let mut tmp_crt: Vec<String> = vec![];
    let mut tmp_score: Vec<u64> = vec![];
    // let mut c1:[u64;SIZE_DF] = [064; SIZE_DF ];
    // rng.fill(&mut c1[..]);

    for _i in 1..=SIZE_DF {
        let vlr: f64 = rng.gen_range(100. ..10_000.);
        let ts = rng.gen_range(dt_start_ts..dt_end_ts);
        let dt = NaiveDateTime::from_timestamp_opt(ts, 0);
        &tmp_pos.push(POS[rng.gen_range(1..POS.len())]);
        &tmp_mcc.push(MCC[rng.gen_range(1..MCC.len())]);
        &tmp_ec_id.push(rng.gen_range(100000..999999).to_string());
        &tmp_ec.push("ec_teste".to_string());
        &tmp_crt.push(
            rng.gen_range(41873777_u64 + 0_u64..41873777_u64 + 10_000_u64)
                .to_string(),
        );
        &tmp_score.push(rng.gen_range(0..100));
        &tmp_vlr.push((vlr * 100.).round() / 100.0);
        &tmp_ts.push(ts as f64 / (24 * 3600) as f64);
        &tmp_dt.push(dt.unwrap());
    }

    let df = df!(
       "vlr"        => &tmp_vlr,
       "dttime_ts"  => &tmp_ts,
       "dttime"     => &tmp_dt,
       "score"      => &tmp_score,
       "crt"        => &tmp_crt,
       "ec"         => &tmp_ec,
       "ec_id"      => &tmp_ec_id,
       "mcc"        => &tmp_mcc,
       "pos"        => &tmp_pos
    )
    .unwrap()
    .with_row_count("row_number", None)
    .unwrap();

    df
}
