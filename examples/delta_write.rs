use anyhow::Error;
use arrow::array::Int64Array;
use arrow::{
    array::{Int32Array, StringArray},
    datatypes::{DataType, Field, Schema as ArrowSchema},
    record_batch::RecordBatch,
};
use chrono::{DateTime, Duration, NaiveDate, Utc};
use clap::{Args, Command};
use deltalake::DeltaTableBuilder;
use deltalake::{protocol::SaveMode, DeltaOps};
use fake::faker::name::raw::*;
use fake::locales::*;
use fake::Fake;
use std::sync::Arc;
use std::{collections::HashMap, ops::Add};

#[derive(Debug, Args)]
struct DSOption {
    #[arg(long, help = "Table uri")]
    uri: String,

    #[arg(long, help = "S3 endpoint")]
    endpoint: Option<String>,

    #[arg(long, help = "S3 accesskey")]
    accesskey: Option<String>,

    #[arg(long, help = "S3 secretkey")]
    secretkey: Option<String>,

    #[arg(long, help = "S3 region")]
    region: Option<String>,

    #[arg(long, help = "Start date")]
    date: Option<String>,

    #[arg(long, help = "Partitions")]
    partitions: Option<String>,

    #[arg(long, help = "Minutes")]
    minutes: i32,

    #[arg(long, help = "Ranges")]
    ranges: i64,

    #[arg(long, help = "Records")]
    records: usize,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Error> {
    let cmd = Command::new("DeltaTable");
    let cmd = DSOption::augment_args(cmd);
    let args = cmd.get_matches();

    let uri = args.get_one::<String>("uri").unwrap();
    let minutes = args.get_one::<i32>("minutes").unwrap();
    let ranges = args.get_one::<i64>("ranges").unwrap();
    let records = args.get_one::<usize>("records").unwrap();

    let mut storage_options = HashMap::<String, String>::new();
    if let Some(endpoint) = args.get_one::<String>("endpoint") {
        storage_options.insert("AWS_ENDPOINT_URL".to_string(), endpoint.clone());
    }
    if let Some(accesskey) = args.get_one::<String>("accesskey") {
        storage_options.insert("AWS_ACCESS_KEY_ID".to_string(), accesskey.clone());
    }
    if let Some(secretkey) = args.get_one::<String>("secretkey") {
        storage_options.insert("AWS_SECRET_ACCESS_KEY".to_string(), secretkey.clone());
    }
    if let Some(region) = args.get_one::<String>("region") {
        storage_options.insert("AWS_REGION".to_string(), region.clone());
    }
    storage_options.insert("AWS_S3_ALLOW_UNSAFE_RENAME".to_string(), "true".to_string());
    storage_options.insert("AWS_ALLOW_HTTP".to_string(), "true".to_string());

    let mut table = DeltaTableBuilder::from_uri(&uri)
        .with_storage_options(storage_options)
        .build()
        .unwrap();
    let _ = table.load().await;

    let mut datetime = if let Some(date) = args.get_one::<String>("date") {
        let naive_date = NaiveDate::parse_from_str(date, "%Y-%m-%d").unwrap();
        let naive_datetime = naive_date.and_hms_opt(0, 0, 0).unwrap();
        DateTime::<Utc>::from_naive_utc_and_offset(naive_datetime, Utc)
    } else {
        Utc::now()
    };

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("date", DataType::Utf8, false),
        Field::new("hour", DataType::Utf8, false),
        Field::new("timestamp", DataType::Int64, false),
        Field::new("company", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("score", DataType::Int32, false),
    ]));

    for _ in 0..*minutes {
        let date = datetime.format("%Y-%m-%d").to_string();
        let hour = datetime.format("%H").to_string();
        let timestamp = datetime.timestamp_millis();

        let mut companies = Vec::<String>::new();
        let mut names = Vec::<String>::new();
        let mut scores = Vec::<i32>::new();

        for _ in 0..*records {
            let company = format!("{}{}", "it", (0..*ranges).fake::<i64>());
            let name = Name(EN).fake::<String>();
            let score = (0..).fake::<i32>();

            companies.push(company);
            names.push(name);
            scores.push(score);
        }

        let dates = Arc::new(StringArray::from(vec![date; *records]));
        let hours = Arc::new(StringArray::from(vec![hour; *records]));
        let timestamps = Arc::new(Int64Array::from(vec![timestamp; *records]));
        let companies = Arc::new(StringArray::from(companies));
        let names = Arc::new(StringArray::from(names));
        let scores = Arc::new(Int32Array::from(scores));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                dates.clone(),
                hours.clone(),
                timestamps.clone(),
                companies.clone(),
                names.clone(),
                scores.clone(),
            ],
        )
        .unwrap();

        let mut builder = DeltaOps(table)
            .write(vec![batch])
            .with_target_file_size(1024 * 1024 * 1024 * 4);
        builder = builder.with_save_mode(SaveMode::Append);
        if let Some(partitions) = args.get_one::<String>("partitions") {
            builder = builder.with_partition_columns(partitions.split(","));
        }

        table = builder.await?;

        datetime = datetime.add(Duration::minutes(1));
    }

    Ok(())
}
