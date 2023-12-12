use arrow_cast::pretty::pretty_format_batches;
use clap::{Args, Command};
use deltalake::datafusion::common::Result;
use deltalake::datafusion::execution::context::SessionContext;
use deltalake::delta_datafusion::{DeltaScanConfigBuilder, DeltaTableProvider};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

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

    #[arg(long, help = "Target query")]
    query: Option<String>,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), deltalake::DeltaTableError> {
    let cmd = Command::new("DatafusionQuery");
    let cmd = DSOption::augment_args(cmd);
    let args = cmd.get_matches();

    let uri = args.get_one::<String>("uri").unwrap();
    let query = args.get_one::<String>("query").unwrap();

    let path = Path::new(uri);

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

    let table = deltalake::open_table_with_storage_options(uri, storage_options).await?;
    let config = DeltaScanConfigBuilder::new().build(&table.state)?;
    let provider = DeltaTableProvider::try_new(table.state.clone(), table.log_store(), config)?;

    let ctx = SessionContext::new();
    ctx.register_table(
        path.file_name().unwrap().to_str().unwrap(),
        Arc::new(provider),
    )?;

    let df = ctx.sql(query).await?;
    let batches = df.collect().await?;

    println!("{}", pretty_format_batches(&batches).unwrap());

    Ok(())
}
