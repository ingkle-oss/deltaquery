use anyhow::Error;
use clap::{Args, Command};
use deltaquery::compute::register_compute_factory;
use deltaquery::computes::duckdb::DQDuckDBComputeFactory;
use deltaquery::configs::DQConfig;
use deltaquery::servers::flightsql;
use deltaquery::servers::flightsql::{FlightSqlServiceSimple, FlightSqlServiceSingle};
use deltaquery::state::DQState;
use deltaquery::table::register_table_factory;
use deltaquery::tables::delta::DQDeltaTableFactory;
use env_logger::Builder;
use std::env;
use std::fs::File;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::Mutex;
use tokio::time;

#[derive(Debug, Args)]
pub struct DQOption {
    #[arg(short = 'c', long, help = "Config file")]
    config: Option<String>,

    #[arg(short = 't', long, help = "Catalog file")]
    catalog: Option<String>,

    #[arg(short = 'l', long, help = "Log filters")]
    logfilter: Option<String>,
}

fn handle_state(state: Arc<Mutex<DQState>>) {
    tokio::spawn(async move {
        let mut interval = time::interval(match env::var("DELTAQUERY_UPDATE_INTERVAL") {
            Ok(value) => duration_str::parse(&value).expect("could not parse update interval"),
            Err(_) => Duration::from_secs(60),
        });

        loop {
            {
                let mut state = state.lock().await;
                state.update_tables().await;

                for (_, table) in state.get_tables() {
                    let time0 = Instant::now();

                    let _ = table.update().await;

                    log::info!("updated for {} milliseconds", time0.elapsed().as_millis());
                }
            }

            interval.tick().await;
        }
    });
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Error> {
    let cmd = Command::new("DeltaQuery");
    let cmd = DQOption::augment_args(cmd);
    let args = cmd.get_matches();

    if let Some(filter) = args.get_one::<String>("logfilter") {
        Builder::new().parse_filters(filter.as_str()).init();
    } else {
        env_logger::init();
    }

    let config = match args.get_one::<String>("config") {
        Some(config) => {
            let f = File::open(config).expect("could not open file");
            let c: DQConfig = serde_yaml::from_reader(f).expect("could not parse yaml");

            c
        }
        None => panic!("could not find config file"),
    };

    let catalog = match args.get_one::<String>("catalog") {
        Some(catalog) => {
            let f = File::open(catalog).expect("could not open file");
            let c = serde_yaml::from_reader(f).expect("could not parse yaml");

            c
        }
        None => serde_yaml::Value::default(),
    };

    register_table_factory("delta", Box::new(DQDeltaTableFactory::new())).await;
    register_compute_factory("duckdb", Box::new(DQDuckDBComputeFactory::new())).await;

    let state = Arc::new(Mutex::new(DQState::new(config.clone()).await));
    handle_state(state.clone());

    match config.server.as_str() {
        "single" => {
            flightsql::server::serve(
                config,
                FlightSqlServiceSingle::new(state.clone(), catalog).await,
            )
            .await?;
        }
        _ => {
            flightsql::server::serve(
                config,
                FlightSqlServiceSimple::new(state.clone(), catalog).await,
            )
            .await?;
        }
    };

    Ok(())
}
