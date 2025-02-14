use std::{convert::TryFrom, net::SocketAddr, path::PathBuf, sync::Arc, time::Instant};

use chrono::{DateTime, Utc};
use clap::Parser;
use dbschema::{DbTableId, DualVersioned, HasSchema, HasTableDef, Identified, TimeRange, Timeline};
use futures::{StreamExt, TryStreamExt};
use rpc::GenericValue;
use rustls::pki_types::ServerName;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use dbdaemon_api::{BackendDbProto, BackendDbServiceStub};

#[derive(Parser)]
struct Args {
    #[clap(
        long,
        default_value = "/usr/share/continuousc/certs/relation-graph-engine-client"
    )]
    certs_dir: PathBuf,
    /// The path to the CA certificate.
    #[clap(long, default_value = "ca.crt")]
    ca: PathBuf,
    /// The path to the client certificate.
    #[clap(long, default_value = "tls.crt")]
    cert: PathBuf,
    /// The path to the client key.
    #[clap(long, default_value = "tls.key")]
    key: PathBuf,
    #[clap(long, default_value = "dbdaemon")]
    hostname: String,
    #[clap(long, default_value = "127.0.0.1:9999")]
    connect: SocketAddr,
    #[clap(subcommand)]
    command: Command,
}

#[derive(clap::Subcommand)]
enum Command {
    VerifyTableData(VerifyArgs),
    Benchmark,
}

#[derive(clap::Parser)]
struct VerifyArgs {
    table_id: String,
    #[clap(long)]
    from: Option<DateTime<Utc>>,
    #[clap(long)]
    to: Option<DateTime<Utc>>,
}

type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
enum Error {
    #[error("rpc error: {0}")]
    Rpc(#[from] rpc::Error),
    #[error("db error: {0}")]
    DbDaemon(String),
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let args = Args::parse();
    if let Err(e) = run(&args).await {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}

async fn run(args: &Args) -> Result<()> {
    /* Connect to dbdaemon. */

    let client = rpc::AsyncClient::<BackendDbProto, serde_json::Value, ()>::builder()
        .tcp(args.connect)
        .await
        // .unix("dbdaemon.sock")
        .tls(
            rpc::tls_client_config(
                &args.certs_dir.join(&args.ca),
                &args.certs_dir.join(&args.cert),
                &args.certs_dir.join(&args.key),
            )
            .await?,
            ServerName::try_from(args.hostname.to_string()).unwrap(),
        )
        .json();
    let client = Arc::new(BackendDbServiceStub::new(client));

    client
        .inner()
        .connected(std::time::Duration::from_secs(30))
        .await?;
    client.wait_for_databases().await.map_err(Error::DbDaemon)?;

    match &args.command {
        Command::VerifyTableData(verify_args) => verify_table_data(&client, verify_args).await,
        Command::Benchmark => benchmark(&client).await,
    }
}

async fn verify_table_data<
    C: rpc::RequestHandler<BackendDbProto, V, ExtraArgs = ()>,
    V: GenericValue,
>(
    client: &BackendDbServiceStub<C, V>,
    args: &VerifyArgs,
) -> Result<()> {
    let proc_id = client
        .verify_table_data_start(
            DbTableId::from_string(args.table_id.clone()),
            Some(TimeRange::new(args.from, args.to)),
        )
        .await
        .map_err(Error::DbDaemon)?;

    eprintln!("Started verification of table \"{}\"", &args.table_id);

    let mut overlaps = 0;
    let mut reuses = 0;

    while let Some(msgs) = client
        .verify_table_data_next(proc_id)
        .await
        .map_err(Error::DbDaemon)?
    {
        for msg in msgs {
            match msg {
                dbdaemon_api::VerificationMsg::Overlap(p) => {
                    overlaps += 1;
                    println!(
                        "Found overlapping versions for object {}:\n\
						 \t{} is valid till {:?},\n\
						 \twhile {} is valid from {:?}",
                        p.object_id, p.prev_version_id, p.prev_to, p.cur_version_id, p.cur_from
                    );
                }
                dbdaemon_api::VerificationMsg::Gap(p) => {
                    reuses += 1;
                    println!(
                        "Found reused id for object {}:\n\
						 \t{} is valid till {:?},\n\
						 \twhile {} is valid from {:?}",
                        p.object_id, p.prev_version_id, p.prev_to, p.cur_version_id, p.cur_from
                    );
                }
                dbdaemon_api::VerificationMsg::Progress(n) => {
                    eprintln!("Processed {n} docs...");
                }
                dbdaemon_api::VerificationMsg::Error(e) => {
                    eprintln!("Error: {e}");
                }
            }
        }
    }

    eprintln!("Finished verification of table \"{}\"", &args.table_id);
    eprintln!("Found {overlaps} overlapping version(s), {reuses} reused object id(s)");

    Ok(())
}

async fn benchmark<C: rpc::RequestHandler<BackendDbProto, V, ExtraArgs = ()>, V: GenericValue>(
    client: &BackendDbServiceStub<C, V>,
) -> Result<()> {
    /* Register test table. */

    let test_table = DbTableId::new("test-config");

    client
        .register_table(test_table.clone(), TestDocument::table_def())
        .await
        .map_err(Error::DbDaemon)?;

    /* Create objects. */

    const NOBJS: usize = 10000;
    const NPAR: usize = 100;

    let start = Instant::now();
    eprintln!("Creating {NOBJS} objects (individual reqs, {NPAR} in parallel)...");

    let _ops = futures::stream::iter(0..NOBJS)
        .map(|i| {
            let test_table = test_table.clone();
            async move {
                let object_id = client
                    .create_config_object(
                        test_table.clone(),
                        serde_json::to_value(Test {
                            test: format!("{i}"),
                        })
                        .unwrap(),
                        true,
                    )
                    .await?;
                client.activate_config_object(test_table, object_id).await
            }
        })
        .buffer_unordered(NPAR)
        .try_collect::<Vec<_>>()
        .await
        .map_err(Error::DbDaemon)?;

    eprintln!(
        "Took {:.3} seconds",
        start.elapsed().as_millis() as f64 / 1000.
    );

    /* Query objects. */

    let start = Instant::now();
    eprintln!("Querying objects...");

    let objs = client
        .query_config_objects(
            test_table.clone(),
            dbschema::Filter::All(vec![]),
            Timeline::Current,
        )
        .await
        .map_err(Error::DbDaemon)?;
    eprintln!("Found {} test objects.", objs.len());
    eprintln!(
        "Took {:.3} seconds",
        start.elapsed().as_millis() as f64 / 1000.
    );

    /* Remove objects. */

    let start = Instant::now();
    eprintln!("Updating objects (individual reqs, {NPAR} in parallel)...");

    let _ops = futures::stream::iter(objs.into_iter())
        .map(|(object_id, value)| {
            let test_table = test_table.clone();
            async move {
                let value = serde_json::from_value::<Test>(value.value).unwrap();
                let updated = Test {
                    test: format!("{} (updated)", value.test),
                };
                client
                    .update_config_object(
                        test_table.clone(),
                        object_id.clone(),
                        serde_json::to_value(&updated).unwrap(),
                        true,
                    )
                    .await?;
                client.activate_config_object(test_table, object_id).await
            }
        })
        .buffer_unordered(NPAR)
        .try_collect::<Vec<_>>()
        .await
        .map_err(Error::DbDaemon)?;

    eprintln!(
        "Took {:.3} seconds",
        start.elapsed().as_millis() as f64 / 1000.
    );

    /* Query objects (again). */

    let start = Instant::now();
    eprintln!("Querying objects...");

    let objs = client
        .query_config_objects(
            test_table.clone(),
            dbschema::Filter::All(vec![]),
            Timeline::Current,
        )
        .await
        .map_err(Error::DbDaemon)?;

    eprintln!("Found {} test objects.", objs.len());
    eprintln!(
        "Took {:.3} seconds",
        start.elapsed().as_millis() as f64 / 1000.
    );

    /* Remove objects. */

    let start = Instant::now();
    eprintln!("Removing objects (individual reqs, {NPAR} in parallel)...");

    let _ops = futures::stream::iter(objs.into_keys())
        .map(|object_id| {
            let test_table = test_table.clone();
            async move {
                client
                    .remove_config_object(test_table.clone(), object_id.clone())
                    .await?;
                client.activate_config_object(test_table, object_id).await
            }
        })
        .buffer_unordered(NPAR)
        .try_collect::<Vec<_>>()
        .await
        .map_err(Error::DbDaemon)?;

    eprintln!(
        "Took {:.3} seconds",
        start.elapsed().as_millis() as f64 / 1000.
    );

    // let start = Instant::now();
    // eprintln!("Removing objects (bulk request)...");

    // client
    //     .bulk_update_discovery_objects(
    //         test_table.clone(),
    //         objs.into_keys()
    //             .map(|object_id| (object_id, Operation::Remove))
    //             .collect(),
    //     )
    //     .await
    //     .map_err(Error::DbDaemon)?;

    // eprintln!(
    //     "Took {:.3} seconds",
    //     start.elapsed().as_millis() as f64 / 1000.
    // );

    Ok(())
}

type TestDocument = Identified<DualVersioned<Test>>;

#[derive(HasSchema, Serialize, Deserialize)]
struct Test {
    test: String,
}
