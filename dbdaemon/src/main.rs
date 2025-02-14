/******************************************************************************
 * Copyright ContinuousC. Licensed under the "Elastic License 2.0".           *
 ******************************************************************************/

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;
use log::info;
use mimalloc::MiMalloc;
use thiserror::Error;
use tokio::{
    io,
    signal::unix::{signal, SignalKind},
};

use dbdaemon::{daemon::DbDaemon, database::elastic};
use dbdaemon_api::BackendDbHandler;
use opentelemetry::trace::TracerProvider;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

/// ContinuousC Database Daemon.
/// Provides database functions for ContinuousC.
#[derive(Parser)]
struct Args {
    #[clap(long, env, help = "The application's instance id, for tracing.")]
    instance_id: Option<String>,
    #[clap(long, env, help = "The name of the Node.")]
    k8s_node_name: Option<String>,
    #[clap(
        long,
        env,
        help = "The name of the namespace that the pod is running in."
    )]
    k8s_namespace_name: Option<String>,
    #[clap(long, env, help = "The name of the Pod.")]
    k8s_pod_name: Option<String>,
    #[clap(long, env, help = "The UID of the Pod.")]
    k8s_pod_uid: Option<String>,
    #[clap(
        long,
        env,
        help = "The name of the Container from Pod specification, must be unique within a Pod."
    )]
    k8s_container_name: Option<String>,
    /// The tcp socket address.
    #[clap(env = "DB_BIND", long)]
    bind: SocketAddr,
    // /// Where to store data files.
    // #[clap(long, short, default_value = "/var/lib/dbdaemon")]
    // data: PathBuf,
    // /// The broker address.
    // #[clap(long, short)]
    // broker: SocketAddr,
    // /// The path to the log file.
    // #[clap(long, short)]
    // log: Option<PathBuf>,
    // /// The path to the configuration directory.
    // #[clap(long, short, default_value = "/etc/dbdaemon")]
    // config: PathBuf,
    /// The path in which to look for certificates.
    #[clap(
        env = "DB_CERTS_DIR",
        long,
        default_value = "/usr/share/continuousc/certs/dbdaemon"
    )]
    certs_dir: PathBuf,
    /// The path to the CA certificate.
    #[clap(env = "DB_CA", long, default_value = "ca.crt")]
    ca: PathBuf,
    /// The path to the server certificate.
    #[clap(env = "DB_CERT", long, default_value = "dbdaemon.crt")]
    cert: PathBuf,
    /// The path to the server key.
    #[clap(env = "DB_KEY", long, default_value = "dbdaemon.key")]
    key: PathBuf,
    /// Increase log verbosity.
    #[clap(env = "DB_VERBOSE", long, short, action = clap::ArgAction::Count)]
    verbose: u8,
    #[clap(flatten, next_help_heading = "Elasticsearch")]
    elastic: elastic::DatabaseConfig,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // env_logger::init();

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .build()
        .map_err(Error::OpenTelemetry)?;

    let mut attrs = vec![
        opentelemetry::KeyValue::new("service.namespace", "continuousc"),
        opentelemetry::KeyValue::new("service.name", "dbdaemon"),
        opentelemetry::KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
    ];

    for (attr, value) in [
        ("service.instance.id", &args.instance_id),
        ("k8s.node.name", &args.k8s_node_name),
        ("k8s.namespace.name", &args.k8s_namespace_name),
        ("k8s.pod.name", &args.k8s_pod_name),
        ("k8s.pod.uid", &args.k8s_pod_uid),
        ("k8s.container.name", &args.k8s_container_name),
    ] {
        if let Some(value) = value.as_ref() {
            attrs.push(opentelemetry::KeyValue::new(attr, value.to_string()));
        }
    }

    let tracer = opentelemetry_sdk::trace::TracerProvider::builder()
        .with_batch_exporter(exporter, opentelemetry_sdk::runtime::Tokio)
        // .with_sampler(opentelemetry_sdk::Sampler::AlwaysOn)
        // .with_id_generator(opentelemetry_sdk::RandomIdGenerator::default())
        // .with_max_events_per_span(64)
        // .with_max_attributes_per_span(16)
        // .with_max_events_per_span(16)
        .with_resource(opentelemetry_sdk::Resource::new(attrs))
        .build()
        .tracer("DBDaemon");

    let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);

    tracing_subscriber::FmtSubscriber::builder()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .finish()
        .with(telemetry)
        .init();

    opentelemetry::global::set_text_map_propagator(
        opentelemetry_sdk::propagation::TraceContextPropagator::new(),
    );

    // add arguments to daemon: enable broker, debug, ...
    let r = run(args).await;

    if let Err(e) = &r {
        eprintln!("Error: {e}");
    }

    opentelemetry::global::shutdown_tracer_provider();
    r
}

// struct Args {
//     socket: SocketAddr,
//     config: PathBuf,
//     // certs: PathBuf,
//     data: PathBuf,
//     ca: PathBuf,
//     cert: PathBuf,
//     key: PathBuf,
//     // #[cfg(feature = "agent")]
//     // broker_addr: SocketAddr,
//     // #[cfg(feature = "agent")]
//     // broker_domain: String
// }

async fn run(args: Args) -> Result<()> {
    // create daemon
    let daemon = DbDaemon::new(args.elastic).await?;

    info!("daemon started");
    info!(
        "using objectdb: {}",
        "elastic" /* &daemon.elastic.whoami() */
    );
    // debug!("daemon: {:?}", &daemon);

    // signal handling
    //let mut sighup = signal(SignalKind::hangup()).map_err(Error::SignalInit)?;
    let mut sigint = signal(SignalKind::interrupt()).map_err(Error::SignalInit)?;
    let mut sigterm = signal(SignalKind::terminate()).map_err(Error::SignalInit)?;

    info!("listening to requests at: {}", &args.bind);

    let server = rpc::AsyncServer::builder()
        .tcp(args.bind)
        .await
        // .unix("dbdaemon.sock")
        .map_err(Error::Rpc)?
        .tls(
            rpc::tls_server_config(
                &args.certs_dir.join(&args.ca),
                &args.certs_dir.join(&args.cert),
                &args.certs_dir.join(&args.key),
            )
            .await
            .map_err(Error::Rpc)?,
        )
        .json()
        .handler(BackendDbHandler::new(daemon));

    tokio::select! {
        _ = sigint.recv() => {}
        _ = sigterm.recv() => {}
    };

    info!("Awaiting open connections (press ctrl-c to force shutdown)...");

    tokio::select! {
        r = server.shutdown() => { r.map_err(Error::Rpc)? }
        _ = sigint.recv() => {
            info!("Received SIGINT; force shutdown!");
        }
        _ = sigterm.recv() => {
            info!("Received SIGTERM; force shutdown!");
        }
    };

    //daemon.save_state().await.map_err(Error::Shutdown)?;

    Ok(())
}

type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
enum Error {
    #[error("Failed to initialize logger: {0}")]
    Logger(#[from] log::SetLoggerError),
    #[error("Failed to initialize tracer : {0}")]
    OpenTelemetry(opentelemetry::trace::TraceError),
    // #[error("Failed to initialize database daemon: {0}")]
    // DbDaemonInit(#[from] dbdaemon::daemon::InitializationError),
    // #[error("Failed to create socket directory {0}: {1}")]
    // SocketDir(String, io::Error),
    // #[error("Failed to bind socket to {0}: {1}")]
    // SocketBind(String, io::Error),
    // #[error("Could not listen on socket: {0}")]
    // SocketListen(io::Error),
    // #[error("Failed to remove socket: {0}")]
    // SocketRemove(io::Error),
    #[error("Failed to install signal handler: {0}")]
    SignalInit(io::Error),
    // #[cfg(feature = "agent")]
    // #[error("Error connecting to broker: {0}")]
    // Broker(#[from] AgentError),
    // #[error("Failed to send termination signal to connection handlers: {0}")]
    // SendTerm(watch::error::SendError<bool>),
    #[error(transparent)]
    Daemon(#[from] dbdaemon::daemon::Error),
    #[error("rpc error: {0}")]
    Rpc(rpc::Error),
}
