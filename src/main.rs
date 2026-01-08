mod cluster;
mod collector;
mod config;
mod error;
mod export;
mod http;
mod kafka;
mod metrics;

use crate::cluster::ClusterManager;
use crate::config::Config;
use crate::export::prometheus::PrometheusExporter;
use crate::http::server::HttpServer;
use crate::metrics::registry::MetricsRegistry;
use clap::Parser;
use std::sync::Arc;
use tokio::signal;
use tokio::sync::broadcast;
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[derive(Parser, Debug)]
#[command(name = "klag-exporter")]
#[command(about = "Kafka consumer group lag exporter with offset and time lag metrics")]
#[command(version)]
struct Args {
    /// Path to configuration file
    #[arg(short, long, default_value = "config.toml")]
    config: String,

    /// Log level (trace, debug, info, warn, error)
    #[arg(short, long, default_value = "info")]
    log_level: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // Initialize logging
    init_logging(&args.log_level);

    info!("Starting klag-exporter");

    // Load configuration
    let config = Config::load(Some(&args.config))?;
    info!(
        clusters = config.clusters.len(),
        poll_interval = ?config.exporter.poll_interval,
        "Configuration loaded"
    );

    // Create shared metrics registry
    let registry = Arc::new(MetricsRegistry::new());

    // Create shutdown channel
    let (shutdown_tx, _) = broadcast::channel::<()>(1);

    // Spawn cluster managers
    let mut handles = Vec::new();
    for cluster_config in config.clusters.clone() {
        let registry = Arc::clone(&registry);
        let shutdown_rx = shutdown_tx.subscribe();
        let exporter_config = config.exporter.clone();

        let handle = tokio::spawn(async move {
            let manager =
                match ClusterManager::new(cluster_config.clone(), registry, &exporter_config) {
                    Ok(m) => m,
                    Err(e) => {
                        error!(
                            cluster = cluster_config.name,
                            error = %e,
                            "Failed to create cluster manager"
                        );
                        return;
                    }
                };

            manager.run(shutdown_rx).await;
        });

        handles.push(handle);
    }

    // Create and start HTTP server
    let prometheus_exporter = PrometheusExporter::new(Arc::clone(&registry));
    let http_server = HttpServer::new(
        &config.exporter.http_host,
        config.exporter.http_port,
        prometheus_exporter,
        Arc::clone(&registry),
    );

    let shutdown_rx = shutdown_tx.subscribe();
    let server_handle = tokio::spawn(async move {
        if let Err(e) = http_server.run(shutdown_rx).await {
            error!(error = %e, "HTTP server error");
        }
    });

    // Start OpenTelemetry exporter if enabled
    if config.exporter.otel.enabled {
        let registry = Arc::clone(&registry);
        let otel_config = config.exporter.otel.clone();
        let shutdown_rx = shutdown_tx.subscribe();

        tokio::spawn(async move {
            if let Err(e) =
                crate::export::otel::run_otel_exporter(registry, otel_config, shutdown_rx).await
            {
                error!(error = %e, "OpenTelemetry exporter error");
            }
        });
    }

    // Wait for shutdown signal
    shutdown_signal().await;
    info!("Shutdown signal received, stopping...");

    // Notify all tasks to shutdown
    let _ = shutdown_tx.send(());

    // Wait for server to shutdown
    let _ = server_handle.await;

    // Wait for cluster managers to shutdown (with timeout)
    let shutdown_timeout = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        futures::future::join_all(handles),
    );

    match shutdown_timeout.await {
        Ok(_) => info!("All cluster managers stopped"),
        Err(_) => error!("Timeout waiting for cluster managers to stop"),
    }

    info!("klag-exporter stopped");
    Ok(())
}

fn init_logging(level: &str) {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(level));

    tracing_subscriber::registry()
        .with(env_filter)
        .with(tracing_subscriber::fmt::layer())
        .init();
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
