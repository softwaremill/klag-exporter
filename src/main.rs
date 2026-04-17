#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// Tune jemalloc: fewer arenas, faster page release, background cleanup.
// Can be overridden at runtime via MALLOC_CONF env var.
#[cfg(feature = "jemalloc")]
#[used]
#[allow(non_upper_case_globals)]
#[unsafe(export_name = "_rjem_malloc_conf")]
pub static malloc_conf: &[u8] =
    b"narenas:2,dirty_decay_ms:5000,muzzy_decay_ms:5000,background_thread:true\0";

mod cluster;
mod collector;
mod config;
mod error;
mod export;
mod http;
mod kafka;
mod leadership;
mod metrics;

use crate::cluster::ClusterManager;
use crate::config::Config;
use crate::export::prometheus::PrometheusExporter;
use crate::http::server::HttpServer;
use crate::leadership::{LeadershipProvider, LeadershipStatus};
use crate::metrics::registry::MetricsRegistry;
use clap::Parser;
use std::sync::Arc;
use tokio::signal;
use tokio::sync::broadcast;
#[cfg(not(feature = "kubernetes"))]
use tracing::warn;
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

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // Initialize logging
    init_logging(&args.log_level);

    info!("Starting klag-exporter");

    // Load configuration before building the runtime — the blocking-thread
    // pool size comes from config, and we want an early, clear error on
    // config problems rather than a cryptic runtime-setup failure.
    let config = Config::load(Some(&args.config))?;
    info!(
        clusters = config.clusters.len(),
        poll_interval = ?config.exporter.poll_interval,
        max_blocking_threads = config.exporter.performance.max_blocking_threads,
        "Configuration loaded"
    );

    // One-time deprecation notice for config fields that no longer do
    // anything but are still accepted for backward compatibility.
    // `max_concurrent_watermarks` was the per-partition fetch_watermarks
    // concurrency cap; the Tier 1 batched ListOffsets refactor replaced
    // that fan-out with two broker-level calls, so this knob has no
    // effect. Only warn when the user explicitly set a non-default value.
    let watermarks_default = crate::config::PerformanceConfig::default().max_concurrent_watermarks;
    if config.exporter.performance.max_concurrent_watermarks != watermarks_default {
        info!(
            configured_value = config.exporter.performance.max_concurrent_watermarks,
            "performance.max_concurrent_watermarks is deprecated and has no effect since the \
             batched Admin API replaced per-partition watermark fetch. Safe to remove from your \
             config."
        );
    }

    // Bound the tokio blocking-thread pool. The default (512) commits up to
    // several GB of virtual memory just for native thread stacks; this
    // exporter only needs enough threads to cover concurrent FFI calls from
    // the collector + timestamp sampler.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .max_blocking_threads(config.exporter.performance.max_blocking_threads)
        .build()?;

    runtime.block_on(async_main(config))
}

async fn async_main(config: Config) -> anyhow::Result<()> {
    // Create shared metrics registry. Stale-cluster filtering uses the
    // explicit `staleness_threshold` when configured, otherwise
    // `poll_interval * 3` so it scales automatically with the poll cadence.
    let staleness_threshold = config
        .exporter
        .staleness_threshold
        .unwrap_or_else(|| config.exporter.poll_interval.saturating_mul(3));
    info!(
        threshold_secs = staleness_threshold.as_secs(),
        poll_interval_secs = config.exporter.poll_interval.as_secs(),
        explicit = config.exporter.staleness_threshold.is_some(),
        "Metrics staleness threshold configured"
    );
    let registry = Arc::new(MetricsRegistry::with_staleness_threshold(
        staleness_threshold,
    ));

    // Create shutdown channel
    let (shutdown_tx, _) = broadcast::channel::<()>(1);

    // Initialize leadership provider
    let (leadership_provider, leadership_status) =
        create_leadership_provider(&config.exporter.leadership).await?;

    if config.exporter.leadership.enabled {
        info!(
            provider = ?config.exporter.leadership.provider,
            lease_name = %config.exporter.leadership.lease_name,
            namespace = %config.exporter.leadership.lease_namespace,
            "Leader election enabled"
        );
    } else {
        info!("Running in single-instance mode (leader election disabled)");
    }

    // Spawn cluster managers
    let mut handles = Vec::new();
    for cluster_config in config.clusters.clone() {
        let registry = Arc::clone(&registry);
        let shutdown_rx = shutdown_tx.subscribe();
        let exporter_config = config.exporter.clone();
        let leadership = leadership_status.clone();

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

            manager.run(shutdown_rx, leadership).await;
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
        leadership_status.clone(),
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

    // Stop leadership provider
    leadership_provider.stop().await;
    info!("Leadership provider stopped");

    info!("klag-exporter stopped");
    Ok(())
}

/// Create the appropriate leadership provider based on configuration.
async fn create_leadership_provider(
    config: &crate::config::LeadershipConfig,
) -> anyhow::Result<(Box<dyn LeadershipProvider>, LeadershipStatus)> {
    if !config.enabled {
        // Single-instance mode - use noop provider
        let provider = leadership::noop::NoopLeader::new();
        let status = provider.start().await?;
        return Ok((Box::new(provider), status));
    }

    // Leader election enabled
    match config.provider {
        crate::config::LeadershipProvider::Kubernetes => {
            #[cfg(feature = "kubernetes")]
            {
                let provider = leadership::kubernetes::create_kubernetes_leader(
                    &config.lease_name,
                    &config.lease_namespace,
                    config.identity.as_deref(),
                    Some(config.lease_duration_secs),
                    Some(config.grace_period_secs),
                )?;
                let status = provider.start().await?;
                Ok((Box::new(provider), status))
            }
            #[cfg(not(feature = "kubernetes"))]
            {
                warn!(
                    "Kubernetes leadership is enabled in config but the 'kubernetes' feature is not compiled in. \
                    Falling back to single-instance mode. \
                    Rebuild with --features kubernetes to enable leader election."
                );
                let provider = leadership::noop::NoopLeader::new();
                let status = provider.start().await?;
                Ok((Box::new(provider), status))
            }
        }
    }
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
