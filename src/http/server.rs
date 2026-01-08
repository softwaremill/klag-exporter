use crate::error::{KlagError, Result};
use crate::export::prometheus::PrometheusExporter;
use crate::metrics::registry::MetricsRegistry;
use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::info;

#[derive(Clone)]
struct AppState {
    prometheus: PrometheusExporter,
    registry: Arc<MetricsRegistry>,
}

pub struct HttpServer {
    addr: SocketAddr,
    state: AppState,
}

impl HttpServer {
    pub fn new(
        host: &str,
        port: u16,
        prometheus: PrometheusExporter,
        registry: Arc<MetricsRegistry>,
    ) -> Self {
        let addr: SocketAddr = format!("{}:{}", host, port)
            .parse()
            .expect("Invalid address");

        Self {
            addr,
            state: AppState {
                prometheus,
                registry,
            },
        }
    }

    pub async fn run(self, mut shutdown: broadcast::Receiver<()>) -> Result<()> {
        let app = Router::new()
            .route("/metrics", get(metrics_handler))
            .route("/health", get(health_handler))
            .route("/ready", get(ready_handler))
            .route("/", get(root_handler))
            .with_state(self.state);

        info!(addr = %self.addr, "Starting HTTP server");

        let listener = tokio::net::TcpListener::bind(self.addr)
            .await
            .map_err(|e| KlagError::Http(e.to_string()))?;

        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = shutdown.recv().await;
                info!("HTTP server shutting down");
            })
            .await
            .map_err(|e| KlagError::Http(e.to_string()))?;

        Ok(())
    }
}

async fn metrics_handler(State(state): State<AppState>) -> Response {
    let metrics = state.prometheus.render_metrics();
    (
        StatusCode::OK,
        [("content-type", "text/plain; charset=utf-8")],
        metrics,
    )
        .into_response()
}

async fn health_handler(State(state): State<AppState>) -> Response {
    if state.registry.is_healthy() {
        (StatusCode::OK, "OK").into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "Unhealthy").into_response()
    }
}

async fn ready_handler(State(state): State<AppState>) -> Response {
    // Ready if we have at least one cluster reporting metrics
    if state.registry.cluster_count() > 0 {
        (StatusCode::OK, "Ready").into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "Not ready - no cluster data",
        )
            .into_response()
    }
}

async fn root_handler() -> Response {
    let html = r#"<!DOCTYPE html>
<html>
<head><title>Kafka Lag Exporter</title></head>
<body>
<h1>Kafka Lag Exporter</h1>
<p><a href="/metrics">Metrics</a></p>
<p><a href="/health">Health</a></p>
<p><a href="/ready">Ready</a></p>
</body>
</html>"#;

    (
        StatusCode::OK,
        [("content-type", "text/html; charset=utf-8")],
        html,
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    fn make_app() -> Router {
        let registry = Arc::new(MetricsRegistry::new());
        let prometheus = PrometheusExporter::new(Arc::clone(&registry));

        Router::new()
            .route("/metrics", get(metrics_handler))
            .route("/health", get(health_handler))
            .route("/ready", get(ready_handler))
            .with_state(AppState {
                prometheus,
                registry,
            })
    }

    #[tokio::test]
    async fn test_metrics_endpoint() {
        let app = make_app();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_health_endpoint() {
        let app = make_app();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_ready_endpoint_not_ready() {
        let app = make_app();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/ready")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // Not ready because no cluster data
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
}
