use crate::error::{KlagError, Result};
use crate::export::prometheus::PrometheusExporter;
use crate::leadership::LeadershipStatus;
use crate::metrics::registry::MetricsRegistry;
use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use serde::Serialize;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::info;

#[derive(Clone)]
struct AppState {
    prometheus: PrometheusExporter,
    registry: Arc<MetricsRegistry>,
    leadership: LeadershipStatus,
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
        leadership: LeadershipStatus,
    ) -> Self {
        let addr: SocketAddr = format!("{host}:{port}").parse().expect("Invalid address");

        Self {
            addr,
            state: AppState {
                prometheus,
                registry,
                leadership,
            },
        }
    }

    pub async fn run(self, mut shutdown: broadcast::Receiver<()>) -> Result<()> {
        let app = Router::new()
            .route("/metrics", get(metrics_handler))
            .route("/health", get(health_handler))
            .route("/ready", get(ready_handler))
            .route("/leader", get(leader_handler))
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
    // Not ready if not leader (standby instance)
    if !state.leadership.is_leader() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "Not ready - standby instance (not leader)",
        )
            .into_response();
    }

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

/// Response body for the /leader endpoint.
#[derive(Serialize)]
struct LeaderResponse {
    is_leader: bool,
}

async fn leader_handler(State(state): State<AppState>) -> Json<LeaderResponse> {
    Json(LeaderResponse {
        is_leader: state.leadership.is_leader(),
    })
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
<p><a href="/leader">Leader Status</a></p>
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
    use crate::leadership::LeadershipState;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    fn make_app_with_leadership(
        initial_state: LeadershipState,
    ) -> (Router, crate::leadership::LeadershipStateUpdater) {
        let registry = Arc::new(MetricsRegistry::new());
        let prometheus = PrometheusExporter::new(Arc::clone(&registry));
        let (leadership, updater) = LeadershipStatus::new(initial_state);

        let router = Router::new()
            .route("/metrics", get(metrics_handler))
            .route("/health", get(health_handler))
            .route("/ready", get(ready_handler))
            .route("/leader", get(leader_handler))
            .with_state(AppState {
                prometheus,
                registry,
                leadership,
            });

        (router, updater)
    }

    fn make_app() -> Router {
        let (router, _updater) = make_app_with_leadership(LeadershipState::Leader);
        router
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
    async fn test_ready_endpoint_not_ready_no_data() {
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

        // Not ready because no cluster data (but we are leader)
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn test_ready_endpoint_not_ready_standby() {
        let (app, _updater) = make_app_with_leadership(LeadershipState::Standby);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/ready")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // Not ready because we're on standby (not leader)
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn test_leader_endpoint_is_leader() {
        let (app, _updater) = make_app_with_leadership(LeadershipState::Leader);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/leader")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["is_leader"], true);
    }

    #[tokio::test]
    async fn test_leader_endpoint_is_standby() {
        let (app, _updater) = make_app_with_leadership(LeadershipState::Standby);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/leader")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["is_leader"], false);
    }
}
