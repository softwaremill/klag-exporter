use std::collections::HashMap;

#[derive(Debug, Clone)]
pub enum MetricValue {
    Gauge(f64),
    Counter(f64),
}

impl MetricValue {
    pub fn as_f64(&self) -> f64 {
        match self {
            MetricValue::Gauge(v) => *v,
            MetricValue::Counter(v) => *v,
        }
    }
}

pub type Labels = HashMap<String, String>;

#[derive(Debug, Clone)]
pub struct MetricPoint {
    pub name: String,
    pub labels: Labels,
    pub value: MetricValue,
    pub help: &'static str,
    pub metric_type: MetricType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricType {
    Gauge,
    Counter,
}

impl MetricType {
    pub fn as_str(&self) -> &'static str {
        match self {
            MetricType::Gauge => "gauge",
            MetricType::Counter => "counter",
        }
    }
}

impl MetricPoint {
    pub fn gauge(name: impl Into<String>, labels: Labels, value: f64, help: &'static str) -> Self {
        Self {
            name: name.into(),
            labels,
            value: MetricValue::Gauge(value),
            help,
            metric_type: MetricType::Gauge,
        }
    }

    #[allow(dead_code)]
    pub fn counter(
        name: impl Into<String>,
        labels: Labels,
        value: f64,
        help: &'static str,
    ) -> Self {
        Self {
            name: name.into(),
            labels,
            value: MetricValue::Counter(value),
            help,
            metric_type: MetricType::Counter,
        }
    }
}

#[derive(Debug, Clone)]
pub struct OtelMetric {
    pub name: String,
    #[allow(dead_code)]
    pub description: String,
    #[allow(dead_code)]
    pub unit: String,
    pub data_points: Vec<OtelDataPoint>,
}

#[derive(Debug, Clone)]
pub struct OtelDataPoint {
    pub attributes: HashMap<String, String>,
    pub value: f64,
    #[allow(dead_code)]
    pub timestamp_ms: i64,
}
