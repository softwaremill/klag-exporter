//! Rate-based time-lag estimation.
//!
//! The classic way to compute per-partition time-lag is to read the message
//! at the consumer's committed offset and subtract its produce timestamp from
//! "now". That works but it scales poorly on large clusters: every laggy
//! partition requires a full `assign()` + `poll()` round trip through a
//! librdkafka BaseConsumer, and the consumer pool itself occupies tens to
//! hundreds of MB of resident memory (each `BaseConsumer` is a full
//! librdkafka client with its own metadata cache and background threads).
//!
//! Rate-based estimation avoids the pool entirely. For each partition we
//! keep a short ring buffer of `(observation_time, high_watermark)` samples
//! taken at each collection cycle. The broker's production rate is then
//! `Δhigh_watermark / Δtime`, and the estimated time lag for a consumer at
//! committed offset `C` with high watermark `H` is `(H - C) / rate`.
//!
//! Accuracy: this assumes the producer's rate has been steady over the
//! history window. In practice that's good enough for lag alerting — alerts
//! care about magnitude (minutes vs. hours), not precision. When rate is
//! below a configurable floor or history is insufficient, we return `None`
//! and the metric is omitted, matching the behavior of the message-read mode
//! when a broker error prevents a fetch.

use crate::kafka::client::TopicPartition;
use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy)]
struct WatermarkSample {
    at: Instant,
    hwm: i64,
}

/// Shared rate-computation helper. Returns `None` if < 2 samples, Δtime
/// ≤ 0, Δhigh_watermark ≤ 0 (retention rewind), or rate below floor.
fn compute_rate(buf: &VecDeque<WatermarkSample>, min_msgs_per_sec: f64) -> Option<f64> {
    if buf.len() < 2 {
        return None;
    }
    let first = *buf.front()?;
    let last = *buf.back()?;
    let d_hwm = (last.hwm - first.hwm) as f64;
    let d_t = last.at.saturating_duration_since(first.at).as_secs_f64();
    if d_t <= 0.0 || d_hwm <= 0.0 {
        return None;
    }
    let rate = d_hwm / d_t;
    if rate < min_msgs_per_sec {
        return None;
    }
    Some(rate)
}

pub struct RateSampler {
    history: Mutex<HashMap<TopicPartition, VecDeque<WatermarkSample>>>,
    /// Maximum samples kept per partition.
    max_samples: usize,
    /// Drop samples older than this during periodic pruning.
    max_age: Duration,
    /// Below this rate (messages/sec across the full history window) we
    /// treat the estimate as unreliable and return None. Prevents dividing
    /// by near-zero rates on idle partitions.
    min_msgs_per_sec: f64,
}

impl RateSampler {
    pub fn new(max_samples: usize, max_age: Duration, min_msgs_per_sec: f64) -> Self {
        Self {
            history: Mutex::new(HashMap::new()),
            max_samples: max_samples.max(2),
            max_age,
            min_msgs_per_sec,
        }
    }

    /// Record one high-watermark observation per partition. Call this once
    /// per collection cycle, after watermarks have been fetched, before
    /// calling [`estimate_lag_seconds`]. Also prunes entries for partitions
    /// no longer present in `watermarks` (topic deleted / filter change).
    pub fn record_watermarks(&self, watermarks: &HashMap<TopicPartition, (i64, i64)>) {
        let now = Instant::now();
        let mut history = self.history.lock().unwrap_or_else(|p| p.into_inner());

        // Drop entries for partitions no longer being monitored.
        history.retain(|tp, _| watermarks.contains_key(tp));

        for (tp, (_low, high)) in watermarks {
            let buf = history.entry(tp.clone()).or_default();
            // Drop samples past max_age.
            while let Some(front) = buf.front() {
                if now.duration_since(front.at) > self.max_age {
                    buf.pop_front();
                } else {
                    break;
                }
            }
            buf.push_back(WatermarkSample {
                at: now,
                hwm: *high,
            });
            while buf.len() > self.max_samples {
                buf.pop_front();
            }
        }
    }

    /// Return the estimated time lag in seconds, or `None` if the history
    /// is insufficient or the rate is below the reliability floor.
    ///
    /// This is a convenience for single-partition callers / tests. For
    /// batch per-cycle computation over many partitions, prefer
    /// [`RateSampler::rates_snapshot`] — it takes the history lock once
    /// instead of once per call, which matters on large clusters with
    /// thousands of laggy partitions.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn estimate_lag_seconds(&self, tp: &TopicPartition, lag: i64) -> Option<f64> {
        if lag <= 0 {
            return Some(0.0);
        }
        let history = self.history.lock().unwrap_or_else(|p| p.into_inner());
        compute_rate(history.get(tp)?, self.min_msgs_per_sec).map(|rate| lag as f64 / rate)
    }

    /// Compute the current production rate for every tracked partition
    /// whose history is sufficient and whose rate is above the floor.
    /// Holds the history lock exactly once for the whole pass.
    ///
    /// Callers perform the per-partition division themselves:
    ///     `time_lag = lag / rate`
    ///
    /// Partitions absent from the returned map have no reliable estimate
    /// available this cycle (insufficient samples, below floor, or
    /// retention rewind).
    pub fn rates_snapshot(&self) -> HashMap<TopicPartition, f64> {
        let history = self.history.lock().unwrap_or_else(|p| p.into_inner());
        let mut out = HashMap::with_capacity(history.len());
        for (tp, buf) in history.iter() {
            if let Some(rate) = compute_rate(buf, self.min_msgs_per_sec) {
                out.insert(tp.clone(), rate);
            }
        }
        out
    }

    /// Current number of partitions with history entries. Used for
    /// diagnostics / metrics.
    pub fn tracked_partitions(&self) -> usize {
        self.history.lock().unwrap_or_else(|p| p.into_inner()).len()
    }

    /// Drop all history. Used when a cluster-wide reset is needed (e.g.
    /// leadership lost) — currently not wired; stale history auto-recovers
    /// because rate is computed from endpoints of the window.
    #[allow(dead_code)]
    pub fn clear(&self) {
        self.history
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tp(topic: &str, partition: i32) -> TopicPartition {
        TopicPartition::new(topic, partition)
    }

    #[test]
    fn single_sample_returns_none() {
        let s = RateSampler::new(5, Duration::from_secs(600), 0.01);
        let mut wm = HashMap::new();
        wm.insert(tp("t", 0), (0, 100));
        s.record_watermarks(&wm);
        assert_eq!(s.estimate_lag_seconds(&tp("t", 0), 50), None);
    }

    #[test]
    fn two_samples_compute_rate() {
        let s = RateSampler::new(5, Duration::from_secs(600), 0.01);

        let mut wm1 = HashMap::new();
        wm1.insert(tp("t", 0), (0, 100));
        s.record_watermarks(&wm1);

        // A longer sleep reduces the relative impact of scheduler jitter
        // on CI. We don't assert a tight window — only that the estimate
        // is positive and physically plausible for the setup (lag=500,
        // min rate enforced at 0.01 msgs/s ⇒ upper bound is huge). The
        // intent is to exercise the math end-to-end, not the OS scheduler.
        std::thread::sleep(Duration::from_millis(250));

        let mut wm2 = HashMap::new();
        wm2.insert(tp("t", 0), (0, 1100));
        s.record_watermarks(&wm2);

        let est = s
            .estimate_lag_seconds(&tp("t", 0), 500)
            .expect("should compute rate");
        assert!(est > 0.0 && est.is_finite(), "non-positive estimate: {est}");
    }

    #[test]
    fn zero_lag_is_zero_seconds() {
        let s = RateSampler::new(5, Duration::from_secs(600), 0.01);
        let mut wm = HashMap::new();
        wm.insert(tp("t", 0), (0, 100));
        s.record_watermarks(&wm);
        assert_eq!(s.estimate_lag_seconds(&tp("t", 0), 0), Some(0.0));
        // Even a single-sample partition returns Some(0.0) for zero lag.
    }

    #[test]
    fn idle_partition_below_min_rate_returns_none() {
        let s = RateSampler::new(5, Duration::from_secs(600), 100.0);

        let mut wm1 = HashMap::new();
        wm1.insert(tp("t", 0), (0, 100));
        s.record_watermarks(&wm1);
        std::thread::sleep(Duration::from_millis(50));
        // hwm unchanged → rate = 0 → below floor → None.
        s.record_watermarks(&wm1);

        assert_eq!(s.estimate_lag_seconds(&tp("t", 0), 10), None);
    }

    #[test]
    fn history_bounded_by_max_samples() {
        let s = RateSampler::new(3, Duration::from_secs(600), 0.01);
        let mut wm = HashMap::new();
        wm.insert(tp("t", 0), (0, 0));
        for i in 0..10 {
            wm.insert(tp("t", 0), (0, i));
            s.record_watermarks(&wm);
        }
        let history = s.history.lock().unwrap();
        let buf = history.get(&tp("t", 0)).unwrap();
        assert_eq!(buf.len(), 3, "must be bounded to max_samples");
    }

    #[test]
    fn unmonitored_partitions_pruned() {
        let s = RateSampler::new(5, Duration::from_secs(600), 0.01);
        let mut wm = HashMap::new();
        wm.insert(tp("t", 0), (0, 100));
        wm.insert(tp("other", 0), (0, 100));
        s.record_watermarks(&wm);
        assert_eq!(s.tracked_partitions(), 2);

        let mut wm2 = HashMap::new();
        wm2.insert(tp("t", 0), (0, 101));
        s.record_watermarks(&wm2);
        assert_eq!(s.tracked_partitions(), 1);
    }

    #[test]
    fn retention_rewind_returns_none() {
        // Unusual but possible: offsets reset / retention-driven rewind.
        // d_hwm ≤ 0 → return None.
        let s = RateSampler::new(5, Duration::from_secs(600), 0.01);
        let mut wm1 = HashMap::new();
        wm1.insert(tp("t", 0), (0, 1000));
        s.record_watermarks(&wm1);
        std::thread::sleep(Duration::from_millis(30));
        let mut wm2 = HashMap::new();
        wm2.insert(tp("t", 0), (0, 500));
        s.record_watermarks(&wm2);
        assert_eq!(s.estimate_lag_seconds(&tp("t", 0), 100), None);
    }
}
