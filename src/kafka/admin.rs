//! Batched Admin API FFI wrappers around librdkafka's `rd_kafka_*` functions.
//!
//! These wrappers provide per-cycle bulk operations that replace per-partition
//! / per-group call fan-outs. All `unsafe` blocks are isolated to this module
//! and every C object allocated is released via an RAII guard on any exit path.
//!
//! The pattern mirrors the existing single-group `list_consumer_group_offsets`
//! wrapper that currently lives in `kafka/client.rs`.

use crate::error::{KlagError, Result};
use crate::kafka::client::TopicPartition;
use rdkafka::admin::AdminClient;
use rdkafka::bindings::*;
use rdkafka::client::DefaultClientContext;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;

/// Offset spec for `list_offsets_batched` — mirrors `RD_KAFKA_OFFSET_SPEC_*`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OffsetSpec {
    Earliest,
    Latest,
}

impl OffsetSpec {
    fn as_c_value(self) -> i64 {
        match self {
            OffsetSpec::Earliest => rd_kafka_OffsetSpec_t::RD_KAFKA_OFFSET_SPEC_EARLIEST as i64,
            OffsetSpec::Latest => rd_kafka_OffsetSpec_t::RD_KAFKA_OFFSET_SPEC_LATEST as i64,
        }
    }
}

/// Copy the librdkafka errstr buffer out as a String.
pub(crate) fn errstr_to_string(buf: &[c_char]) -> String {
    // SAFETY: librdkafka null-terminates; buf is a stack array owned by caller.
    unsafe { CStr::from_ptr(buf.as_ptr()).to_string_lossy().to_string() }
}

/// Build a C cstring from a Rust &str, returning a KlagError on embedded NULs.
pub(crate) fn cstring_or_err(s: &str) -> Result<CString> {
    CString::new(s).map_err(|e| KlagError::Admin(format!("Invalid C string '{s}': {e}")))
}

/// Keep the admin client alive for the duration of an FFI call. This type is
/// intentionally opaque: callers pass `&AdminClient` and we hold references
/// that must not outlive it.
pub(crate) fn admin_native_ptr(admin: &AdminClient<DefaultClientContext>) -> *mut rd_kafka_t {
    admin.inner().native_ptr()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cstring_or_err_rejects_embedded_nul() {
        let err = cstring_or_err("bad\0name").unwrap_err();
        assert!(err.to_string().contains("Invalid C string"));
    }

    #[test]
    fn cstring_or_err_accepts_normal_string() {
        let s = cstring_or_err("my-group").unwrap();
        assert_eq!(s.to_str().unwrap(), "my-group");
    }

    #[test]
    fn offset_spec_constants_match_librdkafka() {
        // RD_KAFKA_OFFSET_SPEC_EARLIEST == -2, _LATEST == -1 (from rdkafka.h).
        assert_eq!(OffsetSpec::Earliest.as_c_value(), -2);
        assert_eq!(OffsetSpec::Latest.as_c_value(), -1);
    }
}
