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
use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::ptr;
use std::time::Duration;
use tracing::{debug, warn};

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

/// Fetch offsets (EARLIEST or LATEST per `spec`) for a set of partitions in a
/// single batched Admin API call. librdkafka routes per leader broker
/// internally, collapsing O(partitions) round trips to O(brokers).
///
/// Partial failure policy: per-partition errors are logged at WARN and omitted
/// from the returned map. Top-level event errors propagate as `KlagError::Admin`.
pub fn list_offsets_batched(
    admin: &AdminClient<DefaultClientContext>,
    partitions: &[TopicPartition],
    spec: OffsetSpec,
    timeout: Duration,
) -> Result<HashMap<TopicPartition, i64>> {
    if partitions.is_empty() {
        return Ok(HashMap::new());
    }

    let timeout_ms = timeout.as_millis().min(i32::MAX as u128) as i32;
    let rk = admin_native_ptr(admin);

    // RAII cleanup guard — mirrors existing pattern in client.rs.
    struct Cleanup {
        tpl: *mut rd_kafka_topic_partition_list_t,
        options: *mut rd_kafka_AdminOptions_t,
        queue: *mut rd_kafka_queue_t,
        event: *mut rd_kafka_event_t,
        _topic_cstrings: Vec<CString>,
    }
    impl Drop for Cleanup {
        fn drop(&mut self) {
            unsafe {
                if !self.event.is_null() {
                    rd_kafka_event_destroy(self.event);
                }
                if !self.queue.is_null() {
                    rd_kafka_queue_destroy(self.queue);
                }
                if !self.options.is_null() {
                    rd_kafka_AdminOptions_destroy(self.options);
                }
                // rd_kafka_ListOffsets does NOT take ownership — free it ourselves.
                if !self.tpl.is_null() {
                    rd_kafka_topic_partition_list_destroy(self.tpl);
                }
            }
        }
    }

    unsafe {
        let c_tpl = rd_kafka_topic_partition_list_new(partitions.len() as i32);
        if c_tpl.is_null() {
            return Err(KlagError::Admin(
                "Failed to create topic partition list".into(),
            ));
        }

        let mut cleanup = Cleanup {
            tpl: c_tpl,
            options: ptr::null_mut(),
            queue: ptr::null_mut(),
            event: ptr::null_mut(),
            _topic_cstrings: Vec::with_capacity(partitions.len()),
        };

        let spec_value = spec.as_c_value();
        for tp in partitions {
            let topic_cstr = cstring_or_err(&tp.topic)?;
            cleanup._topic_cstrings.push(topic_cstr);
            let cstr_ptr = cleanup._topic_cstrings.last().unwrap().as_ptr();
            let elem = rd_kafka_topic_partition_list_add(c_tpl, cstr_ptr, tp.partition);
            if elem.is_null() {
                return Err(KlagError::Admin(
                    "Failed to add partition to ListOffsets request".into(),
                ));
            }
            // ListOffsets API: offset field holds the OffsetSpec sentinel.
            (*elem).offset = spec_value;
        }

        let options = rd_kafka_AdminOptions_new(
            rk,
            rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_LISTOFFSETS,
        );
        if options.is_null() {
            return Err(KlagError::Admin(
                "Failed to create AdminOptions (ListOffsets)".into(),
            ));
        }
        cleanup.options = options;

        let mut errstr_buf = [0 as c_char; 512];
        let err = rd_kafka_AdminOptions_set_request_timeout(
            options,
            timeout_ms,
            errstr_buf.as_mut_ptr(),
            errstr_buf.len(),
        );
        if err != rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
            return Err(KlagError::Admin(format!(
                "Failed to set request timeout (ListOffsets): {}",
                errstr_to_string(&errstr_buf)
            )));
        }

        let queue = rd_kafka_queue_new(rk);
        if queue.is_null() {
            return Err(KlagError::Admin(
                "Failed to create queue (ListOffsets)".into(),
            ));
        }
        cleanup.queue = queue;

        rd_kafka_ListOffsets(rk, c_tpl, options, queue);

        let event = rd_kafka_queue_poll(queue, timeout_ms);
        if event.is_null() {
            return Err(KlagError::Admin("ListOffsets timed out".into()));
        }
        cleanup.event = event;

        let event_type = rd_kafka_event_type(event);
        if event_type != RD_KAFKA_EVENT_LISTOFFSETS_RESULT {
            return Err(KlagError::Admin(format!(
                "Unexpected event type (ListOffsets): {event_type}"
            )));
        }

        let resp_err = rd_kafka_event_error(event);
        if resp_err != rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
            let err_cstr = rd_kafka_event_error_string(event);
            let err_msg = if err_cstr.is_null() {
                "unknown error".to_string()
            } else {
                CStr::from_ptr(err_cstr).to_string_lossy().to_string()
            };
            return Err(KlagError::Admin(format!("ListOffsets failed: {err_msg}")));
        }

        let result = rd_kafka_event_ListOffsets_result(event);
        if result.is_null() {
            return Err(KlagError::Admin("ListOffsets result is null".into()));
        }

        let mut n_infos: usize = 0;
        let infos_ptr = rd_kafka_ListOffsets_result_infos(result, &mut n_infos);
        let mut out = HashMap::with_capacity(n_infos);

        if infos_ptr.is_null() || n_infos == 0 {
            debug!(spec = ?spec, "No ListOffsets results returned");
            return Ok(out);
        }

        for i in 0..n_infos {
            let info = *infos_ptr.add(i);
            let tp_ptr = rd_kafka_ListOffsetsResultInfo_topic_partition(info);
            if tp_ptr.is_null() {
                continue;
            }
            let tp_ref = &*tp_ptr;

            if tp_ref.err != rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
                let err_name = rd_kafka_err2name(tp_ref.err);
                let err_str = if err_name.is_null() {
                    "unknown".to_string()
                } else {
                    CStr::from_ptr(err_name).to_string_lossy().to_string()
                };
                let topic = if tp_ref.topic.is_null() {
                    "<null>".to_string()
                } else {
                    CStr::from_ptr(tp_ref.topic).to_string_lossy().to_string()
                };
                warn!(
                    topic = %topic,
                    partition = tp_ref.partition,
                    spec = ?spec,
                    error = %err_str,
                    "ListOffsets per-partition error"
                );
                continue;
            }

            if tp_ref.topic.is_null() {
                continue;
            }
            let topic = CStr::from_ptr(tp_ref.topic).to_string_lossy().to_string();
            out.insert(TopicPartition::new(topic, tp_ref.partition), tp_ref.offset);
        }

        debug!(
            spec = ?spec,
            requested = partitions.len(),
            returned = out.len(),
            "Batched ListOffsets complete"
        );
        Ok(out)
    }
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
