//! Batched Admin API FFI wrappers around librdkafka's `rd_kafka_*` functions.
//!
//! These wrappers provide per-cycle bulk operations that replace per-partition
//! / per-group call fan-outs. All `unsafe` blocks are isolated to this module
//! and every C object allocated is released via an RAII guard on any exit
//! path (panic-safe).
//!
//! The cleanup-guard pattern originated with the single-group
//! `list_consumer_group_offsets` wrapper introduced in commit `9e46820` /
//! PR #57 (FFI memory-leak fix). That single-group wrapper was replaced by
//! the batched functions in this module; the pattern is preserved.

use crate::error::{KlagError, Result};
use crate::kafka::client::TopicPartition;
use crate::retry::full_jitter_backoff;
use rdkafka::admin::AdminClient;
use rdkafka::bindings::{
    rd_kafka_AdminOptions_destroy, rd_kafka_AdminOptions_new,
    rd_kafka_AdminOptions_set_request_timeout, rd_kafka_AdminOptions_t,
    rd_kafka_ConsumerGroupDescription_error, rd_kafka_ConsumerGroupDescription_group_id,
    rd_kafka_ConsumerGroupDescription_member, rd_kafka_ConsumerGroupDescription_member_count,
    rd_kafka_ConsumerGroupDescription_state, rd_kafka_DescribeConsumerGroups,
    rd_kafka_DescribeConsumerGroups_result_groups, rd_kafka_ListConsumerGroupOffsets,
    rd_kafka_ListConsumerGroupOffsets_destroy, rd_kafka_ListConsumerGroupOffsets_new,
    rd_kafka_ListConsumerGroupOffsets_result_groups, rd_kafka_ListConsumerGroupOffsets_t,
    rd_kafka_ListOffsets, rd_kafka_ListOffsetsResultInfo_topic_partition,
    rd_kafka_ListOffsets_result_infos, rd_kafka_MemberAssignment_partitions,
    rd_kafka_MemberDescription_assignment, rd_kafka_MemberDescription_client_id,
    rd_kafka_MemberDescription_consumer_id, rd_kafka_MemberDescription_host, rd_kafka_OffsetSpec_t,
    rd_kafka_admin_op_t, rd_kafka_consumer_group_state_name, rd_kafka_err2name,
    rd_kafka_error_code, rd_kafka_error_is_retriable, rd_kafka_error_string,
    rd_kafka_event_DescribeConsumerGroups_result, rd_kafka_event_ListConsumerGroupOffsets_result,
    rd_kafka_event_ListOffsets_result, rd_kafka_event_destroy, rd_kafka_event_error,
    rd_kafka_event_error_string, rd_kafka_event_t, rd_kafka_event_type,
    rd_kafka_group_result_error, rd_kafka_group_result_name, rd_kafka_group_result_partitions,
    rd_kafka_queue_destroy, rd_kafka_queue_new, rd_kafka_queue_poll, rd_kafka_queue_t,
    rd_kafka_resp_err_t, rd_kafka_t, rd_kafka_topic_partition_list_add,
    rd_kafka_topic_partition_list_destroy, rd_kafka_topic_partition_list_new,
    rd_kafka_topic_partition_list_t, RD_KAFKA_EVENT_DESCRIBECONSUMERGROUPS_RESULT,
    RD_KAFKA_EVENT_LISTCONSUMERGROUPOFFSETS_RESULT, RD_KAFKA_EVENT_LISTOFFSETS_RESULT,
};
use rdkafka::client::DefaultClientContext;
use std::collections::{HashMap, HashSet};
use std::ffi::{CStr, CString};
use std::fmt;
use std::os::raw::c_char;
use std::ptr;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, warn};

/// Per-call topic-name interner: one `Arc<str>` allocation per unique topic,
/// so sibling partitions of the same topic share storage in the result map.
/// On large clusters the same batched `ListOffsets` result contains every
/// partition on the cluster — without interning, 19K `Arc<str>` allocations
/// are wasted on topic names that only have a few dozen unique values.
///
/// The set is keyed directly by `Arc<str>`; `get_key_value(&str)` works
/// because `Arc<T>: Borrow<T>`, so the lookup hashes the topic as a `str`
/// but returns the existing `Arc<str>` key to clone. This keeps the
/// allocation count at exactly one `Arc<str>` per unique topic.
#[derive(Default)]
struct TopicInterner {
    by_name: HashMap<Arc<str>, ()>,
}

impl TopicInterner {
    fn intern(&mut self, topic: &str) -> Arc<str> {
        if let Some((a, ())) = self.by_name.get_key_value(topic) {
            return Arc::clone(a);
        }
        let a: Arc<str> = Arc::from(topic);
        self.by_name.insert(Arc::clone(&a), ());
        a
    }
}

/// Offset spec for `list_offsets_batched` — mirrors `RD_KAFKA_OFFSET_SPEC_*`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OffsetSpec {
    Earliest,
    Latest,
}

impl OffsetSpec {
    const fn as_c_value(self) -> i64 {
        match self {
            Self::Earliest => rd_kafka_OffsetSpec_t::RD_KAFKA_OFFSET_SPEC_EARLIEST as i64,
            Self::Latest => rd_kafka_OffsetSpec_t::RD_KAFKA_OFFSET_SPEC_LATEST as i64,
        }
    }
}

/// Copy the librdkafka errstr buffer out as a String.
pub fn errstr_to_string(buf: &[c_char]) -> String {
    // SAFETY: librdkafka null-terminates; buf is a stack array owned by caller.
    unsafe { CStr::from_ptr(buf.as_ptr()).to_string_lossy().to_string() }
}

/// Build a C cstring from a Rust &str, returning a `KlagError` on embedded NULs.
pub fn cstring_or_err(s: &str) -> Result<CString> {
    CString::new(s).map_err(|e| KlagError::Admin(format!("Invalid C string '{s}': {e}")))
}

#[derive(Debug, Clone)]
pub(crate) struct AdminRequestError {
    operation: &'static str,
    code: Option<rd_kafka_resp_err_t>,
    message: String,
    retriable: bool,
}

impl AdminRequestError {
    fn new(
        operation: &'static str,
        code: Option<rd_kafka_resp_err_t>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            operation,
            code,
            message: message.into(),
            retriable: code.is_some_and(is_retriable_admin_code),
        }
    }

    fn from_group_error(
        operation: &'static str,
        error: *const rdkafka::bindings::rd_kafka_error_t,
    ) -> Self {
        // SAFETY: callers obtain `error` from a live Admin API result and keep
        // its event alive for this entire conversion.
        let code = unsafe { rd_kafka_error_code(error) };
        let message = unsafe { ptr_to_string(rd_kafka_error_string(error)) };
        let native_retriable = unsafe { rd_kafka_error_is_retriable(error) != 0 };
        let mut converted = Self::new(operation, Some(code), message);
        // Some Admin API result constructors copy only the code and message,
        // dropping librdkafka's retriable flag. Keep the native flag when it
        // survives, and otherwise use the documented code allow-list below.
        converted.retriable |= native_retriable;
        converted
    }

    pub(crate) fn task(operation: &'static str, message: impl Into<String>) -> Self {
        Self::new(operation, None, message)
    }

    pub(crate) const fn is_retriable(&self) -> bool {
        self.retriable
    }
}

impl fmt::Display for AdminRequestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.code {
            Some(code) => write!(f, "{}: {} ({code:?})", self.operation, self.message),
            None => write!(f, "{}: {}", self.operation, self.message),
        }
    }
}

/// Conservative allow-list for transient Admin API errors.
/// Anything not in this list is considered non-retriable.
const fn is_retriable_admin_code(code: rd_kafka_resp_err_t) -> bool {
    matches!(
        code,
        rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR__TRANSPORT
            | rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN
            | rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR__TIMED_OUT
            | rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR__TIMED_OUT_QUEUE
            | rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR__WAIT_COORD
            | rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT
            | rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_BROKER_NOT_AVAILABLE
            | rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NETWORK_EXCEPTION
            | rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_COORDINATOR_LOAD_IN_PROGRESS
            | rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_COORDINATOR_NOT_AVAILABLE
            | rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NOT_COORDINATOR
    )
}

/// Keep the admin client alive for the duration of an FFI call. This type is
/// intentionally opaque: callers pass `&AdminClient` and we hold references
/// that must not outlive it.
pub fn admin_native_ptr(admin: &AdminClient<DefaultClientContext>) -> *mut rd_kafka_t {
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
        topic_cstrings: Vec<CString>,
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
            topic_cstrings: Vec::with_capacity(partitions.len()),
        };

        let spec_value = spec.as_c_value();
        for tp in partitions {
            let topic_cstr = cstring_or_err(&tp.topic)?;
            cleanup.topic_cstrings.push(topic_cstr);
            let cstr_ptr = cleanup.topic_cstrings.last().unwrap().as_ptr();
            let elem = rd_kafka_topic_partition_list_add(c_tpl, cstr_ptr, tp.partition);
            if elem.is_null() {
                return Err(KlagError::Admin(
                    "Failed to add partition to ListOffsets request".into(),
                ));
            }
            // ListOffsets API: offset field holds the OffsetSpec sentinel.
            (*elem).offset = spec_value;
        }

        let options =
            rd_kafka_AdminOptions_new(rk, rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_LISTOFFSETS);
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
        let infos_ptr = rd_kafka_ListOffsets_result_infos(result, &raw mut n_infos);
        let mut out = HashMap::with_capacity(n_infos);

        if infos_ptr.is_null() || n_infos == 0 {
            debug!(spec = ?spec, "No ListOffsets results returned");
            return Ok(out);
        }

        let mut interner = TopicInterner::default();

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
            let topic_str = CStr::from_ptr(tp_ref.topic).to_string_lossy();
            let topic_arc = interner.intern(topic_str.as_ref());
            out.insert(
                TopicPartition::new(topic_arc, tp_ref.partition),
                tp_ref.offset,
            );
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

/// Ready-to-consume representation of a consumer group description. Mirrors
/// `kafka::client::GroupDescription` shape so callers don't need to translate
/// beyond trivial field rename.
#[derive(Debug, Clone)]
pub struct BatchedGroupDescription {
    pub group_id: String,
    pub state: String,
    pub members: Vec<BatchedMember>,
}

#[derive(Debug, Clone)]
pub struct BatchedMember {
    pub member_id: String,
    pub client_id: String,
    pub client_host: String,
    pub assignments: Vec<TopicPartition>,
}

/// Describe consumer groups in batches via `rd_kafka_DescribeConsumerGroups`.
/// Chunks the input into sub-calls of at most `chunk_size` groups each and
/// dispatches the chunks concurrently (bounded by `max_concurrent_chunks`).
/// Per-group errors inside a successful chunk are logged at WARN; a whole
/// chunk that fails at the FFI/event layer is logged at WARN and the
/// surviving chunks' results are returned.
///
/// When `parse_assignments = false`, each returned `BatchedMember` has an
/// empty `assignments: Vec<TopicPartition>`. This skips a per-member
/// iteration over the assignment's `rd_kafka_topic_partition_list_t` — the
/// data is only consumed by per-partition metrics (granularity = "partition"),
/// so it's pure wasted work at the default topic granularity.
pub async fn describe_consumer_groups_batched(
    admin: Arc<AdminClient<DefaultClientContext>>,
    group_ids: &[&str],
    timeout: Duration,
    chunk_size: usize,
    parse_assignments: bool,
    max_concurrent_chunks: usize,
    max_retries: usize,
) -> Result<Vec<BatchedGroupDescription>> {
    if group_ids.is_empty() {
        return Ok(Vec::new());
    }
    let chunk_size = chunk_size.max(1);
    let max_concurrent = max_concurrent_chunks.max(1);
    let semaphore = Arc::new(tokio::sync::Semaphore::new(max_concurrent));
    let mut out = Vec::with_capacity(group_ids.len());
    let mut pending: Vec<String> = group_ids.iter().map(|group| (*group).to_string()).collect();
    let max_attempts = max_retries.saturating_add(1);

    for attempt in 1..=max_attempts {
        if attempt > 1 {
            let retry_number = attempt - 1;
            let delay = full_jitter_backoff(retry_number);
            debug!(
                failed_groups = pending.len(),
                attempt = attempt,
                max_attempts = max_attempts,
                backoff_ms = delay.as_millis(),
                "Retrying failed DescribeConsumerGroups requests"
            );
            tokio::time::sleep(delay).await;
        }

        // Own each chunk so its blocking task cannot borrow the caller's
        // group-id slice. Retried rounds contain only groups that failed in
        // the previous round.
        let chunks: Vec<Vec<String>> = pending.chunks(chunk_size).map(<[String]>::to_vec).collect();
        let mut calls = Vec::with_capacity(chunks.len());
        for chunk in chunks {
            let permit = Arc::clone(&semaphore);
            let admin = Arc::clone(&admin);
            calls.push(async move {
                let _permit = permit.acquire_owned().await.expect("semaphore closed");
                let requested = chunk.clone();
                let result = tokio::task::spawn_blocking(move || {
                    let refs: Vec<&str> = chunk.iter().map(std::string::String::as_str).collect();
                    describe_consumer_groups_one_chunk(&admin, &refs, timeout, parse_assignments)
                })
                .await;
                (requested, result)
            });
        }

        let results = futures::future::join_all(calls).await;
        let mut retryable_failures = Vec::new();

        for (requested, result) in results {
            let response = match result {
                Ok(Ok(response)) => response,
                Ok(Err(error)) => DescribeGroupsResponse::failed(requested, error),
                Err(error) => {
                    let mode = if error.is_cancelled() {
                        "cancelled"
                    } else if error.is_panic() {
                        "panicked"
                    } else {
                        "failed"
                    };
                    let error = AdminRequestError::task(
                        "DescribeConsumerGroups",
                        format!("blocking task {mode}: {error}"),
                    );
                    DescribeGroupsResponse::failed(requested, error)
                }
            };

            let partitioned = response.partition_for_retry(attempt < max_attempts);
            out.extend(partitioned.descriptions);
            retryable_failures.extend(partitioned.retryable_group_ids);
            for failure in partitioned.terminal_failures {
                warn!(
                    group = %failure.group_id,
                    error = %failure.error,
                    attempts = attempt,
                    retriable = failure.error.is_retriable(),
                    "DescribeConsumerGroups per-group error"
                );
            }
        }

        if retryable_failures.is_empty() {
            break;
        }
        pending = retryable_failures;
    }

    Ok(out)
}

#[derive(Debug)]
struct GroupRequestFailure {
    group_id: String,
    error: AdminRequestError,
}

#[derive(Debug)]
struct DescribeGroupsResponse {
    descriptions: Vec<BatchedGroupDescription>,
    failures: Vec<GroupRequestFailure>,
}

impl DescribeGroupsResponse {
    fn failed(group_ids: Vec<String>, error: AdminRequestError) -> Self {
        Self {
            descriptions: Vec::new(),
            failures: group_ids
                .into_iter()
                .map(|group_id| GroupRequestFailure {
                    group_id,
                    error: error.clone(),
                })
                .collect(),
        }
    }

    fn partition_for_retry(self, can_retry: bool) -> DescribeRetryPartition {
        let mut retryable_group_ids = Vec::new();
        let mut terminal_failures = Vec::new();
        for failure in self.failures {
            if can_retry && failure.error.is_retriable() {
                retryable_group_ids.push(failure.group_id);
            } else {
                terminal_failures.push(failure);
            }
        }
        DescribeRetryPartition {
            descriptions: self.descriptions,
            retryable_group_ids,
            terminal_failures,
        }
    }
}

struct DescribeRetryPartition {
    descriptions: Vec<BatchedGroupDescription>,
    retryable_group_ids: Vec<String>,
    terminal_failures: Vec<GroupRequestFailure>,
}

fn describe_consumer_groups_one_chunk(
    admin: &AdminClient<DefaultClientContext>,
    group_ids: &[&str],
    timeout: Duration,
    parse_assignments: bool,
) -> std::result::Result<DescribeGroupsResponse, AdminRequestError> {
    let timeout_ms = timeout.as_millis().min(i32::MAX as u128) as i32;
    let rk = admin_native_ptr(admin);

    // Build array of *const c_char pointing to CString buffers. `cstrings`
    // must outlive the FFI call (ptrs borrow from it).
    let cstrings: Vec<CString> = group_ids
        .iter()
        .map(|g| cstring_or_err(g))
        .collect::<Result<Vec<_>>>()
        .map_err(|error| AdminRequestError::task("DescribeConsumerGroups", error.to_string()))?;
    let mut ptrs: Vec<*const c_char> = cstrings.iter().map(|c| c.as_ptr()).collect();

    struct Cleanup {
        options: *mut rd_kafka_AdminOptions_t,
        queue: *mut rd_kafka_queue_t,
        event: *mut rd_kafka_event_t,
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
            }
        }
    }

    unsafe {
        let options = rd_kafka_AdminOptions_new(
            rk,
            rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_DESCRIBECONSUMERGROUPS,
        );
        if options.is_null() {
            return Err(AdminRequestError::task(
                "DescribeConsumerGroups",
                "failed to create AdminOptions",
            ));
        }
        let mut cleanup = Cleanup {
            options,
            queue: ptr::null_mut(),
            event: ptr::null_mut(),
        };

        let mut errstr_buf = [0 as c_char; 512];
        let err = rd_kafka_AdminOptions_set_request_timeout(
            options,
            timeout_ms,
            errstr_buf.as_mut_ptr(),
            errstr_buf.len(),
        );
        if err != rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
            return Err(AdminRequestError::new(
                "DescribeConsumerGroups",
                Some(err),
                format!(
                    "failed to set request timeout: {}",
                    errstr_to_string(&errstr_buf)
                ),
            ));
        }

        let queue = rd_kafka_queue_new(rk);
        if queue.is_null() {
            return Err(AdminRequestError::task(
                "DescribeConsumerGroups",
                "failed to create queue",
            ));
        }
        cleanup.queue = queue;

        rd_kafka_DescribeConsumerGroups(rk, ptrs.as_mut_ptr(), ptrs.len(), options, queue);

        let event = rd_kafka_queue_poll(queue, timeout_ms);
        if event.is_null() {
            return Err(AdminRequestError::new(
                "DescribeConsumerGroups",
                Some(rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR__TIMED_OUT),
                "timed out waiting for response",
            ));
        }
        cleanup.event = event;

        let event_type = rd_kafka_event_type(event);
        if event_type != RD_KAFKA_EVENT_DESCRIBECONSUMERGROUPS_RESULT {
            return Err(AdminRequestError::task(
                "DescribeConsumerGroups",
                format!("unexpected event type: {event_type}"),
            ));
        }

        let resp_err = rd_kafka_event_error(event);
        if resp_err != rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
            let err_cstr = rd_kafka_event_error_string(event);
            let err_msg = if err_cstr.is_null() {
                "unknown error".to_string()
            } else {
                CStr::from_ptr(err_cstr).to_string_lossy().to_string()
            };
            return Err(AdminRequestError::new(
                "DescribeConsumerGroups",
                Some(resp_err),
                format!("request failed: {err_msg}"),
            ));
        }

        let result = rd_kafka_event_DescribeConsumerGroups_result(event);
        if result.is_null() {
            return Err(AdminRequestError::task(
                "DescribeConsumerGroups",
                "result is null",
            ));
        }

        let mut n: usize = 0;
        let groups_ptr = rd_kafka_DescribeConsumerGroups_result_groups(result, &raw mut n);
        let mut descriptions = Vec::with_capacity(n);
        let mut failures = Vec::new();
        let mut returned_groups = HashSet::with_capacity(n);

        if groups_ptr.is_null() {
            n = 0;
        }
        for i in 0..n {
            let grp = *groups_ptr.add(i);
            let group_id = ptr_to_string(rd_kafka_ConsumerGroupDescription_group_id(grp));
            returned_groups.insert(group_id.clone());
            let grp_err = rd_kafka_ConsumerGroupDescription_error(grp);
            if !grp_err.is_null() {
                let code = rd_kafka_error_code(grp_err);
                if code != rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
                    failures.push(GroupRequestFailure {
                        group_id,
                        error: AdminRequestError::from_group_error(
                            "DescribeConsumerGroups",
                            grp_err,
                        ),
                    });
                    continue;
                }
            }

            let state = rd_kafka_ConsumerGroupDescription_state(grp);
            let state_str = ptr_to_string(rd_kafka_consumer_group_state_name(state));

            let member_count = rd_kafka_ConsumerGroupDescription_member_count(grp);
            let mut members = Vec::with_capacity(member_count);
            for m_idx in 0..member_count {
                let member = rd_kafka_ConsumerGroupDescription_member(grp, m_idx);
                if member.is_null() {
                    continue;
                }
                let member_id = ptr_to_string(rd_kafka_MemberDescription_consumer_id(member));
                let client_id = ptr_to_string(rd_kafka_MemberDescription_client_id(member));
                let client_host = ptr_to_string(rd_kafka_MemberDescription_host(member));

                let mut assignments = Vec::new();
                if parse_assignments {
                    let assignment = rd_kafka_MemberDescription_assignment(member);
                    if !assignment.is_null() {
                        let tpl_ptr = rd_kafka_MemberAssignment_partitions(assignment);
                        if !tpl_ptr.is_null() {
                            let tpl = &*tpl_ptr;
                            for j in 0..tpl.cnt {
                                let el = &*tpl.elems.add(j as usize);
                                if el.topic.is_null() {
                                    continue;
                                }
                                let topic = CStr::from_ptr(el.topic).to_string_lossy().to_string();
                                assignments.push(TopicPartition::new(topic, el.partition));
                            }
                        }
                    }
                }

                members.push(BatchedMember {
                    member_id,
                    client_id,
                    client_host,
                    assignments,
                });
            }

            descriptions.push(BatchedGroupDescription {
                group_id,
                state: state_str,
                members,
            });
        }

        // A successful event should contain one result per requested group.
        // Preserve a missing result as an explicit, non-retriable failure
        // instead of silently turning it into absent group data.
        for group_id in group_ids {
            if !returned_groups.contains(*group_id) {
                failures.push(GroupRequestFailure {
                    group_id: (*group_id).to_string(),
                    error: AdminRequestError::task(
                        "DescribeConsumerGroups",
                        "response omitted the requested group",
                    ),
                });
            }
        }

        debug!(
            requested = group_ids.len(),
            returned = descriptions.len(),
            failed = failures.len(),
            "Batched DescribeConsumerGroups complete"
        );
        Ok(DescribeGroupsResponse {
            descriptions,
            failures,
        })
    }
}

unsafe fn ptr_to_string(p: *const c_char) -> String {
    if p.is_null() {
        String::new()
    } else {
        CStr::from_ptr(p).to_string_lossy().to_string()
    }
}

/// Fetch committed offsets for many consumer groups in one batched Admin API
/// call. Passes `NULL` partition list for each group, so the broker returns
/// every committed partition — downstream topic filtering is applied on the
/// (much smaller) response.
///
/// Chunks groups into sub-calls of at most `chunk_size` groups each.
pub(crate) fn list_consumer_group_offsets_batched(
    admin: &AdminClient<DefaultClientContext>,
    group_ids: &[&str],
    timeout: Duration,
    chunk_size: usize,
) -> std::result::Result<GroupOffsetsResponse, AdminRequestError> {
    if group_ids.is_empty() {
        return Ok(GroupOffsetsResponse::default());
    }
    let chunk_size = chunk_size.max(1);
    let mut response = GroupOffsetsResponse {
        offsets: HashMap::with_capacity(group_ids.len()),
        failures: Vec::new(),
    };
    for chunk in group_ids.chunks(chunk_size) {
        let mut part = list_consumer_group_offsets_one_chunk(admin, chunk, timeout)?;
        response.offsets.extend(part.offsets);
        response.failures.append(&mut part.failures);
    }
    Ok(response)
}

#[derive(Debug, Default)]
pub(crate) struct GroupOffsetsResponse {
    offsets: HashMap<String, HashMap<TopicPartition, i64>>,
    failures: Vec<GroupRequestFailure>,
}

impl GroupOffsetsResponse {
    /// Convert the response from the collector's current one-group-per-call
    /// path into a retryable result. A successful empty offset map remains a
    /// success; only an explicit group failure becomes `Err`.
    pub(crate) fn into_single_group_result(
        mut self,
    ) -> std::result::Result<HashMap<String, HashMap<TopicPartition, i64>>, AdminRequestError> {
        if let Some(failure) = self.failures.pop() {
            return Err(failure.error);
        }
        Ok(self.offsets)
    }
}

fn list_consumer_group_offsets_one_chunk(
    admin: &AdminClient<DefaultClientContext>,
    group_ids: &[&str],
    timeout: Duration,
) -> std::result::Result<GroupOffsetsResponse, AdminRequestError> {
    let timeout_ms = timeout.as_millis().min(i32::MAX as u128) as i32;
    let rk = admin_native_ptr(admin);

    let cstrings: Vec<CString> = group_ids
        .iter()
        .map(|g| cstring_or_err(g))
        .collect::<Result<Vec<_>>>()
        .map_err(|error| AdminRequestError::task("ListConsumerGroupOffsets", error.to_string()))?;

    struct Cleanup {
        requests: Vec<*mut rd_kafka_ListConsumerGroupOffsets_t>,
        options: *mut rd_kafka_AdminOptions_t,
        queue: *mut rd_kafka_queue_t,
        event: *mut rd_kafka_event_t,
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
                for r in self.requests.drain(..) {
                    if !r.is_null() {
                        rd_kafka_ListConsumerGroupOffsets_destroy(r);
                    }
                }
            }
        }
    }

    unsafe {
        let mut cleanup = Cleanup {
            requests: Vec::with_capacity(cstrings.len()),
            options: ptr::null_mut(),
            queue: ptr::null_mut(),
            event: ptr::null_mut(),
        };

        for c in &cstrings {
            // NULL partition list → broker returns every committed partition
            // for this group. Eliminates the 19K-entry partition-list clone
            // amplifier from the old per-group call path.
            let req = rd_kafka_ListConsumerGroupOffsets_new(c.as_ptr(), ptr::null_mut());
            if req.is_null() {
                return Err(AdminRequestError::task(
                    "ListConsumerGroupOffsets",
                    "failed to create request",
                ));
            }
            cleanup.requests.push(req);
        }

        let options = rd_kafka_AdminOptions_new(
            rk,
            rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_LISTCONSUMERGROUPOFFSETS,
        );
        if options.is_null() {
            return Err(AdminRequestError::task(
                "ListConsumerGroupOffsets",
                "failed to create AdminOptions",
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
            return Err(AdminRequestError::new(
                "ListConsumerGroupOffsets",
                Some(err),
                format!(
                    "failed to set request timeout: {}",
                    errstr_to_string(&errstr_buf)
                ),
            ));
        }

        let queue = rd_kafka_queue_new(rk);
        if queue.is_null() {
            return Err(AdminRequestError::task(
                "ListConsumerGroupOffsets",
                "failed to create queue",
            ));
        }
        cleanup.queue = queue;

        rd_kafka_ListConsumerGroupOffsets(
            rk,
            cleanup.requests.as_mut_ptr(),
            cleanup.requests.len(),
            options,
            queue,
        );

        let event = rd_kafka_queue_poll(queue, timeout_ms);
        if event.is_null() {
            return Err(AdminRequestError::new(
                "ListConsumerGroupOffsets",
                Some(rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR__TIMED_OUT),
                "timed out waiting for response",
            ));
        }
        cleanup.event = event;

        let event_type = rd_kafka_event_type(event);
        if event_type != RD_KAFKA_EVENT_LISTCONSUMERGROUPOFFSETS_RESULT {
            return Err(AdminRequestError::task(
                "ListConsumerGroupOffsets",
                format!("unexpected event type: {event_type}"),
            ));
        }

        let resp_err = rd_kafka_event_error(event);
        if resp_err != rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
            let err_cstr = rd_kafka_event_error_string(event);
            let err_msg = if err_cstr.is_null() {
                "unknown error".to_string()
            } else {
                CStr::from_ptr(err_cstr).to_string_lossy().to_string()
            };
            return Err(AdminRequestError::new(
                "ListConsumerGroupOffsets",
                Some(resp_err),
                format!("request failed: {err_msg}"),
            ));
        }

        let result = rd_kafka_event_ListConsumerGroupOffsets_result(event);
        if result.is_null() {
            return Err(AdminRequestError::task(
                "ListConsumerGroupOffsets",
                "result is null",
            ));
        }

        let mut n_groups: usize = 0;
        let groups_ptr = rd_kafka_ListConsumerGroupOffsets_result_groups(result, &raw mut n_groups);
        let mut offsets_by_group = HashMap::with_capacity(n_groups);
        let mut failures = Vec::new();
        let mut returned_groups = HashSet::with_capacity(n_groups);

        if groups_ptr.is_null() {
            n_groups = 0;
        }
        for i in 0..n_groups {
            let group = *groups_ptr.add(i);
            let group_name = rd_kafka_group_result_name(group);
            let group_id = ptr_to_string(group_name);
            returned_groups.insert(group_id.clone());

            let group_error = rd_kafka_group_result_error(group);
            if !group_error.is_null() {
                let code = rd_kafka_error_code(group_error);
                if code != rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
                    failures.push(GroupRequestFailure {
                        group_id,
                        error: AdminRequestError::from_group_error(
                            "ListConsumerGroupOffsets",
                            group_error,
                        ),
                    });
                    continue;
                }
            }

            let partitions = rd_kafka_group_result_partitions(group);
            let mut offsets = HashMap::new();
            if !partitions.is_null() {
                let cnt = (*partitions).cnt;
                let elems = (*partitions).elems;
                if !elems.is_null() {
                    for j in 0..cnt {
                        let elem = &*elems.add(j as usize);
                        // offset == RD_KAFKA_OFFSET_INVALID (-1001) means no committed offset.
                        if elem.offset >= 0 && !elem.topic.is_null() {
                            let topic = CStr::from_ptr(elem.topic).to_string_lossy().to_string();
                            offsets.insert(TopicPartition::new(topic, elem.partition), elem.offset);
                        }
                    }
                }
            }
            // An empty map here is a successful response with no committed
            // offsets, distinct from the explicit failure path above.
            offsets_by_group.insert(group_id, offsets);
        }

        for group_id in group_ids {
            if !returned_groups.contains(*group_id) {
                failures.push(GroupRequestFailure {
                    group_id: (*group_id).to_string(),
                    error: AdminRequestError::task(
                        "ListConsumerGroupOffsets",
                        "response omitted the requested group",
                    ),
                });
            }
        }

        debug!(
            requested = group_ids.len(),
            returned = offsets_by_group.len(),
            failed = failures.len(),
            "Batched ListConsumerGroupOffsets complete"
        );
        Ok(GroupOffsetsResponse {
            offsets: offsets_by_group,
            failures,
        })
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

    #[test]
    fn topic_interner_returns_same_arc_for_same_name() {
        let mut interner = TopicInterner::default();
        let a = interner.intern("foo");
        let b = interner.intern("foo");
        assert!(Arc::ptr_eq(&a, &b), "same topic must share the Arc");
        let c = interner.intern("bar");
        assert!(!Arc::ptr_eq(&a, &c));
    }

    #[test]
    fn transient_group_admin_errors_are_retriable() {
        assert!(is_retriable_admin_code(
            rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR__TRANSPORT
        ));
        assert!(is_retriable_admin_code(
            rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT
        ));
        assert!(is_retriable_admin_code(
            rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_COORDINATOR_NOT_AVAILABLE
        ));
        assert!(is_retriable_admin_code(
            rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NOT_COORDINATOR
        ));
    }

    #[test]
    fn permanent_group_admin_errors_are_not_retriable() {
        assert!(!is_retriable_admin_code(
            rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR__AUTHENTICATION
        ));
        assert!(!is_retriable_admin_code(
            rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_GROUP_AUTHORIZATION_FAILED
        ));
        assert!(!is_retriable_admin_code(
            rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_INVALID_GROUP_ID
        ));
    }

    #[test]
    fn successful_empty_group_offsets_are_not_a_failure() {
        let response = GroupOffsetsResponse {
            offsets: HashMap::from([("group-a".to_string(), HashMap::new())]),
            failures: Vec::new(),
        };

        let offsets = response.into_single_group_result().unwrap();
        assert!(offsets.contains_key("group-a"));
        assert!(offsets["group-a"].is_empty());
    }

    #[test]
    fn explicit_group_offset_failure_is_preserved() {
        let response = GroupOffsetsResponse {
            offsets: HashMap::new(),
            failures: vec![GroupRequestFailure {
                group_id: "group-a".to_string(),
                error: AdminRequestError::new(
                    "ListConsumerGroupOffsets",
                    Some(rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR__TRANSPORT),
                    "transport failure",
                ),
            }],
        };

        let error = response.into_single_group_result().unwrap_err();
        assert!(error.is_retriable());
    }

    #[test]
    fn describe_retry_partition_selects_only_retriable_failures() {
        let response = DescribeGroupsResponse {
            descriptions: vec![BatchedGroupDescription {
                group_id: "successful-group".to_string(),
                state: "Stable".to_string(),
                members: Vec::new(),
            }],
            failures: vec![
                GroupRequestFailure {
                    group_id: "transient-failure".to_string(),
                    error: AdminRequestError::new(
                        "DescribeConsumerGroups",
                        Some(rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR__TRANSPORT),
                        "transport failure",
                    ),
                },
                GroupRequestFailure {
                    group_id: "permanent-failure".to_string(),
                    error: AdminRequestError::new(
                        "DescribeConsumerGroups",
                        Some(rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_GROUP_AUTHORIZATION_FAILED),
                        "authorization failure",
                    ),
                },
            ],
        };

        let partitioned = response.partition_for_retry(true);

        assert_eq!(partitioned.descriptions.len(), 1);
        assert_eq!(partitioned.descriptions[0].group_id, "successful-group");
        assert_eq!(partitioned.retryable_group_ids, vec!["transient-failure"]);
        assert_eq!(partitioned.terminal_failures.len(), 1);
        assert_eq!(
            partitioned.terminal_failures[0].group_id,
            "permanent-failure"
        );
    }
}
