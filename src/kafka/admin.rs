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
/// aggregates results. Per-group errors are logged at WARN and the group is
/// omitted from the returned Vec.
///
/// When `parse_assignments = false`, each returned `BatchedMember` has an
/// empty `assignments: Vec<TopicPartition>`. This skips a per-member
/// iteration over the assignment's `rd_kafka_topic_partition_list_t` — the
/// data is only consumed by per-partition metrics (granularity = "partition"),
/// so it's pure wasted work at the default topic granularity.
pub fn describe_consumer_groups_batched(
    admin: &AdminClient<DefaultClientContext>,
    group_ids: &[&str],
    timeout: Duration,
    chunk_size: usize,
    parse_assignments: bool,
) -> Result<Vec<BatchedGroupDescription>> {
    if group_ids.is_empty() {
        return Ok(Vec::new());
    }
    let chunk_size = chunk_size.max(1);
    let mut out = Vec::with_capacity(group_ids.len());
    for chunk in group_ids.chunks(chunk_size) {
        let mut part =
            describe_consumer_groups_one_chunk(admin, chunk, timeout, parse_assignments)?;
        out.append(&mut part);
    }
    Ok(out)
}

fn describe_consumer_groups_one_chunk(
    admin: &AdminClient<DefaultClientContext>,
    group_ids: &[&str],
    timeout: Duration,
    parse_assignments: bool,
) -> Result<Vec<BatchedGroupDescription>> {
    let timeout_ms = timeout.as_millis().min(i32::MAX as u128) as i32;
    let rk = admin_native_ptr(admin);

    // Build array of *const c_char pointing to CString buffers. `cstrings`
    // must outlive the FFI call (ptrs borrow from it).
    let cstrings: Vec<CString> = group_ids
        .iter()
        .map(|g| cstring_or_err(g))
        .collect::<Result<Vec<_>>>()?;
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
            return Err(KlagError::Admin(
                "Failed to create AdminOptions (DescribeConsumerGroups)".into(),
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
            return Err(KlagError::Admin(format!(
                "Failed to set request timeout (DescribeConsumerGroups): {}",
                errstr_to_string(&errstr_buf)
            )));
        }

        let queue = rd_kafka_queue_new(rk);
        if queue.is_null() {
            return Err(KlagError::Admin(
                "Failed to create queue (DescribeConsumerGroups)".into(),
            ));
        }
        cleanup.queue = queue;

        rd_kafka_DescribeConsumerGroups(rk, ptrs.as_mut_ptr(), ptrs.len(), options, queue);

        let event = rd_kafka_queue_poll(queue, timeout_ms);
        if event.is_null() {
            return Err(KlagError::Admin("DescribeConsumerGroups timed out".into()));
        }
        cleanup.event = event;

        let event_type = rd_kafka_event_type(event);
        if event_type != RD_KAFKA_EVENT_DESCRIBECONSUMERGROUPS_RESULT {
            return Err(KlagError::Admin(format!(
                "Unexpected event type (DescribeConsumerGroups): {event_type}"
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
            return Err(KlagError::Admin(format!(
                "DescribeConsumerGroups failed: {err_msg}"
            )));
        }

        let result = rd_kafka_event_DescribeConsumerGroups_result(event);
        if result.is_null() {
            return Err(KlagError::Admin(
                "DescribeConsumerGroups result is null".into(),
            ));
        }

        let mut n: usize = 0;
        let groups_ptr = rd_kafka_DescribeConsumerGroups_result_groups(result, &mut n);
        let mut out = Vec::with_capacity(n);
        if groups_ptr.is_null() || n == 0 {
            return Ok(out);
        }

        for i in 0..n {
            let grp = *groups_ptr.add(i);
            let group_id = ptr_to_string(rd_kafka_ConsumerGroupDescription_group_id(grp));
            let grp_err = rd_kafka_ConsumerGroupDescription_error(grp);
            if !grp_err.is_null() {
                let code = rd_kafka_error_code(grp_err);
                if code != rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
                    let msg = ptr_to_string(rd_kafka_error_string(grp_err));
                    warn!(group = %group_id, error = %msg, "DescribeConsumerGroups per-group error");
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

            out.push(BatchedGroupDescription {
                group_id,
                state: state_str,
                members,
            });
        }

        debug!(
            requested = group_ids.len(),
            returned = out.len(),
            "Batched DescribeConsumerGroups complete"
        );
        Ok(out)
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
pub fn list_consumer_group_offsets_batched(
    admin: &AdminClient<DefaultClientContext>,
    group_ids: &[&str],
    timeout: Duration,
    chunk_size: usize,
) -> Result<HashMap<String, HashMap<TopicPartition, i64>>> {
    if group_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let chunk_size = chunk_size.max(1);
    let mut out = HashMap::with_capacity(group_ids.len());
    for chunk in group_ids.chunks(chunk_size) {
        let part = list_consumer_group_offsets_one_chunk(admin, chunk, timeout)?;
        out.extend(part);
    }
    Ok(out)
}

fn list_consumer_group_offsets_one_chunk(
    admin: &AdminClient<DefaultClientContext>,
    group_ids: &[&str],
    timeout: Duration,
) -> Result<HashMap<String, HashMap<TopicPartition, i64>>> {
    let timeout_ms = timeout.as_millis().min(i32::MAX as u128) as i32;
    let rk = admin_native_ptr(admin);

    let cstrings: Vec<CString> = group_ids
        .iter()
        .map(|g| cstring_or_err(g))
        .collect::<Result<Vec<_>>>()?;

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
                return Err(KlagError::Admin(
                    "Failed to create ListConsumerGroupOffsets request".into(),
                ));
            }
            cleanup.requests.push(req);
        }

        let options = rd_kafka_AdminOptions_new(
            rk,
            rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_LISTCONSUMERGROUPOFFSETS,
        );
        if options.is_null() {
            return Err(KlagError::Admin(
                "Failed to create AdminOptions (ListConsumerGroupOffsets)".into(),
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
                "Failed to set request timeout (ListConsumerGroupOffsets batched): {}",
                errstr_to_string(&errstr_buf)
            )));
        }

        let queue = rd_kafka_queue_new(rk);
        if queue.is_null() {
            return Err(KlagError::Admin(
                "Failed to create queue (ListConsumerGroupOffsets batched)".into(),
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
            return Err(KlagError::Admin(
                "ListConsumerGroupOffsets (batched) timed out".into(),
            ));
        }
        cleanup.event = event;

        let event_type = rd_kafka_event_type(event);
        if event_type != RD_KAFKA_EVENT_LISTCONSUMERGROUPOFFSETS_RESULT {
            return Err(KlagError::Admin(format!(
                "Unexpected event type (ListConsumerGroupOffsets batched): {event_type}"
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
            return Err(KlagError::Admin(format!(
                "ListConsumerGroupOffsets (batched) failed: {err_msg}"
            )));
        }

        let result = rd_kafka_event_ListConsumerGroupOffsets_result(event);
        if result.is_null() {
            return Err(KlagError::Admin(
                "ListConsumerGroupOffsets (batched) result is null".into(),
            ));
        }

        let mut n_groups: usize = 0;
        let groups_ptr = rd_kafka_ListConsumerGroupOffsets_result_groups(result, &mut n_groups);
        let mut out: HashMap<String, HashMap<TopicPartition, i64>> =
            HashMap::with_capacity(n_groups);
        if groups_ptr.is_null() || n_groups == 0 {
            return Ok(out);
        }

        for i in 0..n_groups {
            let group = *groups_ptr.add(i);
            let group_name = rd_kafka_group_result_name(group);
            let group_id = ptr_to_string(group_name);

            let group_error = rd_kafka_group_result_error(group);
            if !group_error.is_null() {
                let code = rd_kafka_error_code(group_error);
                if code != rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
                    let msg = ptr_to_string(rd_kafka_error_string(group_error));
                    warn!(group = %group_id, error = %msg, "ListConsumerGroupOffsets per-group error");
                    out.insert(group_id, HashMap::new());
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
            out.insert(group_id, offsets);
        }

        debug!(
            requested = group_ids.len(),
            returned = out.len(),
            "Batched ListConsumerGroupOffsets complete"
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
