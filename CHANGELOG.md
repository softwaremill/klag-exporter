# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.22](https://github.com/softwaremill/klag-exporter/compare/v0.1.21...v0.1.22) - 2026-04-15

### Added

- switch time-lag default to rate-based estimation (Tier 3)
- *(collector)* add RateSampler for rate-based time-lag estimation

### Other

- address Copilot PR feedback (Tier 4)
- max_concurrent_watermarks (no-op since Tier 1 batched ListOffsets)
- *(collector)* per-phase elapsed_ms in the cycle-complete debug log
- *(collector)* TTL cache list_consumer_groups (default 60s)
- *(kafka)* parallelize DescribeConsumerGroups chunks
- address Copilot PR feedback (Tier 3)
- *(readme)* document rate vs message time-lag modes
- address Copilot PR feedback (Tier 2)
- *(kafka)* intern topic names as Arc<str> with per-call dedup
- *(collector)* add TTL cache for filtered monitored-topic set
- *(collector)* add TTL cache for compacted-topic detection
- *(kafka)* skip member-assignment parsing unless granularity=partition
- *(runtime)* cap tokio blocking-thread pool (default 64)

## [0.1.21](https://github.com/softwaremill/klag-exporter/compare/v0.1.20...v0.1.21) - 2026-04-15

### Added

- *(kafka)* add fetch_watermarks_for_partitions using batched ListOffsets
- *(kafka)* add batched list_consumer_group_offsets_batched FFI wrapper
- *(kafka)* add batched describe_consumer_groups_batched FFI wrapper
- *(kafka)* expose admin_handle for batched Admin API callers
- *(kafka)* add batched list_offsets_batched FFI wrapper
- *(kafka)* scaffold admin module for batched Admin API wrappers

### Other

- address Copilot PR feedback (Tier 1)
- *(readme)* describe batched Admin API collection model and updated sizing guidance
- *(collector)* rewire hot path to batched Admin API with pre-filtered topics
- *(kafka)* replace sequential describe_consumer_groups with batched Admin API

## [0.1.20](https://github.com/softwaremill/klag-exporter/compare/v0.1.19...v0.1.20) - 2026-03-25

### Other

- Update helm/klag-exporter/templates/configmap.yaml
- Update helm/klag-exporter/values.yaml
- Update helm/klag-exporter/templates/configmap.yaml
- Support the new config option in helm chart

## [0.1.19](https://github.com/softwaremill/klag-exporter/compare/v0.1.18...v0.1.19) - 2026-03-17

### Other

- Fix native FFI memory leak and improve memory management

## [0.1.18](https://github.com/softwaremill/klag-exporter/compare/v0.1.17...v0.1.18) - 2026-03-13

### Other

- updated the docs with new metric info

## [0.1.17](https://github.com/softwaremill/klag-exporter/compare/v0.1.16...v0.1.17) - 2026-03-13

### Added

- add kafka_consumergroup_group_state metric

### Other

- updated the docs with new metric info
- fix cargo fmt formatting

## [0.1.16](https://github.com/softwaremill/klag-exporter/compare/v0.1.15...v0.1.16) - 2026-02-24

### Fixed

- fix missing compression libs

## [0.1.15](https://github.com/softwaremill/klag-exporter/compare/v0.1.14...v0.1.15) - 2026-02-19

### Fixed

- fixing copilot review
- fix formatting

### Other

- copilot review
- Consumer Pool for Timestamps
- klag-exporter/src/kafka/client.rs
- make base consumer reusable with Arc
- added testing env for large cluster setup

## [0.1.14](https://github.com/softwaremill/klag-exporter/compare/v0.1.13...v0.1.14) - 2026-01-30

### Added

- *(performance)* add configuration settings for concurrency and timeouts on large clusters

### Fixed

- fixed copilot suggestions
- fixed cargo fmt
- fixed copilot auto-review issues

## [0.1.13](https://github.com/softwaremill/klag-exporter/compare/v0.1.12...v0.1.13) - 2026-01-29

### Fixed

- cluster consumer_properties isn't inherit by group client ([#29](https://github.com/softwaremill/klag-exporter/pull/29))

## [0.1.12](https://github.com/softwaremill/klag-exporter/compare/v0.1.11...v0.1.12) - 2026-01-29

### Other

- Quote keys in toml ([#38](https://github.com/softwaremill/klag-exporter/pull/38))

## [0.1.10](https://github.com/softwaremill/klag-exporter/compare/v0.1.9...v0.1.10) - 2026-01-19

### Other

- add data loss detection metrics documentation ([#17](https://github.com/softwaremill/klag-exporter/pull/17))
- Skipping commits in cl ([#16](https://github.com/softwaremill/klag-exporter/pull/16))

## [0.1.9](https://github.com/softwaremill/klag-exporter/compare/v0.1.8...v0.1.9) - 2026-01-16

### Other

- release v0.1.8 ([#14](https://github.com/softwaremill/klag-exporter/pull/14))
- add release-plz configuration, move the release job to "on pr merge" ([#13](https://github.com/softwaremill/klag-exporter/pull/13))
- downgrade version to what is actually released ([#12](https://github.com/softwaremill/klag-exporter/pull/12))

## [0.1.8](https://github.com/softwaremill/klag-exporter/compare/v0.1.7...v0.1.8) - 2026-01-16

### Other

- add release-plz configuration, move the release job to "on pr merge" ([#13](https://github.com/softwaremill/klag-exporter/pull/13))
- downgrade version to what is actually released ([#12](https://github.com/softwaremill/klag-exporter/pull/12))
