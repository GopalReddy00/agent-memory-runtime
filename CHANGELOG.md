# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/). While the major version is `0`, minor
releases may contain breaking changes.

## [0.1.0] - 2026-11-19

First public developer preview.

### Added
- Event-backed turn log with atomic sequence allocation, idempotency keys and an
  immutable session-to-customer binding.
- Provenance-aware claims with authority tiers (0-4), symmetric disputes for equal
  untrusted tiers, a terminal `rejected` status for lower-authority claims, and audited
  dispute resolution.
- Budgeted context reconstruction (pinned roots, recent turns, active facts, disputed
  facts, summary) with a reserved summary floor and a pluggable token counter.
- Crash-safe compaction: checkpoint + outbox committed atomically, idempotent replay via
  per-claim ingest receipts, bounded retries with a `failed` terminal state.
- Confirmation protocol for high-risk actions: runtime-minted token (stored hashed),
  payload binding, expiry, user-turn-after-challenge and staleness checks, atomic consume.
- Decay / expiry sweep for claims.
- `InMemoryStorageAdapter` (reference/testing) and `MongoStorageAdapter`
  (`agent-memory-runtime/mongo`, requires a replica set).
- Reusable adapter contract test suite (21 tests) in `tests/adapter-contract.ts`.
- Confirmation challenge issuance is atomic with the session sequence and rejects multiple
  user replies after a challenge.
- Context reads require an existing immutable session-to-customer binding.
- Added failed outbox-job redrive and tenant/customer data erasure APIs.

### Known limitations
See the "Limitations" section of the README.
