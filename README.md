# agent-memory-runtime

[![npm](https://img.shields.io/npm/v/agent-memory-runtime)](https://www.npmjs.com/package/agent-memory-runtime)
[![CI](https://github.com/GopalReddy00/agent-memory-runtime/actions/workflows/ci.yml/badge.svg)](https://github.com/GopalReddy00/agent-memory-runtime/actions/workflows/ci.yml)
[![license](https://img.shields.io/npm/l/agent-memory-runtime)](LICENSE)

Memory and safety primitives for LLM agents that talk to real customers.

Most agent memory is "stuff the last N messages into the prompt and let a model summarize the rest."
That breaks in predictable ways: a stale or hallucinated fact overwrites a verified one, a crash during
summarization loses data, retries double-count, and a model that was told "yes" three turns ago still
cancels the policy. This library treats those as storage-and-state problems and solves them with
explicit data structures and atomic operations instead of prompting.

> **Status: developer preview (0.x).** The API may change in minor releases. Read
> [Limitations](#limitations) before putting this near production traffic.

## What problem maps to what mechanism

| Failure in typical agent memory | Mechanism here |
| --- | --- |
| An LLM-extracted "fact" overrides a CRM value | **Authority tiers**: a weaker claim is stored as `rejected` and never touches a stronger one |
| Two untrusted sources disagree and the agent picks one silently | **Symmetric disputes**: both claims become `disputed`, are shown to the model labeled as untrusted, and resolved by an audited call |
| Crash between "summary saved" and "facts saved" | **Checkpoint + outbox** committed atomically; replay is idempotent via per-claim ingest receipts |
| Retries and duplicate webhooks | **Idempotency keys** on turns; deterministic claim IDs on outbox replay |
| Context window overflow starves older memory | **Budgeted context**: pinned > recent turns > active facts > disputed facts > summary, with a reserved summary floor |
| Prompt-injected "I am an admin" persisted as memory | **Allowlisted attributes + per-attribute validators**; PII attributes blocked; untrusted tier by default |
| One session reading or writing another customer's data | **Immutable session to customer binding**, enforced in storage; tenant-scoped everything |
| "Yes" from three turns ago authorizes a destructive action | **Confirmation protocol**: runtime-minted single-use token (stored hashed), payload-bound, expiring, requires a *user turn after* the challenge, invalidated by any later user turn |
| Stale memory lives forever | **Decay and expiry sweep** (decay never applies to tier 0/1 claims; an explicit `expiresAt`/`validTo` still archives them) |

## Install

```bash
npm install agent-memory-runtime
# only if you use the MongoDB adapter (replica set required):
npm install mongodb
```

Requires Node.js 18+. The package is ESM-only. The core has no runtime dependencies (it uses `node:crypto`).

## Quickstart

```ts
import { createHash } from "node:crypto";
import { AgentMemoryRuntime, InMemoryStorageAdapter } from "agent-memory-runtime";

// Your summarizer. Receives the turns being compacted and returns a new summary plus any claims.
// Output is validated (allowlist, ranges, per-attribute validators) before anything is stored.
const llm = {
  async compactAndExtract({ previousSummary, turns, allowedAttributes }) {
    // call your model here
    return { updatedSummary: `${previousSummary} (${turns.length} turns)`.trim(), extractedClaims: [] };
  },
};

const runtime = new AgentMemoryRuntime(new InMemoryStorageAdapter(), llm);
const ids = { tenantId: "acme", sessionId: "s1", customerId: "c42" };

// 1. Record conversation turns (idempotent by key; session is bound to this customer).
await runtime.appendTurn({ ...ids, idempotencyKey: "m1", role: "user", content: "Please call me in Telugu" });

// 2. Store facts with the authority they actually have.
await runtime.ingestSystemOfRecordClaim({
  tenantId: ids.tenantId, customerId: ids.customerId,
  attributeKey: "preferred_language", value: "Telugu",
  volatility: "CLASS_C_PREFERENCE", sourceEventId: "crm:123",
});

// A model-extracted claim that disagrees can never override the CRM value.
const weak = await runtime.ingestExtractedClaim({
  tenantId: ids.tenantId, customerId: ids.customerId,
  attributeKey: "preferred_language", value: "Hindi",
  volatility: "CLASS_C_PREFERENCE", confidence: 0.9, importance: 0.9, sourceEventId: "turn:1",
});
console.log(weak.status); // "rejected"

// 3. Build a budgeted, policy-filtered context for your prompt.
const ctx = await runtime.buildContext({
  principal: { tenantId: ids.tenantId, agentId: "voice-agent-1", maxAllowedSensitivity: "internal" },
  sessionId: ids.sessionId,
  customerId: ids.customerId,
});
console.log(ctx.gen2ActiveFacts); // [{ key: "preferred_language", value: "Telugu", tier: 0, ... }]
```

`principal` (tenant, agent, sensitivity ceiling) must come from **your** auth layer, never from the model.

### Gating a high-risk action

```ts
// (a) Your IAM layer, after a real OTP/KYC check. This is the ONLY way to mark a session authenticated.
const a = await runtime.setSessionAuthenticated({ ...ids, authenticated: true, authMethod: "otp", expectedVersion: 0 });

// (b) The agent records what the customer wants. This can never elevate authentication.
const b = await runtime.updateWorkflowIntent(
  { ...ids, currentIntent: "cancel", pendingAction: "cancelPolicy" }, a.newVersion);

// (c) Mint a challenge BEFORE appending the assistant's "are you sure?" message.
//     The token is returned once; only its hash is stored. Deliver it out-of-band (UI button, signed callback).
const payloadHash = createHash("sha256").update(JSON.stringify({ policyId: "P-1" })).digest("hex");
const ch = await runtime.issueConfirmationChallenge(
  { ...ids, actionName: "cancelPolicy", payloadHash, expectedVersion: b.newVersion });

// (d) The customer replies, which is a user turn after the challenge.
await runtime.appendTurn({ ...ids, idempotencyKey: "m2", role: "user", content: "Yes, cancel it" });

// (e) Atomically verify and consume. Succeeds at most once, even across many instances.
const r = await runtime.authorizeAndCreateExecutionCommand({
  tenantId: ids.tenantId, sessionId: ids.sessionId,
  actionName: "cancelPolicy", payloadHash, providedConfirmationToken: ch.token!,
});
if (r.allowed) { /* perform the action using r.command */ } else { console.log(r.reason); }
```

The runtime verifies: authenticated, intent matches, token matches (constant-time, hash compared), payload matches,
not expired, a user turn exists after the challenge, no *other* user turn arrived since (default `maxTurnStaleness = 1`),
and no new event arrived during the atomic consume. **Deciding that the customer's reply actually means "yes" is your
application's job**: the runtime only enforces that a fresh user turn exists. Treat the token like a capability.

## MongoDB adapter

```ts
import { MongoClient } from "mongodb";
import { AgentMemoryRuntime } from "agent-memory-runtime";
import { MongoStorageAdapter } from "agent-memory-runtime/mongo";

const client = new MongoClient(process.env.MONGO_URI!); // must be a replica set (transactions)
await client.connect();
const storage = new MongoStorageAdapter(client, client.db("agent_memory"));
await storage.ensureIndexes(); // run once at startup

const runtime = new AgentMemoryRuntime(storage, myLlm);
```

Design notes: one anchor document per session holds the sequence counter, customer binding, checkpoint and workflow
state, so every compare-and-swap is a single-document operation. A small guard document per
(tenant, customer, attribute) serializes claim mutations. Without it two concurrent ingests that both see an empty
open set would both insert (write skew; snapshot isolation does not prevent it). Unique indexes back every idempotency,
receipt and ID guarantee. See [docs/ADAPTERS.md](docs/ADAPTERS.md).

## Concepts

### Three generations of memory

| Layer | Contents | Notes |
| --- | --- | --- |
| Gen 0 | Recent raw turns after the compaction watermark | Pinned turns (e.g. system roots) are always included first and never compacted |
| Gen 1 | Rolling summary | Written by your LLM; treated as untrusted text |
| Gen 2 | Claims: structured, sourced, time-bounded facts | The only layer with authority semantics |

### Authority tiers (lower = stronger)

| Tier | Source | Ingest method |
| --- | --- | --- |
| 0 | System of record (CRM / IAM) | `ingestSystemOfRecordClaim` |
| 1 | Verified workflow event (OTP / KYC) | `ingestVerifiedCustomerClaim` |
| 2 | Authenticated customer statement | `ingestAuthenticatedCustomerClaim` |
| 3 | LLM / automated extraction (untrusted) | `ingestExtractedClaim` (and all compaction output) |
| 4 | Unauthenticated / third party | reserved |

Callers cannot pick a tier: each ingest method hard-codes its own. LLM compaction output is always tier 3.

### Claim lifecycle

| Status | Meaning |
| --- | --- |
| `active` | The current value |
| `disputed` | One of two or more same-tier untrusted claims that disagree. Resolve with `resolveDispute` (audited, must cover every other disputed claim) |
| `superseded` | Replaced by a stronger claim, or (tiers 0-2) by a newer one of equal tier |
| `rejected` | Arrived with lower authority than an open claim. Kept for audit, never in context, never mutates the stronger claim |
| `archived` | Expired or decayed by the sweep |

Same value from a same-or-weaker tier reinforces the existing claim; the same value from a *stronger* tier upgrades
provenance. Expired-but-unswept claims neither absorb nor conflict.

### Compaction and recovery

`appendTurn` triggers background compaction when uncompacted tokens exceed `maxGen0Tokens`. The checkpoint (with
compare-and-swap on version and a monotonic watermark) and the outbox job are committed together. If the process dies
afterwards, `recoverPendingOutbox(tenantId)` finishes the job; replays are no-ops because every claim ID is
`jobId::index` and storage keeps an ingest receipt. Jobs that keep failing are marked `failed` after
`maxOutboxAttempts`.

**You must schedule** `recoverPendingOutbox` and `sweepCustomerClaims` yourself (cron, BullMQ, etc.).

### Configuration

All optional: `maxGen0Tokens` (4000), `maxContextTokens` (6000), `minSummaryTokens` (400), `keepRecentTurns` (4),
`maxExtractedClaimsPerSweep` (10), `allowedAttributes`, `attributeValidators`, `blockedPiiAttributes`,
`gen2DecayHalfLifeDays` (30), `gen2ArchiveThreshold` (0.15), `rejectedRetentionDays` (30), `maxOutboxAttempts` (5),
`readTouchThrottleMs` (1h), `tokenCounter`, `onError`.

The default attribute allowlist (`preferred_language`, `callback_window`, `policy_interest`,
`communication_channel`) is deliberately tiny. Set your own. The default token counter is `ceil(chars / 4)`; plug in
a real tokenizer (e.g. tiktoken), especially for non-English text.

## Verification

- Runtime behavior: unit tests including randomized claim-state invariants, run both sequentially and through an
  adapter that injects random delays to force interleavings.
- Storage contract: **one suite (21 tests)** that every adapter must pass, covering concurrent appends, session binding,
  checkpoint CAS, ingest receipts, write skew, dispute resolution, workflow versioning and atomic consume.
- The MongoDB adapter passes that suite plus an end-to-end runtime suite on a real MongoDB replica set.
- The tests themselves are checked with deliberate mutations (removing a guard must make a test fail).

What this does **not** prove: behavior under failover, network partitions, long-running load, or a multi-node cluster.

```bash
npm test                 # unit + in-memory contract (Mongo tests skipped)
npm run test:mongo       # also runs against a Mongo replica set at 127.0.0.1:27017
```

## Limitations

Please read these.

- **Developer preview.** Expect API changes. No published benchmarks yet; any quality claim about agent behavior
  would be unmeasured.
- **Trust boundary is yours.** `tenantId`, `customerId`, `PrincipalContext` and the authentication result must come
  from trusted code. Anyone with direct write access to the database can bypass every check, including forging
  workflow state.
- **Prompt-injection handling is layered but not complete.** The regexes are a weak supplementary heuristic. The real
  defenses are the attribute allowlist, validators and authority tiers. Free-text attributes (`callback_window`,
  `policy_interest`) are restricted to conservative character sets but still carry model-visible text. The summary
  and conversation turns are raw model/user text; treat them as untrusted when you build your prompt.
- **Not a PII scanner.** `blockedPiiAttributes` blocks *named attributes*. Conversation turns are stored verbatim.
  Encryption at rest, retention policy and access control on the database are your responsibility.
- **Erasure requires orchestration.** `eraseCustomerData` deletes customer-bound runtime collections through the
  configured storage adapter. External databases, backups, logs and derived indexes are outside its scope and require
  tenant retention/deletion procedures.
- **The runtime does not interpret "yes".** It enforces freshness and ordering of the confirmation, not intent.
- **Clocks.** Expiry and decay use the application's `Date.now()`. Keep instance clocks in sync.
- **Summaries can lose or distort information.** The reserved summary budget protects space, not correctness.
- **Token counting is a heuristic** unless you provide a tokenizer.
- **ESM-only**, Node 18+.
- Tested on a single-node MongoDB replica set only.

## API overview

| Area | Methods |
| --- | --- |
| Turns | `appendTurn` |
| Context | `buildContext` |
| Compaction | `runSessionCompaction`, `processOutboxJob`, `recoverPendingOutbox`, `redriveFailedOutboxJob` |
| Claims | `ingestSystemOfRecordClaim`, `ingestVerifiedCustomerClaim`, `ingestAuthenticatedCustomerClaim`, `ingestExtractedClaim`, `resolveDispute`, `sweepCustomerClaims` |
| Workflow | `setSessionAuthenticated`, `updateWorkflowIntent`, `issueConfirmationChallenge`, `authorizeAndCreateExecutionCommand` |
| Storage | `RuntimeStorageAdapter` interface, `InMemoryStorageAdapter`, `MongoStorageAdapter`, `eraseCustomerData` |

All public types are exported from the package root.

## Roadmap

- Export the adapter contract suite as a package for third-party adapters
- Postgres adapter
- Published, reproducible benchmarks against sliding-window and RAG baselines
- CommonJS build

## Contributing and security

See [CONTRIBUTING.md](CONTRIBUTING.md) and [SECURITY.md](SECURITY.md).

## License

[MIT](LICENSE) © GurakaGopalReddy
