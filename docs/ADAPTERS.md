# Writing a storage adapter

`AgentMemoryRuntime` is storage-agnostic. All durability and concurrency guarantees come from
the adapter, so an adapter is correct only if it makes the operations below **atomic**.
The in-memory adapter is atomic only because its method bodies never `await`; a real database
needs transactions, conditional writes, and unique indexes.

## Required atomicity

| Method | Must be atomic as one unit |
| --- | --- |
| `appendTurnAtomic` | idempotency check + session-to-customer binding + sequence allocation + insert. Same key + same payload returns `inserted: false` without burning a seq; same key + different payload throws `IdempotencyConflictError`; a different customer on a bound session throws `AccessDeniedError` and consumes no seq |
| `commitCheckpointWithOutboxAtomic` | compare-and-swap on checkpoint `version` (`expectedVersion`), monotonic `compactedUpToSeq`, and the outbox insert. On failure nothing is written |
| `ingestClaimAtomic` | receipt lookup + conflict classification + all claim mutations + new claim insert + receipt insert, **serialized per (tenant, customer, attribute)** |
| `archiveClaim` | archive + remove reference from peers + reactivate a lone disputed survivor |
| `resolveDisputeAtomic` | validate + supersede losers + activate winner + reference cleanup + audit record |
| `saveWorkflowStateAtomic` | `state.version === expectedVersion + 1`, version CAS, customer binding (also binds an unbound session) |
| `consumeTokenAndCreateCommandAtomic` | re-verify version, auth, action, token hash, payload hash, expiry and `latestSeq`; then clear the challenge and insert the command. Succeeds at most once |

## The two traps

1. **Write skew on claims.** Two concurrent `ingestClaimAtomic` calls that both read an empty "open claims" set will
   both insert, leaving two active claims. Snapshot isolation does not detect this because they write different
   documents. Serialize them by having every claim mutation write to a shared guard row/document keyed by
   `(tenant, customer, attribute)`. The MongoDB adapter does exactly this.
2. **Check-then-insert on first use.** Creating the session record on first turn races with the workflow and checkpoint
   writers. Use a single atomic upsert whose filter pins the customer, and treat a duplicate-key error as "retry".

## Semantics you must reproduce

- **Classification** (against *live*, non-expired `active`/`disputed` claims of the same tenant/customer/attribute):
  1. Same value (case-insensitive, trimmed) from a same-or-weaker tier than an open claim: reinforce it
     (`reinforcementCount += 1`, `confidence = max`). Receipt records the *existing* claim's id.
  2. Otherwise, if the new tier is weaker than any open claim: store the new claim as `rejected` with
     `conflictsWith = [stronger ids]`, and mutate nothing else.
  3. Otherwise: open claims with a weaker tier, or equal tier <= 2, become `superseded`
     (`supersededBy`, `validTo = now`); equal tier >= 3 become `disputed` symmetrically.
- Every `ingestClaimAtomic` writes a receipt keyed by `(tenant, claimId)`, so replays return the original result.
- Query results must not leak storage-internal fields such as Mongo's `_id`.
- Everything is tenant-scoped; identical ids in different tenants must never collide.

## Verifying your adapter

`tests/adapter-contract.ts` exports `runAdapterContractTests(name, factory)`. Copy it into your repo (exporting it as a
package is on the roadmap) and run it against your adapter:

```ts
import { runAdapterContractTests } from "./adapter-contract";

runAdapterContractTests("MyAdapter", async () => {
  const adapter = await createFreshIsolatedAdapter(); // new database/schema per test
  return { adapter, cleanup: async () => { /* drop it */ } };
});
```

The suite includes concurrency tests (parallel appends, claim write skew, parallel consume, parallel dispute
resolution). An adapter that only passes the sequential tests is not safe.
