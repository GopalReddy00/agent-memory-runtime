import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { createHash } from "node:crypto";
import {
  AccessDeniedError,
  IdempotencyConflictError,
  type RuntimeStorageAdapter,
  type MemoryClaim,
  type AuthorityTier,
  type TurnInput,
  type CompactionOutboxJob,
  type Checkpoint,
  type WorkflowState,
  type ExecutionCommand,
} from "../src/index";

const sha256 = (s: string) => createHash("sha256").update(s).digest("hex");

export interface AdapterTestContext {
  adapter: RuntimeStorageAdapter;
  cleanup: () => Promise<void>;
}

const T = "tenant_1";
const C = "cust_1";
const S = "sess_1";

let idc = 0;
const uid = (p: string) => `${p}_${++idc}_${Math.random().toString(36).slice(2, 8)}`;

const turn = (over: Partial<TurnInput> = {}): TurnInput => ({
  tenantId: T, sessionId: S, customerId: C, idempotencyKey: uid("k"),
  role: "user", content: "hello", tokens: 2, pinned: false, ...over,
});

function mkClaim(id: string, value: string, tier: AuthorityTier, over: Partial<MemoryClaim> = {}): MemoryClaim {
  const now = Date.now();
  return {
    claimId: id, tenantId: T, customerId: C, attributeKey: "preferred_language", value,
    generation: tier <= 1 ? 2 : 1, authorityTier: tier, volatility: "CLASS_C_PREFERENCE",
    sensitivity: "internal", status: "active", confidence: 0.9, importance: 0.8,
    reinforcementCount: 1, recordedAt: now, validFrom: now, validTo: null, lastReadAt: now,
    expiresAt: null, sourceType: "test", sourceEventId: "ev", conflictsWith: [], supersededBy: null,
    ...over,
  };
}

const job = (id: string, over: Partial<CompactionOutboxJob> = {}): CompactionOutboxJob => ({
  jobId: id, tenantId: T, sessionId: S, customerId: C, watermarkSeq: 5, claims: [],
  status: "pending", attempts: 0, createdAt: Date.now(), ...over,
});
const cp = (version: number, seq: number): Checkpoint => ({
  tenantId: T, sessionId: S, version, compactedUpToSeq: seq, summary: `v${version}`, updatedAt: Date.now(),
});
const wf = (version: number, over: Partial<WorkflowState> = {}): WorkflowState => ({
  tenantId: T, sessionId: S, customerId: C, version, authenticated: true, authMethod: "otp",
  currentIntent: "cancel", pendingAction: "cancelPolicy", confirmationTokenHash: null,
  confirmationPayloadHash: null, confirmationExpiresAt: null, confirmationChallengeSeq: null,
  updatedAt: Date.now(), ...over,
});
const cmd = (id: string): ExecutionCommand => ({
  commandId: id, tenantId: T, sessionId: S, customerId: C, actionName: "cancelPolicy",
  payloadHash: "h1", createdAt: Date.now(), status: "authorized",
});

export function runAdapterContractTests(name: string, factory: () => Promise<AdapterTestContext>) {
  describe(`RuntimeStorageAdapter contract: ${name}`, () => {
    let a: RuntimeStorageAdapter;
    let cleanup: () => Promise<void>;

    beforeEach(async () => {
      const ctx = await factory();
      a = ctx.adapter;
      cleanup = ctx.cleanup;
    });
    afterEach(async () => cleanup());

    const allClaims = () =>
      a.getClaimsByCustomer(T, C, { maxSensitivity: "restricted", includeExpired: true });

    async function assertInvariants() {
      const all = await allClaims();
      const byAttr = new Map<string, { active: number; disputed: number }>();
      for (const c of all) {
        const o = byAttr.get(c.attributeKey) ?? { active: 0, disputed: 0 };
        if (c.status === "active") o.active++;
        if (c.status === "disputed") o.disputed++;
        byAttr.set(c.attributeKey, o);
      }
      for (const o of byAttr.values()) {
        expect(o.active).toBeLessThanOrEqual(1);
        if (o.active === 1) expect(o.disputed).toBe(0);
        expect(o.disputed).not.toBe(1);
      }
      for (const d of all.filter((c) => c.status === "disputed")) {
        expect(d.conflictsWith.length).toBeGreaterThan(0);
        for (const id of d.conflictsWith) expect(all.find((x) => x.claimId === id)!.status).toBe("disputed");
      }
    }

    // ---------------- turns ----------------
    it("turns: concurrent appends get unique monotonic seqs; idempotent replay; payload conflict", async () => {
      const inputs = Array.from({ length: 25 }, (_, i) => turn({ idempotencyKey: `k_${i}`, content: `msg ${i}` }));
      const results = await Promise.all(inputs.map((i) => a.appendTurnAtomic(i)));
      expect(results.every((r) => r.inserted)).toBe(true);
      expect(results.map((r) => r.event.seq).sort((x, y) => x - y)).toEqual(Array.from({ length: 25 }, (_, i) => i + 1));
      expect(await a.getLatestSeq(T, S)).toBe(25);

      const replay = await a.appendTurnAtomic(inputs[0]);
      expect(replay.inserted).toBe(false);
      expect(replay.event.seq).toBe(results[0].event.seq);
      expect((replay.event as unknown as Record<string, unknown>)._id).toBeUndefined();
      expect(await a.getLatestSeq(T, S)).toBe(25); // replay must not burn a seq

      await expect(a.appendTurnAtomic({ ...inputs[0], content: "TAMPERED" })).rejects.toThrow(IdempotencyConflictError);
    });

    it("turns: concurrent identical idempotency keys insert exactly once", async () => {
      const same = turn({ idempotencyKey: "dup" });
      const rs = await Promise.all(Array.from({ length: 10 }, () => a.appendTurnAtomic(same)));
      expect(rs.filter((r) => r.inserted)).toHaveLength(1);
      expect(new Set(rs.map((r) => r.event.seq)).size).toBe(1);
      expect(await a.getLatestSeq(T, S)).toBe(1);
    });

    it("turns: session is bound to one customer, even under a first-turn race", async () => {
      const rs = await Promise.allSettled(
        Array.from({ length: 12 }, (_, i) => a.appendTurnAtomic(turn({ customerId: i % 2 ? "cust_A" : "cust_B" })))
      );
      const ok = rs.filter((r) => r.status === "fulfilled");
      const bound = await a.getSessionCustomer(T, S);
      expect(["cust_A", "cust_B"]).toContain(bound);
      expect(ok).toHaveLength(6); // exactly the winner's six
      for (const r of rs) if (r.status === "rejected") expect(r.reason).toBeInstanceOf(AccessDeniedError);
      expect(await a.getLatestSeq(T, S)).toBe(6); // rejected appends consume no seq
      const turns = await a.getTurnsAfterSeq(T, S, 0);
      expect(turns.every((t) => t.customerId === bound)).toBe(true);
    });

    it("turns: queries (after-seq ordering, pinned, token sum ignores pinned, first user turn after)", async () => {
      await a.appendTurnAtomic(turn({ role: "system", pinned: true, tokens: 100, content: "root" })); // 1
      await a.appendTurnAtomic(turn({ role: "assistant", tokens: 3 })); // 2
      await a.appendTurnAtomic(turn({ role: "user", tokens: 4 })); // 3
      await a.appendTurnAtomic(turn({ role: "assistant", tokens: 5 })); // 4
      await a.appendTurnAtomic(turn({ role: "user", tokens: 6 })); // 5
      expect((await a.getTurnsAfterSeq(T, S, 2)).map((t) => t.seq)).toEqual([3, 4, 5]);
      expect((await a.getPinnedTurns(T, S)).map((t) => t.seq)).toEqual([1]);
      expect(await a.getUncompactedTokenSum(T, S, 0)).toBe(18);
      expect(await a.getUncompactedTokenSum(T, S, 3)).toBe(11);
      expect(await a.getFirstUserTurnSeqAfter(T, S, 0)).toBe(3);
      expect(await a.getFirstUserTurnSeqAfter(T, S, 3)).toBe(5);
      expect(await a.getFirstUserTurnSeqAfter(T, S, 5)).toBeNull();
      expect(await a.getSessionCustomer(T, "nope")).toBeNull();
      expect(await a.getLatestSeq(T, "nope")).toBe(0);
    });

    // ---------------- checkpoint + outbox ----------------
    it("checkpoint: CAS rejects stale/skipped versions and non-monotonic watermark; no orphan outbox rows", async () => {
      await a.appendTurnAtomic(turn());
      expect(await a.commitCheckpointWithOutboxAtomic(cp(1, 5), job("j1"), 0)).toBe(true);
      expect(await a.commitCheckpointWithOutboxAtomic(cp(1, 9), job("j2"), 0)).toBe(false); // stale
      expect(await a.commitCheckpointWithOutboxAtomic(cp(3, 9), job("j3"), 1)).toBe(false); // skipped
      expect(await a.commitCheckpointWithOutboxAtomic(cp(2, 5), job("j4"), 1)).toBe(false); // same watermark
      expect(await a.commitCheckpointWithOutboxAtomic(cp(2, 9), job("j5"), 1)).toBe(true);
      expect((await a.getPendingOutboxJobs(T)).map((j) => j.jobId).sort()).toEqual(["j1", "j5"]);
      expect((await a.getCheckpoint(T, S))!.version).toBe(2);
    });

    it("checkpoint: 6 concurrent commits from the same expectedVersion -> exactly one wins", async () => {
      await a.appendTurnAtomic(turn());
      const rs = await Promise.all(
        Array.from({ length: 6 }, (_, i) => a.commitCheckpointWithOutboxAtomic(cp(1, 10 + i), job(`jc${i}`), 0))
      );
      expect(rs.filter(Boolean)).toHaveLength(1);
      expect(await a.getPendingOutboxJobs(T)).toHaveLength(1);
    });

    it("outbox: attempts counter, completed and failed leave the pending queue", async () => {
      await a.appendTurnAtomic(turn());
      await a.commitCheckpointWithOutboxAtomic(cp(1, 5), job("o1"), 0);
      await a.commitCheckpointWithOutboxAtomic(cp(2, 6), job("o2"), 1);
      expect(await a.incrementOutboxAttempt(T, "o1")).toBe(1);
      expect(await a.incrementOutboxAttempt(T, "o1")).toBe(2);
      expect(await a.incrementOutboxAttempt(T, "missing")).toBe(0);
      await a.markOutboxJobCompleted(T, "o1");
      expect((await a.getPendingOutboxJobs(T)).map((j) => j.jobId)).toEqual(["o2"]);
      await a.markOutboxJobFailed(T, "o2");
      expect(await a.getPendingOutboxJobs(T)).toHaveLength(0);
      await a.markOutboxJobFailed(T, "o1"); // completed job must not flip to failed
      expect(await a.getPendingOutboxJobs(T)).toHaveLength(0);
    });

    // ---------------- claims ----------------
    it("claims: tiers, rejection, receipts, upgrade-on-stronger-same-value", async () => {
      const c0 = await a.ingestClaimAtomic(mkClaim("clm_0", "Hindi", 0));
      expect(c0.status).toBe("active");
      expect((await a.ingestClaimAtomic(mkClaim("clm_3", "Telugu", 3))).status).toBe("rejected");

      const r1 = await a.ingestClaimAtomic(mkClaim("job_1::0", "Hindi", 3));
      const r2 = await a.ingestClaimAtomic(mkClaim("job_1::0", "Hindi", 3)); // replay
      expect(r1.claimId).toBe("clm_0");
      expect(r2).toEqual(r1);
      const active = await a.getClaimsByCustomer(T, C, { maxSensitivity: "restricted", statuses: ["active"] });
      expect(active).toHaveLength(1);
      expect(active[0].reinforcementCount).toBe(2);
      expect((active[0] as unknown as Record<string, unknown>)._id).toBeUndefined();

      // stronger claim with same value as a weaker one supersedes it
      await a.ingestClaimAtomic(mkClaim("w", "Tamil", 3, { attributeKey: "callback_window" }));
      await a.ingestClaimAtomic(mkClaim("s", "Tamil", 1, { attributeKey: "callback_window" }));
      const cw = (await allClaims()).filter((c) => c.attributeKey === "callback_window");
      expect(cw.find((c) => c.claimId === "s")!.status).toBe("active");
      expect(cw.find((c) => c.claimId === "w")!.status).toBe("superseded");
    });

    it("claims: expired claims neither absorb nor conflict", async () => {
      const past = Date.now() - 10_000;
      await a.ingestClaimAtomic(mkClaim("old", "Hindi", 3, { expiresAt: past + 1000 }));
      const fresh = await a.ingestClaimAtomic(mkClaim("new", "Tamil", 3));
      expect(fresh.status).toBe("active");
      const old = (await allClaims()).find((c) => c.claimId === "old")!;
      expect(old.reinforcementCount).toBe(1);
      expect(old.status).toBe("active"); // untouched until swept
    });

    it("claims: query filters (sensitivity, tier, statuses, expiry)", async () => {
      await a.ingestClaimAtomic(mkClaim("pub", "Hindi", 3, { attributeKey: "a1", sensitivity: "public" }));
      await a.ingestClaimAtomic(mkClaim("res", "Hindi", 3, { attributeKey: "a2", sensitivity: "restricted" }));
      await a.ingestClaimAtomic(mkClaim("t0", "Hindi", 0, { attributeKey: "a3" }));
      await a.ingestClaimAtomic(mkClaim("exp", "Hindi", 3, { attributeKey: "a4", expiresAt: Date.now() - 1 }));
      const ids = async (f: Parameters<RuntimeStorageAdapter["getClaimsByCustomer"]>[2]) =>
        (await a.getClaimsByCustomer(T, C, f)).map((c) => c.claimId).sort();
      expect(await ids({ maxSensitivity: "public" })).toEqual(["pub"]);
      expect(await ids({ maxSensitivity: "internal" })).toEqual(["pub", "t0"]);
      expect(await ids({ maxSensitivity: "restricted" })).toEqual(["pub", "res", "t0"]);
      expect(await ids({ maxSensitivity: "restricted", includeExpired: true })).toEqual(["exp", "pub", "res", "t0"]);
      expect(await ids({ maxSensitivity: "restricted", maxAuthorityTier: 0 })).toEqual(["t0"]);
      expect(await ids({ maxSensitivity: "restricted", statuses: ["disputed"] })).toEqual([]);
      expect(await a.getClaimsByCustomer(T, "someone_else", { maxSensitivity: "restricted" })).toEqual([]);
      expect(await a.getClaimsByCustomer("other_tenant", C, { maxSensitivity: "restricted" })).toEqual([]);
    });

    it("claims: 10 concurrent replays of one claimId reinforce exactly once", async () => {
      await a.ingestClaimAtomic(mkClaim("base", "Hindi", 0));
      const rs = await Promise.all(Array.from({ length: 10 }, () => a.ingestClaimAtomic(mkClaim("job_x::0", "Hindi", 3))));
      expect(new Set(rs.map((r) => r.claimId))).toEqual(new Set(["base"]));
      const base = (await allClaims()).find((c) => c.claimId === "base")!;
      expect(base.reinforcementCount).toBe(2);
    });

    it("claims: concurrent ingests of conflicting claims never violate state invariants (write-skew)", async () => {
      let s = 99;
      const rnd = () => ((s = (s * 1664525 + 1013904223) % 4294967296) / 4294967296);
      const values = ["Hindi", "Telugu", "Tamil"];
      const attrs = ["preferred_language", "communication_channel"];
      for (let round = 0; round < 3; round++) {
        await Promise.all(
          Array.from({ length: 40 }, () =>
            a.ingestClaimAtomic(
              mkClaim(uid("c"), values[Math.floor(rnd() * 3)], Math.floor(rnd() * 5) as AuthorityTier, {
                attributeKey: attrs[Math.floor(rnd() * 2)],
              })
            )
          )
        );
        await assertInvariants();
      }
    });

    it("claims: concurrent equal-tier conflicting ingests on an empty attribute -> all disputed, none active", async () => {
      const vals = ["Hindi", "Telugu", "Tamil", "Kannada", "Marathi"];
      await Promise.all(vals.map((v) => a.ingestClaimAtomic(mkClaim(uid("c"), v, 3))));
      const all = await allClaims();
      await assertInvariants();
      // serialized one at a time: 1st active, 2nd disputes with it, 3rd..5th dispute with the group
      expect(all.filter((c) => c.status === "active")).toHaveLength(0);
      expect(all.filter((c) => c.status === "disputed")).toHaveLength(5);
    });

    // ---------------- disputes / archive ----------------
    it("disputes: guarded resolution, audit, reference cleanup (incl. rejected tier-4 rows)", async () => {
      await a.ingestClaimAtomic(mkClaim("A", "Hindi", 3));
      await a.ingestClaimAtomic(mkClaim("B", "Telugu", 3));
      await a.ingestClaimAtomic(mkClaim("D", "Tamil", 3));
      const rej = await a.ingestClaimAtomic(mkClaim("R", "Kannada", 4));
      expect(rej.status).toBe("rejected");

      const base = { tenantId: T, customerId: C, attributeKey: "preferred_language", resolvedByActor: "agent_1", reason: "confirmed", now: Date.now() };
      expect(await a.resolveDisputeAtomic({ ...base, winningClaimId: "A", losingClaimIds: ["B"] })).toBe(false); // orphan D
      expect(await a.resolveDisputeAtomic({ ...base, winningClaimId: "A", losingClaimIds: ["B", "ghost"] })).toBe(false);
      expect(await a.resolveDisputeAtomic({ ...base, winningClaimId: "A", losingClaimIds: ["B", "B"] })).toBe(false);
      expect(await a.resolveDisputeAtomic({ ...base, winningClaimId: "A", losingClaimIds: ["A", "B"] })).toBe(false);
      expect(await a.resolveDisputeAtomic({ ...base, resolvedByActor: " ", winningClaimId: "A", losingClaimIds: ["B", "D"] })).toBe(false);
      expect(await a.resolveDisputeAtomic({ ...base, customerId: "x", winningClaimId: "A", losingClaimIds: ["B", "D"] })).toBe(false);
      expect(await a.resolveDisputeAtomic({ ...base, winningClaimId: "R", losingClaimIds: ["B", "D"] })).toBe(false); // R not disputed
      expect((await allClaims()).filter((c) => c.status === "disputed")).toHaveLength(3); // nothing mutated

      expect(await a.resolveDisputeAtomic({ ...base, winningClaimId: "A", losingClaimIds: ["B", "D"] })).toBe(true);
      const all = await allClaims();
      expect(all.find((c) => c.claimId === "A")!.status).toBe("active");
      expect(all.filter((c) => c.status === "superseded").map((c) => c.supersededBy)).toEqual(["A", "A"]);
      for (const c of all) expect(c.conflictsWith.filter((id) => id === "B" || id === "D")).toEqual([]);
      const audits = await a.getDisputeAudits(T, C);
      expect(audits).toHaveLength(1);
      expect(audits[0]).toMatchObject({ winningClaimId: "A", resolvedByActor: "agent_1" });
      expect((audits[0] as unknown as Record<string, unknown>)._id).toBeUndefined();
      expect(await a.getDisputeAudits(T, "someone_else")).toEqual([]);
    });

    it("disputes: 5 concurrent identical resolutions -> exactly one succeeds", async () => {
      await a.ingestClaimAtomic(mkClaim("A", "Hindi", 3));
      await a.ingestClaimAtomic(mkClaim("B", "Telugu", 3));
      const p = { tenantId: T, customerId: C, attributeKey: "preferred_language", winningClaimId: "A", losingClaimIds: ["B"], resolvedByActor: "x", reason: "y", now: Date.now() };
      const rs = await Promise.all(Array.from({ length: 5 }, () => a.resolveDisputeAtomic(p)));
      expect(rs.filter(Boolean)).toHaveLength(1);
      expect(await a.getDisputeAudits(T, C)).toHaveLength(1);
    });

    it("archive: peers lose the reference and a lone survivor is reactivated", async () => {
      await a.ingestClaimAtomic(mkClaim("A", "Hindi", 3));
      await a.ingestClaimAtomic(mkClaim("B", "Telugu", 3));
      await a.ingestClaimAtomic(mkClaim("D", "Tamil", 3));
      expect(await a.archiveClaim(T, "D", Date.now())).toBe(true);
      let all = await allClaims();
      expect(all.filter((c) => c.status === "disputed")).toHaveLength(2);
      expect(await a.archiveClaim(T, "B", Date.now())).toBe(true);
      all = await allClaims();
      const A = all.find((c) => c.claimId === "A")!;
      expect(A.status).toBe("active");
      expect(A.conflictsWith).toEqual([]);
      expect(await a.archiveClaim(T, "ghost", Date.now())).toBe(false);
      expect(await a.recordClaimRead(T, "A", 12345)).toBe(true);
      expect((await allClaims()).find((c) => c.claimId === "A")!.lastReadAt).toBe(12345);
      expect(await a.recordClaimRead(T, "ghost", 1)).toBe(false);
      await assertInvariants();
    });

    // ---------------- workflow ----------------
    it("workflow: version CAS, customer binding in both orders, concurrent first save", async () => {
      expect(await a.saveWorkflowStateAtomic(wf(2), 0)).toBe(false); // version must be expected+1
      expect(await a.saveWorkflowStateAtomic(wf(1), 5)).toBe(false); // stale expected
      expect(await a.saveWorkflowStateAtomic(wf(1), 0)).toBe(true);
      expect(await a.saveWorkflowStateAtomic(wf(2, { customerId: "evil" }), 1)).toBe(false);
      expect(await a.saveWorkflowStateAtomic(wf(2), 1)).toBe(true);
      expect((await a.getWorkflowState(T, S))!.version).toBe(2);
      expect(await a.getWorkflowState(T, "nope")).toBeNull();
      // session was bound by the workflow save; a turn from another customer is refused
      expect(await a.getSessionCustomer(T, S)).toBe(C);
      await expect(a.appendTurnAtomic(turn({ customerId: "evil" }))).rejects.toThrow(AccessDeniedError);
    });

    it("workflow: first save from another customer after a turn is refused; concurrent first saves -> one winner", async () => {
      await a.appendTurnAtomic(turn({ sessionId: "s_b" }));
      expect(await a.saveWorkflowStateAtomic(wf(1, { sessionId: "s_b", customerId: "evil" }), 0)).toBe(false);
      const rs = await Promise.all(Array.from({ length: 6 }, () => a.saveWorkflowStateAtomic(wf(1, { sessionId: "s_c" }), 0)));
      expect(rs.filter(Boolean)).toHaveLength(1);
    });

    it("consume: checks every precondition and succeeds exactly once under concurrency", async () => {
      const now = Date.now();
      const token = "cfm_secret";
      const th = sha256(token);
      await a.appendTurnAtomic(turn());
      await a.saveWorkflowStateAtomic(
        wf(1, { confirmationTokenHash: th, confirmationPayloadHash: "h1", confirmationExpiresAt: now + 60_000, confirmationChallengeSeq: 0 }), 0
      );
      const p = {
        tenantId: T, sessionId: S, expectedVersion: 1, expectedLatestSeq: 1, actionName: "cancelPolicy",
        expectedLatestUserSeq: 1,
        expectedTokenHash: th, expectedPayloadHash: "h1", now, command: cmd("c0"),
      };
      expect((await a.consumeTokenAndCreateCommandAtomic({ ...p, expectedVersion: 0 })).reason).toBe("CONCURRENT_WORKFLOW_UPDATE");
      expect((await a.consumeTokenAndCreateCommandAtomic({ ...p, expectedLatestSeq: 0 })).reason).toBe("NEW_EVENT_ARRIVED_DURING_AUTH");
      expect((await a.consumeTokenAndCreateCommandAtomic({ ...p, actionName: "wire" })).reason).toBe("INTENT_MISMATCH");
      expect((await a.consumeTokenAndCreateCommandAtomic({ ...p, expectedTokenHash: sha256("bad") })).reason).toBe("INVALID_CONFIRMATION_TOKEN");
      expect((await a.consumeTokenAndCreateCommandAtomic({ ...p, expectedPayloadHash: "zzz" })).reason).toBe("PAYLOAD_BINDING_MISMATCH");
      expect((await a.consumeTokenAndCreateCommandAtomic({ ...p, now: now + 120_000 })).reason).toBe("CONFIRMATION_TOKEN_EXPIRED");
      expect((await a.getWorkflowState(T, S))!.version).toBe(1); // failed attempts mutate nothing

      const rs = await Promise.all(Array.from({ length: 8 }, (_, i) => a.consumeTokenAndCreateCommandAtomic({ ...p, command: cmd(`c${i}`) })));
      expect(rs.filter((r) => r.ok)).toHaveLength(1);
      const after = (await a.getWorkflowState(T, S))!;
      expect(after).toMatchObject({ version: 2, pendingAction: null, confirmationTokenHash: null, confirmationChallengeSeq: null });
      expect((await a.consumeTokenAndCreateCommandAtomic({ ...p, expectedVersion: 2 })).ok).toBe(false); // replay
    });

    it("consume: a turn arriving concurrently invalidates the authorization", async () => {
      const th = sha256("t");
      await a.appendTurnAtomic(turn());
      await a.saveWorkflowStateAtomic(wf(1, { confirmationTokenHash: th, confirmationPayloadHash: "h1", confirmationExpiresAt: Date.now() + 60_000, confirmationChallengeSeq: 0 }), 0);
      await a.appendTurnAtomic(turn()); // latest seq is now 2, caller thinks 1
      const r = await a.consumeTokenAndCreateCommandAtomic({
        tenantId: T, sessionId: S, expectedVersion: 1, expectedLatestSeq: 1, actionName: "cancelPolicy",
        expectedTokenHash: th, expectedPayloadHash: "h1", now: Date.now(), command: cmd("x"),
      });
      expect(r).toEqual({ ok: false, reason: "NEW_EVENT_ARRIVED_DURING_AUTH" });
    });

    it("tenant isolation: identical ids in different tenants never collide", async () => {
      await a.appendTurnAtomic(turn({ idempotencyKey: "same" }));
      const other = await a.appendTurnAtomic(turn({ tenantId: "tenant_2", idempotencyKey: "same" }));
      expect(other.inserted).toBe(true);
      expect(other.event.seq).toBe(1);
      await a.ingestClaimAtomic(mkClaim("same", "Hindi", 0));
      const r = await a.ingestClaimAtomic(mkClaim("same", "Telugu", 0, { tenantId: "tenant_2" }));
      expect(r.status).toBe("active");
      expect(await a.getClaimsByCustomer("tenant_2", C, { maxSensitivity: "restricted" })).toHaveLength(1);
    });
  });
}
