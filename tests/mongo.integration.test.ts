import { MongoClient, type Db } from "mongodb";
import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { AgentMemoryRuntime, type CompactionLLM, type RuntimeStorageAdapter } from "../src/index";
import { MongoStorageAdapter } from "../src/adapters/mongo";

const URI = process.env.MONGO_URI;
const T = "t1", C = "c1", S = "s1";
const PRINCIPAL = { tenantId: T, agentId: "a", maxAllowedSensitivity: "restricted" as const };

const llm = (value?: string): CompactionLLM => ({
  async compactAndExtract() {
    return {
      updatedSummary: "summary",
      extractedClaims: value
        ? [{ attributeKey: "preferred_language", value, confidence: 0.9, importance: 0.8, volatility: "CLASS_C_PREFERENCE", sensitivity: "internal" }]
        : [],
    };
  },
});

(URI ? describe : describe.skip)("AgentMemoryRuntime on MongoStorageAdapter (end to end)", () => {
  let client: MongoClient, db: Db, adapter: MongoStorageAdapter;
  beforeEach(async () => {
    client = new MongoClient(URI!);
    await client.connect();
    db = client.db(`amr_it_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`);
    adapter = new MongoStorageAdapter(client, db);
    await adapter.ensureIndexes();
  });
  afterEach(async () => { await db.dropDatabase(); await client.close(); });

  const turn = (rt: AgentMemoryRuntime, i: number, role: "user" | "assistant" = "user", content = `turn ${i}`) =>
    rt.appendTurn({ tenantId: T, sessionId: S, customerId: C, idempotencyKey: `k_${role}_${i}`, role, content, triggerAsyncGC: false });

  it("5 runtime instances compact concurrently -> exactly one wins; context stays within budget", async () => {
    const rts = Array.from({ length: 5 }, () => new AgentMemoryRuntime(adapter, llm("Telugu"), { keepRecentTurns: 2, maxContextTokens: 200 }));
    for (let i = 0; i < 8; i++) await turn(rts[0], i);
    const res = await Promise.all(rts.map((r) => r.runSessionCompaction(T, S, C)));
    expect(res.filter((r) => r.compacted)).toHaveLength(1);
    expect((await adapter.getCheckpoint(T, S))!.version).toBe(1);
    expect(await adapter.getPendingOutboxJobs(T)).toHaveLength(0);
    const ctx = await rts[0].buildContext({ principal: PRINCIPAL, sessionId: S, customerId: C });
    expect(ctx.estimatedTokens).toBeLessThanOrEqual(200);
    expect(ctx.gen0Tail).toHaveLength(2);
    expect(ctx.gen2ActiveFacts.map((f) => f.value)).toEqual(["Telugu"]);
  });

  it("crash after checkpoint commit -> recovery ingests once; replaying the job does not inflate counts", async () => {
    const rt = new AgentMemoryRuntime(adapter, llm("Telugu"), { keepRecentTurns: 2 });
    await rt.ingestAuthenticatedCustomerClaim({ tenantId: T, customerId: C, attributeKey: "preferred_language", value: "Telugu", volatility: "CLASS_C_PREFERENCE", sourceEventId: "e" });
    for (let i = 0; i < 6; i++) await turn(rt, i);
    await expect(rt.runSessionCompaction(T, S, C, { simulateCrashBeforeOutboxFlush: true })).rejects.toThrow();
    const [job] = await adapter.getPendingOutboxJobs(T);
    await expect(rt.processOutboxJob(job, { simulateCrashBeforeMarkCompleted: true })).rejects.toThrow();
    expect(await rt.recoverPendingOutbox(T)).toEqual({ recovered: 1, failed: 0 });
    await rt.processOutboxJob(job); // a stale worker replays the same job
    const [claim] = await adapter.getClaimsByCustomer(T, C, { maxSensitivity: "restricted", statuses: ["active"] });
    expect(claim.reinforcementCount).toBe(2);
  });

  it("confirmation flow: only one of 12 concurrent authorizations (across 4 instances) creates a command", async () => {
    const rts = Array.from({ length: 4 }, () => new AgentMemoryRuntime(adapter, llm()));
    const rt = rts[0];
    await turn(rt, 1, "user", "cancel my policy");
    const a = await rt.setSessionAuthenticated({ tenantId: T, sessionId: S, customerId: C, authenticated: true, authMethod: "otp", expectedVersion: 0 });
    const b = await rt.updateWorkflowIntent({ tenantId: T, sessionId: S, customerId: C, currentIntent: "cancel", pendingAction: "cancelPolicy" }, a.newVersion);
    const ch = await rt.issueConfirmationChallenge({ tenantId: T, sessionId: S, customerId: C, actionName: "cancelPolicy", payloadHash: "h1", expectedVersion: b.newVersion });
    expect(ch.issued).toBe(true);
    await turn(rt, 2, "assistant", "please confirm");
    await turn(rt, 3, "user", "yes");

    const auth = (r: AgentMemoryRuntime, tok = ch.token!) =>
      r.authorizeAndCreateExecutionCommand({ tenantId: T, sessionId: S, actionName: "cancelPolicy", payloadHash: "h1", providedConfirmationToken: tok });
    expect((await auth(rt, "cfm_wrong")).reason).toBe("INVALID_CONFIRMATION_TOKEN");
    const results = await Promise.all(Array.from({ length: 12 }, (_, i) => auth(rts[i % 4])));
    expect(results.filter((r) => r.allowed)).toHaveLength(1);
    expect(await db.collection("amr_commands").countDocuments()).toBe(1);
    expect((await auth(rt)).allowed).toBe(false); // replay
    const st = (await adapter.getWorkflowState(T, S))!;
    expect(JSON.stringify(st)).not.toContain(ch.token!);
  });

  it("session hijack attempts are refused end to end", async () => {
    const rt = new AgentMemoryRuntime(adapter, llm());
    await turn(rt, 1);
    await expect(rt.appendTurn({ tenantId: T, sessionId: S, customerId: "evil", idempotencyKey: "x", role: "user", content: "hi", triggerAsyncGC: false })).rejects.toThrow();
    expect((await rt.setSessionAuthenticated({ tenantId: T, sessionId: S, customerId: "evil", authenticated: true, authMethod: "otp", expectedVersion: 0 })).saved).toBe(false);
    await expect(rt.buildContext({ principal: PRINCIPAL, sessionId: S, customerId: "evil" })).rejects.toThrow();
  });

  it("unique indexes exist and are enforced by the database itself", async () => {
    const idx = async (n: string) => (await db.collection(n).indexes()).filter((i) => i.unique).map((i) => Object.keys(i.key).join(","));
    expect(await idx("amr_turns")).toEqual(expect.arrayContaining(["tenantId,sessionId,idempotencyKey", "tenantId,sessionId,seq"]));
    expect(await idx("amr_claims")).toContain("tenantId,claimId");
    expect(await idx("amr_outbox")).toContain("tenantId,jobId");
    expect(await idx("amr_commands")).toContain("tenantId,commandId");
    await expect(
      db.collection("amr_commands").insertMany([{ tenantId: T, commandId: "dup" }, { tenantId: T, commandId: "dup" }])
    ).rejects.toThrow(/E11000/);
  });

  it("adapter instances are interchangeable with the in-memory contract type", () => {
    const a: RuntimeStorageAdapter = adapter;
    expect(typeof a.consumeTokenAndCreateCommandAtomic).toBe("function");
  });
});
