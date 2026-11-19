// Smoke test of the BUILT package. Runs the README quickstart end to end.
// Default target is ../dist; set AMR_IMPORT / AMR_MONGO_IMPORT to test an installed tarball.
import assert from "node:assert/strict";
import { createHash } from "node:crypto";

const core = await import(process.env.AMR_IMPORT ?? "../dist/index.js");
const mongo = await import(process.env.AMR_MONGO_IMPORT ?? "../dist/adapters/mongo.js");
const { AgentMemoryRuntime, InMemoryStorageAdapter } = core;

assert.equal(typeof mongo.MongoStorageAdapter, "function", "mongo adapter must be exported");

// ---- README quickstart -----------------------------------------------------
const llm = {
  async compactAndExtract({ previousSummary, turns }) {
    // call your model here; return a summary and any claims it extracted
    return { updatedSummary: `${previousSummary} (${turns.length} turns)`.trim(), extractedClaims: [] };
  },
};

const runtime = new AgentMemoryRuntime(new InMemoryStorageAdapter(), llm);
const ids = { tenantId: "acme", sessionId: "s1", customerId: "c42" };

await runtime.appendTurn({ ...ids, idempotencyKey: "m1", role: "user", content: "Please call me in Telugu" });

await runtime.ingestSystemOfRecordClaim({
  tenantId: ids.tenantId, customerId: ids.customerId,
  attributeKey: "preferred_language", value: "Telugu",
  volatility: "CLASS_C_PREFERENCE", sourceEventId: "crm:123",
});

// a model-extracted claim that disagrees can never override the CRM value
const weak = await runtime.ingestExtractedClaim({
  tenantId: ids.tenantId, customerId: ids.customerId,
  attributeKey: "preferred_language", value: "Hindi",
  volatility: "CLASS_C_PREFERENCE", confidence: 0.9, importance: 0.9, sourceEventId: "turn:1",
});
assert.equal(weak.status, "rejected");

const ctx = await runtime.buildContext({
  principal: { tenantId: ids.tenantId, agentId: "voice-agent-1", maxAllowedSensitivity: "internal" },
  sessionId: ids.sessionId, customerId: ids.customerId,
});
assert.deepEqual(ctx.gen2ActiveFacts.map((f) => [f.key, f.value, f.tier]), [["preferred_language", "Telugu", 0]]);
assert.equal(ctx.gen0Tail.length, 1);
assert.ok(ctx.estimatedTokens <= 6000);

// ---- README: confirmation protocol ---------------------------------------
const payloadHash = createHash("sha256").update(JSON.stringify({ policyId: "P-1" })).digest("hex");
const a = await runtime.setSessionAuthenticated({ ...ids, authenticated: true, authMethod: "otp", expectedVersion: 0 });
const b = await runtime.updateWorkflowIntent({ ...ids, currentIntent: "cancel", pendingAction: "cancelPolicy" }, a.newVersion);
const ch = await runtime.issueConfirmationChallenge({ ...ids, actionName: "cancelPolicy", payloadHash, expectedVersion: b.newVersion });
assert.ok(ch.issued && ch.token);

const args = { tenantId: ids.tenantId, sessionId: ids.sessionId, actionName: "cancelPolicy", payloadHash, providedConfirmationToken: ch.token };
assert.equal((await runtime.authorizeAndCreateExecutionCommand(args)).reason, "AWAITING_USER_CONFIRMATION_TURN");

await runtime.appendTurn({ ...ids, idempotencyKey: "m2", role: "user", content: "Yes, cancel it" });
const ok = await runtime.authorizeAndCreateExecutionCommand(args);
assert.equal(ok.allowed, true);
assert.equal((await runtime.authorizeAndCreateExecutionCommand(args)).allowed, false, "token is single-use");

console.log("smoke OK");
