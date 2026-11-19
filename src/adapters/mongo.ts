/**
 * MongoDB adapter for RuntimeStorageAdapter.
 *
 * Requires a replica set (multi-document transactions). Design:
 *  - amr_sessions: ONE anchor doc per (tenant, session) holding seqCounter, customerId binding,
 *    checkpoint and workflow state. Every seq allocation / CAS is a single-document operation
 *    on this doc, so concurrent writers conflict on it and are serialized by Mongo.
 *  - amr_claim_guards: one tiny doc per (tenant, customer, attribute). Every claim mutation
 *    touches it inside its transaction. Without it two concurrent ingests that both see an empty
 *    "open" set would both insert (write skew: snapshot isolation does not catch it).
 *  - Unique indexes back every idempotency / receipt / id guarantee.
 */
import { createHash, randomUUID, timingSafeEqual } from "node:crypto";
import type { MongoClient, Db, Collection, ClientSession, Filter } from "mongodb";
import {
  AccessDeniedError,
  IdempotencyConflictError,
  type RuntimeStorageAdapter,
  type TurnInput,
  type TurnEvent,
  type Checkpoint,
  type CompactionOutboxJob,
  type MemoryClaim,
  type ClaimStatus,
  type ClaimQueryFilter,
  type DisputeAuditRecord,
  type WorkflowState,
  type ExecutionCommand,
  type SensitivityLevel,
} from "../index.js";

const SENSITIVITY_RANK: Record<SensitivityLevel, number> = {
  public: 0,
  internal: 1,
  confidential: 2,
  restricted: 3,
};

interface SessionAnchorDoc {
  _id: string;
  tenantId: string;
  sessionId: string;
  customerId: string;
  seqCounter: number;
  checkpoint: Checkpoint | null;
  workflow: WorkflowState | null;
}
type TurnDoc = TurnEvent & { payloadSig: string };
interface ReceiptDoc {
  _id: string;
  tenantId: string;
  claimId: string;
  status: ClaimStatus;
  resultingClaimId: string;
  createdAt: number;
}
interface GuardDoc {
  _id: string;
  v: number;
}

const digestEqual = (a: string, b: string) =>
  timingSafeEqual(createHash("sha256").update(a).digest(), createHash("sha256").update(b).digest());

function strip<T extends object>(doc: T & { _id?: unknown }): T {
  const { _id: _ignored, ...rest } = doc;
  return rest as unknown as T;
}
const stripTurn = (d: TurnDoc & { _id?: unknown }): TurnEvent => {
  const { _id: _a, payloadSig: _b, ...rest } = d;
  return rest as TurnEvent;
};

const isDuplicateKey = (e: unknown) =>
  typeof e === "object" && e !== null && (e as { code?: number }).code === 11000;

export class MongoStorageAdapter implements RuntimeStorageAdapter {
  private client: MongoClient;
  private db: Db;
  private prefix: string;
  private sessions: Collection<SessionAnchorDoc>;
  private turns: Collection<TurnDoc>;
  private outbox: Collection<CompactionOutboxJob>;
  private claims: Collection<MemoryClaim>;
  private receipts: Collection<ReceiptDoc>;
  private guards: Collection<GuardDoc>;
  private audits: Collection<DisputeAuditRecord>;
  private commands: Collection<ExecutionCommand>;

  constructor(client: MongoClient, db: Db, prefix = "amr_") {
    this.client = client;
    this.db = db;
    this.prefix = prefix;
    const opts = { readConcern: { level: "majority" }, writeConcern: { w: "majority" } } as const;
    const col = <T extends object>(n: string) => db.collection<T & { _id?: never }>(`${prefix}${n}`, opts) as unknown as Collection<T>;
    this.sessions = db.collection<SessionAnchorDoc>(`${prefix}sessions`, opts);
    this.guards = db.collection<GuardDoc>(`${prefix}claim_guards`, opts);
    this.receipts = db.collection<ReceiptDoc>(`${prefix}ingest_receipts`, opts);
    this.turns = col<TurnDoc>("turns");
    this.outbox = col<CompactionOutboxJob>("outbox");
    this.claims = col<MemoryClaim>("claims");
    this.audits = col<DisputeAuditRecord>("dispute_audits");
    this.commands = col<ExecutionCommand>("commands");
  }

  /** Run once at startup. Creates collections (transactions need them to exist) and indexes. */
  async ensureIndexes(): Promise<void> {
    const existing = new Set((await this.db.listCollections().toArray()).map((c) => c.name));
    for (const n of ["sessions", "claim_guards", "ingest_receipts", "turns", "outbox", "claims", "dispute_audits", "commands"]) {
      if (!existing.has(`${this.prefix}${n}`)) {
        try { await this.db.createCollection(`${this.prefix}${n}`); } catch (e) { if ((e as { code?: number }).code !== 48) throw e; }
      }
    }
    await Promise.all([
      this.turns.createIndex({ tenantId: 1, sessionId: 1, idempotencyKey: 1 }, { unique: true }),
      this.turns.createIndex({ tenantId: 1, sessionId: 1, seq: 1 }, { unique: true }),
      this.turns.createIndex({ tenantId: 1, sessionId: 1, role: 1, seq: 1 }),
      this.turns.createIndex({ tenantId: 1, sessionId: 1, pinned: 1, seq: 1 }),
      this.outbox.createIndex({ tenantId: 1, jobId: 1 }, { unique: true }),
      this.outbox.createIndex({ tenantId: 1, status: 1, createdAt: 1 }),
      this.claims.createIndex({ tenantId: 1, claimId: 1 }, { unique: true }),
      this.claims.createIndex({ tenantId: 1, customerId: 1, attributeKey: 1, status: 1 }),
      this.audits.createIndex({ tenantId: 1, customerId: 1, resolvedAt: -1 }),
      this.commands.createIndex({ tenantId: 1, commandId: 1 }, { unique: true }),
    ]);
  }

  private anchorId(t: string, s: string) {
    return `${t}::${s}`;
  }

  /**
   * Runs fn in a snapshot transaction. withTransaction retries TransientTransactionError
   * (write conflicts). Concurrent upserts of the same _id can also surface E11000; that is
   * safe to retry because the aborted transaction left no trace and the retry sees the winner.
   */
  private async runTx<T>(fn: (s: ClientSession) => Promise<T>): Promise<T> {
    for (let attempt = 0; ; attempt++) {
      const session = this.client.startSession();
      try {
        let result!: T;
        await session.withTransaction(
          async () => { result = await fn(session); },
          { readConcern: { level: "snapshot" }, writeConcern: { w: "majority" } }
        );
        return result;
      } catch (e) {
        if (isDuplicateKey(e) && attempt < 8) continue;
        throw e;
      } finally {
        await session.endSession();
      }
    }
  }

  /** Serializes all claim mutations for one (tenant, customer, attribute). */
  private async lockAttr(s: ClientSession, tenantId: string, customerId: string, attr: string) {
    await this.guards.updateOne(
      { _id: `${tenantId}::${customerId}::${attr}` },
      { $inc: { v: 1 } },
      { upsert: true, session: s }
    );
  }

  // ---------------------------------------------------------------- turns

  async getSessionCustomer(tenantId: string, sessionId: string): Promise<string | null> {
    const a = await this.sessions.findOne({ _id: this.anchorId(tenantId, sessionId) }, { projection: { customerId: 1 } });
    return a?.customerId ?? null;
  }

  async appendTurnAtomic(input: TurnInput): Promise<{ inserted: boolean; event: TurnEvent }> {
    const payloadSig = JSON.stringify({
      customerId: input.customerId, role: input.role, content: input.content, pinned: input.pinned,
    });
    const anchorId = this.anchorId(input.tenantId, input.sessionId);

    return this.runTx(async (s) => {
      const bound = await this.sessions.findOne({ _id: anchorId }, { session: s });
      if (bound && bound.customerId !== input.customerId) {
        throw new AccessDeniedError(`Session ${input.sessionId} is bound to another customer`);
      }

      const existing = await this.turns.findOne(
        { tenantId: input.tenantId, sessionId: input.sessionId, idempotencyKey: input.idempotencyKey },
        { session: s }
      );
      if (existing) {
        if (existing.payloadSig !== payloadSig) {
          throw new IdempotencyConflictError(`Idempotency key "${input.idempotencyKey}" reused with different payload`);
        }
        return { inserted: false, event: stripTurn(existing) };
      }

      // One atomic op: create-or-bind the anchor AND allocate the seq. The customerId filter makes a
      // mismatching writer fail to match; the upsert then collides on _id (E11000) and is rejected.
      let anchor;
      try {
        anchor = await this.sessions.findOneAndUpdate(
          { _id: anchorId, customerId: input.customerId },
          {
            $inc: { seqCounter: 1 },
            $setOnInsert: { tenantId: input.tenantId, sessionId: input.sessionId, checkpoint: null, workflow: null },
          },
          { upsert: true, returnDocument: "after", session: s }
        );
      } catch (e) {
        if (isDuplicateKey(e)) {
          const now = await this.sessions.findOne({ _id: anchorId }, { session: s });
          if (now && now.customerId !== input.customerId) {
            throw new AccessDeniedError(`Session ${input.sessionId} is bound to another customer`);
          }
        }
        throw e;
      }
      if (!anchor) throw new Error("session anchor upsert returned no document");

      const event: TurnEvent = { ...input, seq: anchor.seqCounter, timestamp: Date.now() };
      await this.turns.insertOne({ ...event, payloadSig }, { session: s });
      return { inserted: true, event };
    });
  }

  async getTurnsAfterSeq(tenantId: string, sessionId: string, afterSeq: number): Promise<TurnEvent[]> {
    const docs = await this.turns.find({ tenantId, sessionId, seq: { $gt: afterSeq } }).sort({ seq: 1 }).toArray();
    return docs.map(stripTurn);
  }

  async getUncompactedTokenSum(tenantId: string, sessionId: string, afterSeq: number): Promise<number> {
    const [row] = await this.turns
      .aggregate<{ total: number }>([
        { $match: { tenantId, sessionId, seq: { $gt: afterSeq }, pinned: false } },
        { $group: { _id: null, total: { $sum: "$tokens" } } },
      ])
      .toArray();
    return row?.total ?? 0;
  }

  async getPinnedTurns(tenantId: string, sessionId: string): Promise<TurnEvent[]> {
    const docs = await this.turns.find({ tenantId, sessionId, pinned: true }).sort({ seq: 1 }).toArray();
    return docs.map(stripTurn);
  }

  async getLatestSeq(tenantId: string, sessionId: string): Promise<number> {
    const a = await this.sessions.findOne({ _id: this.anchorId(tenantId, sessionId) }, { projection: { seqCounter: 1 } });
    return a?.seqCounter ?? 0;
  }

  async getFirstUserTurnSeqAfter(tenantId: string, sessionId: string, afterSeq: number): Promise<number | null> {
    const d = await this.turns.findOne(
      { tenantId, sessionId, role: "user", seq: { $gt: afterSeq } },
      { sort: { seq: 1 }, projection: { seq: 1 } }
    );
    return d?.seq ?? null;
  }

  async getLatestUserTurnSeqAfter(tenantId: string, sessionId: string, afterSeq: number): Promise<number | null> {
    const d = await this.turns.findOne(
      { tenantId, sessionId, role: "user", seq: { $gt: afterSeq } },
      { sort: { seq: -1 }, projection: { seq: 1 } }
    );
    return d?.seq ?? null;
  }

  // ---------------------------------------------------------------- checkpoint + outbox

  async getCheckpoint(tenantId: string, sessionId: string): Promise<Checkpoint | null> {
    const a = await this.sessions.findOne({ _id: this.anchorId(tenantId, sessionId) }, { projection: { checkpoint: 1 } });
    return a?.checkpoint ?? null;
  }

  async commitCheckpointWithOutboxAtomic(
    checkpoint: Checkpoint,
    outboxJob: CompactionOutboxJob,
    expectedVersion: number
  ): Promise<boolean> {
    if (checkpoint.version !== expectedVersion + 1) return false;
    const anchorId = this.anchorId(checkpoint.tenantId, checkpoint.sessionId);

    return this.runTx(async (s) => {
      const anchor = await this.sessions.findOne({ _id: anchorId }, { session: s });
      if (!anchor) return false; // a checkpoint without any turn is meaningless
      if (anchor.customerId !== outboxJob.customerId) return false;
      const currentVersion = anchor.checkpoint?.version ?? 0;
      if (currentVersion !== expectedVersion) return false;
      if (anchor.checkpoint && checkpoint.compactedUpToSeq <= anchor.checkpoint.compactedUpToSeq) return false;

      const res = await this.sessions.updateOne(
        {
          _id: anchorId,
          customerId: anchor.customerId,
          ...(expectedVersion === 0 ? { checkpoint: null } : { "checkpoint.version": expectedVersion }),
        },
        { $set: { checkpoint } },
        { session: s }
      );
      if (res.matchedCount !== 1) return false;
      await this.outbox.insertOne({ ...outboxJob }, { session: s });
      return true;
    });
  }

  async getPendingOutboxJobs(tenantId: string): Promise<CompactionOutboxJob[]> {
    const docs = await this.outbox.find({ tenantId, status: "pending" }).sort({ createdAt: 1 }).toArray();
    return docs.map(strip);
  }

  async getFailedOutboxJobs(tenantId: string): Promise<CompactionOutboxJob[]> {
    const docs = await this.outbox.find({ tenantId, status: "failed" }).sort({ createdAt: 1 }).toArray();
    return docs.map(strip);
  }

  async redriveOutboxJob(tenantId: string, jobId: string): Promise<boolean> {
    const r = await this.outbox.updateOne(
      { tenantId, jobId, status: "failed" },
      { $set: { status: "pending", attempts: 0 } }
    );
    return r.matchedCount === 1;
  }

  async incrementOutboxAttempt(tenantId: string, jobId: string): Promise<number> {
    const d = await this.outbox.findOneAndUpdate({ tenantId, jobId }, { $inc: { attempts: 1 } }, { returnDocument: "after" });
    return d?.attempts ?? 0;
  }

  async markOutboxJobCompleted(tenantId: string, jobId: string): Promise<void> {
    await this.outbox.updateOne({ tenantId, jobId, status: "pending" }, { $set: { status: "completed" } });
  }

  async markOutboxJobFailed(tenantId: string, jobId: string): Promise<void> {
    await this.outbox.updateOne({ tenantId, jobId, status: "pending" }, { $set: { status: "failed" } });
  }

  // ---------------------------------------------------------------- claims

  async getClaimsByCustomer(tenantId: string, customerId: string, filter: ClaimQueryFilter): Promise<MemoryClaim[]> {
    const asOf = filter.asOf ?? Date.now();
    const allowed = (Object.keys(SENSITIVITY_RANK) as SensitivityLevel[]).filter(
      (x) => SENSITIVITY_RANK[x] <= SENSITIVITY_RANK[filter.maxSensitivity]
    );
    const q: Record<string, unknown> = { tenantId, customerId, sensitivity: { $in: allowed } };
    if (filter.statuses) q.status = { $in: filter.statuses };
    if (filter.maxAuthorityTier !== undefined) q.authorityTier = { $lte: filter.maxAuthorityTier };
    if (!filter.includeExpired) {
      q.validFrom = { $lte: asOf };
      q.$and = [
        { $or: [{ validTo: null }, { validTo: { $gt: asOf } }] },
        { $or: [{ expiresAt: null }, { expiresAt: { $gt: asOf } }] },
      ];
    }
    return (await this.claims.find(q as Filter<MemoryClaim>).toArray()).map(strip);
  }

  async ingestClaimAtomic(claim: MemoryClaim): Promise<{ status: ClaimStatus; claimId: string }> {
    const receiptId = `${claim.tenantId}::${claim.claimId}`;

    return this.runTx(async (s) => {
      await this.lockAttr(s, claim.tenantId, claim.customerId, claim.attributeKey);

      const receipt = await this.receipts.findOne({ _id: receiptId }, { session: s });
      if (receipt) return { status: receipt.status, claimId: receipt.resultingClaimId };

      const now = Date.now();
      const open = await this.claims
        .find(
          {
            tenantId: claim.tenantId, customerId: claim.customerId, attributeKey: claim.attributeKey,
            status: { $in: ["active", "disputed"] },
            $and: [
              { $or: [{ validTo: null }, { validTo: { $gt: now } }] },
              { $or: [{ expiresAt: null }, { expiresAt: { $gt: now } }] },
            ],
          },
          { session: s }
        )
        .toArray();

      const writeReceipt = (status: ClaimStatus, resultingClaimId: string) =>
        this.receipts.insertOne(
          { _id: receiptId, tenantId: claim.tenantId, claimId: claim.claimId, status, resultingClaimId, createdAt: now },
          { session: s }
        );

      const norm = (v: string) => v.trim().toLowerCase();
      const exact = open.find((p) => norm(p.value) === norm(claim.value) && p.authorityTier <= claim.authorityTier);
      if (exact) {
        await this.claims.updateOne(
          { tenantId: claim.tenantId, claimId: exact.claimId },
          { $inc: { reinforcementCount: 1 }, $max: { confidence: claim.confidence } },
          { session: s }
        );
        await writeReceipt(exact.status, exact.claimId);
        return { status: exact.status, claimId: exact.claimId };
      }

      const toSupersede: MemoryClaim[] = [];
      const toDispute: MemoryClaim[] = [];
      const strongerIds: string[] = [];
      for (const prev of open) {
        if (claim.authorityTier > prev.authorityTier) strongerIds.push(prev.claimId);
        else if (claim.authorityTier < prev.authorityTier || claim.authorityTier <= 2) toSupersede.push(prev);
        else toDispute.push(prev);
      }

      const newClaim: MemoryClaim = { ...claim, conflictsWith: [] };
      if (strongerIds.length > 0) {
        // lower authority than something already open: audit-only, mutate nothing else
        newClaim.status = "rejected";
        newClaim.conflictsWith = strongerIds;
      } else {
        if (toSupersede.length > 0) {
          await this.claims.updateMany(
            { tenantId: claim.tenantId, claimId: { $in: toSupersede.map((p) => p.claimId) } },
            { $set: { status: "superseded", supersededBy: newClaim.claimId, validTo: now } },
            { session: s }
          );
        }
        if (toDispute.length > 0) {
          newClaim.status = "disputed";
          newClaim.conflictsWith = toDispute.map((d) => d.claimId);
          await this.claims.updateMany(
            { tenantId: claim.tenantId, claimId: { $in: newClaim.conflictsWith } },
            { $set: { status: "disputed" }, $addToSet: { conflictsWith: newClaim.claimId } },
            { session: s }
          );
        }
      }

      await this.claims.insertOne({ ...newClaim }, { session: s });
      await writeReceipt(newClaim.status, newClaim.claimId);
      return { status: newClaim.status, claimId: newClaim.claimId };
    });
  }

  async recordClaimRead(tenantId: string, claimId: string, now: number): Promise<boolean> {
    const r = await this.claims.updateOne({ tenantId, claimId }, { $set: { lastReadAt: now } });
    return r.matchedCount === 1;
  }

  async archiveClaim(tenantId: string, claimId: string, now: number): Promise<boolean> {
    const pre = await this.claims.findOne({ tenantId, claimId });
    if (!pre) return false;
    return this.runTx(async (s) => {
      await this.lockAttr(s, tenantId, pre.customerId, pre.attributeKey);
      const target = await this.claims.findOne({ tenantId, claimId }, { session: s });
      if (!target) return false;

      await this.claims.updateOne({ tenantId, claimId }, { $set: { status: "archived", validTo: now } }, { session: s });

      // A dispute needs an opponent: drop this claim from peers; anyone left alone becomes active.
      const group = { tenantId, customerId: target.customerId, attributeKey: target.attributeKey, status: "disputed" as const };
      await this.claims.updateMany(group, { $pull: { conflictsWith: claimId } }, { session: s });
      await this.claims.updateMany({ ...group, conflictsWith: { $size: 0 } }, { $set: { status: "active" } }, { session: s });
      return true;
    });
  }

  async resolveDisputeAtomic(params: {
    tenantId: string; customerId: string; attributeKey: string; winningClaimId: string;
    losingClaimIds: string[]; resolvedByActor: string; reason: string; now: number;
  }): Promise<boolean> {
    if (!params.resolvedByActor.trim() || !params.reason.trim()) return false;
    if (params.losingClaimIds.length === 0) return false;
    const losers = new Set(params.losingClaimIds);
    if (losers.size !== params.losingClaimIds.length) return false;
    if (losers.has(params.winningClaimId)) return false;

    return this.runTx(async (s) => {
      await this.lockAttr(s, params.tenantId, params.customerId, params.attributeKey);
      const disputed = await this.claims
        .find(
          { tenantId: params.tenantId, customerId: params.customerId, attributeKey: params.attributeKey, status: "disputed" },
          { session: s }
        )
        .toArray();

      if (!disputed.some((c) => c.claimId === params.winningClaimId)) return false;
      const others = disputed.filter((c) => c.claimId !== params.winningClaimId);
      if (others.length !== losers.size || others.some((o) => !losers.has(o.claimId))) return false;

      const loserIds = [...losers];
      await this.claims.updateMany(
        { tenantId: params.tenantId, claimId: { $in: loserIds } },
        { $set: { status: "superseded", supersededBy: params.winningClaimId, validTo: params.now, conflictsWith: [] } },
        { session: s }
      );
      await this.claims.updateOne(
        { tenantId: params.tenantId, claimId: params.winningClaimId },
        { $set: { status: "active", conflictsWith: [] } },
        { session: s }
      );
      // cleanup: nobody (e.g. rejected lower-tier rows) keeps dangling references to resolved losers
      await this.claims.updateMany(
        { tenantId: params.tenantId, customerId: params.customerId, attributeKey: params.attributeKey },
        { $pull: { conflictsWith: { $in: loserIds } } },
        { session: s }
      );
      await this.audits.insertOne(
        {
          auditId: `aud_${randomUUID()}`, tenantId: params.tenantId, customerId: params.customerId,
          attributeKey: params.attributeKey, winningClaimId: params.winningClaimId, losingClaimIds: [...params.losingClaimIds],
          resolvedByActor: params.resolvedByActor.trim(), reason: params.reason.trim(), resolvedAt: params.now,
        },
        { session: s }
      );
      return true;
    });
  }

  async getDisputeAudits(tenantId: string, customerId: string): Promise<DisputeAuditRecord[]> {
    return (await this.audits.find({ tenantId, customerId }).toArray()).map(strip);
  }

  // ---------------------------------------------------------------- workflow

  async getWorkflowState(tenantId: string, sessionId: string): Promise<WorkflowState | null> {
    const a = await this.sessions.findOne({ _id: this.anchorId(tenantId, sessionId) }, { projection: { workflow: 1 } });
    return a?.workflow ?? null;
  }

  async saveWorkflowStateAtomic(state: WorkflowState, expectedVersion: number): Promise<boolean> {
    if (state.version !== expectedVersion + 1) return false;
    const anchorId = this.anchorId(state.tenantId, state.sessionId);

    return this.runTx(async (s) => {
      const anchor = await this.sessions.findOne({ _id: anchorId }, { session: s });
      if (anchor && anchor.customerId !== state.customerId) return false;
      if ((anchor?.workflow?.version ?? 0) !== expectedVersion) return false;

      // Filter pins customer + current version. If no anchor exists we upsert; a concurrent creator
      // makes the upsert collide on _id (E11000) -> runTx retries and sees the anchor.
      const res = await this.sessions.updateOne(
        {
          _id: anchorId,
          customerId: state.customerId,
          ...(expectedVersion === 0 ? { workflow: null } : { "workflow.version": expectedVersion }),
        },
        {
          $set: { workflow: state },
          $setOnInsert: { tenantId: state.tenantId, sessionId: state.sessionId, seqCounter: 0, checkpoint: null },
        },
        { upsert: !anchor, session: s }
      );
      return res.matchedCount === 1 || res.upsertedCount === 1;
    });
  }

  async issueConfirmationChallengeAtomic(
    state: WorkflowState,
    expectedVersion: number
  ): Promise<{ saved: boolean; challengeSeq: number; reason?: string }> {
    if (state.version !== expectedVersion + 1) {
      return { saved: false, challengeSeq: 0, reason: "CONCURRENT_WORKFLOW_UPDATE" };
    }
    const anchorId = this.anchorId(state.tenantId, state.sessionId);
    return this.runTx(async (s) => {
      const anchor = await this.sessions.findOne({ _id: anchorId }, { session: s });
      if (!anchor) return { saved: false, challengeSeq: 0, reason: "NO_ACTIVE_WORKFLOW_STATE" };
      if (anchor.customerId !== state.customerId) return { saved: false, challengeSeq: 0, reason: "SESSION_CUSTOMER_MISMATCH" };
      if ((anchor.workflow?.version ?? 0) !== expectedVersion) {
        return { saved: false, challengeSeq: 0, reason: "CONCURRENT_WORKFLOW_UPDATE" };
      }
      const challengeSeq = anchor.seqCounter;
      const savedState = { ...state, confirmationChallengeSeq: challengeSeq };
      const res = await this.sessions.updateOne(
        { _id: anchorId, customerId: state.customerId, "workflow.version": expectedVersion },
        { $set: { workflow: savedState } },
        { session: s }
      );
      if (res.matchedCount !== 1) {
        return { saved: false, challengeSeq: 0, reason: "CONCURRENT_WORKFLOW_UPDATE" };
      }
      return { saved: true, challengeSeq };
    });
  }

  async consumeTokenAndCreateCommandAtomic(params: {
    tenantId: string; sessionId: string; expectedVersion: number; expectedLatestSeq: number; expectedLatestUserSeq?: number | null;
    actionName: string; expectedTokenHash: string; expectedPayloadHash: string; now: number;
    command: ExecutionCommand;
  }): Promise<{ ok: boolean; reason?: string }> {
    const anchorId = this.anchorId(params.tenantId, params.sessionId);
    return this.runTx(async (s) => {
      const anchor = await this.sessions.findOne({ _id: anchorId }, { session: s });
      const w = anchor?.workflow;
      if (!anchor || !w || w.version !== params.expectedVersion) return { ok: false, reason: "CONCURRENT_WORKFLOW_UPDATE" };
      if (!w.authenticated) return { ok: false, reason: "CUSTOMER_NOT_AUTHENTICATED" };
      if (w.pendingAction !== params.actionName) return { ok: false, reason: "INTENT_MISMATCH" };
      if (!w.confirmationTokenHash || !digestEqual(w.confirmationTokenHash, params.expectedTokenHash)) {
        return { ok: false, reason: "INVALID_CONFIRMATION_TOKEN" };
      }
      if (w.confirmationPayloadHash === null || !digestEqual(w.confirmationPayloadHash, params.expectedPayloadHash)) {
        return { ok: false, reason: "PAYLOAD_BINDING_MISMATCH" };
      }
      if (w.confirmationExpiresAt === null || w.confirmationExpiresAt <= params.now) {
        return { ok: false, reason: "CONFIRMATION_TOKEN_EXPIRED" };
      }
      if (anchor.seqCounter !== params.expectedLatestSeq) return { ok: false, reason: "NEW_EVENT_ARRIVED_DURING_AUTH" };
      const latestUser = await this.turns.findOne(
        { tenantId: params.tenantId, sessionId: params.sessionId, role: "user", seq: { $gt: w.confirmationChallengeSeq ?? 0 } },
        { sort: { seq: -1 }, projection: { seq: 1 }, session: s }
      );
      if ((latestUser?.seq ?? null) !== (params.expectedLatestUserSeq ?? null)) {
        return { ok: false, reason: "NEW_USER_TURN_DURING_AUTH" };
      }

      const res = await this.sessions.updateOne(
        { _id: anchorId, seqCounter: params.expectedLatestSeq, "workflow.version": params.expectedVersion },
        {
          $set: {
            "workflow.version": params.expectedVersion + 1,
            "workflow.pendingAction": null,
            "workflow.confirmationTokenHash": null,
            "workflow.confirmationPayloadHash": null,
            "workflow.confirmationExpiresAt": null,
            "workflow.confirmationChallengeSeq": null,
            "workflow.updatedAt": params.now,
          },
        },
        { session: s }
      );
      if (res.matchedCount !== 1) return { ok: false, reason: "CONCURRENT_WORKFLOW_UPDATE" };
      await this.commands.insertOne({ ...params.command }, { session: s });
      return { ok: true };
    });
  }

  async eraseCustomerDataAtomic(
    tenantId: string,
    customerId: string,
    reason: string,
    now: number
  ): Promise<{
    deleted: boolean;
    sessions: number;
    turns: number;
    claims: number;
    outboxJobs: number;
    receipts: number;
    disputes: number;
    commands: number;
  }> {
    return this.runTx(async (s) => {
      const customerClaims = await this.claims
        .find({ tenantId, customerId }, { projection: { claimId: 1 }, session: s })
        .toArray();
      const claimIds = customerClaims.map((claim) => claim.claimId);

      const [
        sessions,
        turns,
        claims,
        outboxJobs,
        receipts,
        disputes,
        commands,
      ] = await Promise.all([
        this.sessions.deleteMany({ tenantId, customerId }, { session: s }),
        this.turns.deleteMany({ tenantId, customerId }, { session: s }),
        this.claims.deleteMany({ tenantId, customerId }, { session: s }),
        this.outbox.deleteMany({ tenantId, customerId }, { session: s }),
        claimIds.length > 0
          ? this.receipts.deleteMany({ tenantId, claimId: { $in: claimIds } }, { session: s })
          : Promise.resolve({ deletedCount: 0 }),
        this.audits.deleteMany({ tenantId, customerId }, { session: s }),
        this.commands.deleteMany({ tenantId, customerId }, { session: s }),
      ]);

      await this.guards.deleteMany(
        { _id: { $regex: `^${tenantId.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}::${customerId.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}::` } },
        { session: s }
      );
      void reason;
      void now;

      return {
        deleted: true,
        sessions: sessions.deletedCount,
        turns: turns.deletedCount,
        claims: claims.deletedCount,
        outboxJobs: outboxJobs.deletedCount,
        receipts: receipts.deletedCount,
        disputes: disputes.deletedCount,
        commands: commands.deletedCount,
      };
    });
  }
}
