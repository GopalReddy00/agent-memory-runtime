/**
 * Agent Memory & Reliability Runtime (v0.1.0 - Developer Preview)
 * Event-backed conversational memory, provenance-aware claims, budgeted context
 * reconstruction, and workflow safety primitives.
 *
 * Only dependency: node:crypto (built in).
 */
import { createHash, randomUUID, timingSafeEqual } from "node:crypto";

// ============================================================================
// 1. TYPES, ERRORS & INTERFACES
// ============================================================================

export type Generation = 0 | 1 | 2;
/**
 * active      - the current trusted value
 * disputed    - one of >= 2 same-tier untrusted claims that disagree
 * superseded  - replaced by a stronger/newer claim
 * rejected    - arrived with LOWER authority than an open claim; kept for audit only,
 *               never shown in context and never mutates the stronger claim
 * archived    - decayed / expired
 */
export type ClaimStatus = "active" | "disputed" | "superseded" | "archived" | "rejected";
export type VolatilityClass = "CLASS_A_PII" | "CLASS_B_SEMI_DURABLE" | "CLASS_C_PREFERENCE";
export type SensitivityLevel = "public" | "internal" | "confidential" | "restricted";

/**
 * Authority Tiers (Lower = Higher Authority)
 * 0: Core System of Record (CRM / IAM)
 * 1: Verified Workflow Event (OTP / KYC)
 * 2: Authenticated Customer Statement
 * 3: Automated / LLM Extraction (Untrusted)
 * 4: Unauthenticated / Third-Party
 */
export type AuthorityTier = 0 | 1 | 2 | 3 | 4;

const SENSITIVITY_RANK: Record<SensitivityLevel, number> = {
  public: 0,
  internal: 1,
  confidential: 2,
  restricted: 3,
};

export class IdempotencyConflictError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "IdempotencyConflictError";
  }
}

export class AccessDeniedError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "AccessDeniedError";
  }
}

export interface PrincipalContext {
  tenantId: string;
  agentId: string;
  maxAllowedSensitivity: SensitivityLevel;
}

export interface TurnInput {
  tenantId: string;
  sessionId: string;
  customerId: string;
  idempotencyKey: string;
  role: "system" | "user" | "assistant" | "tool";
  content: string;
  tokens: number;
  pinned: boolean;
}

export interface TurnEvent extends TurnInput {
  seq: number;
  timestamp: number;
}

export interface Checkpoint {
  tenantId: string;
  sessionId: string;
  version: number;
  compactedUpToSeq: number;
  summary: string;
  updatedAt: number;
}

export interface PendingClaimDraft {
  attributeKey: string;
  value: string;
  confidence: number;
  importance: number;
  volatility: VolatilityClass;
  sensitivity: SensitivityLevel;
}

export interface CompactionOutboxJob {
  jobId: string;
  tenantId: string;
  sessionId: string;
  customerId: string;
  watermarkSeq: number;
  claims: PendingClaimDraft[];
  status: "pending" | "completed" | "failed";
  attempts: number;
  createdAt: number;
}

export interface MemoryClaim {
  readonly claimId: string;
  readonly tenantId: string;
  readonly customerId: string;
  readonly attributeKey: string;
  readonly value: string;
  readonly authorityTier: AuthorityTier;
  readonly volatility: VolatilityClass;
  readonly sensitivity: SensitivityLevel;
  readonly sourceType: string;
  readonly sourceEventId: string;
  readonly recordedAt: number;
  readonly validFrom: number;
  generation: Generation;
  status: ClaimStatus;
  confidence: number;
  importance: number;
  reinforcementCount: number;
  validTo: number | null;
  lastReadAt: number;
  expiresAt: number | null;
  conflictsWith: string[];
  supersededBy: string | null;
}

export interface DisputeAuditRecord {
  auditId: string;
  tenantId: string;
  customerId: string;
  attributeKey: string;
  winningClaimId: string;
  losingClaimIds: string[];
  resolvedByActor: string;
  reason: string;
  resolvedAt: number;
}

export interface WorkflowState {
  tenantId: string;
  sessionId: string;
  customerId: string;
  version: number;
  authenticated: boolean;
  authMethod: string | null;
  currentIntent: string;
  pendingAction: string | null;
  /** SHA-256 hex of the minted token. The plaintext token is never stored. */
  confirmationTokenHash: string | null;
  confirmationPayloadHash: string | null;
  confirmationExpiresAt: number | null;
  /** Latest session seq at issuance. A USER turn after this seq is the customer's reply. */
  confirmationChallengeSeq: number | null;
  updatedAt: number;
}

export interface ExecutionCommand {
  commandId: string;
  tenantId: string;
  sessionId: string;
  customerId: string;
  actionName: string;
  payloadHash: string;
  createdAt: number;
  status: "authorized" | "executed" | "aborted";
}

export interface CompactionOutput {
  updatedSummary: string;
  extractedClaims: PendingClaimDraft[];
}

export interface ClaimQueryFilter {
  maxSensitivity: SensitivityLevel;
  statuses?: ClaimStatus[];
  maxAuthorityTier?: AuthorityTier;
  asOf?: number;
  includeExpired?: boolean;
}

export interface CompactionLLM {
  compactAndExtract(input: {
    previousSummary: string;
    turns: TurnEvent[];
    allowedAttributes: string[];
  }): Promise<unknown>;
}

/**
 * Every *Atomic method MUST execute as a single transaction / conditional write.
 * The in-memory adapter is atomic only because each method body has no `await`.
 */
export interface RuntimeStorageAdapter {
  appendTurnAtomic(input: TurnInput): Promise<{ inserted: boolean; event: TurnEvent }>;
  getTurnsAfterSeq(tenantId: string, sessionId: string, afterSeq: number): Promise<TurnEvent[]>;
  /** Sum of tokens of UNPINNED turns after afterSeq (pinned turns are never compacted). */
  getUncompactedTokenSum(tenantId: string, sessionId: string, afterSeq: number): Promise<number>;
  getPinnedTurns(tenantId: string, sessionId: string): Promise<TurnEvent[]>;
  getLatestSeq(tenantId: string, sessionId: string): Promise<number>;
  getFirstUserTurnSeqAfter(
    tenantId: string,
    sessionId: string,
    afterSeq: number
  ): Promise<number | null>;
  getLatestUserTurnSeqAfter(
    tenantId: string,
    sessionId: string,
    afterSeq: number
  ): Promise<number | null>;
  getSessionCustomer(tenantId: string, sessionId: string): Promise<string | null>;

  getCheckpoint(tenantId: string, sessionId: string): Promise<Checkpoint | null>;
  commitCheckpointWithOutboxAtomic(
    checkpoint: Checkpoint,
    outboxJob: CompactionOutboxJob,
    expectedVersion: number
  ): Promise<boolean>;
  getPendingOutboxJobs(tenantId: string): Promise<CompactionOutboxJob[]>;
  getFailedOutboxJobs(tenantId: string): Promise<CompactionOutboxJob[]>;
  redriveOutboxJob(tenantId: string, jobId: string): Promise<boolean>;
  /** Returns the new attempt count (0 if the job does not exist). */
  incrementOutboxAttempt(tenantId: string, jobId: string): Promise<number>;
  markOutboxJobCompleted(tenantId: string, jobId: string): Promise<void>;
  markOutboxJobFailed(tenantId: string, jobId: string): Promise<void>;

  getClaimsByCustomer(
    tenantId: string,
    customerId: string,
    filter: ClaimQueryFilter
  ): Promise<MemoryClaim[]>;
  ingestClaimAtomic(claim: MemoryClaim): Promise<{ status: ClaimStatus; claimId: string }>;
  recordClaimRead(tenantId: string, claimId: string, now: number): Promise<boolean>;
  archiveClaim(tenantId: string, claimId: string, now: number): Promise<boolean>;
  resolveDisputeAtomic(params: {
    tenantId: string;
    customerId: string;
    attributeKey: string;
    winningClaimId: string;
    losingClaimIds: string[];
    resolvedByActor: string;
    reason: string;
    now: number;
  }): Promise<boolean>;
  getDisputeAudits(tenantId: string, customerId: string): Promise<DisputeAuditRecord[]>;

  getWorkflowState(tenantId: string, sessionId: string): Promise<WorkflowState | null>;
  saveWorkflowStateAtomic(state: WorkflowState, expectedVersion: number): Promise<boolean>;
  issueConfirmationChallengeAtomic(
    state: WorkflowState,
    expectedVersion: number
  ): Promise<{ saved: boolean; challengeSeq: number; reason?: string }>;
  consumeTokenAndCreateCommandAtomic(params: {
    tenantId: string;
    sessionId: string;
    expectedVersion: number;
    expectedLatestSeq: number;
    expectedLatestUserSeq?: number | null;
    actionName: string;
    expectedTokenHash: string;
    expectedPayloadHash: string;
    now: number;
    command: ExecutionCommand;
  }): Promise<{ ok: boolean; reason?: string }>;
  eraseCustomerDataAtomic(
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
  }>;
}

export interface RuntimeConfig {
  maxGen0Tokens?: number;
  maxContextTokens?: number;
  minSummaryTokens?: number;
  keepRecentTurns?: number;
  maxExtractedClaimsPerSweep?: number;
  allowedAttributes?: string[];
  attributeValidators?: Record<string, (value: string) => boolean>;
  blockedPiiAttributes?: string[];
  gen2DecayHalfLifeDays?: number;
  gen2ArchiveThreshold?: number;
  rejectedRetentionDays?: number;
  maxOutboxAttempts?: number;
  readTouchThrottleMs?: number;
  tokenCounter?: (text: string) => number;
  onError?: (context: string, err: unknown) => void;
}

export interface BuiltContext {
  pinnedRoots: TurnEvent[];
  gen1Summary: string;
  gen0Tail: TurnEvent[];
  gen2ActiveFacts: Array<{
    key: string;
    value: string;
    source: string;
    tier: AuthorityTier;
    sensitivity: SensitivityLevel;
  }>;
  disputedClaims: Array<{
    key: string;
    value: string;
    source: string;
    tier: AuthorityTier;
    untrustedDataWarning: true;
  }>;
  estimatedTokens: number;
  truncated: boolean;
}

// ============================================================================
// 2. VALIDATION & SUPPLEMENTARY GUARDS
// ============================================================================

const DAY_MS = 24 * 60 * 60 * 1000;

const DEFAULT_ATTRIBUTE_VALIDATORS: Record<string, (val: string) => boolean> = {
  preferred_language: (v) =>
    ["english", "telugu", "hindi", "tamil", "kannada", "marathi", "bengali"].includes(
      v.toLowerCase()
    ),
  communication_channel: (v) => ["voice", "whatsapp", "sms", "email"].includes(v.toLowerCase()),
  // Conservative character sets (letters, digits, light punctuation) shrink the free-text surface.
  callback_window: (v) => /^[\p{L}\p{N} :\-–.,/]{1,60}$/u.test(v),
  policy_interest: (v) => /^[\p{L}\p{N} _\-.,&/()]{1,100}$/u.test(v),
};

// Supplementary detector only; allowlists, validators and IAM are the real boundaries.
const SUPPLEMENTARY_INJECTION_PATTERNS = [
  /ignore (all )?(previous|prior) (instructions|rules|restrictions)/i,
  /from now on[, ]+(remember|treat|assume) (that )?i am (an? )?(admin|administrator|superuser)/i,
  /bypass (security|verification|otp|auth)/i,
  /system prompt/i,
];

function sha256Hex(s: string): string {
  return createHash("sha256").update(s).digest("hex");
}

/** Constant-time comparison of arbitrary-length strings (compares fixed-size digests). */
function safeEqual(a: string, b: string): boolean {
  const da = createHash("sha256").update(a).digest();
  const db = createHash("sha256").update(b).digest();
  return timingSafeEqual(da, db);
}

export function validateCompactionOutput(
  raw: unknown,
  allowedAttributes: string[],
  maxClaims = 10,
  validators: Record<string, (val: string) => boolean> = DEFAULT_ATTRIBUTE_VALIDATORS
): CompactionOutput {
  if (!raw || typeof raw !== "object") {
    throw new Error("Invalid CompactionOutput: expected object");
  }
  const obj = raw as Record<string, unknown>;
  if (typeof obj.updatedSummary !== "string") {
    throw new Error("Invalid CompactionOutput: updatedSummary must be a string");
  }
  if (obj.updatedSummary.length > 8000) {
    throw new Error("Invalid CompactionOutput: updatedSummary exceeds max length");
  }
  if (!Array.isArray(obj.extractedClaims)) {
    throw new Error("Invalid CompactionOutput: extractedClaims must be an array");
  }

  const allowedSet = new Set(allowedAttributes);
  const validVolatilities = new Set<VolatilityClass>([
    "CLASS_A_PII",
    "CLASS_B_SEMI_DURABLE",
    "CLASS_C_PREFERENCE",
  ]);
  const validSensitivities = new Set<SensitivityLevel>([
    "public",
    "internal",
    "confidential",
    "restricted",
  ]);

  const extractedClaims: PendingClaimDraft[] = [];

  for (const item of obj.extractedClaims.slice(0, maxClaims)) {
    if (!item || typeof item !== "object") continue;
    const c = item as Record<string, unknown>;

    if (typeof c.attributeKey !== "string" || typeof c.value !== "string") continue;
    const key = c.attributeKey.trim();
    const val = c.value.trim();

    if (!key || !val || key.length > 100 || val.length > 500) continue;
    if (!allowedSet.has(key)) continue;

    const validator = validators[key];
    if (validator && !validator(val)) continue;

    if (
      typeof c.confidence !== "number" ||
      !Number.isFinite(c.confidence) ||
      c.confidence < 0 ||
      c.confidence > 1
    ) {
      continue;
    }
    if (
      typeof c.importance !== "number" ||
      !Number.isFinite(c.importance) ||
      c.importance < 0 ||
      c.importance > 1
    ) {
      continue;
    }
    if (!validVolatilities.has(c.volatility as VolatilityClass)) continue;

    const sensitivity = validSensitivities.has(c.sensitivity as SensitivityLevel)
      ? (c.sensitivity as SensitivityLevel)
      : "internal";

    extractedClaims.push({
      attributeKey: key,
      value: val,
      confidence: c.confidence,
      importance: c.importance,
      volatility: c.volatility as VolatilityClass,
      sensitivity,
    });
  }

  return {
    updatedSummary: obj.updatedSummary.trim().slice(0, 4000),
    extractedClaims,
  };
}

// ============================================================================
// 3. CORE RUNTIME ENGINE
// ============================================================================

export class AgentMemoryRuntime {
  private storage: RuntimeStorageAdapter;
  private llm: CompactionLLM;
  private config: Required<RuntimeConfig>;
  // Single-node optimization only; multi-instance safety is enforced by storage CAS.
  private inFlightCompactions = new Set<string>();

  constructor(storage: RuntimeStorageAdapter, llm: CompactionLLM, config: RuntimeConfig = {}) {
    this.storage = storage;
    this.llm = llm;
    this.config = {
      maxGen0Tokens: config.maxGen0Tokens ?? 4000,
      maxContextTokens: config.maxContextTokens ?? 6000,
      minSummaryTokens: config.minSummaryTokens ?? 400,
      keepRecentTurns: config.keepRecentTurns ?? 4,
      maxExtractedClaimsPerSweep: config.maxExtractedClaimsPerSweep ?? 10,
      allowedAttributes: config.allowedAttributes ?? [
        "preferred_language",
        "callback_window",
        "policy_interest",
        "communication_channel",
      ],
      attributeValidators: {
        ...DEFAULT_ATTRIBUTE_VALIDATORS,
        ...(config.attributeValidators ?? {}),
      },
      blockedPiiAttributes: config.blockedPiiAttributes ?? [
        "registered_mobile",
        "bank_account",
        "otp",
        "password",
        "aadhaar",
        "pan",
        "ssn",
      ],
      gen2DecayHalfLifeDays: config.gen2DecayHalfLifeDays ?? 30,
      gen2ArchiveThreshold: config.gen2ArchiveThreshold ?? 0.15,
      rejectedRetentionDays: config.rejectedRetentionDays ?? 30,
      maxOutboxAttempts: config.maxOutboxAttempts ?? 5,
      readTouchThrottleMs: config.readTouchThrottleMs ?? 60 * 60 * 1000,
      tokenCounter: config.tokenCounter ?? ((text) => Math.ceil(text.length / 4)),
      onError: config.onError ?? ((ctx, err) => console.error(`[AgentMemoryRuntime:${ctx}]`, err)),
    };
  }

  // --------------------------------------------------------------------------
  // A. HOT PATH: Atomic, Customer-Bound, Idempotent Event Ingestion
  // --------------------------------------------------------------------------

  async appendTurn(params: {
    tenantId: string;
    sessionId: string;
    customerId: string;
    idempotencyKey: string;
    role: TurnEvent["role"];
    content: string;
    pinned?: boolean;
    triggerAsyncGC?: boolean;
  }): Promise<{ inserted: boolean; event: TurnEvent }> {
    const result = await this.storage.appendTurnAtomic({
      tenantId: params.tenantId,
      sessionId: params.sessionId,
      customerId: params.customerId,
      idempotencyKey: params.idempotencyKey,
      role: params.role,
      content: params.content,
      tokens: this.config.tokenCounter(params.content),
      pinned: params.pinned ?? false,
    });

    if (!result.inserted) return result;

    if (params.triggerAsyncGC !== false) {
      const checkpoint = await this.storage.getCheckpoint(params.tenantId, params.sessionId);
      const watermark = checkpoint?.compactedUpToSeq ?? 0;
      const totalTokens = await this.storage.getUncompactedTokenSum(
        params.tenantId,
        params.sessionId,
        watermark
      );

      if (totalTokens > this.config.maxGen0Tokens) {
        this.runSessionCompaction(params.tenantId, params.sessionId, params.customerId).catch(
          (err) => this.config.onError("asyncGC", err)
        );
      }
    }

    return result;
  }

  // --------------------------------------------------------------------------
  // B. HOT PATH: Policy-Filtered, Budgeted Context Reconstruction
  // --------------------------------------------------------------------------

  async buildContext(params: {
    principal: PrincipalContext;
    sessionId: string;
    customerId: string;
    requestedSensitivity?: SensitivityLevel;
    includeGen2?: boolean;
    asOf?: number;
  }): Promise<BuiltContext> {
    const { principal } = params;
    const requested = params.requestedSensitivity ?? "internal";

    if (SENSITIVITY_RANK[requested] > SENSITIVITY_RANK[principal.maxAllowedSensitivity]) {
      throw new AccessDeniedError(
        `Agent ${principal.agentId} is not permitted to access ${requested} memory`
      );
    }

    const bound = await this.storage.getSessionCustomer(principal.tenantId, params.sessionId);
    if (bound === null) {
      throw new AccessDeniedError(`Session ${params.sessionId} is not bound to a customer`);
    }
    if (bound !== params.customerId) {
      throw new AccessDeniedError(`Session ${params.sessionId} is bound to another customer`);
    }

    const asOf = params.asOf ?? Date.now();
    const countTokens = this.config.tokenCounter;

    const [pinnedRoots, checkpoint, allClaims] = await Promise.all([
      this.storage.getPinnedTurns(principal.tenantId, params.sessionId),
      this.storage.getCheckpoint(principal.tenantId, params.sessionId),
      params.includeGen2 !== false
        ? this.storage.getClaimsByCustomer(principal.tenantId, params.customerId, {
            maxSensitivity: requested,
            statuses: ["active", "disputed"],
            asOf,
            includeExpired: false,
          })
        : Promise.resolve([]),
    ]);

    const watermark = checkpoint?.compactedUpToSeq ?? 0;
    const rawTail = await this.storage.getTurnsAfterSeq(
      principal.tenantId,
      params.sessionId,
      watermark
    );
    const unpinnedTail = rawTail.filter((t) => !t.pinned);

    const safeClaims = allClaims.filter(
      (c) =>
        !this.config.blockedPiiAttributes.includes(c.attributeKey) &&
        c.volatility !== "CLASS_A_PII" &&
        SENSITIVITY_RANK[c.sensitivity] <= SENSITIVITY_RANK[requested] &&
        c.validFrom <= asOf &&
        (c.validTo === null || c.validTo > asOf) &&
        (c.expiresAt === null || c.expiresAt > asOf)
    );

    const activeClaims = safeClaims
      .filter((c) => c.status === "active")
      .sort((a, b) => a.authorityTier - b.authorityTier || b.importance - a.importance);

    const disputedClaimsRaw = safeClaims
      .filter((c) => c.status === "disputed")
      .sort((a, b) => a.authorityTier - b.authorityTier || b.importance - a.importance);

    let truncated = false;
    const maxBudget = this.config.maxContextTokens;
    const pinnedTokens = pinnedRoots.reduce((s, t) => s + t.tokens, 0);

    if (pinnedTokens >= maxBudget) {
      return {
        pinnedRoots,
        gen1Summary: "",
        gen0Tail: [],
        gen2ActiveFacts: [],
        disputedClaims: [],
        estimatedTokens: pinnedTokens,
        truncated: true,
      };
    }

    let remaining = maxBudget - pinnedTokens;

    const rawSummary = checkpoint?.summary ?? "";
    const rawSummaryTokens = rawSummary ? countTokens(rawSummary) : 0;
    const reservedSummaryBudget = rawSummary
      ? Math.min(this.config.minSummaryTokens, rawSummaryTokens, remaining)
      : 0;
    remaining -= reservedSummaryBudget;

    // 1. Fit most recent Gen 0 turns
    const fittedGen0: TurnEvent[] = [];
    for (let i = unpinnedTail.length - 1; i >= 0; i--) {
      if (remaining >= unpinnedTail[i].tokens) {
        fittedGen0.unshift(unpinnedTail[i]);
        remaining -= unpinnedTail[i].tokens;
      } else {
        truncated = true;
        break;
      }
    }

    // 2. Fit Gen 2 active facts
    const gen2ActiveFacts: BuiltContext["gen2ActiveFacts"] = [];
    let gen2Tokens = 0;
    for (const c of activeClaims) {
      const factObj = {
        key: c.attributeKey,
        value: c.value,
        source: c.sourceType,
        tier: c.authorityTier,
        sensitivity: c.sensitivity,
      };
      const cost = countTokens(JSON.stringify(factObj));
      if (remaining >= cost) {
        gen2ActiveFacts.push(factObj);
        remaining -= cost;
        gen2Tokens += cost;

        if (asOf - c.lastReadAt >= this.config.readTouchThrottleMs) {
          this.storage.recordClaimRead(principal.tenantId, c.claimId, asOf).catch((err) => {
            this.config.onError("recordClaimRead", err);
          });
        }
      } else {
        truncated = true;
      }
    }

    // 3. Fit disputed claims (explicitly labeled as untrusted data)
    const disputedClaims: BuiltContext["disputedClaims"] = [];
    let disputedTokens = 0;
    for (const c of disputedClaimsRaw) {
      const dObj = {
        key: c.attributeKey,
        value: c.value,
        source: c.sourceType,
        tier: c.authorityTier,
        untrustedDataWarning: true as const,
      };
      const cost = countTokens(JSON.stringify(dObj));
      if (remaining >= cost) {
        disputedClaims.push(dObj);
        remaining -= cost;
        disputedTokens += cost;
      } else {
        truncated = true;
      }
    }

    // 4. Fit Gen 1 summary with iterative trimming
    const summaryBudget = reservedSummaryBudget + remaining;
    let gen1Summary = rawSummary;
    let finalSummaryTokens = rawSummaryTokens;

    if (finalSummaryTokens > summaryBudget) {
      truncated = true;
      const endIdx = Math.floor(
        rawSummary.length * (summaryBudget / Math.max(rawSummaryTokens, 1))
      );
      gen1Summary = rawSummary.slice(0, Math.max(0, endIdx));
      finalSummaryTokens = gen1Summary ? countTokens(gen1Summary) : 0;

      while (finalSummaryTokens > summaryBudget && gen1Summary.length > 0) {
        gen1Summary = gen1Summary.slice(0, Math.max(0, gen1Summary.length - 16));
        finalSummaryTokens = gen1Summary ? countTokens(gen1Summary) : 0;
      }
    }

    const estimatedTokens =
      pinnedTokens +
      fittedGen0.reduce((s, t) => s + t.tokens, 0) +
      gen2Tokens +
      disputedTokens +
      finalSummaryTokens;

    return {
      pinnedRoots,
      gen1Summary,
      gen0Tail: fittedGen0,
      gen2ActiveFacts,
      disputedClaims,
      estimatedTokens,
      truncated,
    };
  }

  // --------------------------------------------------------------------------
  // C. BACKGROUND GC: Atomic Checkpoint + Outbox + Fault-Tolerant Recovery
  // --------------------------------------------------------------------------

  async runSessionCompaction(
    tenantId: string,
    sessionId: string,
    customerId: string,
    options?: { simulateCrashBeforeOutboxFlush?: boolean }
  ): Promise<{ compacted: boolean; newWatermark: number; outboxJob?: CompactionOutboxJob }> {
    const lockKey = `${tenantId}:${sessionId}`;
    if (this.inFlightCompactions.has(lockKey)) {
      return { compacted: false, newWatermark: 0 };
    }

    this.inFlightCompactions.add(lockKey);
    try {
      const checkpoint = await this.storage.getCheckpoint(tenantId, sessionId);
      const currentWatermark = checkpoint?.compactedUpToSeq ?? 0;
      const currentVersion = checkpoint?.version ?? 0;

      const uncompacted = await this.storage.getTurnsAfterSeq(
        tenantId,
        sessionId,
        currentWatermark
      );
      const compactable = uncompacted.filter((t) => !t.pinned);

      if (compactable.length <= this.config.keepRecentTurns) {
        return { compacted: false, newWatermark: currentWatermark };
      }

      const sliceToCompact = compactable.slice(
        0,
        compactable.length - this.config.keepRecentTurns
      );
      const targetWatermark = sliceToCompact[sliceToCompact.length - 1].seq;

      const rawOutput = await this.llm.compactAndExtract({
        previousSummary: checkpoint?.summary ?? "",
        turns: sliceToCompact,
        allowedAttributes: this.config.allowedAttributes,
      });

      const validated = validateCompactionOutput(
        rawOutput,
        this.config.allowedAttributes,
        this.config.maxExtractedClaimsPerSweep,
        this.config.attributeValidators
      );

      const now = Date.now();
      const outboxJob: CompactionOutboxJob = {
        jobId: `job_${randomUUID()}`,
        tenantId,
        sessionId,
        customerId,
        watermarkSeq: targetWatermark,
        claims: validated.extractedClaims,
        status: "pending",
        attempts: 0,
        createdAt: now,
      };

      const committed = await this.storage.commitCheckpointWithOutboxAtomic(
        {
          tenantId,
          sessionId,
          version: currentVersion + 1,
          compactedUpToSeq: targetWatermark,
          summary: validated.updatedSummary,
          updatedAt: now,
        },
        outboxJob,
        currentVersion
      );

      if (!committed) {
        return { compacted: false, newWatermark: currentWatermark };
      }

      if (options?.simulateCrashBeforeOutboxFlush) {
        throw new Error("Simulated process crash after checkpoint commit");
      }

      await this.processOutboxJob(outboxJob);
      return { compacted: true, newWatermark: targetWatermark, outboxJob };
    } finally {
      this.inFlightCompactions.delete(lockKey);
    }
  }

  /**
   * Idempotently processes one outbox job. Claim IDs are deterministic (jobId::index) and the
   * storage layer keeps an ingest receipt per claimId, so replays never double-count.
   * After maxOutboxAttempts the job is marked failed and removed from the pending queue.
   */
  async processOutboxJob(
    job: CompactionOutboxJob,
    options?: { simulateCrashBeforeMarkCompleted?: boolean }
  ): Promise<void> {
    const attempts = await this.storage.incrementOutboxAttempt(job.tenantId, job.jobId);
    if (attempts > this.config.maxOutboxAttempts) {
      await this.storage.markOutboxJobFailed(job.tenantId, job.jobId);
      throw new Error(`OUTBOX_MAX_ATTEMPTS_EXCEEDED:${job.jobId}`);
    }

    for (let i = 0; i < job.claims.length; i++) {
      const draft = job.claims[i];
      await this.internalIngestClaim({
        claimId: `${job.jobId}::${i}`,
        tenantId: job.tenantId,
        customerId: job.customerId,
        attributeKey: draft.attributeKey,
        value: draft.value,
        authorityTier: 3,
        volatility: draft.volatility,
        sensitivity: draft.sensitivity,
        confidence: draft.confidence,
        importance: draft.importance,
        sourceType: "llm_extraction",
        sourceEventId: `${job.sessionId}:seq_${job.watermarkSeq}`,
      });
    }

    if (options?.simulateCrashBeforeMarkCompleted) {
      throw new Error("Simulated crash before marking outbox job completed");
    }

    await this.storage.markOutboxJobCompleted(job.tenantId, job.jobId);
  }

  async recoverPendingOutbox(tenantId: string): Promise<{ recovered: number; failed: number }> {
    const pending = await this.storage.getPendingOutboxJobs(tenantId);
    let recovered = 0;
    let failed = 0;

    for (const job of pending) {
      try {
        await this.processOutboxJob(job);
        recovered++;
      } catch (err) {
        failed++;
        this.config.onError(`recoverPendingOutbox:${job.jobId}`, err);
      }
    }
    return { recovered, failed };
  }

  async redriveFailedOutboxJob(
    tenantId: string,
    jobId: string
  ): Promise<{ redriven: boolean; result?: string }> {
    const redriven = await this.storage.redriveOutboxJob(tenantId, jobId);
    if (!redriven) return { redriven: false };

    const failed = await this.storage.getFailedOutboxJobs(tenantId);
    const job = failed.find((candidate) => candidate.jobId === jobId);
    if (job) return { redriven: false };
    return { redriven: true };
  }

  async eraseCustomerData(
    tenantId: string,
    customerId: string,
    reason: string
  ): Promise<Awaited<ReturnType<RuntimeStorageAdapter["eraseCustomerDataAtomic"]>>> {
    if (!reason.trim()) throw new Error("Erasure reason is required");
    return this.storage.eraseCustomerDataAtomic(tenantId, customerId, reason.trim(), Date.now());
  }

  // --------------------------------------------------------------------------
  // D. TRUST-SEPARATED CLAIM INGESTION & DISPUTE RESOLUTION
  // --------------------------------------------------------------------------

  async ingestSystemOfRecordClaim(input: {
    tenantId: string;
    customerId: string;
    attributeKey: string;
    value: string;
    volatility: VolatilityClass;
    sensitivity?: SensitivityLevel;
    sourceEventId: string;
    validFrom?: number;
    validTo?: number | null;
  }) {
    return this.internalIngestClaim({
      ...input,
      authorityTier: 0,
      confidence: 1.0,
      importance: 1.0,
      sourceType: "system_of_record",
    });
  }

  async ingestVerifiedCustomerClaim(input: {
    tenantId: string;
    customerId: string;
    attributeKey: string;
    value: string;
    volatility: VolatilityClass;
    sensitivity?: SensitivityLevel;
    sourceEventId: string;
    validFrom?: number;
    validTo?: number | null;
  }) {
    return this.internalIngestClaim({
      ...input,
      authorityTier: 1,
      confidence: 1.0,
      importance: 0.9,
      sourceType: "verified_workflow",
    });
  }

  async ingestAuthenticatedCustomerClaim(input: {
    tenantId: string;
    customerId: string;
    attributeKey: string;
    value: string;
    volatility: VolatilityClass;
    sensitivity?: SensitivityLevel;
    sourceEventId: string;
    validFrom?: number;
    validTo?: number | null;
  }) {
    return this.internalIngestClaim({
      ...input,
      authorityTier: 2,
      confidence: 0.95,
      importance: 0.85,
      sourceType: "authenticated_customer",
    });
  }

  async ingestExtractedClaim(input: {
    tenantId: string;
    customerId: string;
    attributeKey: string;
    value: string;
    volatility: VolatilityClass;
    sensitivity?: SensitivityLevel;
    confidence: number;
    importance: number;
    sourceEventId: string;
    validFrom?: number;
    validTo?: number | null;
    ttlMs?: number;
  }) {
    return this.internalIngestClaim({
      ...input,
      authorityTier: 3,
      sourceType: "llm_extraction",
    });
  }

  private async internalIngestClaim(input: {
    claimId?: string;
    tenantId: string;
    customerId: string;
    attributeKey: string;
    value: string;
    authorityTier: AuthorityTier;
    volatility: VolatilityClass;
    sensitivity?: SensitivityLevel;
    confidence: number;
    importance: number;
    sourceType: string;
    sourceEventId: string;
    validFrom?: number;
    validTo?: number | null;
    ttlMs?: number;
  }): Promise<{ accepted: boolean; status?: ClaimStatus; claimId?: string; reason?: string }> {
    if (
      !Number.isFinite(input.confidence) ||
      input.confidence < 0 ||
      input.confidence > 1 ||
      !Number.isFinite(input.importance) ||
      input.importance < 0 ||
      input.importance > 1
    ) {
      return { accepted: false, reason: "INVALID_SCORE_RANGE" };
    }

    if (
      input.volatility === "CLASS_A_PII" ||
      this.config.blockedPiiAttributes.includes(input.attributeKey)
    ) {
      return { accepted: false, reason: "BLOCKED_CLASS_A_PII" };
    }

    if (!this.config.allowedAttributes.includes(input.attributeKey)) {
      return { accepted: false, reason: "ATTRIBUTE_NOT_IN_ALLOWLIST" };
    }

    const trimmedValue = input.value.trim();
    const validator = this.config.attributeValidators[input.attributeKey];
    if (validator && !validator(trimmedValue)) {
      return { accepted: false, reason: "ATTRIBUTE_VALUE_VALIDATION_FAILED" };
    }

    for (const pattern of SUPPLEMENTARY_INJECTION_PATTERNS) {
      if (pattern.test(trimmedValue)) {
        return { accepted: false, reason: "PROMPT_INJECTION_DETECTED" };
      }
    }

    const now = Date.now();
    const claim: MemoryClaim = {
      claimId: input.claimId ?? `clm_${randomUUID()}`,
      tenantId: input.tenantId,
      customerId: input.customerId,
      attributeKey: input.attributeKey,
      value: trimmedValue,
      generation: input.authorityTier <= 1 ? 2 : 1,
      authorityTier: input.authorityTier,
      volatility: input.volatility,
      sensitivity: input.sensitivity ?? "internal",
      status: "active",
      confidence: input.confidence,
      importance: input.importance,
      reinforcementCount: 1,
      recordedAt: now,
      validFrom: input.validFrom ?? now,
      validTo: input.validTo ?? null,
      lastReadAt: now,
      expiresAt: input.ttlMs ? now + input.ttlMs : null,
      sourceType: input.sourceType,
      sourceEventId: input.sourceEventId,
      conflictsWith: [],
      supersededBy: null,
    };

    const result = await this.storage.ingestClaimAtomic(claim);
    return { accepted: true, status: result.status, claimId: result.claimId };
  }

  async resolveDispute(params: {
    tenantId: string;
    customerId: string;
    attributeKey: string;
    winningClaimId: string;
    losingClaimIds: string[];
    resolvedByActor: string;
    reason: string;
  }): Promise<boolean> {
    return this.storage.resolveDisputeAtomic({
      ...params,
      now: Date.now(),
    });
  }

  // --------------------------------------------------------------------------
  // E. SCHEDULED SWEEP: Multiplicative Decay & Expiration Archival
  // --------------------------------------------------------------------------

  async sweepCustomerClaims(
    tenantId: string,
    customerId: string
  ): Promise<{ scanned: number; archived: number }> {
    const claims = await this.storage.getClaimsByCustomer(tenantId, customerId, {
      maxSensitivity: "restricted",
      statuses: ["active", "disputed", "rejected"],
      includeExpired: true,
    });
    const now = Date.now();
    let archived = 0;

    for (const claim of claims) {
      if (claim.status === "rejected") {
        // Audit-only rows: keep for a retention window, then archive.
        if (now - claim.recordedAt > this.config.rejectedRetentionDays * DAY_MS) {
          await this.storage.archiveClaim(tenantId, claim.claimId, now);
          archived++;
        }
        continue;
      }

      const isExpired =
        (claim.expiresAt !== null && claim.expiresAt <= now) ||
        (claim.validTo !== null && claim.validTo <= now);

      if (isExpired) {
        await this.storage.archiveClaim(tenantId, claim.claimId, now);
        archived++;
        continue;
      }

      if (claim.authorityTier <= 1) continue;

      const lastActivity = Math.max(claim.recordedAt, claim.lastReadAt);
      const ageDays = (now - lastActivity) / DAY_MS;
      const recency = Math.exp(-ageDays / this.config.gen2DecayHalfLifeDays);
      const score = claim.importance * claim.confidence * recency;

      if (score < this.config.gen2ArchiveThreshold && ageDays > 7) {
        await this.storage.archiveClaim(tenantId, claim.claimId, now);
        archived++;
      }
    }

    return { scanned: claims.length, archived };
  }

  // --------------------------------------------------------------------------
  // F. DETERMINISTIC SAFETY BOUNDARY: Runtime-Minted Confirmation Protocol
  // --------------------------------------------------------------------------

  private async sessionBindingOk(
    tenantId: string,
    sessionId: string,
    customerId: string
  ): Promise<boolean> {
    const bound = await this.storage.getSessionCustomer(tenantId, sessionId);
    return bound === null || bound === customerId;
  }

  /**
   * Trusted IAM/OTP boundary: the ONLY way to mark a session authenticated.
   * Always clears any pending confirmation challenge (re-auth starts clean).
   */
  async setSessionAuthenticated(params: {
    tenantId: string;
    sessionId: string;
    customerId: string;
    authenticated: boolean;
    authMethod: string;
    expectedVersion: number;
  }): Promise<{ saved: boolean; newVersion: number }> {
    const existing = await this.storage.getWorkflowState(params.tenantId, params.sessionId);
    if (
      (existing && existing.customerId !== params.customerId) ||
      !(await this.sessionBindingOk(params.tenantId, params.sessionId, params.customerId))
    ) {
      return { saved: false, newVersion: existing?.version ?? params.expectedVersion };
    }

    const nextVersion = params.expectedVersion + 1;
    const state: WorkflowState = {
      tenantId: params.tenantId,
      sessionId: params.sessionId,
      customerId: params.customerId,
      version: nextVersion,
      authenticated: params.authenticated,
      authMethod: params.authenticated ? params.authMethod : null,
      currentIntent: existing?.currentIntent ?? "none",
      pendingAction: null,
      confirmationTokenHash: null,
      confirmationPayloadHash: null,
      confirmationExpiresAt: null,
      confirmationChallengeSeq: null,
      updatedAt: Date.now(),
    };

    const saved = await this.storage.saveWorkflowStateAtomic(state, params.expectedVersion);
    return { saved, newVersion: saved ? nextVersion : params.expectedVersion };
  }

  /**
   * Updates conversational intent only. Preserves auth state, rejects customer swaps,
   * and clears any prior confirmation challenge.
   */
  async updateWorkflowIntent(
    params: {
      tenantId: string;
      sessionId: string;
      customerId: string;
      currentIntent: string;
      pendingAction: string | null;
    },
    expectedVersion: number
  ): Promise<{ saved: boolean; newVersion: number }> {
    const existing = await this.storage.getWorkflowState(params.tenantId, params.sessionId);
    if (
      (existing && existing.customerId !== params.customerId) ||
      !(await this.sessionBindingOk(params.tenantId, params.sessionId, params.customerId))
    ) {
      return { saved: false, newVersion: existing?.version ?? expectedVersion };
    }

    const nextVersion = expectedVersion + 1;
    const state: WorkflowState = {
      tenantId: params.tenantId,
      sessionId: params.sessionId,
      customerId: params.customerId,
      version: nextVersion,
      authenticated: existing?.authenticated ?? false,
      authMethod: existing?.authMethod ?? null,
      currentIntent: params.currentIntent,
      pendingAction: params.pendingAction,
      confirmationTokenHash: null,
      confirmationPayloadHash: null,
      confirmationExpiresAt: null,
      confirmationChallengeSeq: null,
      updatedAt: Date.now(),
    };

    const saved = await this.storage.saveWorkflowStateAtomic(state, expectedVersion);
    return { saved, newVersion: saved ? nextVersion : expectedVersion };
  }

  /**
   * Mints a confirmation token (returned ONCE for out-of-band delivery; only its hash is stored)
   * and records the current session seq. Authorization later requires a USER turn after that seq.
   * Call this BEFORE appending the assistant's challenge message.
   */
  async issueConfirmationChallenge(params: {
    tenantId: string;
    sessionId: string;
    customerId: string;
    actionName: string;
    payloadHash: string;
    ttlMs?: number;
    expectedVersion: number;
  }): Promise<{ issued: boolean; token?: string; newVersion: number; reason?: string }> {
    const existing = await this.storage.getWorkflowState(params.tenantId, params.sessionId);
    if (!existing) {
      return { issued: false, newVersion: 0, reason: "NO_ACTIVE_WORKFLOW_STATE" };
    }
    if (
      existing.customerId !== params.customerId ||
      !(await this.sessionBindingOk(params.tenantId, params.sessionId, params.customerId))
    ) {
      return { issued: false, newVersion: existing.version, reason: "SESSION_CUSTOMER_MISMATCH" };
    }
    if (!existing.authenticated) {
      return { issued: false, newVersion: existing.version, reason: "CUSTOMER_NOT_AUTHENTICATED" };
    }

    const token = `cfm_${randomUUID()}`;
    const nextVersion = params.expectedVersion + 1;
    const now = Date.now();

    const issued = await this.storage.issueConfirmationChallengeAtomic(
      {
        ...existing,
        version: nextVersion,
        pendingAction: params.actionName,
        confirmationTokenHash: sha256Hex(token),
        confirmationPayloadHash: params.payloadHash,
        confirmationExpiresAt: now + (params.ttlMs ?? 60_000),
        confirmationChallengeSeq: 0,
        updatedAt: now,
      },
      params.expectedVersion
    );

    if (!issued.saved) {
      return { issued: false, newVersion: existing.version, reason: "CONCURRENT_WORKFLOW_UPDATE" };
    }

    return { issued: true, token, newVersion: nextVersion };
  }

  /**
   * maxTurnStaleness (default 1): latestSeq - confirmSeq <= maxLag, where confirmSeq is the FIRST
   * user turn after the challenge. seq N = user's reply, N+1 = assistant's tool-call turn is OK;
   * any further user turn (N+2) invalidates the token.
   */
  async authorizeAndCreateExecutionCommand(params: {
    tenantId: string;
    sessionId: string;
    actionName: string;
    payloadHash: string;
    providedConfirmationToken: string;
    maxTurnStaleness?: number;
  }): Promise<{ allowed: boolean; command?: ExecutionCommand; reason: string }> {
    const state = await this.storage.getWorkflowState(params.tenantId, params.sessionId);
    if (!state) return { allowed: false, reason: "NO_ACTIVE_WORKFLOW_STATE" };
    if (!state.authenticated) return { allowed: false, reason: "CUSTOMER_NOT_AUTHENTICATED" };
    if (state.pendingAction !== params.actionName) {
      return { allowed: false, reason: "INTENT_MISMATCH" };
    }
    if (
      !state.confirmationTokenHash ||
      !safeEqual(state.confirmationTokenHash, sha256Hex(params.providedConfirmationToken))
    ) {
      return { allowed: false, reason: "INVALID_CONFIRMATION_TOKEN" };
    }
    if (
      state.confirmationPayloadHash === null ||
      !safeEqual(state.confirmationPayloadHash, params.payloadHash)
    ) {
      return { allowed: false, reason: "PAYLOAD_BINDING_MISMATCH" };
    }

    const now = Date.now();
    if (state.confirmationExpiresAt === null || state.confirmationExpiresAt <= now) {
      return { allowed: false, reason: "CONFIRMATION_TOKEN_EXPIRED" };
    }
    if (state.confirmationChallengeSeq === null) {
      return { allowed: false, reason: "INVALID_CONFIRMATION_TOKEN" };
    }

    const confirmSeq = await this.storage.getFirstUserTurnSeqAfter(
      params.tenantId,
      params.sessionId,
      state.confirmationChallengeSeq
    );
    if (confirmSeq === null) {
      return { allowed: false, reason: "AWAITING_USER_CONFIRMATION_TURN" };
    }

    const latestUserSeq = await this.storage.getLatestUserTurnSeqAfter(
      params.tenantId,
      params.sessionId,
      state.confirmationChallengeSeq
    );
    if (latestUserSeq !== confirmSeq) {
      return { allowed: false, reason: "MULTIPLE_USER_TURNS_REQUIRE_REVERIFICATION" };
    }

    const latestSeq = await this.storage.getLatestSeq(params.tenantId, params.sessionId);
    const maxLag = params.maxTurnStaleness ?? 1;
    if (latestSeq - confirmSeq > maxLag) {
      return { allowed: false, reason: "STALE_CONFIRMATION_REQUIRES_REVERIFICATION" };
    }

    const command: ExecutionCommand = {
      commandId: `cmd_${randomUUID()}`,
      tenantId: params.tenantId,
      sessionId: params.sessionId,
      customerId: state.customerId,
      actionName: params.actionName,
      payloadHash: params.payloadHash,
      createdAt: now,
      status: "authorized",
    };

    const commit = await this.storage.consumeTokenAndCreateCommandAtomic({
      tenantId: params.tenantId,
      sessionId: params.sessionId,
      expectedVersion: state.version,
      expectedLatestSeq: latestSeq,
      expectedLatestUserSeq: latestUserSeq,
      actionName: params.actionName,
      expectedTokenHash: state.confirmationTokenHash,
      expectedPayloadHash: state.confirmationPayloadHash,
      now,
      command,
    });

    if (!commit.ok) {
      return { allowed: false, reason: commit.reason ?? "CONCURRENT_STATE_CHANGE_ABORTED" };
    }

    return { allowed: true, command, reason: "AUTHORIZED" };
  }
}

// ============================================================================
// 4. IN-MEMORY ADAPTER (Reference / Testing Implementation)
// ============================================================================

function clone<T>(obj: T): T {
  return structuredClone(obj);
}

export class InMemoryStorageAdapter implements RuntimeStorageAdapter {
  private turns: TurnEvent[] = [];
  private sessionCustomerMap = new Map<string, string>();
  private seqCounters = new Map<string, number>();
  private idempotencyIndex = new Map<string, { payloadSig: string; event: TurnEvent }>();
  private checkpoints = new Map<string, Checkpoint>();
  private outbox = new Map<string, CompactionOutboxJob>();
  private claims = new Map<string, MemoryClaim>();
  private ingestReceipts = new Map<string, { status: ClaimStatus; claimId: string }>();
  private disputeAudits: DisputeAuditRecord[] = [];
  private workflows = new Map<string, WorkflowState>();
  private commands = new Map<string, ExecutionCommand>();

  private sessionKey(tenantId: string, sessionId: string): string {
    return `${tenantId}::${sessionId}`;
  }

  async getSessionCustomer(tenantId: string, sessionId: string): Promise<string | null> {
    return this.sessionCustomerMap.get(this.sessionKey(tenantId, sessionId)) ?? null;
  }

  async appendTurnAtomic(input: TurnInput): Promise<{ inserted: boolean; event: TurnEvent }> {
    const sKey = this.sessionKey(input.tenantId, input.sessionId);

    const boundCustomer = this.sessionCustomerMap.get(sKey);
    if (boundCustomer && boundCustomer !== input.customerId) {
      throw new AccessDeniedError(`Session ${input.sessionId} is bound to another customer`);
    }

    const iKey = `${sKey}::${input.idempotencyKey}`;
    const payloadSig = JSON.stringify({
      customerId: input.customerId,
      role: input.role,
      content: input.content,
      pinned: input.pinned,
    });

    const existing = this.idempotencyIndex.get(iKey);
    if (existing) {
      if (existing.payloadSig !== payloadSig) {
        throw new IdempotencyConflictError(
          `Idempotency key "${input.idempotencyKey}" reused with different payload`
        );
      }
      return { inserted: false, event: clone(existing.event) };
    }

    if (!boundCustomer) {
      this.sessionCustomerMap.set(sKey, input.customerId);
    }

    const nextSeq = (this.seqCounters.get(sKey) ?? 0) + 1;
    this.seqCounters.set(sKey, nextSeq);

    const event: TurnEvent = { ...input, seq: nextSeq, timestamp: Date.now() };

    this.turns.push(clone(event));
    this.idempotencyIndex.set(iKey, { payloadSig, event: clone(event) });
    return { inserted: true, event: clone(event) };
  }

  async getTurnsAfterSeq(
    tenantId: string,
    sessionId: string,
    afterSeq: number
  ): Promise<TurnEvent[]> {
    return this.turns
      .filter((t) => t.tenantId === tenantId && t.sessionId === sessionId && t.seq > afterSeq)
      .sort((a, b) => a.seq - b.seq)
      .map(clone);
  }

  async getUncompactedTokenSum(
    tenantId: string,
    sessionId: string,
    afterSeq: number
  ): Promise<number> {
    let sum = 0;
    for (const t of this.turns) {
      if (t.tenantId === tenantId && t.sessionId === sessionId && t.seq > afterSeq && !t.pinned) {
        sum += t.tokens;
      }
    }
    return sum;
  }

  async getPinnedTurns(tenantId: string, sessionId: string): Promise<TurnEvent[]> {
    return this.turns
      .filter((t) => t.tenantId === tenantId && t.sessionId === sessionId && t.pinned)
      .sort((a, b) => a.seq - b.seq)
      .map(clone);
  }

  async getLatestSeq(tenantId: string, sessionId: string): Promise<number> {
    return this.seqCounters.get(this.sessionKey(tenantId, sessionId)) ?? 0;
  }

  async getFirstUserTurnSeqAfter(
    tenantId: string,
    sessionId: string,
    afterSeq: number
  ): Promise<number | null> {
    let min: number | null = null;
    for (const t of this.turns) {
      if (
        t.tenantId === tenantId &&
        t.sessionId === sessionId &&
        t.role === "user" &&
        t.seq > afterSeq &&
        (min === null || t.seq < min)
      ) {
        min = t.seq;
      }
    }
    return min;
  }

  async getLatestUserTurnSeqAfter(
    tenantId: string,
    sessionId: string,
    afterSeq: number
  ): Promise<number | null> {
    let max: number | null = null;
    for (const t of this.turns) {
      if (
        t.tenantId === tenantId &&
        t.sessionId === sessionId &&
        t.role === "user" &&
        t.seq > afterSeq &&
        (max === null || t.seq > max)
      ) {
        max = t.seq;
      }
    }
    return max;
  }

  async getCheckpoint(tenantId: string, sessionId: string): Promise<Checkpoint | null> {
    const cp = this.checkpoints.get(this.sessionKey(tenantId, sessionId));
    return cp ? clone(cp) : null;
  }

  async commitCheckpointWithOutboxAtomic(
    checkpoint: Checkpoint,
    outboxJob: CompactionOutboxJob,
    expectedVersion: number
  ): Promise<boolean> {
    const k = this.sessionKey(checkpoint.tenantId, checkpoint.sessionId);
    if (this.sessionCustomerMap.get(k) !== outboxJob.customerId) return false;
    const existing = this.checkpoints.get(k);
    const currentVersion = existing?.version ?? 0;

    if (currentVersion !== expectedVersion) return false;
    if (checkpoint.version !== expectedVersion + 1) return false;
    if (existing && checkpoint.compactedUpToSeq <= existing.compactedUpToSeq) return false;

    this.checkpoints.set(k, clone(checkpoint));
    this.outbox.set(`${outboxJob.tenantId}::${outboxJob.jobId}`, clone(outboxJob));
    return true;
  }

  async getPendingOutboxJobs(tenantId: string): Promise<CompactionOutboxJob[]> {
    return Array.from(this.outbox.values())
      .filter((j) => j.tenantId === tenantId && j.status === "pending")
      .map(clone);
  }

  async getFailedOutboxJobs(tenantId: string): Promise<CompactionOutboxJob[]> {
    return Array.from(this.outbox.values())
      .filter((j) => j.tenantId === tenantId && j.status === "failed")
      .map(clone);
  }

  async redriveOutboxJob(tenantId: string, jobId: string): Promise<boolean> {
    const job = this.outbox.get(`${tenantId}::${jobId}`);
    if (!job || job.tenantId !== tenantId || job.status !== "failed") return false;
    job.status = "pending";
    job.attempts = 0;
    return true;
  }

  async incrementOutboxAttempt(tenantId: string, jobId: string): Promise<number> {
    const k = `${tenantId}::${jobId}`;
    const job = this.outbox.get(k);
    if (!job || job.tenantId !== tenantId) return 0;
    job.attempts += 1;
    return job.attempts;
  }

  async markOutboxJobCompleted(tenantId: string, jobId: string): Promise<void> {
    const job = this.outbox.get(`${tenantId}::${jobId}`);
    if (job && job.tenantId === tenantId && job.status === "pending") job.status = "completed";
  }

  async markOutboxJobFailed(tenantId: string, jobId: string): Promise<void> {
    const job = this.outbox.get(`${tenantId}::${jobId}`);
    if (job && job.tenantId === tenantId && job.status === "pending") job.status = "failed";
  }

  async getClaimsByCustomer(
    tenantId: string,
    customerId: string,
    filter: ClaimQueryFilter
  ): Promise<MemoryClaim[]> {
    const asOf = filter.asOf ?? Date.now();
    return Array.from(this.claims.values())
      .filter((c) => {
        if (c.tenantId !== tenantId || c.customerId !== customerId) return false;
        if (filter.statuses && !filter.statuses.includes(c.status)) return false;
        if (filter.maxAuthorityTier !== undefined && c.authorityTier > filter.maxAuthorityTier) {
          return false;
        }
        if (SENSITIVITY_RANK[c.sensitivity] > SENSITIVITY_RANK[filter.maxSensitivity]) {
          return false;
        }
        if (!filter.includeExpired) {
          if (c.validFrom > asOf) return false;
          if (c.validTo !== null && c.validTo <= asOf) return false;
          if (c.expiresAt !== null && c.expiresAt <= asOf) return false;
        }
        return true;
      })
      .map(clone);
  }

  async ingestClaimAtomic(claim: MemoryClaim): Promise<{ status: ClaimStatus; claimId: string }> {
    const receiptKey = `${claim.tenantId}::${claim.claimId}`;
    const existingReceipt = this.ingestReceipts.get(receiptKey);
    if (existingReceipt) {
      return { ...existingReceipt };
    }

    const newClaim = clone(claim);
    const now = Date.now();
    const isLive = (c: MemoryClaim) =>
      (c.validTo === null || c.validTo > now) && (c.expiresAt === null || c.expiresAt > now);

    // Expired-but-unswept claims are dead: they neither absorb nor conflict.
    const open = Array.from(this.claims.values()).filter(
      (c) =>
        c.tenantId === newClaim.tenantId &&
        c.customerId === newClaim.customerId &&
        c.attributeKey === newClaim.attributeKey &&
        (c.status === "active" || c.status === "disputed") &&
        isLive(c)
    );

    // Phase 1: exact value match against an open claim of same/higher authority -> reinforce
    const exactMatch = open.find(
      (p) =>
        p.value.trim().toLowerCase() === newClaim.value.trim().toLowerCase() &&
        p.authorityTier <= newClaim.authorityTier
    );

    if (exactMatch) {
      exactMatch.reinforcementCount += 1;
      exactMatch.confidence = Math.max(exactMatch.confidence, newClaim.confidence);
      const res = { status: exactMatch.status, claimId: exactMatch.claimId };
      this.ingestReceipts.set(receiptKey, res);
      return res;
    }

    // Phase 2: classify (no mutation yet)
    const toSupersede: MemoryClaim[] = [];
    const toDispute: MemoryClaim[] = [];
    const strongerIds: string[] = [];

    for (const prev of open) {
      if (newClaim.authorityTier > prev.authorityTier) {
        strongerIds.push(prev.claimId);
      } else if (
        newClaim.authorityTier < prev.authorityTier ||
        newClaim.authorityTier <= 2 // equal authoritative tiers: newest wins
      ) {
        toSupersede.push(prev);
      } else {
        toDispute.push(prev); // equal untrusted tier (>= 3)
      }
    }

    // Phase 3: apply
    if (strongerIds.length > 0) {
      // Lower authority than something already open: record for audit, touch nothing else.
      newClaim.status = "rejected";
      newClaim.conflictsWith = strongerIds;
    } else {
      for (const prev of toSupersede) {
        prev.status = "superseded";
        prev.supersededBy = newClaim.claimId;
        prev.validTo = now;
      }
      if (toDispute.length > 0) {
        newClaim.status = "disputed";
        newClaim.conflictsWith = toDispute.map((d) => d.claimId);
        for (const prev of toDispute) {
          prev.status = "disputed";
          if (!prev.conflictsWith.includes(newClaim.claimId)) {
            prev.conflictsWith.push(newClaim.claimId);
          }
        }
      }
    }

    this.claims.set(receiptKey, newClaim);
    const res = { status: newClaim.status, claimId: newClaim.claimId };
    this.ingestReceipts.set(receiptKey, res);
    return res;
  }

  async recordClaimRead(tenantId: string, claimId: string, now: number): Promise<boolean> {
    const existing = this.claims.get(`${tenantId}::${claimId}`);
    if (!existing || existing.tenantId !== tenantId) return false;
    existing.lastReadAt = now;
    return true;
  }

  async archiveClaim(tenantId: string, claimId: string, now: number): Promise<boolean> {
    const existing = this.claims.get(`${tenantId}::${claimId}`);
    if (!existing || existing.tenantId !== tenantId) return false;
    existing.status = "archived";
    existing.validTo = now;

    // A dispute needs an opponent: drop this claim from peers; a lone survivor becomes active.
    for (const c of this.claims.values()) {
      if (
        c.tenantId === tenantId &&
        c.customerId === existing.customerId &&
        c.attributeKey === existing.attributeKey &&
        c.status === "disputed"
      ) {
        c.conflictsWith = c.conflictsWith.filter((id) => id !== claimId);
        if (c.conflictsWith.length === 0) c.status = "active";
      }
    }
    return true;
  }

  async resolveDisputeAtomic(params: {
    tenantId: string;
    customerId: string;
    attributeKey: string;
    winningClaimId: string;
    losingClaimIds: string[];
    resolvedByActor: string;
    reason: string;
    now: number;
  }): Promise<boolean> {
    if (!params.resolvedByActor.trim() || !params.reason.trim()) return false;
    if (params.losingClaimIds.length === 0) return false;
    const uniqueLosers = new Set(params.losingClaimIds);
    if (uniqueLosers.size !== params.losingClaimIds.length) return false;
    if (uniqueLosers.has(params.winningClaimId)) return false;

    const winner = this.claims.get(`${params.tenantId}::${params.winningClaimId}`);
    if (
      !winner ||
      winner.tenantId !== params.tenantId ||
      winner.customerId !== params.customerId ||
      winner.attributeKey !== params.attributeKey ||
      winner.status !== "disputed"
    ) {
      return false;
    }

    // losers must cover ALL other open disputed claims (no orphans)
    const otherDisputed = Array.from(this.claims.values()).filter(
      (c) =>
        c.tenantId === params.tenantId &&
        c.customerId === params.customerId &&
        c.attributeKey === params.attributeKey &&
        c.status === "disputed" &&
        c.claimId !== params.winningClaimId
    );

    if (otherDisputed.length !== uniqueLosers.size) return false;
    for (const d of otherDisputed) {
      if (!uniqueLosers.has(d.claimId)) return false;
    }

    for (const loser of otherDisputed) {
      loser.status = "superseded";
      loser.supersededBy = winner.claimId;
      loser.validTo = params.now;
      loser.conflictsWith = [];
    }

    winner.status = "active";
    winner.conflictsWith = [];

    // Cleanup pass: no claim keeps dangling references to resolved losers.
    for (const c of this.claims.values()) {
      if (
        c.tenantId === params.tenantId &&
        c.customerId === params.customerId &&
        c.attributeKey === params.attributeKey
      ) {
        c.conflictsWith = c.conflictsWith.filter((id) => !uniqueLosers.has(id));
      }
    }

    this.disputeAudits.push({
      auditId: `aud_${randomUUID()}`,
      tenantId: params.tenantId,
      customerId: params.customerId,
      attributeKey: params.attributeKey,
      winningClaimId: params.winningClaimId,
      losingClaimIds: [...params.losingClaimIds],
      resolvedByActor: params.resolvedByActor.trim(),
      reason: params.reason.trim(),
      resolvedAt: params.now,
    });

    return true;
  }

  async getDisputeAudits(tenantId: string, customerId: string): Promise<DisputeAuditRecord[]> {
    return this.disputeAudits
      .filter((a) => a.tenantId === tenantId && a.customerId === customerId)
      .map(clone);
  }

  async getWorkflowState(tenantId: string, sessionId: string): Promise<WorkflowState | null> {
    const state = this.workflows.get(this.sessionKey(tenantId, sessionId));
    return state ? clone(state) : null;
  }

  async saveWorkflowStateAtomic(state: WorkflowState, expectedVersion: number): Promise<boolean> {
    const k = this.sessionKey(state.tenantId, state.sessionId);
    const existing = this.workflows.get(k);
    const currentVersion = existing?.version ?? 0;

    if (state.version !== expectedVersion + 1) return false;
    if (currentVersion !== expectedVersion) return false;
    if (existing && existing.customerId !== state.customerId) return false;
    const bound = this.sessionCustomerMap.get(k);
    if (bound && bound !== state.customerId) return false;

    if (!bound) this.sessionCustomerMap.set(k, state.customerId);
    this.workflows.set(k, clone(state));
    return true;
  }

  async issueConfirmationChallengeAtomic(
    state: WorkflowState,
    expectedVersion: number
  ): Promise<{ saved: boolean; challengeSeq: number; reason?: string }> {
    const k = this.sessionKey(state.tenantId, state.sessionId);
    const existing = this.workflows.get(k);
    const currentVersion = existing?.version ?? 0;
    if (state.version !== expectedVersion + 1 || currentVersion !== expectedVersion) {
      return { saved: false, challengeSeq: 0, reason: "CONCURRENT_WORKFLOW_UPDATE" };
    }
    if (existing && existing.customerId !== state.customerId) {
      return { saved: false, challengeSeq: 0, reason: "SESSION_CUSTOMER_MISMATCH" };
    }
    const bound = this.sessionCustomerMap.get(k);
    if (bound && bound !== state.customerId) {
      return { saved: false, challengeSeq: 0, reason: "SESSION_CUSTOMER_MISMATCH" };
    }
    if (!bound) this.sessionCustomerMap.set(k, state.customerId);

    const challengeSeq = this.seqCounters.get(k) ?? 0;
    this.workflows.set(k, clone({ ...state, confirmationChallengeSeq: challengeSeq }));
    return { saved: true, challengeSeq };
  }

  async consumeTokenAndCreateCommandAtomic(params: {
    tenantId: string;
    sessionId: string;
    expectedVersion: number;
    expectedLatestSeq: number;
    expectedLatestUserSeq?: number | null;
    actionName: string;
    expectedTokenHash: string;
    expectedPayloadHash: string;
    now: number;
    command: ExecutionCommand;
  }): Promise<{ ok: boolean; reason?: string }> {
    const k = this.sessionKey(params.tenantId, params.sessionId);
    const existing = this.workflows.get(k);
    if (!existing || existing.version !== params.expectedVersion) {
      return { ok: false, reason: "CONCURRENT_WORKFLOW_UPDATE" };
    }
    // Defense in depth: re-verify everything against the row being consumed.
    if (!existing.authenticated) return { ok: false, reason: "CUSTOMER_NOT_AUTHENTICATED" };
    if (existing.pendingAction !== params.actionName) return { ok: false, reason: "INTENT_MISMATCH" };
    if (
      existing.confirmationTokenHash === null ||
      !safeEqual(existing.confirmationTokenHash, params.expectedTokenHash)
    ) {
      return { ok: false, reason: "INVALID_CONFIRMATION_TOKEN" };
    }
    if (
      existing.confirmationPayloadHash === null ||
      !safeEqual(existing.confirmationPayloadHash, params.expectedPayloadHash)
    ) {
      return { ok: false, reason: "PAYLOAD_BINDING_MISMATCH" };
    }
    if (existing.confirmationExpiresAt === null || existing.confirmationExpiresAt <= params.now) {
      return { ok: false, reason: "CONFIRMATION_TOKEN_EXPIRED" };
    }
    const currentSeq = this.seqCounters.get(k) ?? 0;
    if (currentSeq !== params.expectedLatestSeq) {
      return { ok: false, reason: "NEW_EVENT_ARRIVED_DURING_AUTH" };
    }
    const latestUserSeq = this.turns
      .filter((t) =>
        t.tenantId === params.tenantId &&
        t.sessionId === params.sessionId &&
        t.role === "user" &&
        t.seq > (existing.confirmationChallengeSeq ?? 0)
      )
      .reduce<number | null>((latest, turn) => latest === null ? turn.seq : Math.max(latest, turn.seq), null);
    if (latestUserSeq !== (params.expectedLatestUserSeq ?? null)) {
      return { ok: false, reason: "NEW_USER_TURN_DURING_AUTH" };
    }

    existing.version += 1;
    existing.pendingAction = null;
    existing.confirmationTokenHash = null;
    existing.confirmationPayloadHash = null;
    existing.confirmationExpiresAt = null;
    existing.confirmationChallengeSeq = null;
    existing.updatedAt = Date.now();

    this.commands.set(`${params.tenantId}::${params.command.commandId}`, clone(params.command));
    return { ok: true };
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
    const counts = { deleted: true, sessions: 0, turns: 0, claims: 0, outboxJobs: 0, receipts: 0, disputes: 0, commands: 0 };
    const deletedClaimIds = new Set<string>();
    for (const [key, bound] of this.sessionCustomerMap) {
      if (key.startsWith(`${tenantId}::`) && bound === customerId) {
        const [, ...sessionParts] = key.split("::");
        const sessionId = sessionParts.join("::");
        counts.sessions++;
        this.sessionCustomerMap.delete(key);
        this.seqCounters.delete(key);
        this.checkpoints.delete(key);
        this.workflows.delete(key);
        this.turns = this.turns.filter((turn) => {
          if (turn.tenantId === tenantId && turn.sessionId === sessionId && turn.customerId === customerId) {
            counts.turns++;
            return false;
          }
          return true;
        });
        for (const [idempotencyKey] of this.idempotencyIndex) {
          if (idempotencyKey.startsWith(`${key}::`)) this.idempotencyIndex.delete(idempotencyKey);
        }
      }
    }
    for (const [key, claim] of this.claims) {
      if (claim.tenantId === tenantId && claim.customerId === customerId) {
        counts.claims++;
        deletedClaimIds.add(claim.claimId);
        this.claims.delete(key);
      }
    }
    for (const [key, job] of this.outbox) {
      if (job.tenantId === tenantId && job.customerId === customerId) {
        counts.outboxJobs++;
        this.outbox.delete(key);
      }
    }
    for (const [key, receipt] of this.ingestReceipts) {
      if (key.startsWith(`${tenantId}::`) && deletedClaimIds.has(receipt.claimId)) {
        counts.receipts++;
        this.ingestReceipts.delete(key);
      }
    }
    this.disputeAudits = this.disputeAudits.filter((audit) => {
      if (audit.tenantId === tenantId && audit.customerId === customerId) {
        counts.disputes++;
        return false;
      }
      return true;
    });
    for (const [key, command] of this.commands) {
      if (command.tenantId === tenantId && command.customerId === customerId) {
        counts.commands++;
        this.commands.delete(key);
      }
    }
    void reason;
    void now;
    return counts;
  }
}
