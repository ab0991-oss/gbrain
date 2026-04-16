import { createHash } from 'crypto';
import type { BrainEngine } from '../core/engine.ts';
import type { Operation } from '../core/operations.ts';
import { ActionEngine, ActionItemNotFoundError, ActionTransitionError } from './action-engine.ts';
import { MorningBriefGenerator } from './brief.ts';
import {
  defaultCollectorCheckpointPath,
  readWacliCollectorLastSyncAt,
  type WacliStoreConfig,
} from './collector.ts';
import { extractCommitments, type StructuredCommitment, type WhatsAppMessage } from './extractor.ts';
import { runActionIngest } from './ingest-runner.ts';
import { initActionSchema } from './action-schema.ts';

interface QueryResult<T> {
  rows: T[];
}

interface QueryableDb {
  query<T = Record<string, unknown>>(sql: string, params?: unknown[]): Promise<QueryResult<T>>;
  exec?: (sql: string) => Promise<unknown>;
  transaction?: <T>(fn: (txDb: QueryableDb) => Promise<T>) => Promise<T>;
}

interface ExecQueryableDb extends QueryableDb {
  exec: (sql: string) => Promise<unknown>;
}

interface PostgresUnsafeConnection {
  unsafe: (sql: string, params?: unknown[]) => Promise<Record<string, unknown>[]>;
  begin?: <T>(fn: (tx: PostgresUnsafeConnection) => Promise<T>) => Promise<T>;
}

const STATUS_VALUES = ['open', 'waiting_on', 'in_progress', 'stale', 'resolved', 'dropped'] as const;
const initializedEngines = new WeakSet<object>();
const postgresDbCache = new WeakMap<object, QueryableDb>();

export const actionBrainOperations: Operation[] = [
  {
    name: 'action_list',
    description: 'List Action Brain items with optional filters',
    params: {
      status: { type: 'string', enum: [...STATUS_VALUES], description: 'Filter by lifecycle status' },
      owner: { type: 'string', description: 'Filter by owner' },
      stale: { type: 'boolean', description: 'Filter by stale state' },
      limit: { type: 'number', description: 'Max results (default: 100)' },
      offset: { type: 'number', description: 'Pagination offset (default: 0)' },
    },
    cliHints: { name: 'action-list' },
    handler: async (ctx, p) => {
      const db = await ensureActionBrainSchema(ctx.engine);
      const engine = new ActionEngine(db);
      const stale = parseOptionalBoolean(p.stale);

      return engine.listItems({
        status: p.status as any,
        owner: asOptionalNonEmptyString(p.owner) ?? undefined,
        stale,
        limit: asOptionalNumber(p.limit),
        offset: asOptionalNumber(p.offset),
      });
    },
  },
  {
    name: 'action_brief',
    description: 'Generate Action Brain morning brief',
    params: {
      now: { type: 'string', description: 'Optional clock override (ISO timestamp)' },
      last_sync_at: { type: 'string', description: 'Override wacli freshness timestamp (ISO timestamp)' },
      checkpoint_path: {
        type: 'string',
        description: 'Optional wacli checkpoint path used to auto-resolve freshness when last_sync_at is not provided',
      },
      timezone_offset_minutes: {
        type: 'number',
        description: 'Timezone offset in minutes east of UTC for due-today classification',
      },
    },
    cliHints: { name: 'action-brief' },
    handler: async (ctx, p) => {
      const db = await ensureActionBrainSchema(ctx.engine);
      const generator = new MorningBriefGenerator(db);
      const explicitLastSyncAt = parseOptionalDate(p.last_sync_at, 'last_sync_at');
      const checkpointPath = asOptionalNonEmptyString(p.checkpoint_path) ?? defaultCollectorCheckpointPath();

      let inferredLastSyncAt: Date | null = null;
      if (!explicitLastSyncAt) {
        const checkpointLastSyncAt = await readWacliCollectorLastSyncAt(checkpointPath);
        inferredLastSyncAt = parseOptionalDate(checkpointLastSyncAt, 'checkpoint.last_sync_at');
      }

      const brief = await generator.generateMorningBrief({
        now: parseOptionalDate(p.now, 'now') ?? undefined,
        lastSyncAt: explicitLastSyncAt ?? inferredLastSyncAt,
        timezoneOffsetMinutes: asOptionalNumber(p.timezone_offset_minutes),
      });

      return { brief };
    },
  },
  {
    name: 'action_resolve',
    description: 'Mark an Action Brain item as resolved',
    params: {
      id: { type: 'number', required: true, description: 'Action item id' },
      actor: { type: 'string', description: 'Actor writing the status transition' },
    },
    mutating: true,
    cliHints: { name: 'action-resolve', positional: ['id'] },
    handler: async (ctx, p) => {
      const db = await ensureActionBrainSchema(ctx.engine);
      const engine = new ActionEngine(db);
      const id = asRequiredInteger(p.id, 'id');

      if (ctx.dryRun) {
        return { dry_run: true, action: 'action_resolve', id };
      }

      try {
        const item = await engine.resolveItem(id, { actor: asOptionalNonEmptyString(p.actor) ?? 'system' });
        return { status: 'resolved', item };
      } catch (error) {
        if (error instanceof ActionItemNotFoundError || error instanceof ActionTransitionError) {
          throw new Error(error.message);
        }
        throw error;
      }
    },
  },
  {
    name: 'action_mark_fp',
    description: 'Mark an Action Brain item as false-positive extraction',
    params: {
      id: { type: 'number', required: true, description: 'Action item id' },
      actor: { type: 'string', description: 'Actor writing the status transition' },
    },
    mutating: true,
    cliHints: { name: 'action-mark-fp', positional: ['id'] },
    handler: async (ctx, p) => {
      const db = await ensureActionBrainSchema(ctx.engine);
      const engine = new ActionEngine(db);
      const id = asRequiredInteger(p.id, 'id');

      if (ctx.dryRun) {
        return { dry_run: true, action: 'action_mark_fp', id };
      }

      try {
        const item = await engine.updateItemStatus(id, 'dropped', {
          actor: asOptionalNonEmptyString(p.actor) ?? 'human_feedback',
          metadata: { reason: 'false_positive' },
        });
        return { status: 'marked_false_positive', item };
      } catch (error) {
        if (error instanceof ActionItemNotFoundError || error instanceof ActionTransitionError) {
          throw new Error(error.message);
        }
        throw error;
      }
    },
  },
  {
    name: 'action_ingest',
    description: 'Run extraction on a WhatsApp message batch and upsert Action Brain items',
    params: {
      messages: {
        type: 'array',
        description: 'WhatsApp message batch [{ChatName, SenderName, Timestamp, Text, MsgID}]',
        items: { type: 'object' },
      },
      messages_json: { type: 'string', description: 'JSON-encoded WhatsApp message batch (CLI-friendly)' },
      commitments: { type: 'array', description: 'Optional pre-extracted commitments (bypass LLM)', items: { type: 'object' } },
      model: { type: 'string', description: 'Anthropic model override' },
      timeout_ms: { type: 'number', description: 'Extractor timeout in milliseconds' },
      actor: { type: 'string', description: 'Actor writing created events' },
    },
    mutating: true,
    cliHints: { name: 'action-ingest', stdin: 'messages_json' },
    handler: async (ctx, p) => {
      const db = await ensureActionBrainSchema(ctx.engine);
      const engine = new ActionEngine(db);
      const messages = parseMessagesParam(p.messages ?? p.messages_json);
      const providedCommitments = parseCommitmentsParam(p.commitments);
      const actor = asOptionalNonEmptyString(p.actor) ?? 'extractor';

      if (messages.length === 0 && providedCommitments.length === 0) {
        throw new Error('action_ingest requires messages (or commitments for deterministic ingest).');
      }

      const extracted =
        providedCommitments.length > 0
          ? providedCommitments
          : await extractCommitments(messages, {
              model: asOptionalNonEmptyString(p.model) ?? undefined,
              timeoutMs: asOptionalNumber(p.timeout_ms) ?? undefined,
            });

      if (ctx.dryRun) {
        return {
          dry_run: true,
          extracted_count: extracted.length,
          extracted,
        };
      }

      const sourceOrdinalByMessageId = new Map<string, number>();
      const items = [];
      let createdCount = 0;
      for (let i = 0; i < extracted.length; i += 1) {
        const commitment = extracted[i];
        const message = resolveSourceMessage(messages, commitment);
        const sourceMessageId = buildCommitmentSourceId(
          resolveSourceMessageId(messages, commitment, message),
          commitment,
          sourceOrdinalByMessageId
        );
        const dueAt = parseOptionalDate(commitment.by_when, 'by_when');

        const result = await engine.createItemWithResult(
          {
            title: toActionTitle(commitment.owes_what),
            type: commitment.type,
            source_message_id: sourceMessageId,
            owner: commitment.who ?? '',
            waiting_on: null,
            due_at: dueAt,
            confidence: commitment.confidence,
            source_thread: message?.ChatName ?? '',
            source_contact: message?.SenderName ?? '',
            linked_entity_slugs: [],
          },
          {
            actor,
            metadata: {
              ingestion_mode: providedCommitments.length > 0 ? 'direct_commitments' : 'extracted',
            },
          }
        );

        if (result.created) {
          createdCount += 1;
        }
        items.push(result.item);
      }

      return {
        extracted_count: extracted.length,
        created_count: createdCount,
        items,
      };
    },
  },
  {
    name: 'action_ingest_auto',
    description: 'Run the wacli collector + extractor auto-ingest pipeline with preflight health checks',
    params: {
      now: { type: 'string', description: 'Optional clock override (ISO timestamp)' },
      min_confidence: { type: 'number', description: 'Drop commitments below this confidence threshold (default: 0.7)' },
      actor: { type: 'string', description: 'Actor writing created events' },
      model: { type: 'string', description: 'Anthropic model override' },
      timeout_ms: { type: 'number', description: 'Extractor timeout in milliseconds' },
      owner_name: { type: 'string', description: 'Owner name used by extraction prompt grounding' },
      owner_aliases: { type: 'array', items: { type: 'string' }, description: 'Optional owner alias list' },
      owner_aliases_json: { type: 'string', description: 'JSON-encoded owner alias list (CLI-friendly)' },
      checkpoint_path: { type: 'string', description: 'Collector checkpoint path override' },
      wacli_limit: { type: 'number', description: 'Max messages per wacli list call (default: 200)' },
      stale_after_hours: {
        type: 'number',
        description: 'Mark stores degraded when latest sync is older than this many hours (default: 24)',
      },
      fail_on_degraded: {
        type: 'boolean',
        description: 'Fail the run when health is degraded (for strict scheduler gating)',
      },
      personal_store_path: { type: 'string', description: 'Override personal wacli store path' },
      business_store_path: { type: 'string', description: 'Override business wacli store path' },
    },
    mutating: true,
    cliHints: { name: 'action-run' },
    handler: async (ctx, p) => {
      const db = await ensureActionBrainSchema(ctx.engine);
      const execDb = requireExecQueryableDb(db);

      if (ctx.dryRun) {
        return { dry_run: true, action: 'action_ingest_auto' };
      }

      const stores = parseStoreOverrides({
        personalStorePath: p.personal_store_path,
        businessStorePath: p.business_store_path,
      });
      const ownerAliases = parseStringArrayParam(p.owner_aliases ?? p.owner_aliases_json);

      return runActionIngest({
        db: execDb,
        now: parseOptionalDate(p.now, 'now') ?? undefined,
        minConfidence: asOptionalNumber(p.min_confidence),
        actor: asOptionalNonEmptyString(p.actor) ?? undefined,
        model: asOptionalNonEmptyString(p.model) ?? undefined,
        timeoutMs: asOptionalNumber(p.timeout_ms),
        failOnDegraded: parseOptionalBoolean(p.fail_on_degraded) ?? false,
        ownerName: asOptionalNonEmptyString(p.owner_name) ?? undefined,
        ownerAliases: ownerAliases.length > 0 ? ownerAliases : undefined,
        collectorOptions: {
          checkpointPath: asOptionalNonEmptyString(p.checkpoint_path) ?? undefined,
          limit: normalizePositiveInteger(p.wacli_limit),
          staleAfterHours: asOptionalNumber(p.stale_after_hours),
          stores: stores.length > 0 ? stores : undefined,
        },
      });
    },
  },
];

async function ensureActionBrainSchema(engine: BrainEngine): Promise<QueryableDb> {
  const key = engine as unknown as object;
  const db = await resolveActionDb(engine);

  if (initializedEngines.has(key)) {
    return db;
  }

  if (typeof db.exec !== 'function') {
    throw new Error('Action Brain schema initialization requires an exec-capable database adapter.');
  }

  await initActionSchema({ exec: db.exec.bind(db) });
  initializedEngines.add(key);
  return db;
}

async function resolveActionDb(engine: BrainEngine): Promise<QueryableDb> {
  const candidate = engine as unknown as Record<string, unknown>;
  const pgliteDb = candidate.db as QueryableDb | undefined;
  if (pgliteDb && typeof pgliteDb.query === 'function' && typeof pgliteDb.exec === 'function') {
    return pgliteDb;
  }

  const cacheKey = engine as unknown as object;
  const cachedPostgresDb = postgresDbCache.get(cacheKey);
  if (cachedPostgresDb) {
    return cachedPostgresDb;
  }

  const sql = candidate.sql as PostgresUnsafeConnection | undefined;
  if (sql && typeof sql.unsafe === 'function') {
    const wrap = (conn: PostgresUnsafeConnection): QueryableDb => ({
      query: async <T = Record<string, unknown>>(statement: string, params: unknown[] = []) => {
        const rows = params.length === 0 ? await conn.unsafe(statement) : await conn.unsafe(statement, params);
        return { rows: rows as T[] };
      },
      exec: async (statement: string) => {
        await conn.unsafe(statement);
      },
      transaction:
        typeof conn.begin === 'function'
          ? async <T>(fn: (txDb: QueryableDb) => Promise<T>) => {
              return conn.begin(async (tx) => fn(wrap(tx)));
            }
          : undefined,
    });

    const wrapped = wrap(sql);
    postgresDbCache.set(cacheKey, wrapped);
    return wrapped;
  }

  throw new Error('Unsupported engine for Action Brain operations. Expected PGLiteEngine or PostgresEngine.');
}

function requireExecQueryableDb(db: QueryableDb): ExecQueryableDb {
  if (typeof db.exec !== 'function') {
    throw new Error('Action auto-ingest requires an exec-capable database adapter.');
  }
  return db as ExecQueryableDb;
}

function parseMessagesParam(value: unknown): WhatsAppMessage[] {
  const raw = parseJsonArrayInput(value);
  if (raw.length === 0) {
    return [];
  }

  const normalized: WhatsAppMessage[] = [];
  for (const entry of raw) {
    if (!isRecord(entry)) continue;
    const msgId = asOptionalNonEmptyString(entry.MsgID);
    const text = asOptionalNonEmptyString(entry.Text);
    if (!msgId || !text) continue;

    normalized.push({
      ChatName: asOptionalNonEmptyString(entry.ChatName) ?? '',
      SenderName: asOptionalNonEmptyString(entry.SenderName) ?? '',
      Timestamp: asOptionalNonEmptyString(entry.Timestamp) ?? '',
      Text: text,
      MsgID: msgId,
    });
  }

  return normalized;
}

function parseCommitmentsParam(value: unknown): StructuredCommitment[] {
  const raw = parseJsonArrayInput(value);
  if (raw.length === 0) {
    return [];
  }

  const normalized: StructuredCommitment[] = [];
  for (const entry of raw) {
    if (!isRecord(entry)) continue;

    const owesWhat = asOptionalNonEmptyString(entry.owes_what);
    if (!owesWhat) continue;

    const type = asOptionalNonEmptyString(entry.type);
    if (!isActionType(type)) continue;

    normalized.push({
      who: asOptionalNonEmptyString(entry.who) ?? null,
      owes_what: owesWhat,
      to_whom: asOptionalNonEmptyString(entry.to_whom) ?? null,
      by_when: asOptionalNonEmptyString(entry.by_when) ?? null,
      confidence: clampConfidence(entry.confidence),
      type,
      source_message_id: asOptionalNonEmptyString(entry.source_message_id),
    });
  }

  return normalized;
}

const STATUSLESS_ACTION_TYPES = new Set<StructuredCommitment['type']>([
  'commitment',
  'follow_up',
  'decision',
  'question',
  'delegation',
]);

function isActionType(value: string | null): value is StructuredCommitment['type'] {
  return typeof value === 'string' && STATUSLESS_ACTION_TYPES.has(value as StructuredCommitment['type']);
}

function parseJsonArrayInput(value: unknown): unknown[] {
  if (Array.isArray(value)) {
    return value;
  }

  if (typeof value === 'string') {
    try {
      const parsed = JSON.parse(value);
      return Array.isArray(parsed) ? parsed : [];
    } catch {
      return [];
    }
  }

  return [];
}

function parseStringArrayParam(value: unknown): string[] {
  const raw = parseJsonArrayInput(value);
  if (raw.length === 0) {
    return [];
  }

  return raw
    .map((entry) => asOptionalNonEmptyString(entry))
    .filter((entry): entry is string => Boolean(entry));
}

function resolveSourceMessage(messages: WhatsAppMessage[], commitment: StructuredCommitment): WhatsAppMessage | null {
  if (messages.length === 0) {
    return null;
  }

  const explicitSourceMessageId = asOptionalNonEmptyString(commitment.source_message_id);
  if (explicitSourceMessageId) {
    const matched = messages.find((message) => message.MsgID === explicitSourceMessageId);
    if (matched) {
      return matched;
    }
  }

  return messages.length === 1 ? messages[0] : null;
}

function resolveSourceMessageId(
  messages: WhatsAppMessage[],
  commitment: StructuredCommitment,
  message: WhatsAppMessage | null
): string | null {
  if (message) {
    return message.MsgID;
  }

  // Only trust direct source ids when no message batch is available to validate against.
  if (messages.length === 0) {
    return asOptionalNonEmptyString(commitment.source_message_id);
  }

  return null;
}

function buildCommitmentSourceId(
  sourceMessageId: string | null,
  commitment: StructuredCommitment,
  sourceOrdinalByMessageId: Map<string, number>
): string {
  const baseMsgId = asOptionalNonEmptyString(sourceMessageId);
  if (baseMsgId) {
    const nextOrdinal = sourceOrdinalByMessageId.get(baseMsgId) ?? 0;
    sourceOrdinalByMessageId.set(baseMsgId, nextOrdinal + 1);
    return `${baseMsgId}:ab:${nextOrdinal}`;
  }

  const batchKey = 'batch';
  const seed = [
    batchKey,
    normalizeCommitmentField(commitment.who),
    normalizeCommitmentField(commitment.owes_what),
    normalizeCommitmentField(commitment.to_whom),
    normalizeCommitmentField(commitment.by_when),
    commitment.type,
  ].join('|');
  const digest = createHash('sha256').update(seed).digest('hex').slice(0, 16);
  return `${batchKey}:ab:${digest}`;
}

function normalizeCommitmentField(value: string | null | undefined): string {
  if (!value) return '';
  return value.trim().toLowerCase();
}

function toActionTitle(owesWhat: string): string {
  const text = owesWhat.trim();
  if (text.length <= 160) return text;
  return `${text.slice(0, 157)}...`;
}

function parseStoreOverrides(input: {
  personalStorePath: unknown;
  businessStorePath: unknown;
}): WacliStoreConfig[] {
  const personalStorePath = asOptionalNonEmptyString(input.personalStorePath);
  const businessStorePath = asOptionalNonEmptyString(input.businessStorePath);
  const stores: WacliStoreConfig[] = [];

  if (personalStorePath) {
    stores.push({ key: 'personal', storePath: personalStorePath });
  }

  if (businessStorePath && businessStorePath !== personalStorePath) {
    stores.push({ key: 'business', storePath: businessStorePath });
  }

  return stores;
}

function asOptionalNumber(value: unknown): number | undefined {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === 'string' && value.trim().length > 0) {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  return undefined;
}

function normalizePositiveInteger(value: unknown): number | undefined {
  const parsed = asOptionalNumber(value);
  if (parsed === undefined || !Number.isInteger(parsed) || parsed < 1) {
    return undefined;
  }
  return parsed;
}

function asRequiredInteger(value: unknown, param: string): number {
  const parsed = asOptionalNumber(value);
  if (parsed === undefined || !Number.isInteger(parsed)) {
    throw new Error(`Invalid ${param}: expected integer`);
  }
  return parsed;
}

function asOptionalNonEmptyString(value: unknown): string | null {
  if (typeof value !== 'string') {
    return null;
  }
  const normalized = value.trim();
  return normalized.length > 0 ? normalized : null;
}

function parseOptionalBoolean(value: unknown): boolean | undefined {
  if (typeof value === 'boolean') {
    return value;
  }
  if (typeof value === 'string') {
    if (value === 'true') return true;
    if (value === 'false') return false;
  }
  return undefined;
}

function parseOptionalDate(value: unknown, field: string): Date | null {
  const text = asOptionalNonEmptyString(value);
  if (!text) return null;
  const parsed = new Date(text);
  if (Number.isNaN(parsed.getTime())) {
    throw new Error(`Invalid ${field}: ${text}`);
  }
  return parsed;
}

function clampConfidence(value: unknown): number {
  const parsed = asOptionalNumber(value);
  if (parsed === undefined) {
    return 0.5;
  }
  return Math.min(1, Math.max(0, parsed));
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}
