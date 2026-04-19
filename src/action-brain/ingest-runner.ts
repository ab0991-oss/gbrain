import { createHash } from 'crypto';
import { ActionEngine } from './action-engine.ts';
import {
  collectWacliMessages,
  defaultCollectorCheckpointPath,
  readWacliCollectorCheckpoint,
  summarizeWacliHealth,
  type CollectWacliMessagesOptions,
  type WacliCollectorCheckpointState,
  type WacliCollectionResult,
  type WacliStoreCheckpoint,
  type WacliHealthStatus,
  type WacliStoreCollectionResult,
  writeWacliCollectorCheckpoint,
} from './collector.ts';
import { extractCommitments, type StructuredCommitment } from './extractor.ts';
import { initActionSchema } from './action-schema.ts';
import {
  buildSourceMessageIndex,
  resolveSourceMessage as resolveStoreQualifiedSourceMessage,
  resolveSourceMessageId as resolveStoreQualifiedSourceMessageId,
} from './source-identity.ts';

interface QueryResult<T> {
  rows: T[];
}

interface ActionDb {
  query<T = Record<string, unknown>>(sql: string, params?: unknown[]): Promise<QueryResult<T>>;
  exec: (sql: string) => Promise<unknown>;
}

type FailureStage = 'collect' | 'health' | 'extract' | 'store' | 'checkpoint';

export interface ActionIngestFailure {
  stage: FailureStage;
  message: string;
}

export interface ActionIngestRunSummary {
  runAt: string;
  success: boolean;
  degraded: boolean;
  healthStatus: WacliHealthStatus;
  lastSyncAt: string | null;
  alerts: string[];
  checkpointPath: string;
  checkpointAdvanced: boolean;
  messagesScanned: number;
  commitmentsExtracted: number;
  commitmentsCreated: number;
  duplicatesSkipped: number;
  lowConfidenceDropped: number;
  stores: WacliStoreCollectionResult[];
  failure: ActionIngestFailure | null;
}

export interface RunActionIngestOptions {
  db: ActionDb;
  now?: Date;
  minConfidence?: number;
  failOnDegraded?: boolean;
  actor?: string;
  model?: string;
  timeoutMs?: number;
  ownerName?: string;
  ownerAliases?: string[];
  collectorOptions?: Omit<CollectWacliMessagesOptions, 'persistCheckpoint' | 'now'>;
  collector?: (options: CollectWacliMessagesOptions) => Promise<WacliCollectionResult>;
  extractor?: typeof extractCommitments;
}

const DEFAULT_MIN_CONFIDENCE = 0.7;

export async function runActionIngest(options: RunActionIngestOptions): Promise<ActionIngestRunSummary> {
  const now = options.now ? ensureDate(options.now, 'now') : new Date();
  const checkpointPath = options.collectorOptions?.checkpointPath ?? defaultCollectorCheckpointPath();
  const collect = options.collector ?? collectWacliMessages;
  const extract = options.extractor ?? extractCommitments;
  const minConfidence = normalizeConfidenceThreshold(options.minConfidence);
  const failOnDegraded = options.failOnDegraded ?? false;
  const actor = asOptionalNonEmptyString(options.actor) ?? 'extractor';

  await initActionSchema({ exec: options.db.exec.bind(options.db) });

  const summary: ActionIngestRunSummary = {
    runAt: now.toISOString(),
    success: false,
    degraded: false,
    healthStatus: 'failed',
    lastSyncAt: null,
    alerts: [],
    checkpointPath,
    checkpointAdvanced: false,
    messagesScanned: 0,
    commitmentsExtracted: 0,
    commitmentsCreated: 0,
    duplicatesSkipped: 0,
    lowConfidenceDropped: 0,
    stores: [],
    failure: null,
  };

  let collection: WacliCollectionResult;
  try {
    collection = await collect({
      ...(options.collectorOptions ?? {}),
      now,
      persistCheckpoint: false,
    });
  } catch (err) {
    summary.failure = toFailure('collect', err);
    return summary;
  }

  summary.checkpointPath = collection.checkpointPath;
  summary.stores = collection.stores;
  summary.messagesScanned = collection.messages.length;

  const health = summarizeWacliHealth(collection.stores, { now });
  summary.healthStatus = health.status;
  summary.lastSyncAt = health.lastSyncAt;
  summary.alerts = health.alerts;
  summary.degraded = health.status !== 'healthy';

  if (health.status === 'failed') {
    summary.failure = toFailure('health', health.alerts[0] ?? 'wacli health check failed');
    return summary;
  }

  if (health.status === 'degraded' && failOnDegraded) {
    summary.failure = toFailure('health', health.alerts[0] ?? 'wacli health check degraded');
    return summary;
  }

  let extracted: StructuredCommitment[];
  try {
    extracted = await extract(collection.messages, {
      model: asOptionalNonEmptyString(options.model) ?? undefined,
      timeoutMs: options.timeoutMs,
      throwOnError: true,
      ownerName: asOptionalNonEmptyString(options.ownerName) ?? undefined,
      ownerAliases: options.ownerAliases,
    });
  } catch (err) {
    summary.failure = toFailure('extract', err);
    return summary;
  }

  summary.commitmentsExtracted = extracted.length;
  const commitments = extracted.filter((entry) => {
    if (entry.confidence < minConfidence) {
      summary.lowConfidenceDropped += 1;
      return false;
    }
    return true;
  });

  const engine = new ActionEngine(options.db);
  const sourceOrdinalByMessageId = new Map<string, number>();
  const sourceMessageIndex = buildSourceMessageIndex(collection.messages);
  try {
    for (const commitment of commitments) {
      const sourceMessage = resolveStoreQualifiedSourceMessage(collection.messages, commitment, sourceMessageIndex);
      const sourceMessageId = buildCommitmentSourceId(
        resolveStoreQualifiedSourceMessageId(collection.messages, commitment, sourceMessage, sourceMessageIndex),
        commitment,
        sourceOrdinalByMessageId
      );

      const result = await engine.createItemWithResult(
        {
          title: toActionTitle(commitment.owes_what),
          type: commitment.type,
          source_message_id: sourceMessageId,
          owner: commitment.who ?? '',
          waiting_on: null,
          due_at: parseOptionalDate(commitment.by_when, 'by_when'),
          confidence: clampConfidence(commitment.confidence),
          source_thread: sourceMessage?.ChatName ?? '',
          source_contact: sourceMessage?.SenderName ?? '',
          linked_entity_slugs: [],
        },
        {
          actor,
          metadata: {
            ingestion_mode: 'auto_runner',
          },
        }
      );

      if (result.created) {
        summary.commitmentsCreated += 1;
      } else {
        summary.duplicatesSkipped += 1;
      }
    }
  } catch (err) {
    summary.failure = toFailure('store', err);
    return summary;
  }

  const existingCheckpoint = await readWacliCollectorCheckpoint(collection.checkpointPath);
  const shouldPersistCheckpoint = !areCheckpointStatesEqual(existingCheckpoint, collection.checkpoint);
  if (!shouldPersistCheckpoint) {
    summary.success = true;
    return summary;
  }

  try {
    await writeWacliCollectorCheckpoint(collection.checkpointPath, collection.checkpoint);
    summary.checkpointAdvanced = true;
    summary.success = true;
    return summary;
  } catch (err) {
    summary.failure = toFailure('checkpoint', err);
    return summary;
  }
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

function parseOptionalDate(value: string | null | undefined, field: string): Date | null {
  const normalized = asOptionalNonEmptyString(value);
  if (!normalized) return null;
  const parsed = new Date(normalized);
  if (Number.isNaN(parsed.getTime())) {
    // Intentionally throws: an unparseable date from the LLM is a store-stage failure that prevents
    // the checkpoint from advancing past messages we couldn't fully process. This surfaces the issue
    // rather than silently dropping due_at and marking the run as successful.
    throw new Error(`Invalid ${field}: ${normalized}`);
  }
  return parsed;
}

function clampConfidence(value: number): number {
  if (!Number.isFinite(value)) {
    return 0;
  }
  return Math.min(1, Math.max(0, value));
}

function normalizeConfidenceThreshold(value: number | undefined): number {
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    return DEFAULT_MIN_CONFIDENCE;
  }
  return Math.min(1, Math.max(0, value));
}

function toFailure(stage: FailureStage, err: unknown): ActionIngestFailure {
  return {
    stage,
    message: errorMessage(err),
  };
}

function asOptionalNonEmptyString(value: unknown): string | null {
  if (typeof value !== 'string') {
    return null;
  }
  const normalized = value.trim();
  return normalized.length > 0 ? normalized : null;
}

function ensureDate(value: Date, field: string): Date {
  if (!(value instanceof Date) || Number.isNaN(value.getTime())) {
    throw new Error(`Invalid ${field}: expected valid Date`);
  }
  return value;
}

function errorMessage(err: unknown): string {
  if (err instanceof Error) {
    return err.message;
  }
  if (typeof err === 'string') {
    return err;
  }
  return JSON.stringify(err);
}

function areCheckpointStatesEqual(
  a: WacliCollectorCheckpointState,
  b: WacliCollectorCheckpointState
): boolean {
  if (a.version !== b.version) {
    return false;
  }

  const aStores = Object.keys(a.stores).sort();
  const bStores = Object.keys(b.stores).sort();
  if (aStores.length !== bStores.length) {
    return false;
  }

  for (let i = 0; i < aStores.length; i += 1) {
    const key = aStores[i];
    if (key !== bStores[i]) {
      return false;
    }
    const aStore = a.stores[key];
    const bStore = b.stores[key];
    if (!aStore || !bStore || !areStoreCheckpointsEqual(aStore, bStore)) {
      return false;
    }
  }

  return true;
}

function areStoreCheckpointsEqual(a: WacliStoreCheckpoint, b: WacliStoreCheckpoint): boolean {
  if (a.after !== b.after || a.updated_at !== b.updated_at) {
    return false;
  }
  if (a.message_ids_at_after.length !== b.message_ids_at_after.length) {
    return false;
  }
  for (let i = 0; i < a.message_ids_at_after.length; i += 1) {
    if (a.message_ids_at_after[i] !== b.message_ids_at_after[i]) {
      return false;
    }
  }
  return true;
}
