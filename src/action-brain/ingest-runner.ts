import { createHash } from 'crypto';
import { ActionEngine } from './action-engine.ts';
import {
  collectWacliMessages,
  defaultCollectorCheckpointPath,
  summarizeWacliHealth,
  type CollectWacliMessagesOptions,
  type WacliCollectionResult,
  type WacliHealthStatus,
  type WacliStoreCollectionResult,
  writeWacliCollectorCheckpoint,
} from './collector.ts';
import { extractCommitments, type StructuredCommitment, type WhatsAppMessage } from './extractor.ts';
import { initActionSchema } from './action-schema.ts';

interface QueryResult<T> {
  rows: T[];
}

interface ActionQueryDb {
  query<T = Record<string, unknown>>(sql: string, params?: unknown[]): Promise<QueryResult<T>>;
  transaction?<T>(fn: (db: ActionQueryDb) => Promise<T>): Promise<T>;
}

interface ActionDb extends ActionQueryDb {
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
  try {
    await engine.transaction(async (txEngine) => {
      let commitmentsCreated = 0;
      let duplicatesSkipped = 0;

      for (const commitment of commitments) {
        const sourceMessage = resolveSourceMessage(collection.messages, commitment);
        const sourceMessageId = buildCommitmentSourceId(
          resolveSourceMessageId(collection.messages, commitment, sourceMessage),
          commitment
        );

        const result = await txEngine.createItemWithResult(
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
          },
          {
            useTransaction: false,
          }
        );

        if (result.created) {
          commitmentsCreated += 1;
        } else {
          duplicatesSkipped += 1;
        }
      }

      summary.commitmentsCreated = commitmentsCreated;
      summary.duplicatesSkipped = duplicatesSkipped;
    });
  } catch (err) {
    summary.failure = toFailure('store', err);
    return summary;
  }

  const shouldPersistCheckpoint = collection.stores.some((store) => store.checkpointBefore !== store.checkpointAfter);
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

  if (messages.length === 0) {
    return asOptionalNonEmptyString(commitment.source_message_id);
  }

  return null;
}

function buildCommitmentSourceId(sourceMessageId: string | null, commitment: StructuredCommitment): string {
  const baseMsgId = asOptionalNonEmptyString(sourceMessageId) ?? 'batch';
  const seed = [
    baseMsgId,
    normalizeCommitmentField(commitment.who),
    normalizeCommitmentField(commitment.owes_what),
    normalizeCommitmentField(commitment.to_whom),
    normalizeCommitmentField(commitment.by_when),
    commitment.type,
  ].join('|');
  const digest = createHash('sha256').update(seed).digest('hex').slice(0, 16);
  return `${baseMsgId}:ab:${digest}`;
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
