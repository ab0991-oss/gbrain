import { afterEach, describe, expect, test } from 'bun:test';
import { mkdtempSync, rmSync } from 'fs';
import { join } from 'path';
import { tmpdir } from 'os';
import { PGLiteEngine } from '../../src/core/pglite-engine.ts';
import {
  type CollectedWhatsAppMessage,
  readWacliCollectorCheckpoint,
  writeWacliCollectorCheckpoint,
  type CollectWacliMessagesOptions,
  type WacliCollectionResult,
} from '../../src/action-brain/collector.ts';
import { runActionIngest } from '../../src/action-brain/ingest-runner.ts';
import type { StructuredCommitment } from '../../src/action-brain/extractor.ts';

interface ActionDb {
  query<T = Record<string, unknown>>(sql: string, params?: unknown[]): Promise<{ rows: T[] }>;
  exec: (sql: string) => Promise<unknown>;
}

interface EngineWithDb {
  db: ActionDb;
}

const tempDirs: string[] = [];

afterEach(() => {
  while (tempDirs.length > 0) {
    const dir = tempDirs.pop();
    if (!dir) continue;
    rmSync(dir, { recursive: true, force: true });
  }
});

describe('runActionIngest', () => {
  test('runs collect -> extract -> store and advances checkpoint only after store succeeds', async () => {
    await withDb(async (db) => {
      const root = createTempDir();
      const checkpointPath = join(root, 'wacli-checkpoint.json');
      await writeWacliCollectorCheckpoint(checkpointPath, {
        version: 1,
        stores: {
          personal: {
            after: '2026-04-15T00:00:00.000Z',
            message_ids_at_after: ['old-1'],
            updated_at: '2026-04-15T00:00:00.000Z',
          },
        },
      });

      let collectorPersistFlag: boolean | undefined;
      const messages = [
        message('m1', '2026-04-16T08:00:00.000Z', 'Joe to send shipment docs by 5pm'),
        message('m2', '2026-04-16T08:05:00.000Z', 'Mukesh to confirm payout'),
      ];
      const extractorOutput: StructuredCommitment[] = [
        commitment('Joe', 'Send shipment docs', 'm1', 0.92),
        commitment('Mukesh', 'Confirm payout', 'm2', 0.81),
        commitment('Joe', 'FYI mention', 'm1', 0.3),
      ];

      const summary = await runActionIngest({
        db,
        minConfidence: 0.7,
        collectorOptions: { checkpointPath },
        collector: async (options: CollectWacliMessagesOptions): Promise<WacliCollectionResult> => {
          collectorPersistFlag = options.persistCheckpoint;
          return {
            collectedAt: '2026-04-16T08:10:00.000Z',
            checkpointPath,
            limit: 200,
            staleAfterHours: 24,
            stores: [
              {
                storeKey: 'personal',
                storePath: '/stores/personal',
                checkpointBefore: '2026-04-15T00:00:00.000Z',
                checkpointAfter: '2026-04-16T08:05:00.000Z',
                batchSize: 2,
                lastSyncAt: '2026-04-16T08:05:00.000Z',
                degraded: false,
                degradedReason: null,
                error: null,
                messages,
              },
            ],
            messages,
            degraded: false,
            checkpoint: {
              version: 1,
              stores: {
                personal: {
                  after: '2026-04-16T08:05:00.000Z',
                  message_ids_at_after: ['m2'],
                  updated_at: '2026-04-16T08:10:00.000Z',
                },
              },
            },
          };
        },
        extractor: async (_messages) => extractorOutput,
      });

      expect(collectorPersistFlag).toBe(false);
      expect(summary.success).toBe(true);
      expect(summary.healthStatus).toBe('healthy');
      expect(summary.lastSyncAt).toBe('2026-04-16T08:05:00.000Z');
      expect(summary.alerts).toEqual([]);
      expect(summary.messagesScanned).toBe(2);
      expect(summary.commitmentsExtracted).toBe(3);
      expect(summary.lowConfidenceDropped).toBe(1);
      expect(summary.commitmentsCreated).toBe(2);
      expect(summary.duplicatesSkipped).toBe(0);
      expect(summary.checkpointAdvanced).toBe(true);
      expect(summary.failure).toBeNull();

      const checkpoint = await readWacliCollectorCheckpoint(checkpointPath);
      expect(checkpoint.stores.personal?.after).toBe('2026-04-16T08:05:00.000Z');

      const rows = await db.query<{ count: number }>('SELECT count(*)::int AS count FROM action_items');
      expect(rows.rows[0]?.count).toBe(2);
    });
  });

  test('is idempotent across repeated runs and counts duplicates skipped', async () => {
    await withDb(async (db) => {
      const root = createTempDir();
      const checkpointPath = join(root, 'wacli-checkpoint.json');
      const messages = [message('m3', '2026-04-16T09:00:00.000Z', 'Joe to send vessel update')];
      const extractorOutput = [commitment('Joe', 'Send vessel update', 'm3', 0.9)];

      const collector = async (_options: CollectWacliMessagesOptions): Promise<WacliCollectionResult> => ({
        collectedAt: '2026-04-16T09:05:00.000Z',
        checkpointPath,
        limit: 200,
        staleAfterHours: 24,
        stores: [
          {
            storeKey: 'personal',
            storePath: '/stores/personal',
            checkpointBefore: '2026-04-16T08:00:00.000Z',
            checkpointAfter: '2026-04-16T09:00:00.000Z',
            batchSize: 1,
            lastSyncAt: '2026-04-16T09:00:00.000Z',
            degraded: false,
            degradedReason: null,
            error: null,
            messages,
          },
        ],
        messages,
        degraded: false,
        checkpoint: {
          version: 1,
          stores: {
            personal: {
              after: '2026-04-16T09:00:00.000Z',
              message_ids_at_after: ['m3'],
              updated_at: '2026-04-16T09:05:00.000Z',
            },
          },
        },
      });

      const firstRun = await runActionIngest({
        db,
        collectorOptions: { checkpointPath },
        collector,
        extractor: async () => extractorOutput,
      });
      const secondRun = await runActionIngest({
        db,
        collectorOptions: { checkpointPath },
        collector,
        extractor: async () => extractorOutput,
      });

      expect(firstRun.success).toBe(true);
      expect(firstRun.commitmentsCreated).toBe(1);
      expect(firstRun.duplicatesSkipped).toBe(0);
      expect(firstRun.healthStatus).toBe('healthy');

      expect(secondRun.success).toBe(true);
      expect(secondRun.commitmentsCreated).toBe(0);
      expect(secondRun.duplicatesSkipped).toBe(1);
    });
  });

  test('rolls back all stored items and leaves checkpoint unchanged when a later store write fails', async () => {
    await withDb(async (db) => {
      const root = createTempDir();
      const checkpointPath = join(root, 'wacli-checkpoint.json');
      await writeWacliCollectorCheckpoint(checkpointPath, {
        version: 1,
        stores: {
          personal: {
            after: '2026-04-15T22:00:00.000Z',
            message_ids_at_after: ['prev'],
            updated_at: '2026-04-15T22:00:00.000Z',
          },
        },
      });

      const messages = [
        message('m4', '2026-04-16T10:00:00.000Z', 'Joe to send manifest'),
        message('m5', '2026-04-16T10:02:00.000Z', 'Mukesh to confirm payout date'),
      ];
      const summary = await runActionIngest({
        db,
        collectorOptions: { checkpointPath },
        collector: async (_options: CollectWacliMessagesOptions): Promise<WacliCollectionResult> => ({
          collectedAt: '2026-04-16T10:05:00.000Z',
          checkpointPath,
          limit: 200,
          staleAfterHours: 24,
          stores: [
            {
              storeKey: 'personal',
              storePath: '/stores/personal',
              checkpointBefore: '2026-04-15T22:00:00.000Z',
              checkpointAfter: '2026-04-16T10:02:00.000Z',
              batchSize: 2,
              lastSyncAt: '2026-04-16T10:02:00.000Z',
              degraded: false,
              degradedReason: null,
              error: null,
              messages,
            },
          ],
          messages,
          degraded: false,
          checkpoint: {
            version: 1,
            stores: {
              personal: {
                after: '2026-04-16T10:02:00.000Z',
                message_ids_at_after: ['m5'],
                updated_at: '2026-04-16T10:05:00.000Z',
              },
            },
          },
        }),
        extractor: async () => [
          commitment('Joe', 'Send manifest', 'm4', 0.9),
          {
            ...commitment('Mukesh', 'Confirm payout date', 'm5', 0.95),
            by_when: 'not-a-date',
          },
        ],
      });

      expect(summary.success).toBe(false);
      expect(summary.failure?.stage).toBe('store');
      expect(summary.commitmentsCreated).toBe(0);
      expect(summary.duplicatesSkipped).toBe(0);
      expect(summary.checkpointAdvanced).toBe(false);

      const checkpoint = await readWacliCollectorCheckpoint(checkpointPath);
      expect(checkpoint.stores.personal?.after).toBe('2026-04-15T22:00:00.000Z');

      const itemRows = await db.query<{ count: number }>('SELECT count(*)::int AS count FROM action_items');
      expect(itemRows.rows[0]?.count).toBe(0);

      const historyRows = await db.query<{ count: number }>('SELECT count(*)::int AS count FROM action_history');
      expect(historyRows.rows[0]?.count).toBe(0);
    });
  });

  test('fails fast at health preflight when a store is disconnected', async () => {
    await withDb(async (db) => {
      const root = createTempDir();
      const checkpointPath = join(root, 'wacli-checkpoint.json');
      let extractorCalled = false;

      const summary = await runActionIngest({
        db,
        collectorOptions: { checkpointPath },
        collector: async (_options: CollectWacliMessagesOptions): Promise<WacliCollectionResult> => ({
          collectedAt: '2026-04-16T10:05:00.000Z',
          checkpointPath,
          limit: 200,
          staleAfterHours: 24,
          stores: [
            {
              storeKey: 'personal',
              storePath: '/stores/personal',
              checkpointBefore: null,
              checkpointAfter: null,
              batchSize: 0,
              lastSyncAt: null,
              degraded: true,
              degradedReason: 'command_failed',
              error: 'spawn wacli ENOENT',
              messages: [],
            },
          ],
          messages: [],
          degraded: true,
          checkpoint: { version: 1, stores: {} },
        }),
        extractor: async () => {
          extractorCalled = true;
          return [];
        },
      });

      expect(summary.success).toBe(false);
      expect(summary.healthStatus).toBe('failed');
      expect(summary.failure?.stage).toBe('health');
      expect(summary.alerts[0]).toContain('unhealthy');
      expect(extractorCalled).toBe(false);
    });
  });

  test('continues in degraded mode when store health is stale but connected', async () => {
    await withDb(async (db) => {
      const root = createTempDir();
      const checkpointPath = join(root, 'wacli-checkpoint.json');
      const messages = [message('m5', '2026-04-16T11:00:00.000Z', 'Joe to send invoice')];

      const summary = await runActionIngest({
        db,
        collectorOptions: { checkpointPath },
        collector: async (_options: CollectWacliMessagesOptions): Promise<WacliCollectionResult> => ({
          collectedAt: '2026-04-16T12:00:00.000Z',
          checkpointPath,
          limit: 200,
          staleAfterHours: 24,
          stores: [
            {
              storeKey: 'personal',
              storePath: '/stores/personal',
              checkpointBefore: null,
              checkpointAfter: '2026-04-16T11:00:00.000Z',
              batchSize: 1,
              lastSyncAt: '2026-04-15T10:00:00.000Z',
              degraded: true,
              degradedReason: 'last_sync_stale',
              error: null,
              messages,
            },
          ],
          messages,
          degraded: true,
          checkpoint: {
            version: 1,
            stores: {
              personal: {
                after: '2026-04-16T11:00:00.000Z',
                message_ids_at_after: ['m5'],
                updated_at: '2026-04-16T12:00:00.000Z',
              },
            },
          },
        }),
        extractor: async () => [commitment('Joe', 'Send invoice', 'm5', 0.9)],
      });

      expect(summary.success).toBe(true);
      expect(summary.degraded).toBe(true);
      expect(summary.healthStatus).toBe('degraded');
      expect(summary.lastSyncAt).toBe('2026-04-15T10:00:00.000Z');
      expect(summary.alerts[0]).toContain('stale');
      expect(summary.commitmentsCreated).toBe(1);
    });
  });
});

async function withDb<T>(fn: (db: ActionDb) => Promise<T>): Promise<T> {
  const engine = new PGLiteEngine();
  await engine.connect({ engine: 'pglite' } as any);

  const db = (engine as unknown as EngineWithDb).db;

  try {
    return await fn(db);
  } finally {
    await engine.disconnect();
  }
}

function createTempDir(): string {
  const dir = mkdtempSync(join(tmpdir(), 'action-brain-ingest-runner-test-'));
  tempDirs.push(dir);
  return dir;
}

function message(id: string, timestamp: string, text: string): CollectedWhatsAppMessage {
  return {
    MsgID: id,
    Timestamp: timestamp,
    Text: text,
    ChatName: 'Ops',
    SenderName: 'Joe',
    ChatJID: null,
    SenderJID: 'joe@jid',
    FromMe: false,
    store_key: 'personal',
    store_path: '/stores/personal',
  };
}

function commitment(who: string, owesWhat: string, sourceMessageId: string, confidence: number): StructuredCommitment {
  return {
    who,
    owes_what: owesWhat,
    to_whom: 'Abhi',
    by_when: null,
    confidence,
    type: 'commitment',
    source_message_id: sourceMessageId,
  };
}
