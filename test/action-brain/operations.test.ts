import { describe, expect, setDefaultTimeout, test } from 'bun:test';
import { mkdtempSync, rmSync } from 'fs';
import { join } from 'path';
import { tmpdir } from 'os';
import { mergeOperationSets, operations } from '../../src/core/operations.ts';
import type { Operation, OperationContext } from '../../src/core/operations.ts';
import { PGLiteEngine } from '../../src/core/pglite-engine.ts';
import { actionBrainOperations } from '../../src/action-brain/operations.ts';
import { writeWacliCollectorCheckpoint } from '../../src/action-brain/collector.ts';

setDefaultTimeout(15_000);

function makeOperation(name: string, cliName?: string): Operation {
  return {
    name,
    description: `Operation ${name}`,
    params: {},
    handler: async () => ({ ok: true }),
    cliHints: cliName ? { name: cliName } : undefined,
  };
}

interface EngineWithDb {
  db: {
    query: (sql: string, params?: unknown[]) => Promise<{ rows: Array<Record<string, unknown>> }>;
  };
}

function getActionOperation(name: string): Operation {
  const operation = actionBrainOperations.find((op) => op.name === name);
  if (!operation) {
    throw new Error(`Missing action operation: ${name}`);
  }
  return operation;
}

async function withActionContext<T>(fn: (ctx: OperationContext, engine: PGLiteEngine) => Promise<T>): Promise<T> {
  const engine = new PGLiteEngine();
  await engine.connect({ engine: 'pglite' } as any);

  const ctx: OperationContext = {
    engine,
    config: { engine: 'pglite' } as any,
    logger: { info: () => {}, warn: () => {}, error: () => {} },
    dryRun: false,
  };

  try {
    return await fn(ctx, engine);
  } finally {
    await engine.disconnect();
  }
}

describe('Action Brain operation integration', () => {
  test('#22 registers Action Brain operations in the shared contract', () => {
    const names = new Set(operations.map((op) => op.name));
    expect(names.has('action_list')).toBe(true);
    expect(names.has('action_brief')).toBe(true);
    expect(names.has('action_resolve')).toBe(true);
    expect(names.has('action_mark_fp')).toBe(true);
    expect(names.has('action_ingest')).toBe(true);
    expect(names.has('action_ingest_auto')).toBe(true);
  });

  test('action_ingest_auto exposes fail_on_degraded scheduler guard parameter', () => {
    const op = getActionOperation('action_ingest_auto');
    expect(op.params.fail_on_degraded).toBeDefined();
    expect(op.params.fail_on_degraded?.type).toBe('boolean');
  });

  test('#23 mergeOperationSets fails fast on operation and CLI collisions', () => {
    expect(() =>
      mergeOperationSets([makeOperation('alpha', 'alpha-cmd')], [makeOperation('alpha', 'beta-cmd')])
    ).toThrow(/Duplicate operation name/);

    expect(() =>
      mergeOperationSets([makeOperation('alpha', 'shared-cmd')], [makeOperation('beta', 'shared-cmd')])
    ).toThrow(/Duplicate CLI command name/);
  });

  test('#24 supports grouped action CLI commands via "gbrain action <subcommand>"', async () => {
    const proc = Bun.spawn(['bun', 'run', 'src/cli.ts', 'action', 'list', '--help'], {
      cwd: new URL('../..', import.meta.url).pathname,
      stdout: 'pipe',
      stderr: 'pipe',
    });

    const stdout = await new Response(proc.stdout).text();
    const exitCode = await proc.exited;

    expect(exitCode).toBe(0);
    expect(stdout).toContain('Usage: gbrain action list');
  });

  test('supports grouped action auto-ingest command via "gbrain action run"', async () => {
    const proc = Bun.spawn(['bun', 'run', 'src/cli.ts', 'action', 'run', '--help'], {
      cwd: new URL('../..', import.meta.url).pathname,
      stdout: 'pipe',
      stderr: 'pipe',
    });

    const stdout = await new Response(proc.stdout).text();
    const exitCode = await proc.exited;

    expect(exitCode).toBe(0);
    expect(stdout).toContain('Usage: gbrain action run');
    expect(stdout).toContain('--fail-on-degraded');
  });

  test('action_ingest stays idempotent when commitments arrive in different output order', async () => {
    await withActionContext(async (ctx, engine) => {
      const actionIngest = getActionOperation('action_ingest');
      const messages = [
        { ChatName: 'Thread A', SenderName: 'Joe', Timestamp: '2026-04-16T08:00:00.000Z', Text: 'Send docs', MsgID: 'm1' },
        { ChatName: 'Thread B', SenderName: 'Mukesh', Timestamp: '2026-04-16T08:05:00.000Z', Text: 'Approve payout', MsgID: 'm2' },
      ];
      const commitmentA = {
        who: 'Joe',
        owes_what: 'Send docs',
        to_whom: 'Abhi',
        by_when: null,
        confidence: 0.9,
        type: 'commitment',
        source_message_id: 'm1',
      };
      const commitmentB = {
        who: 'Mukesh',
        owes_what: 'Approve payout',
        to_whom: 'Abhi',
        by_when: null,
        confidence: 0.9,
        type: 'commitment',
        source_message_id: 'm2',
      };

      await actionIngest.handler(ctx, { messages, commitments: [commitmentA, commitmentB] });
      await actionIngest.handler(ctx, { messages, commitments: [commitmentB, commitmentA] });

      const db = (engine as unknown as EngineWithDb).db;
      const rows = await db.query(
        `SELECT source_message_id, title
         FROM action_items
         ORDER BY source_message_id`
      );

      expect(rows.rows.length).toBe(2);
      expect(rows.rows.map((row) => row.title)).toEqual(['Send docs', 'Approve payout']);
    });
  });

  test('action_ingest keeps replay idempotent when extractor wording/type drifts for the same source message', async () => {
    await withActionContext(async (ctx, engine) => {
      const actionIngest = getActionOperation('action_ingest');
      const messages = [
        { ChatName: 'Operations', SenderName: 'Joe', Timestamp: '2026-04-16T08:00:00.000Z', Text: 'Send docs today', MsgID: 'm1' },
      ];

      const first = (await actionIngest.handler(ctx, {
        messages,
        commitments: [
          {
            who: 'Joe',
            owes_what: 'Send docs',
            to_whom: 'Abhi',
            by_when: null,
            confidence: 0.9,
            type: 'commitment',
            source_message_id: 'm1',
          },
        ],
      })) as { created_count: number };

      const second = (await actionIngest.handler(ctx, {
        messages,
        commitments: [
          {
            who: 'Joe',
            owes_what: 'Send the documents by end of day',
            to_whom: 'Abhi',
            by_when: null,
            confidence: 0.9,
            type: 'follow_up',
            source_message_id: 'm1',
          },
        ],
      })) as { created_count: number };

      const db = (engine as unknown as EngineWithDb).db;
      const rows = await db.query(
        `SELECT source_message_id, title
         FROM action_items
         ORDER BY source_message_id`
      );

      expect(first.created_count).toBe(1);
      expect(second.created_count).toBe(0);
      expect(rows.rows.length).toBe(1);
      expect(rows.rows[0].source_message_id).toBe('m1:ab:0');
    });
  });

  test('action_ingest uses source_message_id for source thread/contact traceability', async () => {
    await withActionContext(async (ctx, engine) => {
      const actionIngest = getActionOperation('action_ingest');
      const messages = [
        { ChatName: 'Operations', SenderName: 'Joe', Timestamp: '2026-04-16T08:00:00.000Z', Text: 'Two asks', MsgID: 'm1' },
        { ChatName: 'Other Thread', SenderName: 'Mukesh', Timestamp: '2026-04-16T08:05:00.000Z', Text: 'FYI', MsgID: 'm2' },
      ];
      const commitments = [
        {
          who: 'Joe',
          owes_what: 'Send docs',
          to_whom: 'Abhi',
          by_when: null,
          confidence: 0.9,
          type: 'commitment',
          source_message_id: 'm1',
        },
        {
          who: 'Joe',
          owes_what: 'Call port agent',
          to_whom: 'Abhi',
          by_when: null,
          confidence: 0.9,
          type: 'follow_up',
          source_message_id: 'm1',
        },
      ];

      await actionIngest.handler(ctx, { messages, commitments });

      const db = (engine as unknown as EngineWithDb).db;
      const rows = await db.query(
        `SELECT title, source_thread, source_contact
         FROM action_items
         ORDER BY title`
      );

      expect(rows.rows.length).toBe(2);
      expect(rows.rows.map((row) => row.source_thread)).toEqual(['Operations', 'Operations']);
      expect(rows.rows.map((row) => row.source_contact)).toEqual(['Joe', 'Joe']);
    });
  });

  test('action_ingest does not trust unknown source_message_id when multiple messages are present', async () => {
    await withActionContext(async (ctx, engine) => {
      const actionIngest = getActionOperation('action_ingest');
      const messages = [
        { ChatName: 'Ops A', SenderName: 'Joe', Timestamp: '2026-04-16T08:00:00.000Z', Text: 'Send docs', MsgID: 'm1' },
        {
          ChatName: 'Ops B',
          SenderName: 'Mukesh',
          Timestamp: '2026-04-16T08:05:00.000Z',
          Text: 'Approve payout',
          MsgID: 'm2',
        },
      ];
      const commitments = [
        {
          who: 'Joe',
          owes_what: 'Send docs',
          to_whom: 'Abhi',
          by_when: null,
          confidence: 0.9,
          type: 'commitment',
          source_message_id: 'hallucinated-id',
        },
      ];

      await actionIngest.handler(ctx, { messages, commitments });

      const db = (engine as unknown as EngineWithDb).db;
      const rows = await db.query(
        `SELECT source_message_id, source_thread, source_contact
         FROM action_items`
      );

      expect(rows.rows.length).toBe(1);
      expect(rows.rows[0].source_message_id).toMatch(/^batch:ab:/);
      expect(rows.rows[0].source_thread).toBe('');
      expect(rows.rows[0].source_contact).toBe('');
    });
  });

  test('action_ingest falls back to the only message when source_message_id is invalid', async () => {
    await withActionContext(async (ctx, engine) => {
      const actionIngest = getActionOperation('action_ingest');
      const messages = [
        { ChatName: 'Operations', SenderName: 'Joe', Timestamp: '2026-04-16T08:00:00.000Z', Text: 'Send docs', MsgID: 'm1' },
      ];
      const commitments = [
        {
          who: 'Joe',
          owes_what: 'Send docs',
          to_whom: 'Abhi',
          by_when: null,
          confidence: 0.9,
          type: 'commitment',
          source_message_id: 'unknown-msg-id',
        },
      ];

      await actionIngest.handler(ctx, { messages, commitments });

      const db = (engine as unknown as EngineWithDb).db;
      const rows = await db.query(
        `SELECT source_message_id, source_thread, source_contact
         FROM action_items`
      );

      expect(rows.rows.length).toBe(1);
      expect(rows.rows[0].source_message_id).toMatch(/^m1:ab:/);
      expect(rows.rows[0].source_thread).toBe('Operations');
      expect(rows.rows[0].source_contact).toBe('Joe');
    });
  });

  test('action_ingest keeps store-qualified source_message_id isolated when stores share the same MsgID', async () => {
    await withActionContext(async (ctx, engine) => {
      const actionIngest = getActionOperation('action_ingest');
      const messages = [
        {
          ChatName: 'Personal Ops',
          SenderName: 'Joe',
          Timestamp: '2026-04-16T08:00:00.000Z',
          Text: 'Send personal docs',
          MsgID: 'shared-msg',
          store_key: 'personal',
          store_path: '/stores/personal',
        },
        {
          ChatName: 'Business Ops',
          SenderName: 'Mukesh',
          Timestamp: '2026-04-16T08:05:00.000Z',
          Text: 'Send business docs',
          MsgID: 'shared-msg',
          store_key: 'business',
          store_path: '/stores/business',
        },
      ];
      const commitments = [
        {
          who: 'Joe',
          owes_what: 'Send personal docs',
          to_whom: 'Abhi',
          by_when: null,
          confidence: 0.9,
          type: 'commitment',
          source_message_id: 'personal::shared-msg',
        },
        {
          who: 'Mukesh',
          owes_what: 'Send business docs',
          to_whom: 'Abhi',
          by_when: null,
          confidence: 0.9,
          type: 'commitment',
          source_message_id: 'business::shared-msg',
        },
      ];

      await actionIngest.handler(ctx, { messages, commitments });

      const db = (engine as unknown as EngineWithDb).db;
      const rows = await db.query(
        `SELECT source_message_id, source_thread, source_contact
         FROM action_items
         ORDER BY source_message_id`
      );

      expect(rows.rows.length).toBe(2);
      expect(rows.rows.map((row) => row.source_message_id)).toEqual([
        'business::shared-msg:ab:0',
        'personal::shared-msg:ab:0',
      ]);
      expect(rows.rows.map((row) => row.source_thread)).toEqual(['Business Ops', 'Personal Ops']);
      expect(rows.rows.map((row) => row.source_contact)).toEqual(['Mukesh', 'Joe']);
    });
  });

  test('action_ingest rejects ambiguous bare source_message_id values across stores', async () => {
    await withActionContext(async (ctx, engine) => {
      const actionIngest = getActionOperation('action_ingest');
      const messages = [
        {
          ChatName: 'Personal Ops',
          SenderName: 'Joe',
          Timestamp: '2026-04-16T08:00:00.000Z',
          Text: 'Send docs',
          MsgID: 'shared-msg',
          store_key: 'personal',
          store_path: '/stores/personal',
        },
        {
          ChatName: 'Business Ops',
          SenderName: 'Joe',
          Timestamp: '2026-04-16T08:05:00.000Z',
          Text: 'Send docs',
          MsgID: 'shared-msg',
          store_key: 'business',
          store_path: '/stores/business',
        },
      ];
      const commitments = [
        {
          who: 'Joe',
          owes_what: 'Send docs',
          to_whom: 'Abhi',
          by_when: null,
          confidence: 0.9,
          type: 'commitment',
          source_message_id: 'shared-msg',
        },
        {
          who: 'Joe',
          owes_what: 'Send docs',
          to_whom: 'Abhi',
          by_when: null,
          confidence: 0.9,
          type: 'commitment',
          source_message_id: 'shared-msg',
        },
      ];

      await expect(actionIngest.handler(ctx, { messages, commitments })).rejects.toThrow(
        'Ambiguous source_message_id: shared-msg matches multiple store-qualified messages in this batch.'
      );

      const db = (engine as unknown as EngineWithDb).db;
      const rows = await db.query(`SELECT source_message_id FROM action_items`);
      expect(rows.rows).toEqual([]);
    });
  });

  test('action_brief resolves freshness from wacli checkpoint when last_sync_at is omitted', async () => {
    await withActionContext(async (ctx) => {
      const actionBrief = getActionOperation('action_brief');
      const tempDir = mkdtempSync(join(tmpdir(), 'action-brief-checkpoint-test-'));
      const checkpointPath = join(tempDir, 'wacli-checkpoint.json');

      try {
        await writeWacliCollectorCheckpoint(checkpointPath, {
          version: 1,
          stores: {
            personal: {
              after: '2026-04-16T11:30:00.000Z',
              message_ids_at_after: ['m1'],
              updated_at: '2026-04-16T11:31:00.000Z',
            },
          },
        });

        const result = (await actionBrief.handler(ctx, {
          now: '2026-04-16T12:00:00.000Z',
          checkpoint_path: checkpointPath,
        })) as { brief: string };

        expect(result.brief).toContain('wacli freshness: last sync 2026-04-16T11:30:00.000Z (0.5h ago)');
      } finally {
        rmSync(tempDir, { recursive: true, force: true });
      }
    });
  });
});
