import { afterEach, describe, expect, test } from 'bun:test';
import type { Operation, OperationContext } from '../../src/core/operations.ts';
import { PGLiteEngine } from '../../src/core/pglite-engine.ts';
import { actionBrainOperations } from '../../src/action-brain/operations.ts';

let engine: PGLiteEngine | null = null;

afterEach(async () => {
  if (engine) {
    await engine.disconnect();
    engine = null;
  }
});

function getActionOperation(name: string): Operation {
  const operation = actionBrainOperations.find((op) => op.name === name);
  if (!operation) {
    throw new Error(`Missing action operation: ${name}`);
  }
  return operation;
}

async function createActionContext(): Promise<OperationContext> {
  engine = new PGLiteEngine();
  await engine.connect({ engine: 'pglite' } as any);
  await engine.initSchema();

  const ctx: OperationContext = {
    engine,
    config: { engine: 'pglite' } as any,
    logger: { info: () => {}, warn: () => {}, error: () => {} },
    dryRun: false,
  };

  // Ensures Action Brain tables exist.
  await getActionOperation('action_list').handler(ctx, {});
  return ctx;
}

describe('action_brief entity linking', () => {
  test('embeds resolved source_contact entity slug in brief output', async () => {
    const ctx = await createActionContext();
    const actionIngest = getActionOperation('action_ingest');
    const actionBrief = getActionOperation('action_brief');

    await engine!.putPage('people/joe', {
      type: 'person',
      title: 'Joe',
      compiled_truth: 'Joe handles operations follow-ups.',
      timeline: '',
    });
    await engine!.upsertChunks('people/joe', [
      {
        chunk_index: 0,
        chunk_source: 'compiled_truth',
        chunk_text: 'Joe handles operations follow-ups.',
      },
    ]);

    await actionIngest.handler(ctx, {
      messages: [
        {
          ChatName: 'Operations',
          SenderName: 'Joe',
          Timestamp: '2026-04-16T08:00:00.000Z',
          Text: 'Please send the final shipment docs.',
          MsgID: 'm1',
        },
      ],
      commitments: [
        {
          who: 'Joe',
          owes_what: 'Send the final shipment docs',
          to_whom: 'Abhi',
          by_when: null,
          confidence: 0.95,
          type: 'commitment',
          source_message_id: 'm1',
        },
      ],
    });

    const result = await actionBrief.handler(ctx, {
      now: '2026-04-16T12:00:00.000Z',
      last_sync_at: '2026-04-16T11:30:00.000Z',
    });

    expect(result.brief).toContain('source_contact=[Joe](people/joe)');
  });

  test('keeps plain source_contact when no entity page can be resolved', async () => {
    const ctx = await createActionContext();
    const actionIngest = getActionOperation('action_ingest');
    const actionBrief = getActionOperation('action_brief');

    await actionIngest.handler(ctx, {
      messages: [
        {
          ChatName: 'Operations',
          SenderName: 'Ghost Person',
          Timestamp: '2026-04-16T08:00:00.000Z',
          Text: 'I will send the report.',
          MsgID: 'm1',
        },
      ],
      commitments: [
        {
          who: 'Ghost Person',
          owes_what: 'Send the report',
          to_whom: 'Abhi',
          by_when: null,
          confidence: 0.95,
          type: 'commitment',
          source_message_id: 'm1',
        },
      ],
    });

    const result = await actionBrief.handler(ctx, {
      now: '2026-04-16T12:00:00.000Z',
      last_sync_at: '2026-04-16T11:30:00.000Z',
    });

    expect(result.brief).toContain('source_contact=Ghost Person');
    expect(result.brief).not.toContain('source_contact=[Ghost Person](');
  });
});
