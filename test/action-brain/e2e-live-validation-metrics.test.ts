import { describe, expect, test } from 'bun:test';
import type { StructuredCommitment } from '../../src/action-brain/extractor.ts';
import { matchCommitment } from './e2e-live-validation.ts';

function extracted(fields: Partial<StructuredCommitment>): StructuredCommitment {
  return {
    who: fields.who ?? null,
    owes_what: fields.owes_what ?? '',
    to_whom: fields.to_whom ?? null,
    by_when: fields.by_when ?? null,
    confidence: fields.confidence ?? 0.8,
    type: fields.type ?? 'commitment',
    source_message_id: fields.source_message_id ?? null,
  };
}

describe('e2e live validation matching', () => {
  test('matches owner aliases when action and type are compatible', () => {
    const result = matchCommitment(
      extracted({
        who: 'Abhinav Bansal',
        owes_what: 'Pay overdue landlord amount today',
        type: 'commitment',
      }),
      {
        who: 'Abbhinaav',
        action: 'pay',
        type: 'owed_by_me',
      }
    );

    expect(result).toBe(true);
  });

  test('does not match when action differs even if actor and type match', () => {
    const result = matchCommitment(
      extracted({
        who: 'Parathan',
        owes_what: 'Send shipment manifest to port team',
        type: 'commitment',
      }),
      {
        who: 'Parathan',
        action: 'bank account',
        type: 'waiting_on',
      }
    );

    expect(result).toBe(false);
  });

  test('does not match when actor differs', () => {
    const result = matchCommitment(
      extracted({
        who: 'Joe MacPherson',
        owes_what: 'Meet at the restaurant bar',
        type: 'follow_up',
      }),
      {
        who: 'Parathan',
        action: 'meet',
        type: 'waiting_on',
      }
    );

    expect(result).toBe(false);
  });

  test('matches action spelling variants (authorise/authorize)', () => {
    const result = matchCommitment(
      extracted({
        who: 'Abhinav Bansal',
        owes_what: 'Check and authorize the trial payment',
        type: 'delegation',
      }),
      {
        who: 'Abbhinaav',
        action: 'authoris',
        type: 'owed_by_me',
      }
    );

    expect(result).toBe(true);
  });
});
