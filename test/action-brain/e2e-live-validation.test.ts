import { describe, expect, test } from 'bun:test';
import { matchCommitment, normalizeActionText, isTypeCompatible } from './e2e-live-validation.ts';
import type { StructuredCommitment } from '../../src/action-brain/extractor.ts';

function extracted(overrides: Partial<StructuredCommitment>): StructuredCommitment {
  return {
    who: overrides.who ?? null,
    owes_what: overrides.owes_what ?? '',
    to_whom: overrides.to_whom ?? null,
    by_when: overrides.by_when ?? null,
    confidence: overrides.confidence ?? 0.9,
    type: overrides.type ?? 'commitment',
    source_message_id: overrides.source_message_id ?? null,
  };
}

describe('e2e live-validation matching', () => {
  test('#1 strict match requires action text, not just actor + type', () => {
    const result = matchCommitment(
      extracted({ who: 'Jordan', owes_what: 'send vessel docs', type: 'commitment' }),
      { who: 'Jordan', action: 'send invoice bundle', type: 'waiting_on' }
    );

    expect(result).toBe(false);
  });

  test('#2 action normalization treats authorise and authorize as equivalent', () => {
    expect(normalizeActionText('Please authorised trial payment.')).toBe('please authorized trial payment');

    const result = matchCommitment(
      extracted({ who: 'Abhinav Bansal', owes_what: 'Please authorize trial payment now', type: 'commitment' }),
      { who: 'Owner', action: 'authorise trial payment', type: 'owed_by_me' }
    );

    expect(result).toBe(true);
  });

  test('#3 type compatibility allows owed_by_me/waiting_on mapping only for actionable types', () => {
    expect(isTypeCompatible('owed_by_me', 'commitment')).toBe(true);
    expect(isTypeCompatible('waiting_on', 'follow_up')).toBe(true);
    expect(isTypeCompatible('waiting_on', 'question')).toBe(false);
    expect(isTypeCompatible('owed_by_me', 'decision')).toBe(false);
  });
});
