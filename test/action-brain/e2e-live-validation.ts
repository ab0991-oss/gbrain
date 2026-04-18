/**
 * End-to-end extractor validation harness.
 *
 * Uses a sanitized built-in gold set by default to avoid committing sensitive
 * WhatsApp content. For real-message validation, set:
 *   ACTION_BRAIN_LIVE_GOLDSET_PATH=/absolute/path/to/gold-set.json
 *
 * Run: source ~/.zshrc && bun run test/action-brain/e2e-live-validation.ts
 */

import { readFile } from 'node:fs/promises';
import { extractCommitments, type StructuredCommitment, type WhatsAppMessage } from '../../src/action-brain/extractor.ts';

interface ExpectedCommitment {
  who: string;
  action: string;
  type: string;
}

export interface GoldSetEntry {
  message: WhatsAppMessage;
  expectedCommitments: ExpectedCommitment[];
}

interface ValidationTotals {
  expected: number;
  extracted: number;
  matched: number;
  missed: number;
  falsePositives: number;
}

export interface ValidationSummary {
  totals: ValidationTotals;
  recall: number;
  precision: number;
  f1: number;
  threshold: number;
  passed: boolean;
}

export interface RunValidationOptions {
  ownerName?: string;
  ownerAliases?: string[];
  threshold?: number;
}

const DEFAULT_THRESHOLD = 0.9;

export const DEFAULT_GOLD_SET: GoldSetEntry[] = [
  {
    message: {
      MsgID: 'SAN-001',
      ChatName: 'Ops Thread',
      SenderName: 'Jordan',
      Timestamp: '2026-04-16T08:00:00Z',
      Text: 'I will send the signed invoice bundle by 5pm today.',
    },
    expectedCommitments: [
      { who: 'Jordan', action: 'send the signed invoice bundle', type: 'waiting_on' },
    ],
  },
  {
    message: {
      MsgID: 'SAN-002',
      ChatName: 'Ops Thread',
      SenderName: 'Taylor',
      Timestamp: '2026-04-16T08:10:00Z',
      Text: 'Please approve this transfer before noon.',
    },
    expectedCommitments: [
      { who: 'Owner', action: 'approve this transfer', type: 'owed_by_me' },
    ],
  },
  {
    message: {
      MsgID: 'SAN-003',
      ChatName: 'Vendors',
      SenderName: 'Morgan',
      Timestamp: '2026-04-16T08:20:00Z',
      Text: 'I will call the rail dispatcher and confirm the slot this morning.',
    },
    expectedCommitments: [
      { who: 'Morgan', action: 'call the rail dispatcher', type: 'waiting_on' },
    ],
  },
  {
    message: {
      MsgID: 'SAN-004',
      ChatName: 'Finance',
      SenderName: 'Casey',
      Timestamp: '2026-04-16T08:30:00Z',
      Text: 'Could you upload the purchase order copy when you are free?',
    },
    expectedCommitments: [
      { who: 'Owner', action: 'upload the purchase order copy', type: 'owed_by_me' },
    ],
  },
  {
    message: {
      MsgID: 'SAN-005',
      ChatName: 'Ops Thread',
      SenderName: 'Jordan',
      Timestamp: '2026-04-16T08:40:00Z',
      Text: 'Shipment 14 reached the yard. No action required.',
    },
    expectedCommitments: [],
  },
  {
    message: {
      MsgID: 'SAN-006',
      ChatName: 'Ops Thread',
      SenderName: 'Jordan',
      Timestamp: '2026-04-16T08:50:00Z',
      Text: 'Please ask Alex to send the customs declaration by EOD.',
    },
    expectedCommitments: [
      { who: 'Alex', action: 'send the customs declaration', type: 'waiting_on' },
    ],
  },
  {
    message: {
      MsgID: 'SAN-007',
      ChatName: 'Bank',
      SenderName: 'Bank Desk',
      Timestamp: '2026-04-16T09:00:00Z',
      Text: 'Payment is pending. Kindly pay immediately to avoid suspension.',
    },
    expectedCommitments: [
      { who: 'Owner', action: 'pay immediately', type: 'owed_by_me' },
    ],
  },
  {
    message: {
      MsgID: 'SAN-008',
      ChatName: 'Ops Thread',
      SenderName: 'Morgan',
      Timestamp: '2026-04-16T09:10:00Z',
      Text: 'I initiated the transfer, please check and authorised it as trial payment.',
    },
    expectedCommitments: [
      { who: 'Owner', action: 'authorize it', type: 'owed_by_me' },
    ],
  },
];

const OWNER_NAMES = ['owner', 'abhinav bansal', 'abbhinaav', 'abhi', 'abhinav'];

export function isOwnerName(name: string): boolean {
  const normalized = name.trim().toLowerCase();
  return OWNER_NAMES.some((owner) => normalized.includes(owner));
}

export function normalizeActionText(text: string): string {
  return text
    .toLowerCase()
    .replace(/authoris(e|ed|ing)?/g, 'authoriz$1')
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

const ACTION_STOPWORDS = new Set([
  'a',
  'an',
  'the',
  'this',
  'that',
  'to',
  'for',
  'and',
  'of',
  'on',
  'in',
  'at',
  'by',
  'with',
  'before',
  'after',
  'today',
  'tomorrow',
  'now',
  'please',
  'kindly',
  'immediately',
  'it',
  'you',
  'me',
  'us',
  'them',
]);

function extractActionTokens(text: string): string[] {
  return normalizeActionText(text)
    .split(' ')
    .map((token) => token.trim())
    .filter((token) => token.length > 0 && !ACTION_STOPWORDS.has(token));
}

export function isTypeCompatible(expectedType: string, extractedType: string): boolean {
  if (expectedType === extractedType) {
    return true;
  }

  if (expectedType === 'owed_by_me') {
    return ['commitment', 'delegation', 'follow_up'].includes(extractedType);
  }

  if (expectedType === 'waiting_on') {
    return ['commitment', 'delegation', 'follow_up'].includes(extractedType);
  }

  return false;
}

export function matchCommitment(extracted: StructuredCommitment, expected: ExpectedCommitment): boolean {
  const extractedWho = (extracted.who ?? '').toLowerCase();
  const expectedWho = expected.who.toLowerCase();

  const whoMatch =
    extractedWho.includes(expectedWho) ||
    (isOwnerName(expectedWho) && isOwnerName(extractedWho));

  const normalizedExtractedAction = normalizeActionText(extracted.owes_what ?? '');
  const normalizedExpectedAction = normalizeActionText(expected.action);
  const exactActionMatch = normalizedExpectedAction.length > 0 && normalizedExtractedAction.includes(normalizedExpectedAction);

  const expectedTokens = extractActionTokens(expected.action);
  const extractedTokenSet = new Set(extractActionTokens(extracted.owes_what ?? ''));
  const tokenCoverageMatch =
    expectedTokens.length > 0 &&
    expectedTokens.every((token) => extractedTokenSet.has(token));

  const actionMatch = exactActionMatch || tokenCoverageMatch;

  const typeMatch = isTypeCompatible(expected.type, extracted.type);

  // Strict matching: actor + action + type compatibility must all pass.
  return whoMatch && actionMatch && typeMatch;
}

export async function loadGoldSet(path: string): Promise<GoldSetEntry[]> {
  const raw = await readFile(path, 'utf8');
  const parsed = JSON.parse(raw);

  if (!Array.isArray(parsed)) {
    throw new Error(`Gold set at ${path} must be a JSON array.`);
  }

  return parsed as GoldSetEntry[];
}

export async function runValidation(
  goldSet: GoldSetEntry[],
  options: RunValidationOptions = {}
): Promise<ValidationSummary> {
  console.log('=== Action Brain E2E Validation ===\n');
  console.log(`Gold set: ${goldSet.length} messages, ${goldSet.reduce((n, g) => n + g.expectedCommitments.length, 0)} expected commitments\n`);

  let totalExpected = 0;
  let totalMatched = 0;
  let totalExtracted = 0;
  let totalFalsePositives = 0;
  let totalMissed = 0;

  for (const entry of goldSet) {
    const { message, expectedCommitments } = entry;

    try {
      const extracted = await extractCommitments([message], {
        ownerName: options.ownerName ?? 'Abhinav Bansal',
        ownerAliases: options.ownerAliases ?? ['Abbhinaav', 'Abhi', 'Abhinav', 'Owner'],
      });

      totalExtracted += extracted.length;
      totalExpected += expectedCommitments.length;

      const matched = new Set<number>();
      const missedExpected: string[] = [];

      for (const expected of expectedCommitments) {
        let found = false;

        for (let i = 0; i < extracted.length; i += 1) {
          if (!matched.has(i) && matchCommitment(extracted[i], expected)) {
            matched.add(i);
            found = true;
            totalMatched += 1;
            break;
          }
        }

        if (!found) {
          missedExpected.push(`${expected.who}: ${expected.action} (${expected.type})`);
          totalMissed += 1;
        }
      }

      const falsePositives = extracted.length - matched.size;
      totalFalsePositives += falsePositives;

      const status = missedExpected.length === 0 && falsePositives === 0
        ? 'PASS'
        : missedExpected.length === 0
          ? 'WARN'
          : 'FAIL';

      console.log(`[${status}] [${message.SenderName}] "${message.Text.substring(0, 60)}..."`);
      console.log(`   Expected: ${expectedCommitments.length} | Extracted: ${extracted.length} | Matched: ${matched.size} | FP: ${falsePositives}`);

      if (missedExpected.length > 0) {
        console.log(`   MISSED: ${missedExpected.join(', ')}`);
      }

      if (falsePositives > 0) {
        const unexpectedItems = extracted.filter((_, i) => !matched.has(i));
        for (const item of unexpectedItems) {
          console.log(`   UNEXPECTED: ${item.who}: ${item.owes_what} (${item.type}, conf=${item.confidence})`);
        }
      }

      console.log();
    } catch (error) {
      console.log(`[FAIL] [${message.SenderName}] ERROR: ${String(error)}`);
      totalExpected += expectedCommitments.length;
      totalMissed += expectedCommitments.length;
      console.log();
    }
  }

  const recall = totalExpected > 0 ? totalMatched / totalExpected : 0;
  const precision = totalExtracted > 0 ? totalMatched / totalExtracted : 0;
  const f1 = precision + recall > 0 ? (2 * precision * recall) / (precision + recall) : 0;

  const threshold = options.threshold ?? DEFAULT_THRESHOLD;
  const passed = recall >= threshold;

  console.log('=== SUMMARY ===');
  console.log(`Total expected: ${totalExpected}`);
  console.log(`Total extracted: ${totalExtracted}`);
  console.log(`Total matched: ${totalMatched}`);
  console.log(`Total missed: ${totalMissed}`);
  console.log(`Total false positives: ${totalFalsePositives}`);
  console.log(`Recall: ${(recall * 100).toFixed(1)}%`);
  console.log(`Precision: ${(precision * 100).toFixed(1)}%`);
  console.log(`F1: ${(f1 * 100).toFixed(1)}%`);
  console.log(`\nTarget: ${(threshold * 100).toFixed(1)}% recall`);
  console.log(`Verdict: ${passed ? 'PASS' : 'FAIL'}`);

  return {
    totals: {
      expected: totalExpected,
      extracted: totalExtracted,
      matched: totalMatched,
      missed: totalMissed,
      falsePositives: totalFalsePositives,
    },
    recall,
    precision,
    f1,
    threshold,
    passed,
  };
}

async function main() {
  if (!process.env.ANTHROPIC_API_KEY) {
    console.log('SKIP: ANTHROPIC_API_KEY is not set. Live extraction validation requires Anthropic credentials.');
    return;
  }

  const goldSetPath = process.env.ACTION_BRAIN_LIVE_GOLDSET_PATH;
  const goldSet = goldSetPath ? await loadGoldSet(goldSetPath) : DEFAULT_GOLD_SET;
  const summary = await runValidation(goldSet);

  if (!summary.passed) {
    process.exitCode = 1;
  }
}

if (import.meta.main) {
  main().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}
