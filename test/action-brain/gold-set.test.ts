import { describe, expect, test } from 'bun:test';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';

interface WhatsAppMessage {
  ChatName: string;
  SenderName: string;
  Timestamp: string;
  Text: string;
  MsgID: string;
}

type ExpectedType = 'owed_by_me' | 'waiting_on';

interface ExpectedCommitment {
  who: string;
  action: string;
  type: ExpectedType;
}

interface BaselineCommitment {
  who: string | null;
  owes_what: string;
  type: 'commitment' | 'follow_up' | 'delegation' | 'decision' | 'question';
  confidence?: number;
  source_message_id?: string | null;
}

interface GoldSetRow {
  id: string;
  message: WhatsAppMessage;
  expectedCommitments: ExpectedCommitment[];
  baselineCommitments: BaselineCommitment[];
}

const OWNER_ALIASES = ['abhinav bansal', 'abbhinaav', 'abhi', 'abhinav'];
const CHECKED_IN_GOLD_SET_PATH = resolve(import.meta.dir, 'fixtures/gold-set.jsonl');
const PRIVATE_GOLD_SET_PATH = process.env.ACTION_BRAIN_PRIVATE_GOLD_SET_PATH;

function loadGoldSet(path: string): GoldSetRow[] {
  const raw = readFileSync(path, 'utf8');
  const rows: GoldSetRow[] = [];

  for (const [index, line] of raw.split(/\r?\n/).entries()) {
    const trimmed = line.trim();
    if (!trimmed) continue;

    const parsed = JSON.parse(trimmed) as Partial<GoldSetRow>;
    if (!parsed.id || !parsed.message || !Array.isArray(parsed.expectedCommitments) || !Array.isArray(parsed.baselineCommitments)) {
      throw new Error(`Invalid gold-set row at line ${index + 1}`);
    }

    rows.push(parsed as GoldSetRow);
  }

  return rows;
}

function normalizeText(value: string): string {
  return value.toLowerCase().replace(/[^a-z0-9\s]/g, ' ').replace(/\s+/g, ' ').trim();
}

function isOwnerName(name: string): boolean {
  const normalized = normalizeText(name);
  return OWNER_ALIASES.some((alias) => normalized.includes(alias));
}

function responsibilityType(commitment: BaselineCommitment): ExpectedType {
  const who = commitment.who ?? '';
  return isOwnerName(who) ? 'owed_by_me' : 'waiting_on';
}

function namesMatch(expectedWho: string, predictedWho: string | null): boolean {
  if (!predictedWho) return false;

  const expected = normalizeText(expectedWho);
  const predicted = normalizeText(predictedWho);
  if (!expected || !predicted) return false;

  if (predicted.includes(expected) || expected.includes(predicted)) {
    return true;
  }

  return isOwnerName(expected) && isOwnerName(predicted);
}

function actionsMatch(expectedAction: string, predictedAction: string): boolean {
  const expected = normalizeText(expectedAction);
  const predicted = normalizeText(predictedAction);
  if (!expected || !predicted) return false;

  return predicted.includes(expected) || expected.includes(predicted);
}

function computeRecallMetrics(rows: GoldSetRow[]): {
  totalExpected: number;
  totalMatched: number;
  totalPredicted: number;
  falsePositives: number;
  recall: number;
  precision: number;
} {
  let totalExpected = 0;
  let totalMatched = 0;
  let totalPredicted = 0;
  let falsePositives = 0;

  for (const row of rows) {
    const matchedPredictedIndexes = new Set<number>();
    totalExpected += row.expectedCommitments.length;
    totalPredicted += row.baselineCommitments.length;

    for (const expected of row.expectedCommitments) {
      let matched = false;

      for (let i = 0; i < row.baselineCommitments.length; i += 1) {
        if (matchedPredictedIndexes.has(i)) continue;

        const predicted = row.baselineCommitments[i];
        const whoMatch = namesMatch(expected.who, predicted.who);
        const actionMatch = actionsMatch(expected.action, predicted.owes_what);
        const typeMatch = responsibilityType(predicted) === expected.type;

        if (whoMatch && actionMatch && typeMatch) {
          matchedPredictedIndexes.add(i);
          totalMatched += 1;
          matched = true;
          break;
        }
      }

      if (!matched) {
        // Missed expected commitment, counted implicitly by recall denominator.
      }
    }

    falsePositives += row.baselineCommitments.length - matchedPredictedIndexes.size;
  }

  const recall = totalExpected === 0 ? 1 : totalMatched / totalExpected;
  const precision = totalPredicted === 0 ? 1 : totalMatched / totalPredicted;

  return {
    totalExpected,
    totalMatched,
    totalPredicted,
    falsePositives,
    recall,
    precision,
  };
}

describe('action-brain checked-in gold set recall gate', () => {
  test('loads the checked-in 13-message JSONL fixture', () => {
    const rows = loadGoldSet(CHECKED_IN_GOLD_SET_PATH);
    expect(rows.length).toBe(13);

    const expectedCount = rows.reduce((sum, row) => sum + row.expectedCommitments.length, 0);
    expect(expectedCount).toBeGreaterThan(0);
  });

  test('enforces >=90% recall on the checked-in evaluation set', () => {
    const rows = loadGoldSet(CHECKED_IN_GOLD_SET_PATH);
    const metrics = computeRecallMetrics(rows);

    console.log(
      `[gold-set] expected=${metrics.totalExpected} matched=${metrics.totalMatched} ` +
      `predicted=${metrics.totalPredicted} false_positives=${metrics.falsePositives} ` +
      `recall=${metrics.recall.toFixed(3)} precision=${metrics.precision.toFixed(3)}`
    );

    // CI gate for GIT-175: fail the unit lane when recall drops below 90%.
    expect(metrics.recall).toBeGreaterThanOrEqual(0.9);
  });

  test('validates private gold-set contract when ACTION_BRAIN_PRIVATE_GOLD_SET_PATH is set', () => {
    if (!PRIVATE_GOLD_SET_PATH) {
      expect(true).toBe(true);
      return;
    }

    const privateRows = loadGoldSet(PRIVATE_GOLD_SET_PATH);
    expect(privateRows.length).toBeGreaterThanOrEqual(50);

    const expectedCount = privateRows.reduce((sum, row) => sum + row.expectedCommitments.length, 0);
    expect(expectedCount).toBeGreaterThan(0);
  });
});
