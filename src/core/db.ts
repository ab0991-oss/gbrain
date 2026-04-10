import postgres from 'postgres';
import { readFileSync } from 'fs';
import { join, dirname } from 'path';
import { GBrainError, type EngineConfig } from './types.ts';

let sql: ReturnType<typeof postgres> | null = null;

export function getConnection(): ReturnType<typeof postgres> {
  if (!sql) {
    throw new GBrainError(
      'No database connection',
      'connect() has not been called',
      'Run gbrain init --supabase or gbrain init --url <connection_string>',
    );
  }
  return sql;
}

export async function connect(config: EngineConfig): Promise<void> {
  if (sql) return;

  const url = config.database_url;
  if (!url) {
    throw new GBrainError(
      'No database URL',
      'database_url is missing from config',
      'Run gbrain init --supabase or gbrain init --url <connection_string>',
    );
  }

  try {
    sql = postgres(url, {
      max: 10,
      idle_timeout: 20,
      connect_timeout: 10,
      types: {
        // Register pgvector type
        bigint: postgres.BigInt,
      },
    });

    // Test connection
    await sql`SELECT 1`;
  } catch (e: unknown) {
    sql = null;
    const msg = e instanceof Error ? e.message : String(e);
    throw new GBrainError(
      'Cannot connect to database',
      msg,
      'Check your connection URL in ~/.gbrain/config.json',
    );
  }
}

export async function disconnect(): Promise<void> {
  if (sql) {
    await sql.end();
    sql = null;
  }
}

export async function initSchema(): Promise<void> {
  const conn = getConnection();

  // Read schema SQL and execute as a single statement.
  // The postgres driver handles multi-statement SQL natively, including
  // PL/pgSQL functions with $$ delimiter blocks that contain semicolons.
  // The schema uses IF NOT EXISTS / CREATE OR REPLACE for idempotency.
  const schemaPath = join(dirname(new URL(import.meta.url).pathname), '..', 'schema.sql');
  const schemaSql = readFileSync(schemaPath, 'utf-8');

  await conn.unsafe(schemaSql);
}

export async function withTransaction<T>(fn: (tx: ReturnType<typeof postgres>) => Promise<T>): Promise<T> {
  const conn = getConnection();
  return conn.begin(async (tx) => {
    return fn(tx as unknown as ReturnType<typeof postgres>);
  });
}
