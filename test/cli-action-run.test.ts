import { describe, expect, test } from 'bun:test';

const repoRoot = new URL('..', import.meta.url).pathname;
const preloadPath = new URL('./fixtures/cli-action-run.preload.ts', import.meta.url).pathname;

function spawnActionRun(result: unknown) {
  return Bun.spawn(['bun', '--preload', preloadPath, 'src/cli.ts', 'action', 'run'], {
    cwd: repoRoot,
    stdout: 'pipe',
    stderr: 'pipe',
    env: {
      ...process.env,
      GBRAIN_TEST_ACTION_RUN_RESULT: JSON.stringify(result),
    },
  });
}

describe('gbrain action run exit codes', () => {
  test('exits 1 when action_ingest_auto returns success=false', async () => {
    const proc = spawnActionRun({
      success: false,
      degraded: false,
      failure: { stage: 'health', message: 'wacli health check failed' },
    });

    const stdout = await new Response(proc.stdout).text();
    const stderr = await new Response(proc.stderr).text();
    const exitCode = await proc.exited;

    expect(exitCode).toBe(1);
    expect(stderr).toBe('');
    expect(JSON.parse(stdout)).toMatchObject({
      success: false,
      failure: { stage: 'health', message: 'wacli health check failed' },
    });
  });

  test('exits 0 when action_ingest_auto returns success=true', async () => {
    const proc = spawnActionRun({
      success: true,
      degraded: false,
      failure: null,
    });

    const stdout = await new Response(proc.stdout).text();
    const stderr = await new Response(proc.stderr).text();
    const exitCode = await proc.exited;

    expect(exitCode).toBe(0);
    expect(stderr).toBe('');
    expect(JSON.parse(stdout)).toMatchObject({
      success: true,
      failure: null,
    });
  });
});
