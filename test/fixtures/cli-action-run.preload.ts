import { mock } from 'bun:test';

const configModule = new URL('../../src/core/config.ts', import.meta.url).pathname;
const operationsModule = new URL('../../src/core/operations.ts', import.meta.url).pathname;
const engineFactoryModule = new URL('../../src/core/engine-factory.ts', import.meta.url).pathname;

class MockOperationError extends Error {
  code: string;
  suggestion?: string;

  constructor(code: string, message: string, suggestion?: string) {
    super(message);
    this.name = 'OperationError';
    this.code = code;
    this.suggestion = suggestion;
  }
}

mock.module(configModule, () => ({
  loadConfig: () => ({ engine: 'pglite' }),
  toEngineConfig: (config: unknown) => config,
}));

mock.module(operationsModule, () => ({
  OperationError: MockOperationError,
  operations: [
    {
      name: 'action_ingest_auto',
      description: 'mocked action run',
      params: {},
      cliHints: { name: 'action-run' },
      handler: async () => JSON.parse(process.env.GBRAIN_TEST_ACTION_RUN_RESULT ?? '{"success":true}'),
    },
  ],
}));

mock.module(engineFactoryModule, () => ({
  createEngine: async () => ({
    connect: async () => {},
    disconnect: async () => {},
  }),
}));
