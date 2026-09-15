function num(name: string, fallback: number): number {
  const raw = Deno.env.get(name);
  if (raw === undefined) return fallback;
  const parsed = Number(raw);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function nonNegativeInteger(name: string, fallback: number): number {
  const raw = Deno.env.get(name);
  if (raw === undefined) return fallback;
  const parsed = Number(raw);
  if (!Number.isInteger(parsed) || parsed < 0) {
    throw new Error(`${name} must be a non-negative integer`);
  }
  return parsed;
}

export const config = {
  executorServicePath: Deno.env.get("EXECUTOR_SERVICE_PATH") ??
    "./examples/e2b-executor",
  apiKey: Deno.env.get("E2B_API_KEY") ?? "",
  sandboxMemoryMb: num("SANDBOX_MEMORY_MB", 128),
  sandboxWallClockMs: num("SANDBOX_WALL_CLOCK_MS", 3600000),
  cpuLimitMs: num("CPU_LIMIT_MS", 600000),
  executionTimeoutMs: num("EXECUTION_TIMEOUT_MS", 10000),
  executorAsyncTimeoutMs: num("EXECUTOR_ASYNC_TIMEOUT_MS", 12000),
  adapterFetchTimeoutMs: num("ADAPTER_FETCH_TIMEOUT_MS", 15000),
  queueWaitTimeoutMs: num("QUEUE_WAIT_TIMEOUT_MS", 30000),
  maxConcurrentSandboxes: num("MAX_CONCURRENT_SANDBOXES", 128),
  maxConcurrentSandboxCreations: nonNegativeInteger(
    "MAX_CONCURRENT_SANDBOX_CREATIONS",
    0,
  ),
  maxConcurrentExecutions: num("MAX_CONCURRENT_EXECUTIONS", 32),
  maxCodeSizeBytes: num("MAX_CODE_SIZE_BYTES", 262144),
  maxRequestBytes: num("MAX_REQUEST_BYTES", 524288),
  maxOutputBytes: num("MAX_OUTPUT_BYTES", 65536),
  serializeMaxDepth: num("SERIALIZE_MAX_DEPTH", 8),
  serializeMaxEntries: num("SERIALIZE_MAX_ENTRIES", 1000),
  serializeMaxNodes: num("SERIALIZE_MAX_NODES", 10000),
  terminatedIdMemory: num("TERMINATED_ID_MEMORY", 1024),
  cleanupIntervalMs: num("CLEANUP_INTERVAL_MS", 30000),
  defaultListLimit: 100,
};

export const SUPPORTED_TEMPLATES = [
  "base",
  "edge-runtime",
  "code-interpreter-v1",
];
export const SUPPORTED_LANGUAGES = ["javascript", "typescript"];

/** Test-only. The main worker cannot set its own env, so fixtures that need
 * non-default limits inject them here before importing the router. */
export function setLimitsForTesting(
  overrides: Partial<{
    maxConcurrentSandboxes: number;
    maxConcurrentSandboxCreations: number;
    maxConcurrentExecutions: number;
    queueWaitTimeoutMs: number;
    sandboxMemoryMb: number;
    adapterFetchTimeoutMs: number;
  }>,
): void {
  if (
    overrides.maxConcurrentSandboxCreations !== undefined &&
    (!Number.isInteger(overrides.maxConcurrentSandboxCreations) ||
      overrides.maxConcurrentSandboxCreations < 0)
  ) {
    throw new Error(
      "maxConcurrentSandboxCreations must be a non-negative integer",
    );
  }
  Object.assign(config, overrides);
}
