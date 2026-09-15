import { config } from "./config.ts";
import { telemetry } from "./telemetry.ts";

interface WorkerHandle {
  key: string;
  runtimeInitMs: number;
  moduleInitMs: number;
  runtimeInit: {
    loaderVfsMs: number;
    resourceLimitsMs: number;
    jsRuntimeNewMs: number;
    bootstrapMs: number;
    bootstrapBlockingRunMs: number;
    bootstrapBlockingQueueMs: number;
    postSetupBlockingRunMs: number;
    postSetupBlockingQueueMs: number;
  };
  fetch(req: Request, options?: { signal?: AbortSignal }): Promise<Response>;
  terminate(): Promise<boolean>;
  waitForShutdown(): Promise<WorkerShutdown | null>;
}

export interface ExecutorHandle {
  key: string;
  /** Sends a payload and returns the parsed reply.
   *
   * Returns the parsed body rather than a `Response` so the deadline can cover
   * reading it. Bounding only the headers would leave `response.json()`
   * unbounded while the caller still holds the sandbox lock and a global slot.
   */
  execute(
    originalReq: Request,
    payload: unknown,
    timeoutMs: number,
  ): Promise<Record<string, unknown>>;
  terminate(): Promise<boolean>;
  waitForShutdown(): Promise<
    { kind: "value"; shutdown: WorkerShutdown | null } | { kind: "error" }
  >;
}

/** The executor did not answer within the adapter's deadline. */
export class ExecutorUnresponsive extends Error {
  constructor(timeoutMs: number) {
    super(`executor did not respond within ${timeoutMs}ms`);
    this.name = "ExecutorUnresponsive";
  }
}

const UNRESPONSIVE = Symbol("unresponsive");

/** Races a worker call against a real timer.
 *
 * `UserWorker.fetch` only consults the AbortSignal for a pre-check, for piping
 * the request body, and for closing the response body once it has arrived — it
 * never races the signal against the response itself. So for a small JSON body
 * an AbortSignal alone does nothing, and a hung executor would hold the sandbox
 * lock and a global execution slot until its isolate died of the cumulative CPU
 * limit.
 */
async function withDeadline<T>(
  call: Promise<T>,
  timeoutMs: number,
): Promise<T> {
  let timer: number | undefined;
  const guard = new Promise<typeof UNRESPONSIVE>((resolve) => {
    timer = setTimeout(() => resolve(UNRESPONSIVE), timeoutMs);
  });

  try {
    const settled = await Promise.race([call, guard]);
    if (settled === UNRESPONSIVE) throw new ExecutorUnresponsive(timeoutMs);
    return settled;
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
}

/** The executor's module graph, resolved and transpiled once.
 *
 * Passing `maybeEszip` is what keeps sandbox creation cheap: without it every
 * `create` re-resolves the graph, re-transpiles it, and walks the npm vfs, which
 * measured ~330ms of a ~340ms boot. Built from live source on first use, so
 * there is no artifact that can go stale, and cached as the promise so
 * concurrent first creates share one build.
 *
 * No fallback on failure is intentional: a graph that cannot be bundled cannot
 * be booted per-create either, so falling back would only hide the error behind
 * a much slower path.
 */
let executorEszip: Promise<Uint8Array> | undefined;

async function bundledExecutor(): Promise<Uint8Array> {
  if (executorEszip !== undefined) return executorEszip;

  // Cache the pending build so concurrent first creates share it, but clear the
  // entry if it rejects: `??=` would keep a rejected promise forever, so one
  // transient bundle failure would 500 every future create until restart.
  const pending = EdgeRuntime.bundle(
    `${config.executorServicePath}/index.ts`,
  );
  executorEszip = pending;
  try {
    return await pending;
  } catch (error) {
    if (executorEszip === pending) executorEszip = undefined;
    throw error;
  }
}

export async function spawnExecutor(
  envVars: Record<string, string>,
): Promise<ExecutorHandle> {
  const moduleLoadStarted = Date.now();
  let maybeEszip: Uint8Array;
  try {
    maybeEszip = await bundledExecutor();
    telemetry.record("module_load", Date.now() - moduleLoadStarted, "ok");
  } catch (error) {
    telemetry.record("module_load", Date.now() - moduleLoadStarted, "error");
    throw error;
  }

  const workerBootStarted = Date.now();
  let worker: WorkerHandle;
  try {
    worker = await (EdgeRuntime.userWorkers.create({
      servicePath: config.executorServicePath,
      maybeEszip,
      forceCreate: true,
      envVars: [
        ["EXECUTION_TIMEOUT_MS", String(config.executionTimeoutMs)],
        ["EXECUTOR_ASYNC_TIMEOUT_MS", String(config.executorAsyncTimeoutMs)],
        ["MAX_OUTPUT_BYTES", String(config.maxOutputBytes)],
        ["MAX_REQUEST_BYTES", String(config.maxRequestBytes)],
        ["SERIALIZE_MAX_DEPTH", String(config.serializeMaxDepth)],
        ["SERIALIZE_MAX_ENTRIES", String(config.serializeMaxEntries)],
        ["SERIALIZE_MAX_NODES", String(config.serializeMaxNodes)],
        ["SANDBOX_ENV_JSON", JSON.stringify(envVars)],
      ],
      memoryLimitMb: config.sandboxMemoryMb,
      workerTimeoutMs: config.sandboxWallClockMs,
      cpuTimeSoftLimitMs: config.cpuLimitMs,
      cpuTimeHardLimitMs: config.cpuLimitMs,
      runtimeProfile: "e2bExecutor",
      permissions: {
        allow_all: false,
        allow_env: [
          "EXECUTION_TIMEOUT_MS",
          "EXECUTOR_ASYNC_TIMEOUT_MS",
          "MAX_OUTPUT_BYTES",
          "MAX_REQUEST_BYTES",
          "SERIALIZE_MAX_DEPTH",
          "SERIALIZE_MAX_ENTRIES",
          "SERIALIZE_MAX_NODES",
          "SANDBOX_ENV_JSON",
        ],
      },
    }) as unknown as Promise<WorkerHandle>);
    telemetry.record("worker_boot", Date.now() - workerBootStarted, "ok");
  } catch (error) {
    telemetry.record("worker_boot", Date.now() - workerBootStarted, "error");
    throw error;
  }

  telemetry.record("runtime_init", worker.runtimeInitMs, "ok");
  telemetry.record("loader_vfs", worker.runtimeInit.loaderVfsMs, "ok");
  telemetry.record(
    "resource_limits",
    worker.runtimeInit.resourceLimitsMs,
    "ok",
  );
  telemetry.record(
    "js_runtime_new",
    worker.runtimeInit.jsRuntimeNewMs,
    "ok",
  );
  telemetry.record("bootstrap", worker.runtimeInit.bootstrapMs, "ok");
  telemetry.record(
    "bootstrap_blocking_run",
    worker.runtimeInit.bootstrapBlockingRunMs,
    "ok",
  );
  telemetry.record(
    "bootstrap_blocking_queue",
    worker.runtimeInit.bootstrapBlockingQueueMs,
    "ok",
  );
  telemetry.record(
    "post_setup_blocking_run",
    worker.runtimeInit.postSetupBlockingRunMs,
    "ok",
  );
  telemetry.record(
    "post_setup_blocking_queue",
    worker.runtimeInit.postSetupBlockingQueueMs,
    "ok",
  );
  telemetry.record("module_init", worker.moduleInitMs, "ok");

  // Register this before any termination request. The settled wrapper avoids an
  // unhandled rejection while preserving the distinction needed to fail closed.
  const finalShutdown = worker.waitForShutdown().then(
    (shutdown) => ({ kind: "value" as const, shutdown }),
    () => ({ kind: "error" as const }),
  );

  const executor: ExecutorHandle = {
    key: worker.key,
    async execute(req, payload, timeoutMs) {
      const executeReq = new Request("http://executor/internal/execute", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(payload),
      });
      EdgeRuntime.applySupabaseTag(req, executeReq);

      return await withDeadline(
        (async () => {
          const response = await worker.fetch(executeReq);
          if (!response.ok) {
            throw new Error(`executor returned ${response.status}`);
          }
          return await response.json() as Record<string, unknown>;
        })(),
        timeoutMs,
      );
    },
    terminate: async () => {
      try {
        return await withDeadline(
          worker.terminate(),
          config.adapterFetchTimeoutMs,
        );
      } catch {
        return false;
      }
    },
    waitForShutdown: async () => await finalShutdown,
  };

  return executor;
}
