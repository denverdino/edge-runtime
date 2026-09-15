import { authorize } from "./auth.ts";
import { config, SUPPORTED_LANGUAGES, SUPPORTED_TEMPLATES } from "./config.ts";
import { errorResponse, redactedError } from "./errors.ts";
import { runSandboxExecution } from "./execution.ts";
import { type ExecutorHandle, spawnExecutor } from "./executor.ts";
import { handleJupyterExecute } from "./jupyter.ts";
import {
  acquireCreateSlot,
  createGateSnapshot,
  globalExecutionQueueSnapshot,
  QueueTimeout,
} from "./queue.ts";
import * as registry from "./registry.ts";
import { readJsonBounded, stringMap } from "./request.ts";
import { telemetry } from "./telemetry.ts";

async function createSandbox(req: Request): Promise<Response> {
  const createStarted = Date.now();
  let createOutcome:
    | "ok"
    | "rejected"
    | "error"
    | "duplicate"
    | "timeout" = "error";
  let reserved = false;
  let releaseCreateSlot: (() => void) | undefined;

  try {
    const body = await readJsonBounded(req, config.maxRequestBytes);
    if (body === "too_large") {
      createOutcome = "rejected";
      return errorResponse("invalid_request", "request body is too large");
    }
    if (body === null) {
      createOutcome = "rejected";
      return errorResponse("invalid_request", "body must be a JSON object");
    }

    if (body.templateID !== undefined && typeof body.templateID !== "string") {
      createOutcome = "rejected";
      return errorResponse(
        "unsupported_template",
        "templateID must be a string",
      );
    }
    const templateID = body.templateID ?? "base";
    if (!SUPPORTED_TEMPLATES.includes(templateID as string)) {
      createOutcome = "rejected";
      return errorResponse(
        "unsupported_template",
        `Unknown template: ${templateID}`,
      );
    }

    const envVars = stringMap(body.envVars);
    const metadata = stringMap(body.metadata);
    if (envVars === null || metadata === null) {
      createOutcome = "rejected";
      return errorResponse(
        "invalid_request",
        "envVars and metadata must be string maps",
      );
    }

    if (
      body.timeout !== undefined &&
      (typeof body.timeout !== "number" || !Number.isFinite(body.timeout) ||
        body.timeout <= 0)
    ) {
      createOutcome = "rejected";
      return errorResponse(
        "invalid_request",
        "timeout must be a positive number",
      );
    }
    const timeoutSeconds = typeof body.timeout === "number"
      ? body.timeout
      : 300;

    await registry.sweep();
    if (!registry.tryReserve()) {
      createOutcome = "rejected";
      return errorResponse("too_many_sandboxes");
    }
    reserved = true;

    const gateWaitStarted = Date.now();
    try {
      releaseCreateSlot = await acquireCreateSlot();
    } catch (error) {
      if (error instanceof QueueTimeout) {
        createOutcome = "timeout";
        telemetry.record(
          "create_gate_wait",
          Date.now() - gateWaitStarted,
          "timeout",
        );
        return errorResponse("too_many_requests");
      }
      throw error;
    }
    telemetry.record("create_gate_wait", Date.now() - gateWaitStarted, "ok");

    const createdAt = Date.now();
    const wallClockCeiling = createdAt + config.sandboxWallClockMs;
    const sandboxID = crypto.randomUUID();
    const envdAccessToken = crypto.randomUUID();

    let executor: ExecutorHandle;
    try {
      executor = await spawnExecutor(envVars);
    } catch (error) {
      return redactedError("internal_server_error", error);
    }

    // A reused worker would put two sandboxes in one isolate. Refuse rather
    // than serve a sandbox that leaks state to another tenant.
    if (registry.hasExecutorKey(executor.key)) {
      registry.releaseReservation();
      reserved = false;
      createOutcome = "duplicate";
      return redactedError(
        "internal_server_error",
        new Error("executor is already assigned to another sandbox"),
      );
    }

    registry.insert({
      sandboxID,
      templateID: templateID as string,
      envdAccessToken,
      createdAt,
      expiresAt: Math.min(createdAt + timeoutSeconds * 1000, wallClockCeiling),
      wallClockCeiling,
      metadata,
      envVars,
      executor,
      tail: Promise.resolve(),
    });
    registry.releaseReservation();
    reserved = false;
    createOutcome = "ok";

    return Response.json({
      sandboxID,
      templateID,
      clientID: "edge-runtime-adapter",
      envdVersion: registry.ENVD_VERSION,
      envdAccessToken,
      startedAt: new Date(createdAt).toISOString(),
    }, { status: 201 });
  } catch (error) {
    return redactedError("internal_server_error", error);
  } finally {
    if (reserved) registry.releaseReservation();
    releaseCreateSlot?.();
    telemetry.record("create_total", Date.now() - createStarted, createOutcome);
  }
}

async function execute(req: Request): Promise<Response> {
  const body = await readJsonBounded(req, config.maxRequestBytes);
  if (body === "too_large") {
    return errorResponse("invalid_request", "request body is too large");
  }
  if (body === null) return errorResponse("invalid_request");

  const { code, context_id: contextId, language } = body;
  if (typeof code !== "string" || typeof contextId !== "string") {
    return errorResponse("invalid_request", "code and context_id are required");
  }
  if (new TextEncoder().encode(code).length > config.maxCodeSizeBytes) {
    return errorResponse("invalid_request", "code is too large");
  }
  if (typeof language !== "string" || !SUPPORTED_LANGUAGES.includes(language)) {
    return errorResponse("unsupported_language", String(language));
  }

  const envVars = stringMap(body.env_vars);
  if (envVars === null) {
    return errorResponse("invalid_request", "env_vars must be a string map");
  }

  const record = await registry.resolve(contextId);
  if (typeof record === "string") return errorResponse(record);

  const executionId = crypto.randomUUID();
  const startedAt = Date.now();

  const outcome = await runSandboxExecution(req, contextId, record, {
    code,
    language: language as "javascript" | "typescript",
    envVars,
  });

  let payload: Record<string, unknown>;
  switch (outcome.kind) {
    case "stale":
      return errorResponse(outcome.code);
    case "queue_timeout":
      return errorResponse("too_many_requests");
    case "executor_failure":
      return redactedError(outcome.code, outcome.cause);
    case "success":
      payload = outcome.payload;
  }

  return Response.json({
    execution_id: executionId,
    context_id: contextId,
    language,
    result: payload.result ?? null,
    result_type: payload.result_type ?? "undefined",
    result_repr: payload.result_repr ?? null,
    stdout: payload.stdout ?? [],
    stderr: payload.stderr ?? [],
    error: payload.error ?? null,
    duration_ms: Date.now() - startedAt,
  });
}

async function getSandbox(sandboxID: string): Promise<Response> {
  const record = await registry.resolve(sandboxID);
  if (typeof record === "string") return errorResponse(record);
  return Response.json(registry.describe(record));
}

async function listSandboxes(url: URL): Promise<Response> {
  // Sweep first so an expired sandbox is never listed as running.
  await registry.sweep();

  const requested = Number(
    url.searchParams.get("limit") ?? config.defaultListLimit,
  );
  const limit = Number.isFinite(requested)
    ? Math.min(
      Math.max(1, Math.trunc(requested)),
      config.maxConcurrentSandboxes,
    )
    : config.defaultListLimit;

  return Response.json(
    registry.list().slice(0, limit).map(registry.describe),
  );
}

async function deleteSandbox(sandboxID: string): Promise<Response> {
  const record = await registry.resolve(sandboxID);
  if (typeof record === "string") return errorResponse(record);
  // reap terminates the isolate, not just the record.
  await registry.reap(sandboxID);
  return new Response(null, { status: 204 });
}

async function setSandboxTimeout(
  sandboxID: string,
  req: Request,
): Promise<Response> {
  const record = await registry.resolve(sandboxID);
  if (typeof record === "string") return errorResponse(record);

  const body = await readJsonBounded(req, config.maxRequestBytes);
  if (body === "too_large") {
    return errorResponse("invalid_request", "request body is too large");
  }
  const seconds = body === null ? NaN : Number(body.timeout);
  if (!Number.isFinite(seconds) || seconds <= 0) {
    return errorResponse(
      "invalid_request",
      "timeout must be a positive number",
    );
  }

  // The worker wall clock starts at boot and never resets, so a TTL beyond the
  // ceiling could not be honoured. Refuse instead of granting it silently.
  const requested = Date.now() + seconds * 1000;
  if (requested > record.wallClockCeiling) {
    return errorResponse(
      "invalid_request",
      "timeout exceeds the sandbox wall-clock ceiling at " +
        new Date(record.wallClockCeiling).toISOString(),
    );
  }

  record.expiresAt = requested;
  return new Response(null, { status: 204 });
}

async function activeWorkerHeap(): Promise<Record<string, number | boolean>> {
  try {
    const workers = Object.values(
      await EdgeRuntime.userWorkers.memStats(),
    ) as WorkerHeapStatisticsWithServicePath[];
    let count = 0;
    let totalHeapSize = 0;
    let usedHeapSize = 0;
    for (const worker of workers) {
      if (worker.stats === undefined) continue;
      count++;
      totalHeapSize += worker.stats.totalHeapSize;
      usedHeapSize += worker.stats.usedHeapSize;
    }
    return { available: true, count, totalHeapSize, usedHeapSize };
  } catch {
    return { available: false, count: 0, totalHeapSize: 0, usedHeapSize: 0 };
  }
}

async function metrics(): Promise<Response> {
  return Response.json({
    ...telemetry.snapshot(),
    capacity: registry.capacitySnapshot(),
    creationGate: createGateSnapshot(),
    globalExecutionQueue: globalExecutionQueueSnapshot(),
    activeWorkerHeap: await activeWorkerHeap(),
  });
}

// Expiry is otherwise purely lazy: an expired sandbox nobody touches keeps its
// isolate (and its memory limit) until the wall clock. One background sweep
// reclaims it on a fixed period. The interval is unref'd so it never keeps the
// process alive, and a globalThis guard stops a second interval when the module
// is imported again (the integration fixtures import it repeatedly).
const REAPER_STARTED = Symbol.for("e2b-adapter.reaperStarted");
const reaperGuard = globalThis as unknown as Record<symbol, boolean>;
if (!reaperGuard[REAPER_STARTED]) {
  reaperGuard[REAPER_STARTED] = true;
  const reaper = setInterval(() => {
    registry.sweep().catch(() => {});
  }, config.cleanupIntervalMs);
  Deno.unrefTimer(reaper);
}

Deno.serve(async (req: Request) => {
  const url = new URL(req.url);

  const jupyterPath = url.pathname.match(
    /^\/sandboxes\/([^/]+)\/jupyter\/execute$/,
  );
  if (jupyterPath !== null && req.method === "POST") {
    return await handleJupyterExecute(req, jupyterPath[1]);
  }

  // Gate before routing so an unauthenticated caller learns nothing about
  // which non-Jupyter routes exist and cannot reach any other handler.
  if (!await authorize(req)) return errorResponse("unauthorized");

  if (url.pathname === "/internal/metrics" && req.method === "GET") {
    return await metrics();
  }
  if (url.pathname === "/sandboxes" && req.method === "POST") {
    return await createSandbox(req);
  }
  if (url.pathname === "/execute" && req.method === "POST") {
    return await execute(req);
  }
  if (
    (url.pathname === "/sandboxes" || url.pathname === "/v2/sandboxes") &&
    req.method === "GET"
  ) {
    return await listSandboxes(url);
  }

  const sandboxPath = url.pathname.match(/^\/(?:v2\/)?sandboxes\/([^/]+)$/);
  if (sandboxPath !== null && req.method === "GET") {
    return await getSandbox(sandboxPath[1]);
  }
  if (sandboxPath !== null && req.method === "DELETE") {
    return await deleteSandbox(sandboxPath[1]);
  }

  const timeoutPath = url.pathname.match(
    /^\/(?:v2\/)?sandboxes\/([^/]+)\/timeout$/,
  );
  if (timeoutPath !== null && req.method === "POST") {
    return await setSandboxTimeout(timeoutPath[1], req);
  }

  return errorResponse("sandbox_not_found", `No route for ${url.pathname}`);
});
