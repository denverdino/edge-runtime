interface WorkerHandle {
  fetch(req: Request): Promise<Response>;
}

const workers = new Map<string, Promise<WorkerHandle>>();

function sandboxEnvVars(raw: string | null): [string, string][] | Response {
  if (raw === null) return [];

  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return Response.json({ error: "invalid baseline env" }, { status: 400 });
  }
  if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
    return Response.json({ error: "invalid baseline env" }, { status: 400 });
  }
  for (const [key, value] of Object.entries(parsed)) {
    if (typeof key !== "string" || typeof value !== "string") {
      return Response.json({ error: "invalid baseline env" }, { status: 400 });
    }
  }
  return [["SANDBOX_ENV_JSON", raw]];
}

// Boots the executor with deliberately invalid numeric configuration:
// EXECUTOR_ASYNC_TIMEOUT_MS and SERIALIZE_MAX_ENTRIES are non-numeric. The
// executor must fall back to its documented defaults rather than propagating
// NaN (which would time out every async execution and serialize arrays empty).
Deno.serve(async (req: Request) => {
  const sandboxId = req.headers.get("x-sandbox-id") ?? "default";
  const sandboxEnv = req.headers.get("x-sandbox-env");

  let workerPromise = workers.get(sandboxId);
  if (workerPromise === undefined) {
    const baselineEnvVars = sandboxEnvVars(sandboxEnv);
    if (baselineEnvVars instanceof Response) return baselineEnvVars;

    workerPromise = (async () => {
      const worker = await (EdgeRuntime.userWorkers.create({
        servicePath: "../../examples/e2b-executor",
        forceCreate: true,
        envVars: [
          ["EXECUTION_TIMEOUT_MS", "1000"],
          ["EXECUTOR_ASYNC_TIMEOUT_MS", "not-a-number"],
          ["SERIALIZE_MAX_ENTRIES", "not-a-number"],
          ...baselineEnvVars,
        ],
        memoryLimitMb: 128,
        workerTimeoutMs: 3600000,
        cpuTimeSoftLimitMs: 600000,
        cpuTimeHardLimitMs: 600000,
        context: { allowNodeVm: true, allowTranspile: true },
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
      }) as Promise<WorkerHandle>);
      const initReq = new Request("http://executor/internal/init");
      EdgeRuntime.applySupabaseTag(req, initReq);
      const initResponse = await worker.fetch(initReq);
      if (!initResponse.ok) {
        const detail = await initResponse.text();
        throw new Error(
          `executor init failed (${initResponse.status}): ${detail}`,
        );
      }
      return worker;
    })();
    workers.set(sandboxId, workerPromise);
    workerPromise.catch(() => {
      if (workers.get(sandboxId) === workerPromise) {
        workers.delete(sandboxId);
      }
    });
  }

  const worker = await workerPromise;
  return await worker.fetch(req);
});
