import {
  setLimitsForTesting,
} from "../../../../examples/e2b-adapter/config.ts";
import * as registry from "../../../../examples/e2b-adapter/registry.ts";

setLimitsForTesting({ maxConcurrentExecutions: 1, queueWaitTimeoutMs: 5000 });

let holderStarted = false;
let releaseHolder!: () => void;
const holderGate = new Promise<void>((resolve) => {
  releaseHolder = resolve;
});
const executionCounts = new Map<string, number>();

function sandboxEnv(opts: unknown): { __role?: string } {
  const envVars = (opts as { envVars?: [string, string][] }).envVars ?? [];
  const encoded = envVars.find(([key]) => key === "SANDBOX_ENV_JSON")?.[1];
  return encoded === undefined ? {} : JSON.parse(encoded);
}

const rt = EdgeRuntime as unknown as {
  userWorkers: { create: (opts: unknown) => Promise<unknown> };
};
rt.userWorkers.create = (opts: unknown): Promise<unknown> => {
  const role = sandboxEnv(opts).__role ?? "unknown";
  return Promise.resolve({
    key: crypto.randomUUID(),
    runtimeInitMs: 0,
    moduleInitMs: 0,
    runtimeInit: {
      loaderVfsMs: 0,
      resourceLimitsMs: 0,
      jsRuntimeNewMs: 0,
      bootstrapMs: 0,
      bootstrapBlockingRunMs: 0,
      bootstrapBlockingQueueMs: 0,
      postSetupBlockingRunMs: 0,
      postSetupBlockingQueueMs: 0,
    },
    async fetch(req: Request): Promise<Response> {
      const pathname = new URL(req.url).pathname;
      if (pathname === "/internal/init") {
        return new Response(null, { status: 204 });
      }

      executionCounts.set(role, (executionCounts.get(role) ?? 0) + 1);
      if (role === "holder") {
        holderStarted = true;
        await holderGate;
      }
      return Response.json({
        result: 1,
        result_type: "number",
        result_repr: null,
        stdout: [],
        stderr: [],
        error: null,
      });
    },
    terminate(): Promise<boolean> {
      if (role === "queued") releaseHolder();
      return Promise.resolve(true);
    },
    waitForShutdown(): Promise<WorkerShutdown> {
      return Promise.resolve({
        reason: "TerminationRequested",
        cpuTimeUsed: 0,
      });
    },
  });
};

async function tailPending(sandboxID: string): Promise<boolean> {
  const record = registry.get(sandboxID);
  if (record === undefined) return false;

  let settled = false;
  record.tail.then(() => {
    settled = true;
  });
  await Promise.resolve();
  return !settled;
}

const deno = Deno as unknown as {
  serve: (
    handler: (req: Request) => Response | Promise<Response>,
  ) => unknown;
};
const serve = deno.serve.bind(Deno);
deno.serve = (handler) =>
  serve(async (req) => {
    const url = new URL(req.url);
    if (url.pathname === "/__test/holder-started") {
      return Response.json({ holderStarted });
    }
    if (url.pathname === "/__test/lock-pending") {
      return Response.json({
        pending: await tailPending(url.searchParams.get("sandbox") ?? ""),
      });
    }
    if (url.pathname === "/__test/execution-count") {
      const role = url.searchParams.get("role") ?? "unknown";
      return Response.json({ count: executionCounts.get(role) ?? 0 });
    }
    return await handler(req);
  });

await import("../../../../examples/e2b-adapter/index.ts");
