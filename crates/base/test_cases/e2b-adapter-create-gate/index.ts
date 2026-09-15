import { setLimitsForTesting } from "../../../../examples/e2b-adapter/config.ts";

setLimitsForTesting({
  maxConcurrentSandboxes: 3,
  maxConcurrentSandboxCreations: 1,
});

let firstInitStarted = false;
let secondInitStarted = false;
const initOrder: string[] = [];
let releaseFirstInit!: () => void;
let releaseSecondInit!: () => void;
const firstInit = new Promise<void>((resolve) => {
  releaseFirstInit = resolve;
});
const secondInit = new Promise<void>((resolve) => {
  releaseSecondInit = resolve;
});

function sandboxEnv(opts: unknown): { __role?: string } {
  const envVars = (opts as { envVars?: [string, string][] }).envVars ?? [];
  const encoded = envVars.find(([key]) => key === "SANDBOX_ENV_JSON")?.[1];
  return encoded === undefined ? {} : JSON.parse(encoded);
}

const rt = EdgeRuntime as unknown as {
  userWorkers: { create: (opts: unknown) => Promise<unknown> };
};
rt.userWorkers.create = async (opts: unknown): Promise<unknown> => {
  const env = sandboxEnv(opts);
  initOrder.push(env.__role ?? "unmarked");
  if (env.__role === "hold") {
    firstInitStarted = true;
    await firstInit;
  }
  if (env.__role === "second-hold") {
    secondInitStarted = true;
    await secondInit;
  }

  return {
    key: crypto.randomUUID(),
    runtimeInitMs: 1,
    moduleInitMs: 1,
    runtimeInit: {
      loaderVfsMs: 2,
      resourceLimitsMs: 3,
      jsRuntimeNewMs: 4,
      bootstrapMs: 5,
      bootstrapBlockingRunMs: 6,
      bootstrapBlockingQueueMs: 7,
      postSetupBlockingRunMs: 8,
      postSetupBlockingQueueMs: 9,
    },
    fetch(): Promise<Response> {
      return Promise.resolve(Response.json({}));
    },
    terminate(): Promise<boolean> {
      return Promise.resolve(true);
    },
    waitForShutdown(): Promise<WorkerShutdown> {
      return Promise.resolve({
        reason: "TerminationRequested",
        cpuTimeUsed: 0,
      });
    },
  };
};

const deno = Deno as unknown as {
  serve: (
    handler: (req: Request) => Response | Promise<Response>,
  ) => unknown;
};
const serve = deno.serve.bind(Deno);
deno.serve = (handler) =>
  serve(async (req) => {
    const pathname = new URL(req.url).pathname;
    if (pathname === "/__test/create-gate-state") {
      return Response.json({ firstInitStarted, secondInitStarted, initOrder });
    }
    if (pathname === "/__test/release-create-gate") {
      releaseFirstInit();
      return new Response(null, { status: 204 });
    }
    if (pathname === "/__test/release-second-create-gate") {
      releaseSecondInit();
      return new Response(null, { status: 204 });
    }
    if (pathname === "/__test/shorten-create-gate-timeout") {
      setLimitsForTesting({ queueWaitTimeoutMs: 50 });
      return new Response(null, { status: 204 });
    }
    return await handler(req);
  });

await import("../../../../examples/e2b-adapter/index.ts");
