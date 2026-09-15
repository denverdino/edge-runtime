import { setLimitsForTesting } from "../../../../examples/e2b-adapter/config.ts";

setLimitsForTesting({ maxConcurrentSandboxes: 1 });

let releaseFinalShutdown!: () => void;
const finalShutdown = new Promise<WorkerShutdown>((resolve) => {
  releaseFinalShutdown = () => {
    resolve({
      reason: "TerminationRequested",
      cpuTimeUsed: 7,
      memoryUsed: { total: 30, heap: 20, external: 10 },
    });
  };
});

const rt = EdgeRuntime as unknown as {
  userWorkers: { create: (opts: unknown) => Promise<unknown> };
};
rt.userWorkers.create = (_opts: unknown): Promise<unknown> => {
  return Promise.resolve({
    key: crypto.randomUUID(),
    runtimeInitMs: 2,
    moduleInitMs: 3,
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
    fetch(_req: Request): Promise<Response> {
      return Promise.resolve(new Response(null, { status: 204 }));
    },
    terminate(): Promise<boolean> {
      return Promise.resolve(true);
    },
    waitForShutdown(): Promise<WorkerShutdown> {
      return finalShutdown;
    },
  });
};

const deno = Deno as unknown as {
  serve: (
    handler: (req: Request) => Response | Promise<Response>,
  ) => unknown;
};
const serve = deno.serve.bind(Deno);
deno.serve = (handler) =>
  serve(async (req) => {
    if (new URL(req.url).pathname === "/__test/release-final-shutdown") {
      releaseFinalShutdown();
      return new Response(null, { status: 204 });
    }
    return await handler(req);
  });

await import("../../../../examples/e2b-adapter/index.ts");
