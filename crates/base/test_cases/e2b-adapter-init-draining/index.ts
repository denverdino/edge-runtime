import { setLimitsForTesting } from "../../../../examples/e2b-adapter/config.ts";

setLimitsForTesting({ maxConcurrentSandboxes: 1 });

const rt = EdgeRuntime as unknown as {
  userWorkers: { create: (opts: unknown) => Promise<unknown> };
};
rt.userWorkers.create = (_opts: unknown): Promise<unknown> => {
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
    fetch(): Promise<Response> {
      return Promise.resolve(new Response("executor failed", { status: 500 }));
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
  });
};

await import("../../../../examples/e2b-adapter/index.ts");
