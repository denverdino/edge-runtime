import { setLimitsForTesting } from "../../../../examples/e2b-adapter/config.ts";

// A short adapter deadline keeps the hung termination test fast.
setLimitsForTesting({ adapterFetchTimeoutMs: 300 });

// Replace the pool's `create` with a fake worker whose termination can stall.
// `ExecutorHandle.terminate` must still let registry cleanup finish.
function sandboxEnv(opts: unknown): { __fault?: string } {
  const envVars = (opts as { envVars?: [string, string][] }).envVars ?? [];
  const encoded = envVars.find(([key]) => key === "SANDBOX_ENV_JSON")?.[1];
  return encoded === undefined ? {} : JSON.parse(encoded);
}

const rt = EdgeRuntime as unknown as {
  userWorkers: { create: (opts: unknown) => Promise<unknown> };
};
rt.userWorkers.create = (opts: unknown): Promise<unknown> => {
  const fault = sandboxEnv(opts).__fault;
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
      return Promise.resolve(new Response(null, { status: 204 }));
    },
    terminate(): Promise<boolean> {
      if (fault === "hang-terminate") return new Promise<boolean>(() => {});
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
