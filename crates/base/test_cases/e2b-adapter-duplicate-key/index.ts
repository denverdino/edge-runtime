const key = crypto.randomUUID();
let terminated = false;

const worker = {
  key,
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
    if (terminated) {
      return Promise.resolve(new Response(null, { status: 500 }));
    }
    return Promise.resolve(Response.json({
      result: 3,
      result_type: "number",
      result_repr: null,
      stdout: [],
      stderr: [],
      error: null,
    }));
  },
  terminate(): Promise<boolean> {
    terminated = true;
    return Promise.resolve(true);
  },
  waitForShutdown(): Promise<WorkerShutdown> {
    return Promise.resolve({
      reason: "TerminationRequested",
      cpuTimeUsed: 0,
    });
  },
};

const rt = EdgeRuntime as unknown as {
  userWorkers: { create: () => Promise<unknown> };
};
rt.userWorkers.create = () => Promise.resolve(worker);

await import("../../../../examples/e2b-adapter/index.ts");
