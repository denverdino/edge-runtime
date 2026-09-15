import { setLimitsForTesting } from "../../../../examples/e2b-adapter/config.ts";

// Keep the terminate deadline well above the fake terminate delay so terminate
// itself always completes; the point of this fixture is to time the sweep, not
// to trip the deadline.
setLimitsForTesting({ adapterFetchTimeoutMs: 5000 });

// Each fake worker's `terminate()` settles after a fixed delay. A sequential
// sweep therefore costs N * DELAY, while a concurrent sweep costs ~one DELAY.
const TERMINATE_DELAY_MS = 700;

interface FakeReq {
  headers: { get(name: string): string | null };
}
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
    fetch(_req: FakeReq): Promise<Response> {
      // The init probe only checks `.ok`; 204 is enough to boot the sandbox.
      return Promise.resolve(new Response(null, { status: 204 }));
    },
    terminate(): Promise<boolean> {
      return new Promise<boolean>((resolve) => {
        setTimeout(() => resolve(true), TERMINATE_DELAY_MS);
      });
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
