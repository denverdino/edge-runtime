import { setLimitsForTesting } from "../../../../examples/e2b-adapter/config.ts";

// A roomy deadline: this fixture exercises the bundle cache, not a timeout.
setLimitsForTesting({ adapterFetchTimeoutMs: 5000 });

// Fail the first bundle build, then delegate to the real op. A cached rejected
// promise (the old `??=`) would 500 every future create; clearing it on
// rejection must let the retry succeed.
const rt = EdgeRuntime as unknown as {
  bundle: (entrypoint: string) => Promise<Uint8Array>;
};
const realBundle = rt.bundle.bind(rt);
let failuresLeft = 1;
rt.bundle = (entrypoint: string): Promise<Uint8Array> => {
  if (failuresLeft > 0) {
    failuresLeft--;
    return Promise.reject(new Error("bundle boom (injected once)"));
  }
  return realBundle(entrypoint);
};

await import("../../../../examples/e2b-adapter/index.ts");
