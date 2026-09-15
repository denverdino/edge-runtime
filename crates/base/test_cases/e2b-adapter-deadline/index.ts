import { setLimitsForTesting } from "../../../../examples/e2b-adapter/config.ts";

// A short adapter deadline so an unresponsive executor is detected quickly.
setLimitsForTesting({ adapterFetchTimeoutMs: 300 });

await import("../../../../examples/e2b-adapter/index.ts");
