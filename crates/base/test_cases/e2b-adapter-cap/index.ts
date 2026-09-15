import { setLimitsForTesting } from "../../../../examples/e2b-adapter/config.ts";

// Small cap so the concurrent-create test can reach it.
setLimitsForTesting({ maxConcurrentSandboxes: 2 });

await import("../../../../examples/e2b-adapter/index.ts");
