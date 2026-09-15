import { setLimitsForTesting } from "../../../../examples/e2b-adapter/config.ts";

// One execution at a time with a short queue deadline, so later concurrent
// requests are refused rather than waiting.
setLimitsForTesting({ maxConcurrentExecutions: 1, queueWaitTimeoutMs: 50 });

await import("../../../../examples/e2b-adapter/index.ts");
