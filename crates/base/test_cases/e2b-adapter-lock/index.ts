import { setLimitsForTesting } from "../../../../examples/e2b-adapter/config.ts";

// A short lock-wait deadline, with the global slot left generous so the
// per-sandbox lock is what requests contend on.
setLimitsForTesting({ queueWaitTimeoutMs: 50, maxConcurrentExecutions: 32 });

await import("../../../../examples/e2b-adapter/index.ts");
