import { setLimitsForTesting } from "../../../../examples/e2b-adapter/config.ts";

// A small memory limit so a sandbox can be pushed over it quickly.
setLimitsForTesting({ sandboxMemoryMb: 32 });

await import("../../../../examples/e2b-adapter/index.ts");
