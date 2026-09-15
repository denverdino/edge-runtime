const WINDOW_SECONDS = 60;
const BUCKET_LIMITS = [1, 5, 10, 25, 50, 100, 250, 1000, 5000];

export const TELEMETRY_STAGES = [
  "create_total",
  "create_gate_wait",
  "module_load",
  "worker_boot",
  "runtime_init",
  "loader_vfs",
  "resource_limits",
  "js_runtime_new",
  "bootstrap",
  "bootstrap_blocking_run",
  "bootstrap_blocking_queue",
  "post_setup_blocking_run",
  "post_setup_blocking_queue",
  "module_init",
  "terminate_acknowledgement",
  "ack_to_final_shutdown",
] as const;

export const TELEMETRY_OUTCOMES = [
  "ok",
  "rejected",
  "error",
  "timeout",
  "unavailable",
  "duplicate",
] as const;

type TelemetryStage = typeof TELEMETRY_STAGES[number];
type TelemetryOutcome = typeof TELEMETRY_OUTCOMES[number];

type Slot = {
  second: number;
  count: number;
  sumMs: number;
  buckets: number[];
};

function newSlot(): Slot {
  return {
    second: -1,
    count: 0,
    sumMs: 0,
    buckets: Array<number>(BUCKET_LIMITS.length + 1).fill(0),
  };
}

class RollingHistogram {
  #slots = Array.from({ length: WINDOW_SECONDS }, newSlot);

  record(durationMs: number): void {
    const second = Math.floor(Date.now() / 1000);
    const slot = this.#slots[second % WINDOW_SECONDS];
    if (slot.second !== second) {
      slot.second = second;
      slot.count = 0;
      slot.sumMs = 0;
      slot.buckets.fill(0);
    }

    const value = Number.isFinite(durationMs) && durationMs >= 0
      ? durationMs
      : 0;
    slot.count++;
    slot.sumMs += value;
    const bucket = BUCKET_LIMITS.findIndex((limit) => value <= limit);
    slot.buckets[bucket === -1 ? BUCKET_LIMITS.length : bucket]++;
  }

  snapshot(): Record<string, number | Record<string, number>> {
    const now = Math.floor(Date.now() / 1000);
    let count = 0;
    let sumMs = 0;
    const buckets = Array<number>(BUCKET_LIMITS.length + 1).fill(0);
    for (const slot of this.#slots) {
      if (slot.second < now - WINDOW_SECONDS + 1 || slot.second > now) continue;
      count += slot.count;
      sumMs += slot.sumMs;
      for (let index = 0; index < buckets.length; index++) {
        buckets[index] += slot.buckets[index];
      }
    }

    const histogram: Record<string, number> = {};
    for (let index = 0; index < BUCKET_LIMITS.length; index++) {
      histogram[`le_${BUCKET_LIMITS[index]}`] = buckets[index];
    }
    histogram.gt_5000 = buckets[BUCKET_LIMITS.length];
    return { count, sumMs, buckets: histogram };
  }
}

const histograms = Object.fromEntries(
  TELEMETRY_STAGES.map((stage) => [
    stage,
    Object.fromEntries(
      TELEMETRY_OUTCOMES.map((outcome) => [outcome, new RollingHistogram()]),
    ) as Record<TelemetryOutcome, RollingHistogram>,
  ]),
) as Record<TelemetryStage, Record<TelemetryOutcome, RollingHistogram>>;

const shutdownReasons = [
  "event_loop_completed",
  "wall_clock_time",
  "cpu_time",
  "memory",
  "early_drop",
  "termination_requested",
  "uncaught_exception",
  "unexpected_error",
  "supervisor_unavailable",
  "unknown",
] as const;

type ShutdownReason = typeof shutdownReasons[number];

let finalWorkerCount = 0;
let finalCpuTimeUsed = 0;
let finalMemoryTotal = 0;
let finalMemoryHeap = 0;
let finalMemoryExternal = 0;
const finalReasons = Object.fromEntries(
  shutdownReasons.map((reason) => [reason, 0]),
) as Record<ShutdownReason, number>;

function shutdownReason(reason: string): ShutdownReason {
  const normalized = reason.replaceAll(/([a-z])([A-Z])/g, "$1_$2")
    .toLowerCase();
  return shutdownReasons.includes(normalized as ShutdownReason)
    ? normalized as ShutdownReason
    : "unknown";
}

/** Fixed-memory, 60-second telemetry for adapter lifecycle stages. */
export const telemetry = {
  record(
    stage: TelemetryStage,
    durationMs: number,
    outcome: TelemetryOutcome,
  ): void {
    histograms[stage][outcome].record(durationMs);
  },

  recordFinalShutdown(shutdown: WorkerShutdown): void {
    finalWorkerCount++;
    finalCpuTimeUsed += shutdown.cpuTimeUsed;
    finalMemoryTotal += shutdown.memoryUsed?.total ?? 0;
    finalMemoryHeap += shutdown.memoryUsed?.heap ?? 0;
    finalMemoryExternal += shutdown.memoryUsed?.external ?? 0;
    finalReasons[shutdownReason(shutdown.reason)]++;
  },

  snapshot(): Record<string, unknown> {
    return {
      rollingLifecycleWindow: {
        seconds: WINDOW_SECONDS,
        stages: Object.fromEntries(
          TELEMETRY_STAGES.map((stage) => [
            stage,
            Object.fromEntries(
              TELEMETRY_OUTCOMES.map((outcome) => [
                outcome,
                histograms[stage][outcome].snapshot(),
              ]),
            ),
          ]),
        ),
      },
      lifetimeFinalWorkerTotals: {
        count: finalWorkerCount,
        cpuTimeUsed: finalCpuTimeUsed,
        v8Heap: {
          total: finalMemoryTotal,
          heap: finalMemoryHeap,
          external: finalMemoryExternal,
        },
        reasons: finalReasons,
      },
    };
  },
};
