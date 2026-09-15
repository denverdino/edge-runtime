import { config } from "./config.ts";
import type { ExecutorHandle } from "./executor.ts";
import { telemetry } from "./telemetry.ts";

export interface SandboxRecord {
  sandboxID: string;
  templateID: string;
  envdAccessToken: string;
  createdAt: number;
  expiresAt: number;
  wallClockCeiling: number;
  metadata: Record<string, string>;
  envVars: Record<string, string>;
  executor: ExecutorHandle;
  tail: Promise<void>;
}

const records = new Map<string, SandboxRecord>();
// Ids we explicitly terminated, kept in an insertion-ordered Set so
// `wasTerminated` is O(1) and eviction drops the oldest — the same bounded
// pattern as `expired` below. An array with `.includes`/`.shift` was O(n) on
// every 404 lookup and every insert past the cap.
const terminated = new Set<string>();

// Creations in flight. Counted toward the cap because a reservation is taken
// before the worker is created and released only after insert: without it, N
// concurrent creates all read the same pre-insert size and overshoot the limit.
// Force-created workers bypass the pool's own parallelism semaphore, so this
// counter is the only bound on how many isolates exist.
let reserved = 0;
let draining = 0;

export interface CapacitySnapshot {
  active: number;
  reserved: number;
  draining: number;
  total: number;
  limit: number;
}

/** Reserves a slot, or returns false when the cap is already committed. */
export function tryReserve(): boolean {
  if (records.size + reserved + draining >= config.maxConcurrentSandboxes) {
    return false;
  }
  reserved++;
  return true;
}

/** Releases a reservation when no worker was created. */
export function releaseReservation(): void {
  if (reserved > 0) reserved--;
}

export function capacitySnapshot(): CapacitySnapshot {
  const active = records.size;
  return {
    active,
    reserved,
    draining,
    total: active + reserved + draining,
    limit: config.maxConcurrentSandboxes,
  };
}

function beginDraining(executor: ExecutorHandle): void {
  draining++;
  void (async () => {
    const acknowledgementStarted = Date.now();
    const acknowledged = await executor.terminate();
    telemetry.record(
      "terminate_acknowledgement",
      Date.now() - acknowledgementStarted,
      acknowledged ? "ok" : "unavailable",
    );

    const finalShutdownStarted = Date.now();
    const observed = await executor.waitForShutdown();
    if (observed.kind === "error" || observed.shutdown === null) {
      telemetry.record(
        "ack_to_final_shutdown",
        Date.now() - finalShutdownStarted,
        "unavailable",
      );
      // Finalization could not be observed. Keep capacity charged rather than
      // admitting a replacement while an unaccounted worker may still exist.
      return;
    }

    telemetry.record(
      "ack_to_final_shutdown",
      Date.now() - finalShutdownStarted,
      "ok",
    );
    telemetry.recordFinalShutdown(observed.shutdown);
    draining--;
  })().catch(() => {
    // The worker may still be alive, so this is intentionally fail closed.
    telemetry.record("ack_to_final_shutdown", 0, "unavailable");
  });
}

/** Converts a create reservation into charged draining capacity. */
export function moveReservationToDraining(executor: ExecutorHandle): void {
  if (reserved > 0) reserved--;
  beginDraining(executor);
}

export function rememberTerminated(sandboxID: string): void {
  terminated.add(sandboxID);
  while (terminated.size > config.terminatedIdMemory) {
    const oldest = terminated.values().next().value as string;
    terminated.delete(oldest);
  }
}

export function wasTerminated(sandboxID: string): boolean {
  return terminated.has(sandboxID);
}

export function size(): number {
  return records.size;
}

/** True if a live sandbox already holds this executor worker.
 *
 * Under `--policy oneshot` the pool silently ignores `forceCreate` and can hand
 * back an existing worker, which would put two sandboxes in one isolate: state
 * would leak across tenants and deleting either would kill both. Prose in the
 * README is not protection for that, so creation checks it.
 */
export function hasExecutorKey(key: string): boolean {
  for (const record of records.values()) {
    if (record.executor.key === key) return true;
  }
  return false;
}

export function insert(record: SandboxRecord): void {
  records.set(record.sandboxID, record);
}

export function get(sandboxID: string): SandboxRecord | undefined {
  return records.get(sandboxID);
}

export function list(): SandboxRecord[] {
  return [...records.values()];
}

/** Drops the record and immediately starts fail-closed worker draining. */
export async function reap(sandboxID: string): Promise<boolean> {
  const record = records.get(sandboxID);
  if (record === undefined) return false;
  records.delete(sandboxID);
  rememberTerminated(sandboxID);
  beginDraining(record.executor);
  return true;
}

// Ids reaped by TTL rather than by an explicit delete. `reap` records every id
// as terminated, so without this an expired sandbox would report
// `sandbox_terminated`; the spec wants `sandbox_expired`.
const expired = new Set<string>();

function rememberExpired(sandboxID: string): void {
  expired.add(sandboxID);
  while (expired.size > config.terminatedIdMemory) {
    const oldest = expired.values().next().value as string;
    expired.delete(oldest);
  }
}

/** Distinguishes a forgotten id from one we know we terminated. */
export function missingCode(
  sandboxID: string,
): "sandbox_expired" | "sandbox_terminated" | "sandbox_not_found" {
  // Expiry is checked first: `reap` also marks the id terminated.
  if (expired.has(sandboxID)) return "sandbox_expired";
  if (wasTerminated(sandboxID)) return "sandbox_terminated";
  return "sandbox_not_found";
}

export type MissingCode = ReturnType<typeof missingCode>;

/** Reaps the sandbox if its TTL passed, then reports what callers should do.
 *
 * Every route goes through this so expiry policy lives in exactly one place.
 */
export async function resolve(
  sandboxID: string,
): Promise<SandboxRecord | MissingCode> {
  const record = records.get(sandboxID);
  if (record === undefined) return missingCode(sandboxID);

  if (Date.now() >= record.expiresAt) {
    await reap(sandboxID);
    rememberExpired(sandboxID);
    return "sandbox_expired";
  }

  return record;
}

/** Reaps every expired sandbox, so listing never shows one.
 *
 * Scans synchronously and only touches records whose TTL has passed: their ids
 * are marked expired and dropped from the map up front, then the bounded
 * `terminate()` calls run concurrently. The old `for … await resolve(id)` both
 * ran `resolve` for live records and serialized k terminations, so every create
 * and every list waited out k × terminate. A live record is never terminated
 * here. `terminate()` is deadline-bounded, so a pinned worker cannot make this
 * hang unboundedly.
 */
export async function sweep(): Promise<void> {
  const now = Date.now();
  const due: SandboxRecord[] = [];
  for (const record of records.values()) {
    if (now >= record.expiresAt) due.push(record);
  }

  for (const record of due) {
    records.delete(record.sandboxID);
    rememberExpired(record.sandboxID);
    beginDraining(record.executor);
  }
}

/** Reported to clients as the envd version.
 *
 * The official SDK parses this with PEP 440 and refuses anything below 0.1.0,
 * so it cannot be a descriptive string. The runtime identifies itself through
 * `clientID` instead.
 */
export const ENVD_VERSION = "0.1.0";

export function describe(record: SandboxRecord): Record<string, unknown> {
  return {
    sandboxID: record.sandboxID,
    templateID: record.templateID,
    clientID: "edge-runtime-adapter",
    envdVersion: ENVD_VERSION,
    startedAt: new Date(record.createdAt).toISOString(),
    endAt: new Date(record.expiresAt).toISOString(),
    metadata: record.metadata,
    // Required by the SDK's sandbox models. A sandbox gets no dedicated core
    // and no writable disk, and a record exists only while running — expiry and
    // deletion both remove it.
    cpuCount: 1,
    memoryMB: config.sandboxMemoryMb,
    diskSizeMB: 0,
    state: "running",
  };
}
