import { config } from "./config.ts";
import type { SandboxRecord } from "./registry.ts";

export class QueueTimeout extends Error {
  constructor() {
    super("timed out waiting for an execution slot");
    this.name = "QueueTimeout";
  }
}

function deadline<T>(promise: Promise<T>, ms: number): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => reject(new QueueTimeout()), ms);
    promise.then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      (error) => {
        clearTimeout(timer);
        reject(error);
      },
    );
  });
}

/** Runs `fn` after every earlier execution for this sandbox has finished.
 *
 * The tail always resolves and is never rejected, so one failed execution
 * cannot poison the sandbox for its successors.
 */
export async function withSandboxLock<T>(
  record: SandboxRecord,
  fn: () => Promise<T>,
): Promise<T> {
  const previous = record.tail;
  let release!: () => void;
  record.tail = new Promise<void>((resolve) => {
    release = resolve;
  });

  try {
    await deadline(previous, config.queueWaitTimeoutMs);
  } catch (error) {
    // Chain rather than release. Releasing here would resolve the tail our
    // successor is already awaiting, so it would start while the predecessor is
    // still executing — breaking the one-execution-per-sandbox invariant. This
    // keeps the queue ordered while still guaranteeing the tail resolves.
    previous.finally(release);
    throw error;
  }

  try {
    return await fn();
  } finally {
    release();
  }
}

let active = 0;
const waiting: Array<() => void> = [];

let createActive = 0;
const createWaiting: Array<() => void> = [];

export interface QueueSnapshot {
  limit: number;
  active: number;
  waiting: number;
}

/** Hands a create slot to the next FIFO waiter, or frees it. */
function makeCreateRelease(): () => void {
  let released = false;
  return () => {
    if (released) return;
    released = true;
    const next = createWaiting.shift();
    if (next !== undefined) {
      // Keep this slot counted while it is handed to the next FIFO waiter.
      next();
    } else {
      createActive--;
    }
  };
}

/** Bounds concurrent bundle/create/boot/init work in FIFO order. */
export async function acquireCreateSlot(): Promise<() => void> {
  const limit = config.maxConcurrentSandboxCreations;
  if (limit !== 0 && createActive >= limit) {
    let admit!: () => void;
    const queued = new Promise<void>((resolve) => {
      admit = resolve;
    });
    createWaiting.push(admit);

    try {
      await deadline(queued, config.queueWaitTimeoutMs);
    } catch (error) {
      const index = createWaiting.indexOf(admit);
      if (index !== -1) {
        // Still queued: no release reached us. Remove this waiter so a later
        // release cannot hand the slot to a timed-out request.
        createWaiting.splice(index, 1);
      } else {
        // A release handed us this slot in the same turn as the deadline. The
        // caller has timed out, so pass the slot on without starting its boot.
        makeCreateRelease()();
      }
      throw error;
    }
  } else {
    createActive++;
  }

  return makeCreateRelease();
}

export function createGateSnapshot(): QueueSnapshot {
  return {
    limit: config.maxConcurrentSandboxCreations,
    active: createActive,
    waiting: createWaiting.length,
  };
}

export function globalExecutionQueueSnapshot(): QueueSnapshot {
  return {
    limit: config.maxConcurrentExecutions,
    active,
    waiting: waiting.length,
  };
}

/** Hands the slot to the next waiter, or frees it. Idempotent per acquire. */
function makeRelease(): () => void {
  let released = false;
  return () => {
    if (released) return;
    released = true;
    active--;
    const next = waiting.shift();
    if (next !== undefined) next();
  };
}

/** Bounds total in-flight executions across every sandbox. */
export async function acquireGlobalSlot(): Promise<() => void> {
  if (active >= config.maxConcurrentExecutions) {
    let admit!: () => void;
    const queued = new Promise<void>((resolve) => {
      admit = resolve;
    });
    waiting.push(admit);

    try {
      await deadline(queued, config.queueWaitTimeoutMs);
    } catch (error) {
      const index = waiting.indexOf(admit);
      if (index !== -1) {
        // Still queued: no release reached us. Drop our waiter so a later
        // release does not hand the slot to nobody.
        waiting.splice(index, 1);
        throw error;
      }
      // A release() already shifted us off `waiting` and admitted us in the
      // same turn our deadline fired. The slot is ours: if we just threw, it
      // would be used by nobody and no other waiter woken. Take it as a normal
      // acquire would, then release it so the next waiter runs instead.
      active++;
      makeRelease()();
      throw error;
    }
  }

  active++;
  return makeRelease();
}
