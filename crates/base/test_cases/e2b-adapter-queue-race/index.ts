import { setLimitsForTesting } from "../../../../examples/e2b-adapter/config.ts";
import { acquireGlobalSlot } from "../../../../examples/e2b-adapter/queue.ts";

// One global slot, so the second and third acquirers must queue.
setLimitsForTesting({ maxConcurrentExecutions: 1, queueWaitTimeoutMs: 50 });

// Deterministically forces the lost-wakeup interleave: a queued waiter is
// admitted by release() in the very turn its own deadline fires. A hand-driven
// clock lets us fire that deadline synchronously right after the admit — before
// the admit's clearTimeout microtask can cancel it — which is the window a real
// macrotask timer cannot be nudged into by hand. With the bug, the admitted
// waiter throws and the slot it was handed is stranded, so the next waiter
// never runs even though capacity is free. Reports whether the next waiter ran.
Deno.serve(async (_req: Request) => {
  const realSetTimeout = globalThis.setTimeout;
  const realClearTimeout = globalThis.clearTimeout;
  const timers = new Map<number, () => void>();
  let nextId = 1;

  // deno-lint-ignore no-explicit-any
  (globalThis as any).setTimeout = (cb: () => void): number => {
    const id = nextId++;
    timers.set(id, cb);
    return id;
  };
  // deno-lint-ignore no-explicit-any
  (globalThis as any).clearTimeout = (id: number): void => {
    timers.delete(id);
  };

  let secondWaiterRan = false;
  try {
    // Take the only slot; this acquire arms no deadline timer.
    const release0 = await acquireGlobalSlot();

    // Two waiters queue behind it. `first` arms deadline timer id 1, `second`
    // timer id 2.
    const first = acquireGlobalSlot().then((rel) => rel(), () => {});
    const second = acquireGlobalSlot().then((rel) => {
      secondWaiterRan = true;
      rel();
    }, () => {});

    // Hand the slot to `first`, then fire ITS deadline in the same synchronous
    // turn, before the admit's clearTimeout microtask runs.
    release0();
    timers.get(1)?.();

    // Drain over a few real macrotask turns. The fix wakes `second`; the bug
    // leaves it stranded, so we never await it directly.
    for (let i = 0; i < 5; i++) {
      await new Promise((resolve) => realSetTimeout(resolve, 0));
    }
    void first;
    void second;
  } finally {
    globalThis.setTimeout = realSetTimeout;
    globalThis.clearTimeout = realClearTimeout;
  }

  return Response.json({ secondWaiterRan });
});
