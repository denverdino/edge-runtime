export interface TimerScope {
  globals: Record<string, unknown>;
  cancelAll(): void;
  cancelIntervals(): void;
}

export function createTimerScope(
  onCallbackError: (error: unknown) => void = () => {},
): TimerScope {
  const timeouts = new Set<number>();
  const intervals = new Set<number>();

  const runCallback = (
    fn: (...args: unknown[]) => void,
    args: unknown[],
  ) => {
    try {
      fn(...args);
    } catch (error) {
      onCallbackError(error);
    }
  };

  const globals: Record<string, unknown> = {
    setTimeout: (
      fn: (...args: unknown[]) => void,
      ms?: number,
      ...args: unknown[]
    ) => {
      const id = setTimeout(() => {
        timeouts.delete(id);
        runCallback(fn, args);
      }, ms);
      timeouts.add(id);
      return id;
    },
    clearTimeout: (id: number) => {
      timeouts.delete(id);
      clearTimeout(id);
    },
    setInterval: (
      fn: (...args: unknown[]) => void,
      ms?: number,
      ...args: unknown[]
    ) => {
      const id = setInterval(() => runCallback(fn, args), ms);
      intervals.add(id);
      return id;
    },
    clearInterval: (id: number) => {
      intervals.delete(id);
      clearInterval(id);
    },
  };

  return {
    globals,
    cancelAll() {
      for (const id of timeouts) clearTimeout(id);
      for (const id of intervals) clearInterval(id);
      timeouts.clear();
      intervals.clear();
    },
    cancelIntervals() {
      for (const id of intervals) clearInterval(id);
      intervals.clear();
    },
  };
}
