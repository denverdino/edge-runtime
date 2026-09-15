import vm from "node:vm";

function observeError(action: () => void) {
  try {
    action();
    return { name: null, message: null };
  } catch (error) {
    if (error instanceof Error) {
      return { name: error.name, message: error.message };
    }
    return { name: typeof error, message: String(error) };
  }
}

Deno.serve(() => {
  const context = vm.createContext({});
  const result = new vm.Script("1 + 1").runInContext(context);
  const signalHandler = () => {};

  return Response.json({
    vm: { result },
    realPath: observeError(() => Deno.realPathSync(".")),
    mocks: {
      kill: observeError(() => Deno.kill(1)),
      exit: observeError(() => Deno.exit(1)),
      addSignalListener: observeError(() =>
        Deno.addSignalListener("SIGTERM", signalHandler)
      ),
      removeSignalListener: observeError(() =>
        Deno.removeSignalListener("SIGTERM", signalHandler)
      ),
    },
    sharedMemory: observeError(() =>
      new WebAssembly.Memory({ initial: 1, maximum: 1, shared: true })
    ),
    execPath: Deno.execPath(),
    memoryUsage: Deno.memoryUsage(),
  });
});
