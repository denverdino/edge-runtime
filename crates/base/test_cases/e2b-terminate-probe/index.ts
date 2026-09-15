let shutdownWait: Promise<WorkerShutdown | null> | null = null;

Deno.serve(async (req: Request) => {
  if (new URL(req.url).pathname === "/shutdown") {
    return Response.json({ shutdown: await shutdownWait });
  }

  const worker = await EdgeRuntime.userWorkers.create({
    servicePath: "./test_cases/e2b-terminate-target",
    forceCreate: true,
    envVars: [],
    permissions: { allow_all: false },
  });
  const timingProbe = new URL(req.url).searchParams.has("timings");
  const reusedWorker = timingProbe
    ? await EdgeRuntime.userWorkers.create({
      servicePath: "./test_cases/e2b-terminate-target",
      envVars: [],
      permissions: { allow_all: false },
    })
    : null;

  const warm = new Request("http://worker/", { method: "GET" });
  EdgeRuntime.applySupabaseTag(req, warm);
  const warmResponse = await worker.fetch(warm);
  await warmResponse.text();

  const before = Object.keys(await EdgeRuntime.userWorkers.memStats());
  shutdownWait = worker.waitForShutdown();

  const terminated = await worker.terminate();
  const after = Object.keys(await EdgeRuntime.userWorkers.memStats());

  return Response.json({
    terminated,
    trackedBefore: before.includes(worker.key),
    trackedAfter: after.includes(worker.key),
    timing: {
      fresh: {
        key: worker.key,
        runtimeInitMs: worker.runtimeInitMs,
        moduleInitMs: worker.moduleInitMs,
        runtimeInit: worker.runtimeInit,
      },
      reused: reusedWorker && {
        key: reusedWorker.key,
        runtimeInitMs: reusedWorker.runtimeInitMs,
        moduleInitMs: reusedWorker.moduleInitMs,
        runtimeInit: reusedWorker.runtimeInit,
      },
    },
    terminatedAgain: await worker.terminate(),
  });
});
