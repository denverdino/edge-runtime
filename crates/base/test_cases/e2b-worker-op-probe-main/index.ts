// Main worker: spawns the probe user worker and forwards the request so the
// privileged op is invoked from inside a user worker isolate.
Deno.serve(async (req: Request) => {
  const worker = await EdgeRuntime.userWorkers.create({
    servicePath: "./test_cases/e2b-worker-op-probe",
    forceCreate: true,
    envVars: [],
    context: { allowNodeVm: true },
    permissions: { allow_all: false },
  });

  return await worker.fetch(req);
});
