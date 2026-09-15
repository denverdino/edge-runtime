Deno.serve(async (req: Request) => {
  const worker = await EdgeRuntime.userWorkers.create({
    servicePath: "./test_cases/e2b-bootstrap-hardening",
    envVars: [],
    runtimeProfile: "e2bExecutor",
    context: { shouldBootstrapMockFnThrowError: true },
    permissions: { allow_all: false },
  });

  return await worker.fetch(req);
});
