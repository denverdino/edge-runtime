Deno.serve(async (req: Request) => {
  if ("transpile" in EdgeRuntime) {
    return Response.json(
      { error: "EdgeRuntime.transpile is exposed to the main worker" },
      { status: 500 },
    );
  }

  const worker = await EdgeRuntime.userWorkers.create({
    servicePath: "./test_cases/e2b-transpile-probe",
    forceCreate: true,
    envVars: [],
    context: { allowTranspile: true },
    permissions: { allow_all: false },
  });
  return await worker.fetch(req);
});
