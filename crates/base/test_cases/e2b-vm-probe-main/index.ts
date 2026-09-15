Deno.serve(async (req: Request) => {
  const url = new URL(req.url);
  const allowNodeVm = url.searchParams.get("vm") === "1";

  const worker = await EdgeRuntime.userWorkers.create({
    servicePath: "./test_cases/e2b-vm-probe",
    forceCreate: true,
    envVars: [],
    context: { allowNodeVm },
    permissions: { allow_all: false },
  });

  return await worker.fetch(req);
});
