Deno.serve(async (req: Request) => {
  // A user worker that did not opt into transpile must not be able to reach
  // the op: transpile parses arbitrary source on a huge stack, so it is a
  // capability the sandbox grants only to the trusted E2B executor.
  const worker = await EdgeRuntime.userWorkers.create({
    servicePath: "./test_cases/e2b-transpile-denied-probe",
    forceCreate: true,
    envVars: [],
    permissions: { allow_all: false },
  });

  return await worker.fetch(req);
});
