Deno.serve(async (req: Request) => {
  const allowTranspile = new URL(req.url).searchParams.get("transpile") === "1";
  const worker = await EdgeRuntime.userWorkers.create({
    servicePath: "./test_cases/e2b-transpile-probe",
    envVars: [],
    context: { allowTranspile },
    permissions: { allow_all: false },
  });
  const inner = await worker.fetch(req);

  return Response.json(
    { key: worker.key, status: inner.status, body: await inner.text() },
    { status: inner.status },
  );
});
