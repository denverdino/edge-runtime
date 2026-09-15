// Main worker for the pool-reuse identity test. Each request creates a worker
// for the same service path *without* `forceCreate`, choosing its profile and
// `allowNodeVm` from the query string, then forwards the (tagged) request to it
// and reports which isolate served it. If the pool keyed reuse on the service
// path alone, a later standard request would be handed the E2B isolate and
// node:vm would still work. The capability and profile must be part of the
// reuse identity.
Deno.serve(async (req: Request) => {
  const url = new URL(req.url);
  const allowNodeVm = url.searchParams.get("vm") === "1";
  const runtimeProfile = url.searchParams.get("profile") === "e2b"
    ? "e2bExecutor"
    : "standard";

  const worker = await EdgeRuntime.userWorkers.create({
    servicePath: "./test_cases/e2b-vm-probe",
    envVars: [],
    runtimeProfile,
    context: { allowNodeVm },
    permissions: { allow_all: false },
  });

  const inner = await (await worker.fetch(req)).json();

  return Response.json({ key: worker.key, vm: inner });
});
