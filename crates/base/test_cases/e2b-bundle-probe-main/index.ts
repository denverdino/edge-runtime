Deno.serve(async (req: Request) => {
  const url = new URL(req.url);
  const servicePath = "./test_cases/e2b-bundle-probe";

  if (url.searchParams.get("reuse") === "1") {
    // Bundling once and creating twice is the whole point: the second worker
    // must boot from the same bytes rather than re-resolving the module graph.
    const eszip = await EdgeRuntime.bundle(`${servicePath}/index.ts`);
    const bodies: string[] = [];

    for (let i = 0; i < 2; i++) {
      const worker = await EdgeRuntime.userWorkers.create({
        servicePath,
        maybeEszip: eszip,
        forceCreate: true,
        envVars: [],
        permissions: { allow_all: false },
      });

      // `UserWorker.fetch` reads the Supabase tag off the request, so a
      // freshly built one has to inherit it from the incoming request.
      const probeReq = new Request("http://probe/");
      EdgeRuntime.applySupabaseTag(req, probeReq);

      const response = await worker.fetch(probeReq);
      bodies.push(await response.text());
    }

    return Response.json({ ok: true, bodies });
  }

  try {
    const eszip = await EdgeRuntime.bundle(`${servicePath}/index.ts`);
    return Response.json({
      ok: true,
      isUint8Array: eszip instanceof Uint8Array,
      byteLength: eszip.byteLength,
    });
  } catch (error) {
    return Response.json({ ok: false, error: String(error) });
  }
});
