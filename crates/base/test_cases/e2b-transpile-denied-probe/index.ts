Deno.serve(async () => {
  try {
    const out = await EdgeRuntime.transpile("let x: number = 1;", "probe.ts");
    return Response.json({ threw: false, out });
  } catch (error) {
    return Response.json({ threw: true, error: String(error) });
  }
});
