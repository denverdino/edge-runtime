Deno.serve(async () => {
  const results: Record<string, unknown> = {};

  // The shipped executor lives under the bundle root, so the main worker must
  // still be able to bundle it into the eszip that every sandbox boots from.
  try {
    const eszip = await EdgeRuntime.bundle(
      "../../examples/e2b-executor/index.ts",
    );
    results.executor = {
      ok: true,
      isUint8Array: eszip instanceof Uint8Array,
      byteLength: eszip.byteLength,
    };
  } catch (error) {
    results.executor = { ok: false, error: String(error) };
  }

  // An absolute path outside the bundle root must be refused: bundling reads and
  // resolves whatever it points at, so it cannot be allowed to escape the root.
  try {
    await EdgeRuntime.bundle("/etc/passwd");
    results.outside = { ok: true };
  } catch (error) {
    results.outside = { ok: false, error: String(error) };
  }

  return Response.json(results);
});
