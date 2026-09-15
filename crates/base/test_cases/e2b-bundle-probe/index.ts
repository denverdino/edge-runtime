let hits = 0;

Deno.serve(() => {
  hits++;
  return Response.json({
    hits,
    // A user worker must not be able to bundle: that would let sandboxed code
    // resolve and read arbitrary module graphs from the host filesystem.
    canBundle: typeof EdgeRuntime?.bundle === "function",
  });
});
