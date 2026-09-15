import vm from "node:vm";

Deno.serve(() => {
  try {
    const ctx = vm.createContext({});
    new vm.Script("globalThis.__probe = 1 + 1").runInContext(ctx);
    const result = new vm.Script("__probe").runInContext(ctx);
    return Response.json({ ok: true, result });
  } catch (e) {
    return Response.json({ ok: false, error: String(e) });
  }
});
