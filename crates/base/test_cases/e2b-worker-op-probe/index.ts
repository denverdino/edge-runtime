import vm from "node:vm";

// A user worker that reaches a privileged worker-management op the way a real
// sandbox can: a `node:vm` context leaks the host `Deno.core`, so code running
// inside it can call `op_user_worker_terminate`. That op runs in *this* user
// worker, whose OpState has no `UserWorkerMsgs` sender (it is installed only in
// the main worker). The mandatory `borrow()` for the absent sender is a
// non-unwinding panic that aborts the whole runtime; the hardened op must
// instead fail closed with a thrown capability error.
Deno.serve(async () => {
  const ctx = vm.createContext({});

  const pending = new vm.Script(
    `(async () => {
       try {
         await Deno.core.ops.op_user_worker_terminate(
           "00000000-0000-0000-0000-000000000000",
         );
         return { threw: false, error: "" };
       } catch (e) {
         return { threw: true, error: String(e) };
       }
     })()`,
  ).runInContext(ctx);

  const outcome = await pending;

  return Response.json(outcome);
});
