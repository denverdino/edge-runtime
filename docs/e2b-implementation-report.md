# E2B-Compatible Sandbox on Supabase Edge Runtime — Implementation Report

Two services plus five supporting runtime and adapter changes give Edge Runtime
a stateful, E2B-shaped JS/TS sandbox:
`1 sandbox = 1 context_id = 1 long-lived user worker
= 1 persistent vm.Context`.

- `examples/e2b-adapter` — control plane in the **main** worker. Registry, auth,
  TTL, concurrency, error mapping. It never compiles or evaluates user code.
- `examples/e2b-executor` — the **user** worker. One persistent `node:vm`
  context per sandbox, under memory / CPU / wall-clock limits and denied
  permissions.

All figures and outputs below were measured on this branch, not inferred.

## 1. E2B compatibility

### Implemented

| Method | Path                                    | Behavior                                                                                                                                      |
| ------ | --------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| POST   | `/sandboxes`                            | Spawns an executor and returns sandbox metadata plus the create-only `envdAccessToken`; accepts `code-interpreter-v1`                         |
| GET    | `/sandboxes`, `/v2/sandboxes`           | Lists live sandboxes (`defaultListLimit` 100) without exposing the sandbox token                                                              |
| GET    | `/sandboxes/{id}`, `/v2/sandboxes/{id}` | Single sandbox metadata without exposing the sandbox token                                                                                    |
| DELETE | `/sandboxes/{id}`, `/v2/sandboxes/{id}` | Terminates the isolate, returns 204                                                                                                           |
| POST   | `/sandboxes/{id}/timeout`, `/v2/...`    | Sets TTL, clamped to the wall-clock ceiling                                                                                                   |
| POST   | `/sandboxes/{id}/jupyter/execute`       | Official Code Interpreter `run_code` transport; `X-Access-Token`; NDJSON stdout/stderr/result/error events; omitted language means JavaScript |
| POST   | `/execute`                              | Stateful execution against `context_id` (non-E2B; this project's own surface)                                                                 |

`X-API-Key` protects control-plane and custom `/execute` routes. The adapter is
fail-closed: with no `E2B_API_KEY` configured, those requests are 401. The
Jupyter operation instead requires the per-sandbox `X-Access-Token` returned
only by create. When a request detects a dead executor, custom `/execute`
returns `410 sandbox_terminated`; Jupyter returns `500` for that detecting
request, reaps the record, and returns `404` to later requests.

### Not implemented

Filesystem endpoints (`/files`), commands, PTY, process spawn, port forwarding,
metrics, sandbox pause/resume, snapshots, envd ConnectRPC, and production
wildcard-host DNS/TLS routing. **Python inside the sandbox is a declared
non-goal** — `language: "python"` returns `unsupported_language`. Python appears
in this project only as the language of the HTTP test suites.

## 2. Executor

**`context_id` → worker.** `POST /sandboxes` calls
`EdgeRuntime.userWorkers.create({ forceCreate: true })` and records the handle
in the adapter registry keyed by `sandboxID`. Every `/execute` for that
`context_id` reaches the same worker. Creation refuses a worker another live
sandbox already holds, so two tenants can never land in one isolate.

**Persistent state.** The executor holds one `vm.createContext()` per sandbox
for the worker's life. `vm.Script.runInContext` on the same context object
preserves the global lexical environment, so `let`/`const`/`var`/function
declarations survive across requests. Re-declaring a `let` in a later execution
is a `SyntaxError` — raw `vm` semantics, not a REPL that rewrites declarations.

**TypeScript.** `language: "typescript"` is stripped to JavaScript by
`EdgeRuntime.transpile` (`deno_ast`, `op_transpile_ts`) before evaluation. Types
are erased, values persist, and TS and JS share one context.

**Verified stateful demo** (executor harness, real HTTP):

```text
let x = 1                    -> result null   (undefined)
x++                          -> result 1
x                            -> result 2
const t: number = x * 21; t  -> result 42
```

Through the public adapter API, one sandbox, four requests:

```text
POST /sandboxes  -> 201 {"sandboxID":"00bc2567-…","templateID":"base",…}
let n = 1                              -> undefined
n += 41                                -> 42
const label: string = `n=${n}`; label   -> "n=42"
process.env.GREETING                   -> "hi"
process.env.E2B_API_KEY                -> undefined
fetch("https://example.com")           -> runtime_error NotCapable: Requires net access
DELETE /sandboxes/{id}                 -> 204
POST /execute (same context_id)        -> 410 sandbox_terminated
```

## 3. Security

**Per-sandbox worker limits** (`examples/e2b-adapter/config.ts`, defaults):

| Setting                     | Default | Effect                                                                          |
| --------------------------- | ------- | ------------------------------------------------------------------------------- |
| `SANDBOX_MEMORY_MB`         | 128     | `memoryLimitMb` — isolate heap cap                                              |
| `SANDBOX_WALL_CLOCK_MS`     | 3600000 | `workerTimeoutMs` — starts at boot, never resets                                |
| `CPU_LIMIT_MS`              | 600000  | `cpuTimeSoftLimitMs` and `cpuTimeHardLimitMs`, cumulative for the worker's life |
| `EXECUTION_TIMEOUT_MS`      | 10000   | Synchronous `vm` timeout per execution                                          |
| `EXECUTOR_ASYNC_TIMEOUT_MS` | 12000   | Executor-side async deadline                                                    |
| `ADAPTER_FETCH_TIMEOUT_MS`  | 15000   | Adapter's own deadline, raced against a real timer                              |
| `QUEUE_WAIT_TIMEOUT_MS`     | 30000   | Max wait for the per-sandbox lock                                               |

**Permissions.** The executor is created with `allow_all: false` and `allow_env`
restricted to its tuning variables. Nothing else is granted — in this runtime a
permission list is a _grant_, and an empty list grants globally, so capabilities
are denied by **omitting** the field. The vm context shadows non-allowlisted
host globals, including `fetch`, `Deno`, `WebSocket`, and `EdgeRuntime`, while
safe facades provide the required APIs. Denied worker permissions and the JS
bootstrap denylist remain a second layer because escape into the executor realm
is assumed possible and the vm boundary is not the sole security boundary.

**Authentication.** One configured `E2B_API_KEY` defines one control-plane trust
domain. Every key holder can list, execute through the custom endpoint, and
delete every sandbox. This is a single-tenant deployment model, not tenant
isolation. Each create also returns a random `envdAccessToken`; only the
matching `X-Access-Token` authorizes that sandbox's Jupyter route. GET and list
responses do not include it.

**Secrets.** Sandbox `envVars` are delivered per request and scoped to the
execution. The adapter's own `E2B_API_KEY` never reaches a sandbox
(`process.env.E2B_API_KEY` → `undefined`, verified above), and the per-sandbox
token is not logged or exposed after creation.

**Concurrency and size limits:**

| Setting                                       | Default          | Effect                                                               |
| --------------------------------------------- | ---------------- | -------------------------------------------------------------------- |
| `MAX_CONCURRENT_SANDBOXES`                    | 128              | Reserved before spawn, so the cap holds under a create race          |
| `MAX_CONCURRENT_EXECUTIONS`                   | 32               | Global semaphore across sandboxes                                    |
| `MAX_REQUEST_BYTES`                           | 524288           | Body cap enforced _while reading_, before buffering                  |
| `MAX_CODE_SIZE_BYTES`                         | 262144           | Snippet size                                                         |
| `MAX_OUTPUT_BYTES`                            | 65536            | Captured console output                                              |
| `SERIALIZE_MAX_DEPTH` / `_ENTRIES` / `_NODES` | 8 / 1000 / 10000 | Shared result-serialization budget                                   |
| `TERMINATED_ID_MEMORY`                        | 1024             | Dead `context_id`s remembered, so a deleted sandbox is never revived |

Executions on one sandbox are serialized by a FIFO tail-promise lock; different
sandboxes run concurrently.

## 4. Runtime and adapter changes

Five supporting changes, each required by the above:

1. **`ext/node/ops/vm.rs`** — per-worker `node:vm` gate. All seven vm ops route
   through `vm_permitted()`, which honors the `allowNodeVm` context marker set
   by `userWorkers.create`, so a sandbox uses `node:vm` without `allow_run`. The
   `OpState` borrow is released before user code runs: this op is reentrant, and
   a nested op re-borrowing `OpState` would hit a `BorrowMutError` inside a
   function that cannot unwind, aborting the process.
2. **`ext/workers` + `crates/base`** — `op_user_worker_terminate` and
   `UserWorker.prototype.terminate()`, so `DELETE /sandboxes/{id}` reaps that
   one isolate. Termination cancels supervision and then shuts the worker down;
   under per-request policies the isolate is reaped via a V8 interrupt with
   `TerminationRequested`.
3. **`ext/runtime`** — asynchronous `op_transpile_ts` exposed as
   `EdgeRuntime.transpile`. Only an explicitly opted-in worker can call it; the
   adapter grants it only to the executor. `deno_ast`'s parser is recursive
   descent with no depth guard, and a stack overflow is not a panic —
   `catch_unwind` cannot intercept it. Bounding nesting by counting brackets was
   measured to be useless (`!`, `x=>`, `a?1:` and `if(a)` chains all recurse at
   constant bracket depth). The op runs on a dedicated 512 MiB-stack thread,
   caps source at 16 KiB, and uses a process-wide concurrency gate.
4. **`crates/base` bundle op** — only the main worker can bundle, and canonical
   entrypoints are confined to the repository root containing `deno.json`.
   Symlink traversal cannot escape that root.
5. **Adapter lifecycle deadlines** — readiness and execution deadlines include
   the complete response-body read, while worker termination acknowledgements
   are also bounded. Stalled cleanup therefore cannot block create, delete, or
   expiry sweeps indefinitely.

## 5. Verification

| Check                                     | Command                                     | Observed result                                                                  |
| ----------------------------------------- | ------------------------------------------- | -------------------------------------------------------------------------------- |
| Format                                    | `deno run -A ./scripts/format.js`           | **failed, exit 12** — dprint exec plugin download returned HTTP 403              |
| Clippy                                    | `./scripts/clippy.sh`                       | **failed, exit 101** — two baseline `crates/cpu_timer` warnings denied as errors |
| E2B integration (Rust)                    | `./scripts/test.sh test_e2b`                | **56 passed, 0 failed, 0 ignored; 133 filtered out**                             |
| Fresh debug build                         | `cargo build`                               | **passed**                                                                       |
| Adapter via official Code Interpreter SDK | `python3 tests/e2b/test_sdk.py`             | **21/21 passed**                                                                 |
| Executor HTTP (Python)                    | `python3 tests/e2b/test_executor.py`        | **23/23 passed, 0 skipped**                                                      |
| Approved public user flow                 | `LocalSandbox.run_code` against the adapter | **passed** — JavaScript `2`, TypeScript `4`, error recovery retained state `2`   |

The official SDK run used `e2b-code-interpreter 2.8.1` and `e2b 2.46.0`. Both
sync and async clients used the local `_jupyter_url` override. The SDK suite ran
against the freshly built adapter; the executor suite ran separately against
`e2b-executor-harness`, exactly as `tests/e2b/README.md` requires. No required
suite was skipped.

The required formatter was run once and produced exactly:

```text
Error resolving plugin https://plugins.dprint.dev/exec-0.5.0.json: Error downloading https://plugins.dprint.dev/exec-0.5.0.json - Error: https://plugins.dprint.dev/exec-0.5.0.json: status code 403
```

It exited 12 before formatting. No formatter configuration or plugin was
changed, and no substitute formatter was used.

Clippy exited 101 at two pre-existing lines in an untouched file:
`crates/cpu_timer/src/lib.rs:8` has an unused `tokio::sync::Mutex` import and
line 134 binds an unused `tx`. `-D warnings` promotes both to errors. The E2B
test build also reports the unchanged `base::utils::test_utils` import at
`crates/base/tests/integration_tests.rs:30`, and `cargo build` reports the known
macOS `libc::mach_host_self` / `mach_task_self` deprecations in
`ext/node/ops/os/cpus.rs`. None of those warning lines is changed by this work;
no new failure was reported in a task-touched line before clippy stopped.

Both Python suites need a running server; see `tests/e2b/README.md`. The Rust
suite binds real ports and spawns workers, so CI runs it with `-j 1`.

## 6. Deliberate deviations from the design spec

1. **`MAX_TRANSPILE_SOURCE_BYTES` is a compile-time constant**, not an
   environment variable as §9 implies. It is enforced inside `op_transpile_ts`
   in Rust, where an adapter-side env var could not reach it, and it guards
   against a process abort — so it is not a tunable.
2. **On custom `/execute`, `WorkerRequestCancelled` maps to `sandbox_terminated`
   (410) and reaps the sandbox**, whereas §11 records it as "client gone,
   sandbox alive, no response". Jupyter maps the detecting request to `500` and
   later requests to `404`. The spec's premise was wrong: the runtime raises it
   from `WorkerError::RequestCancelledBySupervisor`
   (`crates/base/src/worker/pool.rs`), which fires when the supervisor kills the
   isolate on a CPU or wall-clock limit — not when a client disconnects.
   Treating it as recoverable would hand back an empty context under a live
   `context_id`.

3. **`envdVersion` is `0.1.0`, not the literal `edge-runtime`** that §10
   specifies, and `describe()` additionally emits `cpuCount`, `memoryMB`,
   `diskSizeMB` and `state`. Both were forced by the official E2B Python SDK: it
   parses `envdVersion` with PEP 440 and rejects anything below `0.1.0`
   (`sandbox_api.py`), and its `ListedSandbox` / `SandboxDetail` models require
   those four fields. The runtime still identifies itself through
   `clientID: edge-runtime-adapter`. Without these the SDK fails before reaching
   any behaviour, so the spec's descriptive string cost more compatibility than
   it bought.

## 7. Known limitations

The nine accepted limitations are listed in
`examples/e2b-adapter/README.md#accepted-limitations`. The ones that most affect
operation:

- A synchronous loop inside a **timer callback** scheduled by an earlier
  execution is bounded by neither the execution timeout nor the async deadline;
  it pins the shared worker thread until `CPU_LIMIT_MS`. In debug builds the
  user-worker pool defaults to one thread, so that means every sandbox.
- Infinite microtask loops use only intrinsics and no execution-level timeout
  can interrupt them. The adapter's deadline still frees the request, the lock,
  and the global slot. Custom `/execute` reports `410`; Jupyter reports `500`
  for the detecting request and `404` thereafter. The isolate is what lingers.
- Sandbox lifetime cannot exceed `SANDBOX_WALL_CLOCK_MS`; the wall clock starts
  at boot and never resets, so `POST /{id}/timeout` refuses values past the
  ceiling rather than granting a TTL the runtime will not honor.
- CPU-limit and memory-limit deaths are indistinguishable to the control plane.
  Custom `/execute` maps both to `sandbox_terminated`; Jupyter uses its
  detecting-request `500`, then `404` mapping. The reason appears in
  event-worker logs.
- All state is lost on runtime restart, by design. No database, no snapshots.
- The adapter must not run under `--policy oneshot`: the pool ignores
  `forceCreate` and retires the executor right after its init probe, so no
  sandbox can serve an execution. Creation refuses a worker another live sandbox
  already holds, so this fails loudly rather than leaking state.
