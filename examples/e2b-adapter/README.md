# E2B Adapter

An E2B-compatible control plane for Edge Runtime. Each sandbox is one
force-created User Worker holding a persistent JavaScript context, so variables
survive across HTTP requests.

The adapter itself never compiles or evaluates user code — it only routes and
proxies. All execution happens in `examples/e2b-executor`.

## Compatibility boundary

The sandbox routes mirror E2B's shapes; `/execute` is a **custom extension**.
The official Code Interpreter SDK's `run_code` works locally through the
Jupyter-compatible route when `_jupyter_url` is overridden as shown below. This
is still **not** a drop-in E2B deployment: production wildcard-host DNS/TLS,
envd ConnectRPC, files, and commands are not implemented.

| Capability                                                                                                   | Status                                             |
| ------------------------------------------------------------------------------------------------------------ | -------------------------------------------------- |
| `POST /sandboxes`, `GET`, `DELETE`, `POST /{id}/timeout`, `GET /v2/sandboxes`                                | implemented                                        |
| `POST /sandboxes/{id}/jupyter/execute` (official `run_code`)                                                 | implemented locally, stateful JS/TS                |
| `POST /execute` (stateful JS/TS)                                                                             | implemented, custom extension                      |
| Authenticated `GET /internal/metrics`                                                                        | implemented operational telemetry, outside E2B API |
| Python inside the sandbox                                                                                    | **not supported** — returns `unsupported_language` |
| Template build, envd, filesystem/commands API, PTY, pause/resume, snapshot, fork, volumes, E2B metrics, logs | not implemented                                    |

See `openapi.yaml` for the full surface.

## Running it

Run this from the repository root:

```bash
cargo build

E2B_API_KEY=e2b_0000000000000000000000000000000000000000 \
./target/debug/edge-runtime start --main-service ./examples/e2b-adapter -p 9998
```

Two settings matter:

- **`E2B_API_KEY` is required for lifecycle/control-plane and custom `/execute`
  routes.** The adapter fails closed for those routes: with no key configured,
  they get `401`. Jupyter execution instead uses the per-sandbox
  `X-Access-Token` returned by create.
- **`EXECUTOR_SERVICE_PATH` is resolved relative to the process working
  directory.** It defaults to `./examples/e2b-executor`, matching the command
  above. Override it when starting from another directory. Rust integration
  tests run from `crates/base` and explicitly use `../../examples/e2b-executor`.

One configured API key defines one trust domain. Every holder of that key can
list, execute in every sandbox through the custom `/execute` route, and delete
every sandbox. This adapter is single-tenant; the key authenticates access to
the deployment but does not provide tenant isolation.

A stale binary is a common source of confusing failures: rebuild after changing
any Rust, or worker creation fails with `Unknown built-in "node:" module: vm`.

## Official Code Interpreter SDK

Install `e2b-code-interpreter`, then point the sync or async client's local
Jupyter URL at this adapter. The override is needed because production E2B uses
wildcard-host DNS/TLS routing, which this local HTTP adapter does not implement.

```python
from e2b_code_interpreter import AsyncSandbox, Sandbox

BASE_URL = "http://localhost:9998"
KEY = "e2b_0000000000000000000000000000000000000000"


class LocalSandbox(Sandbox):
    @property
    def _jupyter_url(self) -> str:
        return f"{BASE_URL}/sandboxes/{self.sandbox_id}/jupyter"


class LocalAsyncSandbox(AsyncSandbox):
    @property
    def _jupyter_url(self) -> str:
        return f"{BASE_URL}/sandboxes/{self.sandbox_id}/jupyter"


with LocalSandbox.create(api_key=KEY, api_url=BASE_URL) as sandbox:
    sandbox.run_code("x = 1")
    execution = sandbox.run_code("x += 1; x")
    print(execution.text)
```

This prints `2`. The snippets above use **JavaScript syntax**: when `language`
is omitted, this adapter maps it to JavaScript, unlike E2B Code Interpreter's
normal Python default. Explicit `language="javascript"` and
`language="typescript"` are also supported. Python, commands, files, and envd
ConnectRPC are not supported.

## Walkthrough

Verified output from a real run:

```bash
export KEY=e2b_0000000000000000000000000000000000000000
SANDBOX=$(curl -s -X POST http://localhost:9998/sandboxes \
  -H "x-api-key: $KEY" -H 'content-type: application/json' \
  -d '{"templateID":"base"}' \
  | python3 -c 'import sys,json; print(json.load(sys.stdin)["sandboxID"])')

for code in 'let x = 1' 'x++' 'x'; do
  printf '%s -> ' "$code"
  curl -s -X POST http://localhost:9998/execute \
    -H "x-api-key: $KEY" -H 'content-type: application/json' \
    -d "{\"code\":\"$code\",\"context_id\":\"$SANDBOX\",\"language\":\"javascript\"}"
  echo
done
```

```text
let x = 1 -> {"result":null,"result_type":"undefined",...,"error":null}
x++       -> {"result":1,"result_type":"number",...,"error":null}
x         -> {"result":2,"result_type":"number",...,"error":null}
```

Deleting terminates the isolate, and the id is remembered:

```bash
curl -s -o /dev/null -w '%{http_code}\n' \
  -X DELETE "http://localhost:9998/sandboxes/$SANDBOX" -H "x-api-key: $KEY"
# 204
curl -s "http://localhost:9998/sandboxes/$SANDBOX" -H "x-api-key: $KEY"
# {"code":410,"error_code":"sandbox_terminated","message":"Sandbox terminated"}
```

## Configuration

Every named variable below is environment-overridable. The TypeScript source
limit is deliberately fixed, as explained below.

| Variable                           | Default                   | Meaning                                           |
| ---------------------------------- | ------------------------- | ------------------------------------------------- |
| `E2B_API_KEY`                      | _(none)_                  | Required for control plane and `/execute`         |
| `EXECUTOR_SERVICE_PATH`            | `./examples/e2b-executor` | Executor location, CWD-relative                   |
| `SANDBOX_MEMORY_MB`                | 128                       | Per-sandbox memory limit                          |
| `SANDBOX_WALL_CLOCK_MS`            | 3600000                   | Hard ceiling on sandbox lifetime                  |
| `CPU_LIMIT_MS`                     | 600000                    | Cumulative CPU for the sandbox's whole life       |
| `EXECUTION_TIMEOUT_MS`             | 10000                     | Synchronous execution deadline                    |
| `EXECUTOR_ASYNC_TIMEOUT_MS`        | 12000                     | Awaiting a returned promise                       |
| `ADAPTER_FETCH_TIMEOUT_MS`         | 15000                     | Adapter-to-executor deadline                      |
| `QUEUE_WAIT_TIMEOUT_MS`            | 30000                     | Wait for a lock or slot before `429`              |
| `MAX_CONCURRENT_SANDBOXES`         | 128                       | Sandbox cap                                       |
| `MAX_CONCURRENT_SANDBOX_CREATIONS` | 0                         | `0` unlimited; positive values admit creates FIFO |
| `MAX_CONCURRENT_EXECUTIONS`        | 32                        | Global in-flight executions                       |
| `MAX_CODE_SIZE_BYTES`              | 262144                    | Request code size limit                           |
| _(not overridable)_                | 16384                     | TypeScript source limit, see below                |
| `MAX_OUTPUT_BYTES`                 | 65536                     | stdout+stderr per execution                       |
| `TERMINATED_ID_MEMORY`             | 1024                      | Ids remembered for terminated/expired replies     |

The three deadlines nest — vm timeout < executor async deadline < adapter fetch
deadline — so on custom `/execute`, an ordinary timeout surfaces as `200` with
`error.kind = "execution_timeout"`, while the adapter's own deadline firing
means the executor is unresponsive and is treated as sandbox death. The adapter
deadline covers both the worker fetch and the complete response-body read.
Worker termination acknowledgements use the same bound, so stalled cleanup
cannot block delete or expiry sweeps indefinitely.

`CPU_LIMIT_MS` is cumulative rather than per-request, so it is set high: at a
10s execution timeout it allows roughly 60 worst-case executions.

## Admission, capacity, and telemetry

`MAX_CONCURRENT_SANDBOX_CREATIONS=0` leaves creation admission unlimited. A
positive value bounds concurrent bundle/create/boot work and admits queued
creates in FIFO order. A queued create uses `QUEUE_WAIT_TIMEOUT_MS`; on timeout
it releases its capacity reservation and returns `429 too_many_requests`.
Already-admitted bundle/create/boot work is never cancelled.

Sandbox capacity is committed capacity: `active + reserved + draining`. A
creation reserves capacity before its worker exists, and a sandbox stays charged
while its worker is draining. `DELETE` immediately removes the sandbox logically
and requests worker termination, but a replacement is admitted only when final
worker shutdown releases that draining capacity.

Authenticated `GET /internal/metrics` is operational telemetry, not part of the
E2B compatibility API or OpenAPI surface. It returns aggregate lifecycle,
capacity, and queue snapshots; it never returns sandbox IDs, request data,
environment values, user code, or error payloads. `rollingLifecycleWindow` holds
60-second lifecycle histograms, while `lifetimeFinalWorkerTotals` is monotonic
for the adapter lifetime, including final-worker CPU/V8 totals. Worker memory
metrics are V8 heap values and CPU metrics are worker CPU time; neither is
process RSS. Fresh-worker `runtime_init` is further split into `loader_vfs`
(eszip/module loader/VFS), `resource_limits` (allocator and heap limits),
`js_runtime_new` (V8/Deno runtime construction), and `bootstrap` (post-runtime
setup plus `bootstrapSBEdge`). `bootstrap_blocking_run` and
`post_setup_blocking_run` measure their respective `spawn_blocking_non_send`
closures, while the matching `_queue` stages are outer elapsed time minus
closure time and include blocking-pool scheduling overhead. Blocking run stages
enclose loader/VFS, resource-limit, `JsRuntime::new`, and bootstrap work, so
they overlap those sub-phases and must not be added together. Extension assembly
remains only in the total `runtime_init`.

TypeScript transpilation is available only to the executor user worker, which
explicitly opts in when it is created; other user workers cannot call the op. It
has a lower effective limit than `MAX_CODE_SIZE_BYTES`: 16 KiB. The transpiler
is recursive descent with no depth guard, and deeply nested source overflowed
its stack and aborted the whole process — a stack overflow is not a panic, so it
could not be caught. Parsing now runs on a 512 MiB stack. A single process-wide
gate limits concurrent parser threads to the machine's available parallelism,
and the source limit makes each stack bound sound. Over-limit or over-nested
TypeScript returns `compile_error`. JavaScript is unaffected: V8's parser has
its own stack guard.

## Sandbox creation cost

The executor's module graph is bundled once, on the first create, and every
`userWorkers.create` after that reuses those bytes as `maybeEszip`. Passing a
`servicePath` instead makes the runtime re-resolve the graph, re-transpile it,
and walk the npm cache into a fresh vfs on _every_ create: 338ms of a 343ms
create, against 7ms with the eszip. The cost was never the executor's own code —
it is the root `deno.json` workspace, whose other members drag in npm
dependencies. Bundling costs ~17ms, so even the first create is ~29ms.

Sandbox environment is supplied as worker creation env, so executor module
initialization can install it before serving requests. Creation returns after
the worker loads its module graph; a later executor startup failure surfaces on
the first execution request.

There is no fallback — a graph that cannot be bundled cannot be booted
per-create either — and nothing to invalidate, since the bundle is built from
live source at first use.

The bundle resolves the entrypoint's graph only, skipping the workspace config
and the type check that the per-create path applies to user workers. That is why
the executor imports nothing but `node:` builtins and its own relative files: an
npm dependency or an import-map alias would need the bundle call to be given
that config.

Bundling is confined to the canonical repository root (the ancestor containing
`deno.json`). An entrypoint outside that root, including one reached through a
symlink, is rejected. Only the main worker can bundle; user workers cannot use
this host-filesystem read path.

## Error conventions

- On the custom **`/execute` route, user-code failures are `200`** with
  `error.kind` of `runtime_error`, `compile_error`, or `execution_timeout`. The
  sandbox stays usable.
- On the Jupyter route, user-code failures are NDJSON `error` events with
  `type`, `name`, `value`, and `traceback` fields.
- **API-level failures** use `{code, error_code, message}` with a 4xx/5xx
  status. Internal detail is logged, not returned.
- On custom `/execute`, a request that detects a dead worker returns
  `410 sandbox_terminated`.
- On the Jupyter route, the detecting request returns `500`; the sandbox record
  is reaped, so later requests for that sandbox return `404`.

A dead sandbox is **never** answered by creating a replacement worker, which
would hand back an empty context under the same `context_id` and silently lose
state.

## Accepted limitations

1. Sandbox lifetime is capped by `SANDBOX_WALL_CLOCK_MS` and cannot be extended
   past it; the worker wall clock starts at boot and never resets.
   `POST
   /{id}/timeout` refuses values beyond the ceiling rather than granting
   a TTL the runtime will not honour.
2. Total sandbox CPU is capped by `CPU_LIMIT_MS`. A sandbox exceeding it dies
   and cannot be revived under the same `context_id`.
3. A CPU-bound execution blocks other sandboxes sharing its worker thread. A
   synchronous loop inside the executed snippet is bounded by
   `EXECUTION_TIMEOUT_MS`, but a synchronous loop inside a **timer callback**
   scheduled by an earlier execution is bounded by neither that nor the async
   deadline — it pins the shared thread until `CPU_LIMIT_MS`. In debug builds
   the user-worker pool defaults to a single thread, so that means every
   sandbox.
4. Infinite microtask loops (`function f(){ Promise.resolve().then(f) } f()`)
   use only intrinsics and cannot be interrupted by any execution-level timeout.
   The cumulative CPU limit is the only backstop. The adapter's own deadline
   still frees the request, the sandbox lock, and the global slot. Custom
   `/execute` reports `410`; Jupyter reports `500` for the detecting request and
   `404` thereafter. The isolate is what lingers.
5. Escape from the `vm` context into the executor realm is assumed possible.
   Containment therefore still rests on the worker's denied permissions, not
   only on the vm boundary. The context shadows non-allowlisted host globals:
   `fetch`, `Deno`, `WebSocket`, and `EdgeRuntime` are `undefined`. Safe facades
   expose only the required APIs such as URL handling, text encoding, timers,
   console capture, and scoped environment access.
6. Re-declaring a `let`/`const` binding in a later execution is a `SyntaxError`,
   matching raw `vm` semantics rather than a REPL that rewrites declarations.
7. `execution_cpu_limit` and `execution_memory_limit` are not distinguishable:
   the main worker cannot observe why a worker died. Custom `/execute` maps both
   to `sandbox_terminated`; Jupyter uses its detecting-request `500`, then `404`
   mapping. The precise reason appears in event-worker logs.
8. All state is lost when the runtime restarts, by design. There is no database,
   no snapshotting, and no restart recovery.
9. Under `--policy oneshot` the pool silently ignores `forceCreate` and retires
   the executor after boot, so no sandbox can serve an execution. Creation
   refuses a worker another live sandbox already holds, so the failure is a loud
   API error rather than two tenants in one isolate. The production default is
   `PerWorker`; do not run the adapter under `oneshot`.

## Tests

- `./scripts/test.sh test_e2b_adapter` — Rust integration tests
- `python3 tests/e2b/test_sdk.py` — drives a running adapter through the
  official E2B Code Interpreter Python SDK (`pip install e2b-code-interpreter`)

See `tests/e2b/README.md`.
