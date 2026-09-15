# AGENTS.md

This file provides guidance to the AI agent when working with code in this
repository.

## Behavioral guidelines

### 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:

- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

### 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes,
simplify.

### 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:

- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:

- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

### 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:

- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:

```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it
work") require constant clarification.

## Commands

- Format: `deno run -A ./scripts/format.js` (`--check` to verify). Never use
  `cargo fmt` — formatting goes through dprint, which runs
  `rustfmt --config imports_granularity=item`, so plain `cargo fmt` produces a
  diff CI rejects.
- Lint: `./scripts/clippy.sh` — `--all-targets --all-features` with
  `-D warnings`.
- Test: `./scripts/test.sh [TEST_NAME]`. `./scripts/test_indefinitely.sh [NAME]`
  loops a test to reproduce flakes.
- Run locally: `./scripts/run.sh` (serves `examples/` on `EDGE_RUNTIME_PORT`,
  default 9998).

## Setup

- Rust toolchain is pinned (`rust-toolchain.toml`); don't bump it casually.
- macOS needs `brew install openblas` before building.
- `.env` holds `ONNXRUNTIME_VERSION` / `DENO_VERSION` / `EDGE_RUNTIME_PORT`; the
  `scripts/*.sh` wrappers export it. Prefer those scripts over raw `cargo`.
- `ext/ai` links ONNX Runtime dynamically (`load-dynamic`), so its tests need
  `ORT_DYLIB_PATH` pointing at `libonnxruntime`; install with
  `./scripts/install_onnx.sh "$ONNXRUNTIME_VERSION" <os> <arch> /tmp/onnxruntime`.
- Binary version comes from the `GIT_V_TAG` build-time env var (defaults to
  `0.1.0`).

## Style

- Rust: 80 col max, 2-space indent, one item per `use` line (no brace grouping,
  no glob re-exports) — enforced by rustfmt config above.
- JS/TS/JSON/Markdown: 2-space indent, **double quotes**, 80 col — whatever
  `.dprint.json` produces, since that is what the format check runs. The `fmt`
  block in `deno.json` (single quotes, 100 col) only applies to `deno fmt`,
  which CI does not run; following it makes the check fail.
- `vendor/**` is excluded from formatting; leave its style alone.

## Runtime invariants that are easy to get wrong

These were each established by measurement after a plausible-looking assumption
turned out to be false. Verify with a probe rather than reasoning from the type.

- **Permission lists are grants, not denials.** `Some(empty)` grants a
  capability _globally_ — `allow_net: []` means allow all network. To deny
  something, omit the field entirely. Current user-worker defaults
  (`crates/base/src/runtime/permissions.rs`) therefore grant net/read/write/
  import/env globally; filesystem access is blocked separately in JavaScript
  (`ext/runtime/js/bootstrap.js`), not by permissions.
- **User-worker CPU limits are cumulative for the worker's whole life, and the
  wall clock starts at boot and never resets.** They are sized for one-shot
  functions, so a long-lived worker eventually dies on limits meant for a single
  request. A soft limit of `0` fires immediately rather than disabling the
  check; both limits must be `0` to disable.
- **A sync op holding `&mut OpState` while user JS runs will abort the
  process.** Any nested op that re-borrows OpState hits
  `already borrowed: BorrowMutError` inside a non-unwinding function, which
  aborts — it is not catchable. Take `Rc<RefCell<OpState>>` and drop the borrow
  before running user code (see `op_vm_script_run_in_context`).
- **A stack overflow is not a panic and `catch_unwind` cannot intercept it.**
  `deno_ast`'s parser is recursive descent with no depth guard, so deeply nested
  source aborts the process. Bounding nesting by counting brackets does not
  work: `!`, `x=>`, `a?1:` and `if(a)` chains recurse with zero or constant
  bracket depth. Bound the stack _and_ the input size.
- **Creating a user worker from a `servicePath` rebuilds its eszip every single
  time, and that rebuild resolves the whole workspace.** With `maybe_eszip`
  empty (`crates/base/src/runtime/mod.rs`) each `create` re-resolves the module
  graph and walks the npm cache into a fresh vfs — 338ms of a 343ms create,
  independent of the service's own size (a single-file service still cost
  322ms). The cost is the root `deno.json` workspace, not the service: against
  an empty `DENO_DIR` one create wrote 26MB of npm cache and took 7.6s, because
  the workspace pulls in every member's dependencies. Bundle once with
  `EdgeRuntime.bundle(entrypoint)` (main worker only) and pass the bytes as
  `maybeEszip`: boot drops to 7ms, that same first create writes 396KB and takes
  29ms, and 1000 sandbox create/execute/kill cycles go from 47.6s to 3.1s. The
  op resolves only the entrypoint's graph, so it skips both the workspace config
  and the user-worker `TypeCheckMode::Local` that path applies — fine for a
  service importing `node:` builtins and relative files, not a drop-in for one
  that needs an import map or an npm dependency.

## Architecture gotchas

- `deno/` is a vendored fork of the Deno CLI, and `ext/node/` mirrors Deno's
  `ext/node`. Upgrading Deno means bumping every `deno_*` version in the root
  `Cargo.toml` and re-syncing those directories (see DEVELOPERS.md).
- `vendor/deno_{crypto,fetch,http,telemetry,unsync}` override the crates.io
  versions via `[patch.crates-io]`. Patch Deno extension behavior there, not by
  swapping in an upstream dependency.
- `.cargo/config.toml` bakes `SUPABASE_RESOURCE_LIMIT_*` defaults in at build
  time; changing worker limits usually means editing that file, not just Rust
  code.
- Two runtimes: the _main_ runtime proxies requests and sees all env vars; the
  _user_ runtime executes user code under memory/CPU/timeout limits and only
  gets explicitly allowed env vars.
- The E2B sandbox is two services: `examples/e2b-adapter` is the control plane
  (registry, auth, TTL, concurrency) and must never compile or evaluate user
  code, while `examples/e2b-executor` is the user worker that holds one
  persistent `node:vm` context per sandbox. See `examples/e2b-adapter/README.md`
  for the API and its accepted limitations, and `tests/e2b/` for the Python HTTP
  suites that drive both over a real server.

## Tests

- Integration tests live in `crates/base/tests/integration_tests.rs` and drive
  JS fixtures under `crates/base/test_cases/`; add a fixture directory there
  rather than inlining scripts. They bind real ports and spawn workers, so
  they're resource-heavy — CI runs them with `-j 1`.
- Tests run with the working directory set to `crates/base`, so a fixture's
  `servicePath` is relative to that — `./test_cases/<name>`, and
  `../../examples/<name>` to reach a shipped service.
- `USER_WORKER_RT` has **one** thread in debug builds, which is how `cargo test`
  runs. Set `EDGE_RUNTIME_WORKER_POOL_SIZE` from a `#[ctor]` before any worker
  boots, or anything measuring cross-worker behavior measures one shared thread.
- `#[serial]` tests all enter concurrently and block on a mutex, so the
  harness's "has been running for over 60 seconds" warnings include queue
  waiting. Time a test in isolation before believing it is slow.
- Some suites need credentials that aren't available locally and will fail/skip:
  eszip fixtures come from the private `supabase/edge-runtime-test-eszip` repo
  into `crates/base/tests/fixture/testdata`, and `crates/fs` S3 tests read
  `crates/fs/tests/.env`.
- Running a service by hand uses `target/debug/edge-runtime`, which `cargo test`
  does not rebuild. A stale binary fails in ways that look like code bugs — e.g.
  `Unknown built-in "node:" module: vm`. Run `cargo build` first.

## Repo etiquette

- Conventional Commits with a scope matching the crate, e.g.
  `fix(ext/node): ...`, `fix(deno_facade): ...`, `chore: ...`. Releases are cut
  by semantic-release from `main` (and prereleases from `develop`), so the
  commit type drives the version bump.
