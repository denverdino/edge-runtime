#!/usr/bin/env python3
"""HTTP tests for the E2B sandbox executor.

Runs against the e2b-executor-harness main service (see README.md in this
directory for how to start it). Uses only the Python standard library, so no
packages need installing. Works either as a script or under pytest.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request

BASE_URL = os.environ.get("E2B_BASE_URL", "http://localhost:9998")

# The harness pins these for fast tests; see e2b-executor-harness/index.ts.
SYNC_TIMEOUT_MS = 1000
ASYNC_TIMEOUT_MS = 200


def _post(path: str, payload: dict | None, headers: dict[str, str]) -> tuple[int, dict]:
    body = None if payload is None else json.dumps(payload).encode()
    request = urllib.request.Request(f"{BASE_URL}{path}", data=body, method="POST")
    request.add_header("content-type", "application/json")
    for key, value in headers.items():
        request.add_header(key, value)

    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status, json.loads(response.read() or b"{}")
    except urllib.error.HTTPError as error:
        raw = error.read()
        try:
            return error.code, json.loads(raw or b"{}")
        except json.JSONDecodeError:
            return error.code, {"raw": raw.decode(errors="replace")}


def execute(
    code: str,
    *,
    sandbox: str = "default",
    language: str = "javascript",
    env_vars: dict[str, str] | None = None,
    sandbox_env: dict[str, str] | None = None,
) -> dict:
    """Executes code in a sandbox and returns the parsed response body.

    `sandbox_env` is only honored on the first request for a sandbox id, because
    the harness forwards it to /internal/init at worker creation time.
    """
    payload: dict = {"code": code, "language": language}
    if env_vars is not None:
        payload["env_vars"] = env_vars

    headers = {"x-sandbox-id": sandbox}
    if sandbox_env is not None:
        headers["x-sandbox-env"] = json.dumps(sandbox_env)

    _status, body = _post("/internal/execute", payload, headers)
    return body


def execute_raw(code: str, *, sandbox: str, env_vars: object) -> tuple[int, dict]:
    """Sends a deliberately malformed payload and returns the status too."""
    return _post(
        "/internal/execute",
        {"code": code, "language": "javascript", "env_vars": env_vars},
        {"x-sandbox-id": sandbox},
    )


def worker_creation_count(sandbox: str) -> int:
    _status, body = _post(
        "/internal/harness/worker-creation-count", None, {"x-sandbox-id": sandbox}
    )
    return body["count"]


# --- state persistence -------------------------------------------------------


def test_primitive_state_persists() -> None:
    """The core acceptance case: separate requests share one vm context."""
    assert execute("let x = 1", sandbox="prim")["error"] is None
    assert execute("x++", sandbox="prim")["result"] == 1
    assert execute("x", sandbox="prim")["result"] == 2


def test_object_state_persists() -> None:
    execute("const obj = { count: 1 }", sandbox="obj")
    execute("obj.count++", sandbox="obj")
    assert execute("obj.count", sandbox="obj")["result"] == 2


def test_functions_and_classes_persist() -> None:
    execute("function add(a, b) { return a + b }", sandbox="fn")
    assert execute("add(20, 22)", sandbox="fn")["result"] == 42

    execute(
        "class Counter { constructor() { this.value = 0 } inc() { this.value++ } }",
        sandbox="fn",
    )
    execute("const counter = new Counter()", sandbox="fn")
    execute("counter.inc()", sandbox="fn")
    assert execute("counter.value", sandbox="fn")["result"] == 1


def test_sandboxes_are_isolated() -> None:
    execute("let secret = 'A'", sandbox="iso-a")
    assert execute("typeof secret", sandbox="iso-b")["result"] == "undefined"


def test_worker_is_reused_across_requests() -> None:
    for _ in range(3):
        execute("1", sandbox="reuse")
    assert worker_creation_count("reuse") == 1


# --- TypeScript --------------------------------------------------------------


def test_typescript_state_persists() -> None:
    execute("let x: number = 10", sandbox="ts", language="typescript")
    execute("x += 5", sandbox="ts", language="typescript")
    assert execute("x", sandbox="ts", language="typescript")["result"] == 15


def test_typescript_types_are_erased_but_values_persist() -> None:
    execute(
        "interface User { name: string }\nconst user: User = { name: 'alice' }",
        sandbox="ts-erase",
        language="typescript",
    )
    assert execute("user.name", sandbox="ts-erase")["result"] == "alice"
    assert execute("typeof User", sandbox="ts-erase")["result"] == "undefined"


def test_typescript_and_javascript_share_one_context() -> None:
    execute("let shared: number = 7", sandbox="ts-share", language="typescript")
    assert execute("shared + 1", sandbox="ts-share")["result"] == 8


# --- output and result shape -------------------------------------------------


def test_console_output_is_captured() -> None:
    response = execute(
        "console.log('hello'); console.error('bad'); 1 + 2", sandbox="out"
    )
    assert response["result"] == 3
    assert response["stdout"] == ["hello"]
    assert response["stderr"] == ["bad"]


def test_exotic_results_never_break_the_response() -> None:
    big = execute("123n", sandbox="exotic")
    assert big["result"] is None
    assert big["result_type"] == "bigint"
    assert big["result_repr"] == "123n"

    cyclic = execute("const c = {}; c.self = c; c", sandbox="exotic")
    assert cyclic["error"] is None

    assert execute("new Map([['k', 1]])", sandbox="exotic")["result_type"] == "map"
    assert execute("undefined", sandbox="exotic")["result_type"] == "undefined"
    assert execute("() => 1", sandbox="exotic")["result_type"] == "function"


# --- environment -------------------------------------------------------------


def test_sandbox_env_and_request_override() -> None:
    # The sandbox baseline is installed when the worker is created.
    first = execute(
        "process.env.FOO", sandbox="env", sandbox_env={"FOO": "sandbox"}
    )
    assert first["result"] == "sandbox"

    override = execute("process.env.FOO", sandbox="env", env_vars={"FOO": "request"})
    assert override["result"] == "request"

    # The override lasts exactly one execution.
    assert execute("process.env.FOO", sandbox="env")["result"] == "sandbox"


def test_main_worker_secrets_are_not_visible() -> None:
    assert execute("process.env.E2B_API_KEY", sandbox="secret")["result_type"] == (
        "undefined"
    )


def test_non_string_env_values_are_rejected() -> None:
    status, body = execute_raw("1", sandbox="env-bad", env_vars={"NESTED": {}})
    assert status == 400
    assert body["error"]["kind"] == "internal_error"


# --- isolation ---------------------------------------------------------------
#
# NOTE: the vm context is a null-prototype realm whose globals are an explicit
# allowlist (standard ECMAScript intrinsics plus a small set of utilities such
# as TextEncoder/TextDecoder/URL/console/process/timers). Host capabilities like
# `fetch`, `Deno`, `WebSocket`, and `EdgeRuntime` are NOT present inside the
# sandbox. Isolation rests on that closed boundary; the worker's denied
# permissions are defense in depth, not the primary barrier.


def test_host_globals_are_not_reachable_from_the_vm_context() -> None:
    """The allowlisted realm exposes none of the host's ambient capabilities."""
    probe = "[typeof fetch, typeof Deno, typeof WebSocket, typeof EdgeRuntime].join(',')"
    assert execute(probe, sandbox="globals")["result"] == (
        "undefined,undefined,undefined,undefined"
    )


def test_deno_env_is_stripped() -> None:
    """`Deno` exists, but its env accessor is not reachable from the sandbox."""
    response = execute("Deno.env.get('PATH')", sandbox="deny")
    assert response["error"]["kind"] == "runtime_error"
    assert "undefined" in response["error"]["message"]


def test_filesystem_api_is_removed() -> None:
    """Deno.readTextFile is deleted for user workers by the runtime bootstrap."""
    response = execute("Deno.readTextFile('/etc/passwd')", sandbox="deny")
    assert response["error"]["kind"] == "runtime_error"


def test_imports_are_unavailable() -> None:
    assert execute("import fs from 'node:fs'", sandbox="deny")["error"]["kind"] == (
        "compile_error"
    )
    assert execute("require('fs')", sandbox="deny")["error"]["kind"] == "runtime_error"


# --- nested host ops ---------------------------------------------------------


def test_nested_ops_do_not_abort_the_runtime() -> None:
    """Regression: these once aborted the whole process.

    The synchronous vm run op used to hold a mutable OpState borrow across user
    code, so a nested op that re-borrowed it (deno_url's op_url_parse) panicked
    with BorrowMutError inside a function that cannot unwind.
    """
    execute("let alive = 5", sandbox="nested")

    parsed = execute("new URL('https://example.com/path').host", sandbox="nested")
    assert parsed["error"] is None
    assert parsed["result"] == "example.com"

    # fetch is not defined inside the sandbox, so calling it throws a
    # ReferenceError that must surface as an ordinary runtime_error rather than
    # aborting the runtime.
    fetched = execute("fetch('https://example.com')", sandbox="nested")
    assert fetched["error"]["kind"] == "runtime_error"

    assert execute("alive", sandbox="nested")["result"] == 5


# Reproducers that abort the whole runtime process. Empty since the nested-op
# borrow was fixed; kept so --include-crashers stays available for new ones.
CRASHERS: set[str] = set()


# --- errors and timeouts -----------------------------------------------------


def test_runtime_error_preserves_state() -> None:
    execute("let keep = 1", sandbox="err")
    boom = execute("throw new Error('boom')", sandbox="err")
    assert boom["error"]["kind"] == "runtime_error"
    assert boom["error"]["message"] == "boom"
    assert execute("keep", sandbox="err")["result"] == 1


def test_sync_timeout_is_recoverable() -> None:
    execute("let survive = 7", sandbox="spin")
    spun = execute("while (true) {}", sandbox="spin")
    assert spun["error"]["kind"] == "execution_timeout"
    # The sandbox is still usable after the timeout.
    assert execute("survive", sandbox="spin")["result"] == 7


def test_promise_results_and_async_timeout() -> None:
    assert execute("Promise.resolve(42)", sandbox="async")["result"] == 42
    hung = execute("new Promise(() => {})", sandbox="async")
    assert hung["error"]["kind"] == "execution_timeout"
    assert execute("1 + 1", sandbox="async")["result"] == 2


def test_compile_errors_are_reported_separately() -> None:
    assert execute("let x: = ;;;", sandbox="compile", language="typescript")["error"][
        "kind"
    ] == "compile_error"
    assert execute("await Promise.resolve(1)", sandbox="compile")["error"]["kind"] == (
        "compile_error"
    )
    # A SyntaxError thrown at run time is a runtime error, not a compile error.
    assert execute("throw new SyntaxError('nope')", sandbox="compile")["error"][
        "kind"
    ] == "runtime_error"


def test_python_is_rejected_without_running_code() -> None:
    execute("let untouched = 1", sandbox="lang")
    response = execute(
        "untouched = 999", sandbox="lang", language="python"
    )
    assert response["error"]["kind"] == "unsupported_language"
    # The rejected request must not have executed anything.
    assert execute("untouched", sandbox="lang")["result"] == 1


# --- runner ------------------------------------------------------------------


def _all_tests() -> list:
    module = sys.modules[__name__]
    return [
        getattr(module, name)
        for name in sorted(dir(module))
        if name.startswith("test_") and callable(getattr(module, name))
    ]


def main() -> int:
    global BASE_URL

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default=BASE_URL)
    parser.add_argument("-k", dest="filter", default=None, help="substring filter")
    parser.add_argument(
        "--include-crashers",
        action="store_true",
        help="also run reproducers that abort the whole runtime process",
    )
    arguments = parser.parse_args()
    BASE_URL = arguments.base_url

    try:
        execute("1", sandbox="preflight")
    except OSError as error:
        print(f"cannot reach executor at {BASE_URL}: {error}")
        print("start the harness first - see tests/e2b/README.md")
        return 2

    tests = _all_tests()
    if arguments.filter:
        tests = [test for test in tests if arguments.filter in test.__name__]

    failures = []
    skipped = 0
    for test in tests:
        if test.__name__ in CRASHERS and not arguments.include_crashers:
            print(f"skip {test.__name__} (aborts the runtime; --include-crashers)")
            skipped += 1
            continue
        try:
            test()
        except OSError as error:
            # The process died, so every later test would report a connection
            # error and bury the real cause.
            failures.append((test.__name__, error))
            print(f"FAIL {test.__name__}: {error}")
            print("server appears to have died - stopping early")
            break
        except Exception as error:  # noqa: BLE001 - report every failure
            failures.append((test.__name__, error))
            print(f"FAIL {test.__name__}: {error}")
        else:
            print(f"ok   {test.__name__}")

    executed = len(tests) - skipped
    print(f"\n{executed - len(failures)}/{executed} passed, {skipped} skipped")
    for name, error in failures:
        print(f"  failed: {name}: {error}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
