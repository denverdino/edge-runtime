#!/usr/bin/env python3
"""Functional tests for the E2B adapter through the official Python SDK.

Sandbox lifecycle and normal execution use `e2b_code_interpreter.Sandbox`.
Direct HTTP is limited to Jupyter protocol failures that `run_code()` cannot
express, such as a missing access token or a non-null context ID.

Requires the SDK and a running adapter:

    pip install e2b-code-interpreter
    E2B_API_KEY=e2b_0000000000000000000000000000000000000000 \
      ./target/debug/edge-runtime start --main-service ./examples/e2b-adapter \
      -p 9998
    python3 tests/e2b/test_sdk.py

See README.md in this directory.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib.metadata
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request

try:
    from e2b.exceptions import AuthenticationException
    from e2b.exceptions import SandboxException
    from e2b.exceptions import SandboxNotFoundException
    from e2b_code_interpreter import AsyncSandbox as CodeInterpreterAsyncSandbox
    from e2b_code_interpreter import Sandbox as CodeInterpreterSandbox
    from httpx import HTTPError
except ImportError:  # pragma: no cover - reported by main()
    CodeInterpreterSandbox = None  # type: ignore[assignment,misc]
    CodeInterpreterAsyncSandbox = None  # type: ignore[assignment,misc]
    AuthenticationException = Exception  # type: ignore[misc,assignment]
    SandboxException = Exception  # type: ignore[misc,assignment]
    SandboxNotFoundException = Exception  # type: ignore[misc,assignment]
    HTTPError = Exception  # type: ignore[misc,assignment]

BASE_URL = os.environ.get("E2B_BASE_URL", "http://localhost:9998")
# Older SDK versions validate the key format client-side and reject anything not
# shaped like `e2b_` + hex, so a key like "test-key" never leaves the process.
# The adapter only compares this for equality, so any well-formed value works —
# it must simply match what the adapter was started with.
API_KEY = os.environ.get("E2B_API_KEY", "e2b_" + "0" * 40)


if CodeInterpreterSandbox is not None:

    class LocalSandbox(CodeInterpreterSandbox):
        @property
        def _jupyter_url(self) -> str:
            return f"{BASE_URL}/sandboxes/{self.sandbox_id}/jupyter"


    class LocalAsyncSandbox(CodeInterpreterAsyncSandbox):
        @property
        def _jupyter_url(self) -> str:
            return f"{BASE_URL}/sandboxes/{self.sandbox_id}/jupyter"

else:
    LocalSandbox = None  # type: ignore[misc,assignment]
    LocalAsyncSandbox = None  # type: ignore[misc,assignment]


def sandbox(**options) -> LocalSandbox:
    """Creates a Code Interpreter sandbox against the local adapter."""
    assert LocalSandbox is not None
    return LocalSandbox.create(
        api_key=API_KEY,
        api_url=BASE_URL,
        **options,
    )


def list_sandboxes() -> list:
    """`Sandbox.list` returns a paginator, not a list."""
    assert CodeInterpreterSandbox is not None
    return CodeInterpreterSandbox.list(
        api_key=API_KEY, api_url=BASE_URL
    ).next_items()


PRIMES_SOURCE = """
function primesUpTo(limit) {
  const sieve = new Array(limit + 1).fill(true);
  sieve[0] = false;
  sieve[1] = false;

  for (let n = 2; n * n <= limit; n++) {
    if (!sieve[n]) continue;
    for (let multiple = n * n; multiple <= limit; multiple += n) {
      sieve[multiple] = false;
    }
  }

  const primes = [];
  for (let n = 2; n <= limit; n++) {
    if (sieve[n]) primes.push(n);
  }
  return primes;
}
"""


def primes_below(limit: int) -> list[int]:
    """An independent oracle, so the test does not assert magic numbers."""
    sieve = [True] * (limit + 1)
    sieve[0] = sieve[1] = False
    for n in range(2, int(limit**0.5) + 1):
        if not sieve[n]:
            continue
        for multiple in range(n * n, limit + 1, n):
            sieve[multiple] = False
    return [n for n in range(2, limit + 1) if sieve[n]]


def jupyter_request(
    sandbox_id: str,
    token: str | None,
    body: dict | bytes,
) -> tuple[int, dict]:
    """Calls Jupyter directly for failures `run_code()` cannot express."""
    data = body if isinstance(body, bytes) else json.dumps(body).encode()
    request = urllib.request.Request(
        f"{BASE_URL}/sandboxes/{sandbox_id}/jupyter/execute",
        data=data,
        method="POST",
    )
    request.add_header("content-type", "application/json")
    request.add_header("x-api-key", API_KEY)
    if token is not None:
        request.add_header("x-access-token", token)
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            raw = response.read()
            return response.status, json.loads(raw) if raw else {}
    except urllib.error.HTTPError as error:
        raw = error.read()
        return error.code, json.loads(raw) if raw else {}


def test_sdk_creates_and_kills_a_sandbox() -> None:
    with sandbox() as box:
        assert box.sandbox_id
        info = box.get_info()
        assert info.sandbox_id == box.sandbox_id
        assert info.template_id == "code-interpreter-v1"
        assert info.state == "running"

    # Context-manager exit kills through the official lifecycle API. The same
    # object must report the sandbox as gone rather than reconnecting it.
    try:
        box.get_info()
    except SandboxNotFoundException:
        pass
    except SandboxException as error:
        # The adapter preserves explicit termination as HTTP 410, which e2b
        # 2.46.0 exposes as a generic exception rather than its 404-only
        # SandboxNotFoundException. Accept only that exact deletion response.
        assert str(error) == "410: Sandbox terminated", error
    else:
        raise AssertionError("context-manager exit left the sandbox running")


def test_sdk_lists_running_sandboxes() -> None:
    with sandbox(metadata={"user": "t"}) as box:
        listed = list_sandboxes()
        ids = [item.sandbox_id for item in listed]
        assert box.sandbox_id in ids, f"{box.sandbox_id} missing from {ids}"

        mine = next(i for i in listed if i.sandbox_id == box.sandbox_id)
        assert mine.state == "running"
        assert mine.metadata["user"] == "t"


def test_sdk_sets_timeout_and_refuses_beyond_the_ceiling() -> None:
    with sandbox() as box:
        box.set_timeout(600)

        # The worker wall clock cannot be extended, so an over-ceiling TTL must
        # be refused rather than granted and then silently broken.
        try:
            box.set_timeout(99_999_999)
        except SandboxException as error:
            assert "wall-clock ceiling" in str(error), error
        else:
            raise AssertionError("an over-ceiling timeout was accepted")


def test_sdk_rejects_a_bad_api_key() -> None:
    # A 401 envelope reaches the SDK as AuthenticationException, which is not a
    # SandboxException subclass.
    assert LocalSandbox is not None
    try:
        LocalSandbox.create(api_key="wrong", api_url=BASE_URL)
    except AuthenticationException:
        pass
    else:
        raise AssertionError("a wrong API key was accepted")


def test_sdk_rejects_an_unknown_template() -> None:
    assert LocalSandbox is not None
    try:
        LocalSandbox.create(
            template="does-not-exist",
            api_key=API_KEY,
            api_url=BASE_URL,
        )
    except SandboxException as error:
        # Our envelope's `message` survives into the SDK exception.
        assert "does-not-exist" in str(error), error
    else:
        raise AssertionError("an unknown template was accepted")


def test_run_code_persists_javascript_state() -> None:
    with sandbox() as box:
        box.run_code("x = 1")
        execution = box.run_code("x += 1; x")
        assert execution.text == "2"


def test_run_code_accepts_explicit_javascript_language() -> None:
    with sandbox() as box:
        execution = box.run_code("21 * 2", language="javascript")
        assert execution.error is None
        assert execution.text == "42"


def test_computes_primes_under_1000_across_executions() -> None:
    with sandbox() as box:
        # The function is defined in one execution and called in the next, so
        # this exercises real work on top of a persistent context rather than a
        # single self-contained snippet.
        defined = box.run_code(PRIMES_SOURCE)
        assert defined.error is None

        computed = box.run_code("primesUpTo(1000)")
        assert computed.error is None
        assert computed.results[0].json == primes_below(1000)

        # The sieve state is reusable, not consumed by the first call.
        reused = box.run_code("primesUpTo(30)")
        assert reused.results[0].json == [2, 3, 5, 7, 11, 13, 17, 19, 23, 29]


def test_typescript_shares_the_javascript_context() -> None:
    with sandbox() as box:
        box.run_code("let x: number = 10", language="typescript")
        execution = box.run_code("x + 5", language="typescript")
        assert execution.text == "15"
        # Types are erased; the value lives in the same context as JavaScript.
        assert box.run_code("x").text == "10"


def test_sandboxes_cannot_see_each_other() -> None:
    with sandbox() as first, sandbox() as second:
        first.run_code("let secret = 'A'")
        seen = second.run_code("typeof secret")
        assert seen.text == "undefined"


def test_env_vars_are_scoped_and_secrets_stay_out() -> None:
    with sandbox(envs={"FOO": "sandbox"}) as box:
        assert box.run_code("process.env.FOO").text == "sandbox"

        # A request-level override wins, and lasts exactly one execution. The
        # SDK calls this parameter `envs`; the wire field remains `env_vars`.
        overridden = box.run_code(
            "process.env.FOO",
            envs={"FOO": "request"},
        )
        assert overridden.text == "request"
        assert box.run_code("process.env.FOO").text == "sandbox"

        # The adapter's own key never reaches a sandbox.
        secret = box.run_code("process.env.E2B_API_KEY")
        assert secret.error is None
        assert secret.results == []


def test_network_and_filesystem_stay_denied() -> None:
    with sandbox() as box:
        network = box.run_code("fetch('https://example.com')")
        filesystem = box.run_code("Deno.readTextFile('/etc/passwd')")
        assert network.error is not None
        assert network.error.name == "TypeError"
        assert "fetch" in network.error.value
        assert filesystem.error is not None
        assert filesystem.error.name == "TypeError"
        assert "readTextFile" in filesystem.error.value


def test_user_errors_keep_the_sandbox_alive() -> None:
    with sandbox() as box:
        box.run_code("let keep = 9")

        thrown = box.run_code("throw new Error('boom')")
        assert thrown.error is not None
        assert thrown.error.name == "Error"
        assert thrown.error.value == "boom"

        malformed = box.run_code("let bad: = ;;;")
        assert malformed.error is not None
        assert malformed.error.name == "SyntaxError"

        # Neither failure may damage the sandbox.
        assert box.run_code("keep").text == "9"


def test_run_code_reports_logs_results_and_callbacks() -> None:
    stdout = []
    stderr = []
    results = []
    errors = []
    with sandbox() as box:
        execution = box.run_code(
            "console.log('out'); console.error('err'); 21 * 2",
            on_stdout=lambda message: stdout.append(message.line),
            on_stderr=lambda message: stderr.append(message.line),
            on_result=lambda result: results.append(result.text),
            on_error=lambda error: errors.append(error.value),
        )

    assert execution.text == "42"
    assert execution.logs.stdout == ["out"]
    assert execution.logs.stderr == ["err"]
    assert execution.results[0].text == "42"
    assert execution.error is None
    assert stdout == ["out"]
    assert stderr == ["err"]
    assert results == ["42"]
    assert errors == []


def test_async_run_code_and_callbacks() -> None:
    async def scenario() -> None:
        assert LocalAsyncSandbox is not None
        box = await LocalAsyncSandbox.create(api_key=API_KEY, api_url=BASE_URL)
        try:
            await box.run_code("x = 1")
            stdout = []
            stderr = []
            results = []

            async def on_stdout(message) -> None:
                stdout.append(message.line)

            def on_stderr(message) -> None:
                stderr.append(message.line)

            async def on_result(result) -> None:
                results.append(result.text)

            execution = await box.run_code(
                "console.log('out'); console.error('err'); x += 1; x",
                on_stdout=on_stdout,
                on_stderr=on_stderr,
                on_result=on_result,
            )
            assert execution.text == "2"
            assert stdout == ["out"]
            assert stderr == ["err"]
            assert results == ["2"]
        finally:
            await box.kill()

    asyncio.run(scenario())


def test_async_error_callback_preserves_state() -> None:
    async def scenario() -> None:
        assert LocalAsyncSandbox is not None
        box = await LocalAsyncSandbox.create(api_key=API_KEY, api_url=BASE_URL)
        try:
            await box.run_code("x = 1")
            errors = []

            async def on_error(error) -> None:
                errors.append(error)

            execution = await box.run_code(
                "let bad: = ;;;",
                on_error=on_error,
            )
            assert execution.error is not None
            assert execution.error.name == "SyntaxError"
            assert execution.error.value == "Unexpected token ':'"
            assert execution.error.traceback == "SyntaxError: Unexpected token ':'"
            assert len(errors) == 1
            assert errors[0].name == execution.error.name
            assert errors[0].value == execution.error.value
            assert errors[0].traceback == execution.error.traceback

            survived = await box.run_code("x")
            assert survived.text == "1"
        finally:
            await box.kill()

    asyncio.run(scenario())


def test_run_code_converts_json_and_representation_results() -> None:
    with sandbox() as box:
        array = box.run_code("[1, 'two']").results[0]
        object_ = box.run_code("({ answer: 42 })").results[0]
        null = box.run_code("null").results[0]
        boolean = box.run_code("true").results[0]
        string = box.run_code("'hello'").results[0]
        nan = box.run_code("NaN").results[0]
        bigint = box.run_code("42n").results[0]

    assert array.json == [1, "two"]
    assert array.text == '[1,"two"]'
    assert object_.json == {"answer": 42}
    assert object_.text == '{"answer":42}'
    assert null.json is None
    assert null.text == "null"
    assert boolean.json is True
    assert boolean.text == "true"
    assert string.json == "hello"
    assert string.text == "hello"
    assert nan.json is None
    assert nan.text == "NaN"
    assert bigint.json is None
    assert bigint.text == "42n"


def test_jupyter_requires_access_token_before_parsing_body() -> None:
    with sandbox() as box:
        # Compatibility baseline: e2b-code-interpreter 2.8.1 / e2b 2.46.0.
        # The private token is inspected only for protocol failures that the
        # public run_code API cannot construct.
        token = box._envd_access_token

        # A valid API key is not a Jupyter credential. Even malformed JSON must
        # get 401 before body parsing when X-Access-Token is absent.
        status, body = jupyter_request(box.sandbox_id, None, b"not json {")
        assert status == 401
        assert body["error_code"] == "unauthorized"

        status, body = jupyter_request(
            box.sandbox_id,
            f"wrong-{token}",
            {"code": "1 + 1", "context_id": None, "language": "javascript"},
        )
        assert status == 401
        assert body["error_code"] == "unauthorized"


def test_unsupported_language_runs_nothing() -> None:
    with sandbox() as box:
        # Compatibility baseline: e2b-code-interpreter 2.8.1 / e2b 2.46.0.
        token = box._envd_access_token
        status, body = jupyter_request(
            box.sandbox_id,
            token,
            {
                "code": "globalThis.ran = true",
                "context_id": None,
                "language": "python",
            },
        )
        assert status == 400
        assert body["error_code"] == "unsupported_language"

        # Rejected before evaluation: the marker must not exist.
        assert box.run_code("typeof globalThis.ran").text == "undefined"


def test_jupyter_rejects_non_null_context_id() -> None:
    with sandbox() as box:
        # Compatibility baseline: e2b-code-interpreter 2.8.1 / e2b 2.46.0.
        token = box._envd_access_token
        status, body = jupyter_request(
            box.sandbox_id,
            token,
            {
                "code": "1 + 1",
                "context_id": "unsupported-context",
                "language": "javascript",
            },
        )
        assert status == 400
        assert body["error_code"] == "invalid_request"


def test_ttl_expiry_removes_the_sandbox() -> None:
    box = sandbox(timeout=1)
    assert box.run_code("1 + 1").text == "2"

    time.sleep(1.5)

    try:
        box.get_info()
    except SandboxException as error:
        # e2b 2.46.0 normalizes a control-plane 404 to "not found" and does
        # not retain the adapter's more specific sandbox_expired error code.
        assert "not found" in str(error).lower(), error
    else:
        raise AssertionError("expired sandbox still reported as running")

    listed = list_sandboxes()
    assert all(item.sandbox_id != box.sandbox_id for item in listed)


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
    arguments = parser.parse_args()
    BASE_URL = arguments.base_url

    if CodeInterpreterSandbox is None:
        print(
            "the official E2B Code Interpreter SDK is missing: "
            "pip install e2b-code-interpreter"
        )
        return 2

    versions = []
    for package in ("e2b-code-interpreter", "e2b"):
        try:
            versions.append(f"{package} {importlib.metadata.version(package)}")
        except importlib.metadata.PackageNotFoundError:
            pass
    if versions:
        print("SDK versions: " + ", ".join(versions))

    if re.fullmatch(r"e2b_[0-9a-fA-F]+", API_KEY) is None:
        print(
            f"warning: E2B_API_KEY={API_KEY!r} is not shaped like 'e2b_' + hex.\n"
            "Older SDK versions reject that client-side, so every test fails with\n"
            "a key-format error before reaching the adapter. Start the adapter with\n"
            "a well-formed key and use the same one here."
        )

    try:
        list_sandboxes()
    except AuthenticationException:
        print(f"adapter rejected E2B_API_KEY={API_KEY!r}; set it to match")
        return 2
    except HTTPError as error:
        print(f"cannot reach adapter at {BASE_URL}: {error}")
        print("start it first - see tests/e2b/README.md")
        return 2
    except SandboxException as error:
        print(f"adapter SDK probe failed at {BASE_URL}: {error}")
        return 2

    tests = _all_tests()
    if arguments.filter:
        tests = [test for test in tests if arguments.filter in test.__name__]

    failures = []
    for test in tests:
        try:
            test()
        except Exception as error:  # noqa: BLE001 - report every failure
            failures.append((test.__name__, error))
            print(f"FAIL {test.__name__}: {error}")
        else:
            print(f"ok   {test.__name__}")

    print(f"\n{len(tests) - len(failures)}/{len(tests)} passed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
