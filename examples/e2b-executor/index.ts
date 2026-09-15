import { contextReady, executeInContext, setSandboxEnv } from "./context.ts";
import { positiveIntEnv } from "./env.ts";

globalThis.addEventListener("unhandledrejection", (event: Event) => {
  event.preventDefault();
});

const ownKeys = Reflect.ownKeys;
const getOwnPropertyDescriptor = Object.getOwnPropertyDescriptor;
const createObject = Object.create;
const defineProperty = Object.defineProperty;
const isArray = Array.isArray;

const MAX_REQUEST_BYTES = positiveIntEnv("MAX_REQUEST_BYTES", 524288);

function normalizeEnv(value: unknown): Record<string, string> | null {
  try {
    if (value === null || typeof value !== "object" || isArray(value)) {
      return null;
    }

    const normalized = createObject(null) as Record<string, string>;
    for (const key of ownKeys(value)) {
      const descriptor = getOwnPropertyDescriptor(value, key);
      if (descriptor === undefined || !("value" in descriptor)) return null;
      if (!descriptor.enumerable) continue;
      if (typeof key !== "string" || typeof descriptor.value !== "string") {
        return null;
      }
      defineProperty(normalized, key, {
        value: descriptor.value,
        writable: true,
        configurable: true,
        enumerable: true,
      });
    }
    return normalized;
  } catch {
    return null;
  }
}

function invalidEnvResponse(): Response {
  return Response.json({
    result: null,
    result_type: "undefined",
    stdout: [],
    stderr: [],
    error: {
      kind: "internal_error",
      name: "TypeError",
      message: "environment variables must be an object with string values",
    },
  }, { status: 400 });
}

function invalidBodyResponse(message: string): Response {
  return Response.json({
    result: null,
    result_type: "undefined",
    stdout: [],
    stderr: [],
    error: {
      kind: "invalid_request",
      name: "TypeError",
      message,
    },
  }, { status: 400 });
}

const sandboxEnvJson = Deno.env.get("SANDBOX_ENV_JSON");
if (sandboxEnvJson !== undefined) {
  const parsed = JSON.parse(sandboxEnvJson);
  const sandboxEnv = normalizeEnv(parsed);
  if (sandboxEnv === null) {
    throw new TypeError(
      "environment variables must be an object with string values",
    );
  }
  setSandboxEnv(sandboxEnv);
}

// Reads a JSON body without buffering more than `maxBytes`. `req.json()` would
// buffer the whole body first with no size bound, and would throw an uncaught
// error (500) on malformed or empty input. Mirrors the adapter's reader so the
// executor self-protects even when driven directly.
async function readJsonBounded(
  req: Request,
  maxBytes: number,
): Promise<Record<string, unknown> | "too_large" | null> {
  const declared = Number(req.headers.get("content-length"));
  if (Number.isFinite(declared) && declared > maxBytes) return "too_large";

  if (req.body === null) return null;

  const chunks: Uint8Array[] = [];
  let total = 0;
  const reader = req.body.getReader();
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      total += value.byteLength;
      // Content-length may be absent or wrong, so enforce while reading.
      if (total > maxBytes) return "too_large";
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }

  const body = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    body.set(chunk, offset);
    offset += chunk.byteLength;
  }

  try {
    const parsed = JSON.parse(new TextDecoder().decode(body));
    if (
      parsed === null || typeof parsed !== "object" || isArray(parsed)
    ) {
      return null;
    }
    return parsed as Record<string, unknown>;
  } catch {
    return null;
  }
}

Deno.serve(async (req: Request) => {
  const url = new URL(req.url);

  if (url.pathname === "/internal/init") {
    return contextReady()
      ? new Response(null, { status: 204 })
      : Response.json({ error: "context unavailable" }, { status: 500 });
  }

  if (url.pathname !== "/internal/execute") {
    return new Response(null, { status: 404 });
  }

  const parsed = await readJsonBounded(req, MAX_REQUEST_BYTES);
  if (parsed === "too_large") {
    return invalidBodyResponse("request body is too large");
  }
  if (parsed === null) {
    return invalidBodyResponse("body must be a JSON object");
  }

  const code = parsed.code;
  const language = parsed.language;
  if (typeof code !== "string") {
    return invalidBodyResponse("code must be a string");
  }

  if (language !== "javascript" && language !== "typescript") {
    return Response.json({
      result: null,
      result_type: "undefined",
      stdout: [],
      stderr: [],
      error: {
        kind: "unsupported_language",
        name: "Error",
        message: String(language),
      },
    });
  }

  const requestEnv = normalizeEnv(
    parsed.env_vars === undefined ? {} : parsed.env_vars,
  );
  if (requestEnv === null) return invalidEnvResponse();

  const outcome = await executeInContext(code, requestEnv, language);

  if (!outcome.ok) {
    return Response.json({
      result: null,
      result_type: "undefined",
      stdout: outcome.stdout,
      stderr: outcome.stderr,
      error: {
        kind: outcome.kind,
        name: outcome.name,
        message: outcome.message,
      },
    });
  }

  return Response.json({
    result: outcome.result.value,
    result_type: outcome.result.type,
    result_repr: outcome.result.repr ?? null,
    stdout: outcome.stdout,
    stderr: outcome.stderr,
    error: null,
  });
});
