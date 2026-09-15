import { authorizeSandbox } from "./auth.ts";
import { config } from "./config.ts";
import { errorResponse, redactedError } from "./errors.ts";
import { runSandboxExecution } from "./execution.ts";
import * as registry from "./registry.ts";
import { readJsonBounded, stringMap } from "./request.ts";

type JupyterEvent =
  | { type: "stdout" | "stderr"; text: string; timestamp: number }
  | {
    type: "result";
    text: string;
    json?: unknown;
    is_main_result: true;
  }
  | { type: "error"; name: string; value: string; traceback: string };

function resultText(payload: Record<string, unknown>): string {
  const value = payload.result;
  if (typeof value === "string") return value;
  if (
    value === null || typeof value === "number" || typeof value === "boolean"
  ) {
    if (payload.result_repr === null || payload.result_repr === undefined) {
      return String(value);
    }
  }
  if (Array.isArray(value) || (value !== null && typeof value === "object")) {
    return JSON.stringify(value);
  }
  if (typeof payload.result_repr === "string") return payload.result_repr;
  return String(payload.result_type ?? "undefined");
}

function ndjsonResponse(payload: Record<string, unknown>): Response {
  const events: JupyterEvent[] = [];
  const timestamp = () => Date.now() * 1_000_000;

  for (const text of payload.stdout as string[] ?? []) {
    events.push({ type: "stdout", text, timestamp: timestamp() });
  }
  for (const text of payload.stderr as string[] ?? []) {
    events.push({ type: "stderr", text, timestamp: timestamp() });
  }

  const error = payload.error;
  if (error !== null && typeof error === "object") {
    const details = error as Record<string, unknown>;
    const name = typeof details.name === "string" ? details.name : "Error";
    const value = typeof details.message === "string" ? details.message : "";
    events.push({
      type: "error",
      name,
      value,
      traceback: `${name}: ${value}`,
    });
  } else if (payload.result_type !== "undefined") {
    const includeJson =
      (payload.result_repr === null || payload.result_repr === undefined) &&
      ["null", "boolean", "string", "number", "array", "object"].includes(
        String(payload.result_type),
      );
    events.push({
      type: "result",
      text: resultText(payload),
      ...(includeJson ? { json: payload.result } : {}),
      is_main_result: true,
    });
  }

  const body = events.map((event) => JSON.stringify(event)).join("\n");
  return new Response(body === "" ? "" : `${body}\n`, {
    status: 200,
    headers: { "content-type": "application/x-ndjson" },
  });
}

export async function handleJupyterExecute(
  req: Request,
  sandboxID: string,
): Promise<Response> {
  const record = await registry.resolve(sandboxID);
  if (typeof record === "string") {
    return errorResponse("sandbox_not_found");
  }
  if (!await authorizeSandbox(req, record.envdAccessToken)) {
    return errorResponse("unauthorized");
  }

  const body = await readJsonBounded(req, config.maxRequestBytes);
  if (body === "too_large") {
    return errorResponse("invalid_request", "request body is too large");
  }
  if (body === null) return errorResponse("invalid_request");

  const code = body.code;
  const language = body.language === undefined || body.language === null
    ? "javascript"
    : body.language;
  const contextID = body.context_id;
  const envVars = stringMap(body.env_vars);

  if (typeof code !== "string") {
    return errorResponse("invalid_request", "code must be a string");
  }
  if (new TextEncoder().encode(code).length > config.maxCodeSizeBytes) {
    return errorResponse("invalid_request", "code is too large");
  }
  if (language !== "javascript" && language !== "typescript") {
    return errorResponse("unsupported_language", String(language));
  }
  if (contextID !== undefined && contextID !== null) {
    return errorResponse("invalid_request", "context_id must be null");
  }
  if (envVars === null) {
    return errorResponse("invalid_request", "env_vars must be a string map");
  }

  const outcome = await runSandboxExecution(req, sandboxID, record, {
    code,
    language,
    envVars,
  });
  switch (outcome.kind) {
    case "stale":
      return errorResponse("sandbox_not_found");
    case "queue_timeout":
      return errorResponse("too_many_requests");
    case "executor_failure":
      return redactedError("internal_server_error", outcome.cause);
    case "success":
      return ndjsonResponse(outcome.payload);
  }
}
