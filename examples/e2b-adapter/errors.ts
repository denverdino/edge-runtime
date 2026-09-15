export type ErrorCode =
  | "invalid_request"
  | "unauthorized"
  | "sandbox_not_found"
  | "sandbox_expired"
  | "sandbox_terminated"
  | "unsupported_template"
  | "unsupported_language"
  | "too_many_sandboxes"
  | "too_many_requests"
  | "internal_server_error";

const STATUS: Record<ErrorCode, number> = {
  invalid_request: 400,
  unauthorized: 401,
  sandbox_not_found: 404,
  sandbox_expired: 404,
  sandbox_terminated: 410,
  unsupported_template: 400,
  unsupported_language: 400,
  too_many_sandboxes: 429,
  too_many_requests: 429,
  internal_server_error: 500,
};

const MESSAGE: Record<ErrorCode, string> = {
  invalid_request: "Invalid request",
  unauthorized: "Unauthorized",
  sandbox_not_found: "Sandbox not found",
  sandbox_expired: "Sandbox expired",
  sandbox_terminated: "Sandbox terminated",
  unsupported_template: "Unsupported template",
  unsupported_language: "Unsupported language",
  too_many_sandboxes: "Too many sandboxes",
  too_many_requests: "Too many requests",
  internal_server_error: "Internal server error",
};

export function errorResponse(code: ErrorCode, message?: string): Response {
  const status = STATUS[code];
  return Response.json(
    { code: status, error_code: code, message: message ?? MESSAGE[code] },
    { status },
  );
}

/** Logs the real cause and returns the generic message.
 *
 * Internal failures embed host filesystem paths and module-resolution detail,
 * which must not reach an API client.
 */
export function redactedError(code: ErrorCode, cause: unknown): Response {
  console.error(`e2b-adapter ${code}:`, cause);
  return errorResponse(code);
}

/** Translates a failed executor call into a spec error code.
 *
 * Never answer one of these by creating a replacement worker: that would hand
 * back an empty context under the same context_id and silently lose the
 * sandbox's state. The record is dropped instead, so the caller sees 410.
 */
export function mapExecutorFailure(error: unknown): ErrorCode {
  const name = error instanceof Error ? error.name : "";
  if (
    name === "ExecutorUnresponsive" ||
    name === "WorkerAlreadyRetired" ||
    name === "InvalidWorkerResponse" ||
    name === "WorkerRequestCancelled" ||
    name === "WorkerRequestIdleTimeout" ||
    name === "TimeoutError" ||
    name === "AbortError"
  ) {
    return "sandbox_terminated";
  }
  return "internal_server_error";
}
