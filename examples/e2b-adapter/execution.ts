import { config } from "./config.ts";
import { type ErrorCode, mapExecutorFailure } from "./errors.ts";
import { acquireGlobalSlot, QueueTimeout, withSandboxLock } from "./queue.ts";
import * as registry from "./registry.ts";

export interface SandboxExecutionInput {
  code: string;
  language: "javascript" | "typescript";
  envVars: Record<string, string>;
}

export type SandboxExecutionResult =
  | { kind: "success"; payload: Record<string, unknown> }
  | { kind: "stale"; code: registry.MissingCode }
  | { kind: "queue_timeout" }
  | {
    kind: "executor_failure";
    code: ErrorCode;
    cause: unknown;
  };

export async function runSandboxExecution(
  req: Request,
  sandboxID: string,
  record: registry.SandboxRecord,
  input: SandboxExecutionInput,
): Promise<SandboxExecutionResult> {
  try {
    const payload = await withSandboxLock(record, async () => {
      const current = await registry.resolve(sandboxID);
      if (current !== record) {
        return {
          kind: "stale" as const,
          code: typeof current === "string"
            ? current
            : registry.missingCode(sandboxID),
        };
      }

      const release = await acquireGlobalSlot();
      try {
        // DELETE or TTL expiry can remove this record while it waits for the
        // global slot. Revalidate immediately before dispatch so stale code
        // never reaches an executor.
        const current = await registry.resolve(sandboxID);
        if (current !== record) {
          return {
            kind: "stale" as const,
            code: typeof current === "string"
              ? current
              : registry.missingCode(sandboxID),
          };
        }

        return {
          kind: "success" as const,
          payload: await record.executor.execute(
            req,
            {
              code: input.code,
              language: input.language,
              env_vars: input.envVars,
            },
            config.adapterFetchTimeoutMs,
          ),
        };
      } finally {
        release();
      }
    });
    return payload;
  } catch (error) {
    if (error instanceof QueueTimeout) return { kind: "queue_timeout" };
    const code = mapExecutorFailure(error);
    if (code === "sandbox_terminated") await registry.reap(sandboxID);
    return { kind: "executor_failure", code, cause: error };
  }
}
