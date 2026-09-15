export function stringMap(
  value: unknown,
): Record<string, string> | null {
  if (value === undefined || value === null) return {};
  if (typeof value !== "object" || Array.isArray(value)) return null;
  const out: Record<string, string> = {};
  for (const [key, item] of Object.entries(value)) {
    if (typeof item !== "string") return null;
    out[key] = item;
  }
  return out;
}

/** Reads a JSON body without buffering more than `maxBytes`.
 *
 * `req.json()` would buffer the whole body first, and the main worker has no
 * memory limit — so an oversized request could OOM the control plane for every
 * tenant before any size check ran. `MAX_CODE_SIZE_BYTES` is checked after
 * parsing and so protects nothing on its own.
 */
export async function readJsonBounded(
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
      if (total > maxBytes) {
        // Tear down the peer stream instead of leaving it dangling; the finally
        // still releases the lock.
        await reader.cancel().catch(() => {});
        return "too_large";
      }
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
      parsed === null || typeof parsed !== "object" || Array.isArray(parsed)
    ) {
      return null;
    }
    return parsed as Record<string, unknown>;
  } catch {
    return null;
  }
}
