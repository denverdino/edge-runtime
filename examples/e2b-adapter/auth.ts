import { config } from "./config.ts";

async function digest(value: string): Promise<string> {
  const bytes = new TextEncoder().encode(value);
  const hash = await crypto.subtle.digest("SHA-256", bytes);
  return [...new Uint8Array(hash)]
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");
}

export async function matchesSecret(
  presented: string,
  expected: string,
): Promise<boolean> {
  const [a, b] = await Promise.all([digest(presented), digest(expected)]);
  return a === b;
}

/** Compares digests so the check is not a length or prefix timing oracle.
 *
 * Fails closed: with no configured key nothing is authorized, so a
 * misconfigured deployment refuses traffic instead of serving it wide open.
 */
export async function authorize(req: Request): Promise<boolean> {
  if (config.apiKey === "") return false;
  const presented = req.headers.get("x-api-key");
  return presented !== null && await matchesSecret(presented, config.apiKey);
}

export async function authorizeSandbox(
  req: Request,
  expectedToken: string,
): Promise<boolean> {
  const presented = req.headers.get("x-access-token");
  return presented !== null && presented !== "" &&
    await matchesSecret(presented, expectedToken);
}
