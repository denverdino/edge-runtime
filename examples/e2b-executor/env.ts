/** Reads a positive-integer configuration value from the environment.
 *
 * A bad value — non-numeric, empty, zero, negative, fractional, or non-finite —
 * falls back to the documented default rather than propagating `NaN`. `NaN`
 * would otherwise be silently corrosive: every comparison against it is false,
 * so a `NaN` timeout disables the guard it protects and a `NaN` serialization
 * cap truncates output to nothing. Zero is treated as invalid here because the
 * limits that use this are timeouts and size caps where zero is not a supported
 * "disable" value.
 */
export function positiveIntEnv(name: string, fallback: number): number {
  const parsed = Number(Deno.env.get(name));
  return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback;
}
