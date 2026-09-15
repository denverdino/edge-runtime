import {
  isDate,
  isMap,
  isNativeError,
  isPromise,
  isProxy,
  isSet,
} from "node:util/types";

import { renderBigInt } from "./console.ts";
import { positiveIntEnv } from "./env.ts";

export const SERIALIZE_MAX_DEPTH = positiveIntEnv("SERIALIZE_MAX_DEPTH", 8);
export const SERIALIZE_MAX_ENTRIES = positiveIntEnv(
  "SERIALIZE_MAX_ENTRIES",
  1000,
);
// Bounds total nodes visited across the whole traversal. The depth and entry
// caps are per-level, so a shared-reference DAG could otherwise re-traverse the
// same subtrees combinatorially. The default clears one fully populated level of
// SERIALIZE_MAX_ENTRIES so that case still serializes completely.
export const SERIALIZE_MAX_NODES = positiveIntEnv("SERIALIZE_MAX_NODES", 10000);

export interface SerializedResult {
  value: unknown;
  type: string;
  repr?: string;
}

const TRUNCATED: SerializedResult = {
  value: null,
  type: "truncated",
  repr: "[truncated]",
};

const UNSERIALIZABLE: SerializedResult = {
  value: null,
  type: "unserializable",
  repr: "[unserializable]",
};

const arrayIsArray = Array.isArray;
const getOwnPropertyDescriptor = Object.getOwnPropertyDescriptor;
const getPrototypeOf = Object.getPrototypeOf;
const reflectApply = Reflect.apply;
const dateToISOString = Date.prototype.toISOString;
const mapSizeGetter = getOwnPropertyDescriptor(Map.prototype, "size")?.get;
const setSizeGetter = getOwnPropertyDescriptor(Set.prototype, "size")?.get;

// Bounds prototype-chain walks independently of the value depth cap, so deep
// class hierarchies still serialize while a pathological chain cannot spin.
const MAX_PROTOTYPE_DEPTH = 100;

interface Budget {
  remaining: number;
}

export function serializeResult(value: unknown): SerializedResult {
  try {
    return walk(value, 0, new WeakSet(), { remaining: SERIALIZE_MAX_NODES });
  } catch {
    return UNSERIALIZABLE;
  }
}

function nestedValue(result: SerializedResult): unknown {
  return result.type === "truncated" ? result : result.value;
}

function ownDataValue(
  value: object,
  key: PropertyKey,
): { found: boolean; value?: unknown } {
  const descriptor = getOwnPropertyDescriptor(value, key);
  if (descriptor === undefined) return { found: false };
  if (!("value" in descriptor)) {
    throw new TypeError("accessor properties are unsupported");
  }
  return { found: true, value: descriptor.value };
}

function rejectProxyPrototype(value: object): void {
  let prototype = getPrototypeOf(value);
  let depth = 0;
  while (prototype !== null) {
    if (isProxy(prototype)) {
      throw new TypeError("proxy prototypes are unsupported");
    }
    if (depth++ >= MAX_PROTOTYPE_DEPTH) {
      throw new TypeError("prototype chain is too deep");
    }
    prototype = getPrototypeOf(prototype);
  }
}

// Reads a data property from the value or its prototypes without invoking
// accessors, so error names inherited from `TypeError.prototype` are visible
// while a hostile getter still degrades to `unserializable`.
function dataValue(
  value: object,
  key: PropertyKey,
): { found: boolean; value?: unknown } {
  rejectProxyPrototype(value);
  let cursor: object | null = value;
  let depth = 0;
  while (cursor !== null) {
    const own = ownDataValue(cursor, key);
    if (own.found) return own;
    if (depth++ >= MAX_PROTOTYPE_DEPTH) {
      throw new TypeError("prototype chain is too deep");
    }
    cursor = getPrototypeOf(cursor);
  }
  return { found: false };
}

// Reads a data property without invoking accessors or proxy traps. `dataValue`
// rejects proxy prototypes but not a proxy value itself, whose
// getOwnPropertyDescriptor trap would run user code, so reject that here.
// Exported so error inspection performs the same non-executing read.
export function readDataProperty(
  value: object,
  key: PropertyKey,
): { found: boolean; value?: unknown } {
  if (isProxy(value)) {
    throw new TypeError("proxies are unsupported");
  }
  return dataValue(value, key);
}

function specialResult(value: object): SerializedResult | undefined {
  const tag = ownDataValue(value, Symbol.toStringTag);
  if (tag.found && typeof tag.value !== "string") {
    throw new TypeError("invalid Symbol.toStringTag");
  }

  if (isNativeError(value)) {
    const name = dataValue(value, "name");
    const message = dataValue(value, "message");
    return {
      value: null,
      type: "error",
      repr: `${typeof name.value === "string" ? name.value : "Error"}: ${
        typeof message.value === "string" ? message.value : ""
      }`,
    };
  }

  if (isDate(value)) {
    return {
      value: null,
      type: "date",
      repr: reflectApply(dateToISOString, value, []),
    };
  }

  if (isMap(value)) {
    if (mapSizeGetter === undefined) {
      throw new TypeError("missing Map size getter");
    }
    const size = reflectApply(mapSizeGetter, value, []);
    return { value: null, type: "map", repr: `[map size=${size}]` };
  }

  if (isSet(value)) {
    if (setSizeGetter === undefined) {
      throw new TypeError("missing Set size getter");
    }
    const size = reflectApply(setSizeGetter, value, []);
    return { value: null, type: "set", repr: `[set size=${size}]` };
  }

  if (isPromise(value)) {
    return { value: null, type: "promise", repr: "[Promise]" };
  }

  return undefined;
}

function walk(
  value: unknown,
  depth: number,
  seen: WeakSet<object>,
  budget: Budget,
): SerializedResult {
  if (budget.remaining <= 0) return TRUNCATED;
  budget.remaining--;

  if (value === null) return { value: null, type: "null" };

  if (
    (typeof value === "object" || typeof value === "function") &&
    isProxy(value)
  ) {
    return UNSERIALIZABLE;
  }

  switch (typeof value) {
    case "undefined":
      return { value: null, type: "undefined" };
    case "boolean":
    case "string":
      return { value, type: typeof value };
    case "number":
      return Number.isFinite(value)
        ? { value, type: "number" }
        : { value: null, type: "number", repr: String(value) };
    case "bigint":
      return { value: null, type: "bigint", repr: renderBigInt(value) };
    case "symbol":
      return { value: null, type: "symbol", repr: String(value) };
    case "function": {
      const name = ownDataValue(value, "name");
      return {
        value: null,
        type: "function",
        repr: `[Function: ${
          typeof name.value === "string" && name.value
            ? name.value
            : "anonymous"
        }]`,
      };
    }
  }

  const obj = value as object;
  if (seen.has(obj)) {
    return { value: null, type: "circular", repr: "[Circular]" };
  }
  if (depth >= SERIALIZE_MAX_DEPTH) return TRUNCATED;

  const special = specialResult(obj);
  if (special !== undefined) return special;

  seen.add(obj);
  try {
    if (arrayIsArray(obj)) {
      const length = ownDataValue(obj, "length").value;
      if (
        typeof length !== "number" || !Number.isSafeInteger(length) ||
        length < 0
      ) {
        throw new TypeError("invalid array length");
      }

      const outputLength = Math.min(length, SERIALIZE_MAX_ENTRIES);
      const out: unknown[] = [];
      for (let index = 0; index < outputLength; index++) {
        const item = ownDataValue(obj, String(index));
        out[index] = nestedValue(
          walk(item.found ? item.value : undefined, depth + 1, seen, budget),
        );
      }
      if (length > SERIALIZE_MAX_ENTRIES) {
        out[outputLength] = TRUNCATED;
      }
      return { value: out, type: "array" };
    }

    rejectProxyPrototype(obj);
    const out: Record<string, unknown> = {};
    let count = 0;
    for (const key in obj) {
      const descriptor = getOwnPropertyDescriptor(obj, key);
      if (descriptor === undefined) break;
      if (!descriptor.enumerable) continue;
      if (!("value" in descriptor)) {
        throw new TypeError("accessor properties are unsupported");
      }
      if (count >= SERIALIZE_MAX_ENTRIES) {
        out["[truncated]"] = TRUNCATED;
        break;
      }
      // Assign via defineProperty: a plain `out[key] = value` would, for
      // key === "__proto__", retarget the output object's prototype instead
      // of adding a data property, and the value would be lost by Response.json.
      Object.defineProperty(out, key, {
        value: nestedValue(walk(descriptor.value, depth + 1, seen, budget)),
        enumerable: true,
        writable: true,
        configurable: true,
      });
      count++;
    }
    return { value: out, type: "object" };
  } finally {
    seen.delete(obj);
  }
}
