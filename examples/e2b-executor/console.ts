import { positiveIntEnv } from "./env.ts";

export const MAX_OUTPUT_BYTES = positiveIntEnv("MAX_OUTPUT_BYTES", 65536);

const OUTPUT_TRUNCATED = "[output truncated]";

type OutputStream = "stdout" | "stderr";

export interface Capture {
  console: Record<string, (...args: unknown[]) => void>;
  stdout: string[];
  stderr: string[];
  write(stream: OutputStream, args: unknown[]): void;
}

const RENDER_MAX_DEPTH = 4;
const RENDER_MAX_ENTRIES = 100;
const BIGINT_DECIMAL_LIMIT = 10n ** 100n;
const BIGINT_DECIMAL_MIN = -BIGINT_DECIMAL_LIMIT;

export function renderBigInt(value: bigint): string {
  if (value >= BIGINT_DECIMAL_LIMIT || value <= BIGINT_DECIMAL_MIN) {
    return "[bigint]";
  }
  return `${value}n`;
}

const arrayIsArray = Array.isArray;
const getOwnPropertyDescriptor = Object.getOwnPropertyDescriptor;

type RenderStatus = "complete" | "full" | "unsupported";

class BoundedRenderer {
  bytes = 0;
  entries = 0;
  readonly #output: Uint8Array;
  readonly #encoder: TextEncoder;
  readonly #seen = new WeakSet<object>();

  constructor(output: Uint8Array, encoder: TextEncoder) {
    this.#output = output;
    this.#encoder = encoder;
  }

  append(text: string): RenderStatus {
    const result = this.#encoder.encodeInto(
      text,
      this.#output.subarray(this.bytes),
    );
    this.bytes += result.written;
    return result.read === text.length ? "complete" : "full";
  }

  appendString(value: string, quoted: boolean): RenderStatus {
    if (!quoted) return this.append(value);
    if (this.append('"') === "full") return "full";

    for (let index = 0; index < value.length; index++) {
      const code = value.charCodeAt(index);
      let part: string;
      if (code === 0x22) part = '\\"';
      else if (code === 0x5c) part = "\\\\";
      else if (code === 0x08) part = "\\b";
      else if (code === 0x0c) part = "\\f";
      else if (code === 0x0a) part = "\\n";
      else if (code === 0x0d) part = "\\r";
      else if (code === 0x09) part = "\\t";
      else if (code < 0x20) {
        part = `\\u${code.toString(16).padStart(4, "0")}`;
      } else if (
        code >= 0xd800 && code <= 0xdbff && index + 1 < value.length &&
        value.charCodeAt(index + 1) >= 0xdc00 &&
        value.charCodeAt(index + 1) <= 0xdfff
      ) {
        part = value.slice(index, index + 2);
        index++;
      } else if (code >= 0xd800 && code <= 0xdfff) {
        part = `\\u${code.toString(16).padStart(4, "0")}`;
      } else {
        part = value[index];
      }
      if (this.append(part) === "full") return "full";
    }

    return this.append('"');
  }

  render(value: unknown, depth = 0, nested = false): RenderStatus {
    if (value === null) return this.append("null");

    switch (typeof value) {
      case "string":
        return this.appendString(value, nested);
      case "undefined":
        return nested ? "unsupported" : this.append("undefined");
      case "boolean":
        return this.append(value ? "true" : "false");
      case "number":
        return this.append(String(value));
      case "bigint":
        return this.append(renderBigInt(value));
      case "symbol":
        return nested ? "unsupported" : this.append("[symbol]");
      case "function":
        return nested ? "unsupported" : this.append("[function]");
      case "object":
        return this.renderContainer(value, depth);
    }

    return this.append("[unknown]");
  }

  renderContainer(value: object, depth: number): RenderStatus {
    const byteCheckpoint = this.bytes;
    const entryCheckpoint = this.entries;
    let placeholder = "[object]";

    const fallback = (): RenderStatus => {
      this.bytes = byteCheckpoint;
      this.entries = entryCheckpoint;
      return this.append(placeholder);
    };

    try {
      const isArray = arrayIsArray(value);
      if (isArray) placeholder = "[array]";
      if (this.#seen.has(value)) return this.append("[Circular]");
      if (depth >= RENDER_MAX_DEPTH) return this.append(placeholder);

      this.#seen.add(value);
      try {
        const status = isArray
          ? this.renderArray(value, depth)
          : this.renderObject(value, depth);
        return status === "unsupported" ? fallback() : status;
      } finally {
        this.#seen.delete(value);
      }
    } catch {
      return fallback();
    }
  }

  renderArray(value: object, depth: number): RenderStatus {
    const lengthDescriptor = getOwnPropertyDescriptor(value, "length");
    const length = lengthDescriptor?.value;
    if (!Number.isSafeInteger(length) || length < 0) return "unsupported";
    if (this.entries + length > RENDER_MAX_ENTRIES) return "unsupported";
    if (this.append("[") === "full") return "full";

    for (let index = 0; index < length; index++) {
      if (index > 0 && this.append(",") === "full") return "full";
      const descriptor = getOwnPropertyDescriptor(value, String(index));
      if (!descriptor) {
        if (this.append("null") === "full") return "full";
        continue;
      }
      if (!("value" in descriptor)) return "unsupported";
      this.entries++;
      const status = this.render(descriptor.value, depth + 1, true);
      if (status !== "complete") return status;
    }

    return this.append("]");
  }

  renderObject(value: object, depth: number): RenderStatus {
    if (this.append("{") === "full") return "full";

    let index = 0;
    for (const key in value) {
      const descriptor = getOwnPropertyDescriptor(value, key);
      if (!descriptor || !descriptor.enumerable) continue;
      if (!("value" in descriptor)) return "unsupported";
      if (this.entries >= RENDER_MAX_ENTRIES) {
        if (index > 0 && this.append(",") === "full") return "full";
        if (this.append("[truncated]") === "full") return "full";
        return this.append("}");
      }
      if (index > 0 && this.append(",") === "full") return "full";
      if (this.appendString(key, true) === "full") return "full";
      if (this.append(":") === "full") return "full";
      this.entries++;
      index++;
      const status = this.render(descriptor.value, depth + 1, true);
      if (status !== "complete") return status;
    }

    return this.append("}");
  }
}

function renderBounded(
  args: unknown[],
  maxBytes: number,
  buffer: Uint8Array,
  encoder: TextEncoder,
  decoder: TextDecoder,
): { text: string; bytes: number; complete: boolean } {
  const renderer = new BoundedRenderer(buffer.subarray(0, maxBytes), encoder);

  for (let index = 0; index < args.length; index++) {
    if (index > 0 && renderer.append(" ") === "full") {
      return {
        text: decoder.decode(buffer.subarray(0, renderer.bytes)),
        bytes: renderer.bytes,
        complete: false,
      };
    }
    if (renderer.render(args[index]) === "full") {
      return {
        text: decoder.decode(buffer.subarray(0, renderer.bytes)),
        bytes: renderer.bytes,
        complete: false,
      };
    }
  }

  return {
    text: decoder.decode(buffer.subarray(0, renderer.bytes)),
    bytes: renderer.bytes,
    complete: true,
  };
}

export function createCapture(maxBytes: number = MAX_OUTPUT_BYTES): Capture {
  const stdout: string[] = [];
  const stderr: string[] = [];
  const limit = Number.isFinite(maxBytes) && maxBytes > 0
    ? Math.floor(maxBytes)
    : 0;
  const encoder = new TextEncoder();
  const decoder = new TextDecoder();
  const markerBytes = Math.min(
    encoder.encode(OUTPUT_TRUNCATED).length,
    limit,
  );
  const marker = OUTPUT_TRUNCATED.slice(0, markerBytes);
  const contentLimit = limit - markerBytes;
  const buffer = new Uint8Array(contentLimit);
  let used = 0;
  let truncated = false;

  const push = (sink: string[], args: unknown[]) => {
    if (truncated) return;

    try {
      const rendered = renderBounded(
        args,
        contentLimit - used,
        buffer,
        encoder,
        decoder,
      );
      const charge = Math.max(1, rendered.bytes);
      if (rendered.complete && used + charge <= contentLimit) {
        used += charge;
        sink.push(rendered.text);
        return;
      }

      truncated = true;
      if (rendered.bytes > 0) {
        used += rendered.bytes;
        sink.push(rendered.text);
      }
      if (markerBytes > 0) {
        used += markerBytes;
        sink.push(marker);
      }
    } catch {
      truncated = true;
      if (markerBytes > 0 && used + markerBytes <= limit) {
        used += markerBytes;
        sink.push(marker);
      }
    }
  };

  const write = (stream: OutputStream, args: unknown[]) => {
    push(stream === "stdout" ? stdout : stderr, args);
  };

  return {
    stdout,
    stderr,
    write,
    console: {
      log: (...args: unknown[]) => write("stdout", args),
      info: (...args: unknown[]) => write("stdout", args),
      debug: (...args: unknown[]) => write("stdout", args),
      warn: (...args: unknown[]) => write("stderr", args),
      error: (...args: unknown[]) => write("stderr", args),
    },
  };
}
