import vm from "node:vm";

import { createCapture } from "./console.ts";
import { positiveIntEnv } from "./env.ts";
import {
  readDataProperty,
  type SerializedResult,
  serializeResult,
} from "./serialize.ts";
import { createTimerScope, type TimerScope } from "./timers.ts";

export type ExecuteOutcome =
  | {
    ok: true;
    result: SerializedResult;
    stdout: string[];
    stderr: string[];
  }
  | {
    ok: false;
    kind: "runtime_error" | "compile_error" | "execution_timeout";
    name: string;
    message: string;
    stdout: string[];
    stderr: string[];
  };

export const EXECUTION_TIMEOUT_MS = positiveIntEnv(
  "EXECUTION_TIMEOUT_MS",
  10000,
);

export const EXECUTOR_ASYNC_TIMEOUT_MS = positiveIntEnv(
  "EXECUTOR_ASYNC_TIMEOUT_MS",
  12000,
);

const executionContext = vm.createContext(Object.create(null));
let activeCapture: ReturnType<typeof createCapture> | undefined;
let activeTimerScope: TimerScope | undefined;
let executionTail = Promise.resolve();
let sandboxEnv = createEnv();
let activeEnv = createEnv();

function createEnv(
  ...sources: Record<string, string>[]
): Record<string, string> {
  return Object.assign(Object.create(null), ...sources);
}

const installSandboxBoundary = vm.compileFunction(
  `
    const allowed = new Set([
      'globalThis',
      'Object',
      'Function',
      'Array',
      'Number',
      'parseFloat',
      'parseInt',
      'Infinity',
      'NaN',
      'undefined',
      'Boolean',
      'String',
      'Symbol',
      'Date',
      'Promise',
      'RegExp',
      'Error',
      'AggregateError',
      'EvalError',
      'RangeError',
      'ReferenceError',
      'SyntaxError',
      'TypeError',
      'URIError',
      'globalThis',
      'JSON',
      'Math',
      'Intl',
      'ArrayBuffer',
      'Atomics',
      'Uint8Array',
      'Int8Array',
      'Uint16Array',
      'Int16Array',
      'Uint32Array',
      'Int32Array',
      'Float32Array',
      'Float64Array',
      'Uint8ClampedArray',
      'BigUint64Array',
      'BigInt64Array',
      'DataView',
      'Map',
      'BigInt',
      'Set',
      'WeakMap',
      'WeakSet',
      'Proxy',
      'Reflect',
      'FinalizationRegistry',
      'WeakRef',
      'decodeURI',
      'decodeURIComponent',
      'encodeURI',
      'encodeURIComponent',
      'escape',
      'unescape',
      'eval',
      'isFinite',
      'isNaN',
      'TextEncoder',
      'TextDecoder',
      'URL',
      'URLSearchParams',
      'console',
      'process',
      'setTimeout',
      'clearTimeout',
      'setInterval',
      'clearInterval',
    ]);
    for (const key of Reflect.ownKeys(globalThis)) {
      if (typeof key === 'string' && allowed.has(key)) continue;
      Object.defineProperty(globalThis, key, {
        value: undefined,
        writable: false,
        configurable: false,
        enumerable: false,
      });
    }
  `,
  [],
  { parsingContext: executionContext },
);
installSandboxBoundary();

function webApiResponse(operation: string, payloadJson: string): string {
  try {
    const payload = JSON.parse(payloadJson);
    let value: unknown;

    switch (operation) {
      case "urlConstruct": {
        const url = new URL(
          payload.input,
          payload.base === null ? undefined : payload.base,
        );
        value = urlSnapshot(url);
        break;
      }
      case "urlSet": {
        const url = new URL(payload.href);
        switch (payload.name) {
          case "href":
            url.href = payload.value;
            break;
          case "protocol":
            url.protocol = payload.value;
            break;
          case "username":
            url.username = payload.value;
            break;
          case "password":
            url.password = payload.value;
            break;
          case "host":
            url.host = payload.value;
            break;
          case "hostname":
            url.hostname = payload.value;
            break;
          case "port":
            url.port = payload.value;
            break;
          case "pathname":
            url.pathname = payload.value;
            break;
          case "search":
            url.search = payload.value;
            break;
          case "hash":
            url.hash = payload.value;
            break;
          default:
            throw new TypeError("Invalid URL property");
        }
        value = urlSnapshot(url);
        break;
      }
      case "urlCanParse": {
        value = URL.canParse(
          payload.input,
          payload.base === null ? undefined : payload.base,
        );
        break;
      }
      case "paramsConstruct": {
        value = new URLSearchParams(payload.input).toString();
        break;
      }
      case "paramsRead": {
        const params = new URLSearchParams(payload.input);
        switch (payload.name) {
          case "get":
            value = params.get(payload.key);
            break;
          case "getAll":
            value = params.getAll(payload.key);
            break;
          case "has":
            value = payload.hasValue
              ? params.has(payload.key, payload.value)
              : params.has(payload.key);
            break;
          case "entries":
            value = Array.from(params.entries());
            break;
          case "size":
            value = params.size;
            break;
          default:
            throw new TypeError("Invalid URLSearchParams operation");
        }
        break;
      }
      case "paramsWrite": {
        const params = new URLSearchParams(payload.input);
        switch (payload.name) {
          case "append":
            params.append(payload.key, payload.value);
            break;
          case "delete":
            if (payload.hasValue) params.delete(payload.key, payload.value);
            else params.delete(payload.key);
            break;
          case "set":
            params.set(payload.key, payload.value);
            break;
          case "sort":
            params.sort();
            break;
          default:
            throw new TypeError("Invalid URLSearchParams operation");
        }
        value = params.toString();
        break;
      }
      default:
        throw new TypeError("Invalid Web API operation");
    }

    return JSON.stringify({ ok: true, value });
  } catch (error) {
    const details = describeError(error);
    return JSON.stringify({ ok: false, ...details });
  }
}

function urlSnapshot(url: URL): Record<string, string> {
  return {
    href: url.href,
    origin: url.origin,
    protocol: url.protocol,
    username: url.username,
    password: url.password,
    host: url.host,
    hostname: url.hostname,
    port: url.port,
    pathname: url.pathname,
    search: url.search,
    hash: url.hash,
  };
}

const installWebApis = vm.compileFunction(
  `
    const parse = JSON.parse;
    const stringify = JSON.stringify;
    const toString = String;
    const arrayFrom = Array.from;
    const isView = ArrayBuffer.isView;

    const callHost = (operation, payload) => {
      const response = parse(dispatch(operation, stringify(payload)));
      if (!response.ok) {
        const ErrorConstructor = response.name === 'RangeError'
          ? RangeError
          : response.name === 'SyntaxError'
          ? SyntaxError
          : response.name === 'TypeError'
          ? TypeError
          : Error;
        throw new ErrorConstructor(response.message);
      }
      return response.value;
    };

    const UTF8_LABELS = new Set([
      'unicode-1-1-utf-8',
      'unicode11utf8',
      'unicode20utf8',
      'utf-8',
      'utf8',
      'x-unicode20utf8',
    ]);

    class SafeTextEncoder {
      get encoding() {
        return 'utf-8';
      }

      encode(input = '') {
        const source = toString(input);
        const bytes = [];
        let i = 0;
        while (i < source.length) {
          let cp = source.charCodeAt(i);
          if (cp >= 0xD800 && cp <= 0xDBFF && i + 1 < source.length) {
            const low = source.charCodeAt(i + 1);
            if (low >= 0xDC00 && low <= 0xDFFF) {
              cp = 0x10000 + ((cp - 0xD800) << 10) + (low - 0xDC00);
              i += 2;
            } else {
              cp = 0xFFFD;
              i += 1;
            }
          } else if (cp >= 0xD800 && cp <= 0xDFFF) {
            cp = 0xFFFD;
            i += 1;
          } else {
            i += 1;
          }
          if (cp <= 0x7F) {
            bytes.push(cp);
          } else if (cp <= 0x7FF) {
            bytes.push(0xC0 | (cp >> 6), 0x80 | (cp & 0x3F));
          } else if (cp <= 0xFFFF) {
            bytes.push(
              0xE0 | (cp >> 12),
              0x80 | ((cp >> 6) & 0x3F),
              0x80 | (cp & 0x3F),
            );
          } else {
            bytes.push(
              0xF0 | (cp >> 18),
              0x80 | ((cp >> 12) & 0x3F),
              0x80 | ((cp >> 6) & 0x3F),
              0x80 | (cp & 0x3F),
            );
          }
        }
        return Uint8Array.from(bytes);
      }

      encodeInto(input, destination) {
        if (!(destination instanceof Uint8Array)) {
          throw new TypeError('destination must be a Uint8Array');
        }
        const source = toString(input);
        const capacity = destination.length;
        let read = 0;
        let written = 0;
        let i = 0;
        while (i < source.length) {
          let cp = source.charCodeAt(i);
          let units = 1;
          if (cp >= 0xD800 && cp <= 0xDBFF && i + 1 < source.length) {
            const low = source.charCodeAt(i + 1);
            if (low >= 0xDC00 && low <= 0xDFFF) {
              cp = 0x10000 + ((cp - 0xD800) << 10) + (low - 0xDC00);
              units = 2;
            } else {
              cp = 0xFFFD;
            }
          } else if (cp >= 0xD800 && cp <= 0xDFFF) {
            cp = 0xFFFD;
          }
          const size = cp <= 0x7F ? 1 : cp <= 0x7FF ? 2 : cp <= 0xFFFF ? 3 : 4;
          if (written + size > capacity) break;
          if (size === 1) {
            destination[written++] = cp;
          } else if (size === 2) {
            destination[written++] = 0xC0 | (cp >> 6);
            destination[written++] = 0x80 | (cp & 0x3F);
          } else if (size === 3) {
            destination[written++] = 0xE0 | (cp >> 12);
            destination[written++] = 0x80 | ((cp >> 6) & 0x3F);
            destination[written++] = 0x80 | (cp & 0x3F);
          } else {
            destination[written++] = 0xF0 | (cp >> 18);
            destination[written++] = 0x80 | ((cp >> 12) & 0x3F);
            destination[written++] = 0x80 | ((cp >> 6) & 0x3F);
            destination[written++] = 0x80 | (cp & 0x3F);
          }
          read += units;
          i += units;
        }
        return { read, written };
      }
    }

    class SafeTextDecoder {
      #fatal;
      #ignoreBOM;
      #bytesNeeded = 0;
      #codePoint = 0;
      #bytesSeen = 0;
      #lowerBoundary = 0x80;
      #upperBoundary = 0xBF;
      #bomSeen = false;
      #doNotFlush = false;

      constructor(label = 'utf-8', options = {}) {
        const normalized = toString(label).trim().toLowerCase();
        if (!UTF8_LABELS.has(normalized)) {
          throw new RangeError('The encoding label provided is not supported');
        }
        this.#fatal = Boolean(options.fatal);
        this.#ignoreBOM = Boolean(options.ignoreBOM);
      }

      get encoding() {
        return 'utf-8';
      }

      get fatal() {
        return this.#fatal;
      }

      get ignoreBOM() {
        return this.#ignoreBOM;
      }

      #reset() {
        this.#bytesNeeded = 0;
        this.#codePoint = 0;
        this.#bytesSeen = 0;
        this.#lowerBoundary = 0x80;
        this.#upperBoundary = 0xBF;
      }

      #emit(result, cp) {
        if (!this.#bomSeen) {
          this.#bomSeen = true;
          if (cp === 0xFEFF && !this.#ignoreBOM) return result;
        }
        return result + String.fromCodePoint(cp);
      }

      decode(input, options = {}) {
        let bytes;
        if (input === undefined) {
          bytes = new Uint8Array(0);
        } else if (input instanceof ArrayBuffer) {
          bytes = new Uint8Array(input);
        } else if (isView(input)) {
          bytes = new Uint8Array(
            input.buffer,
            input.byteOffset,
            input.byteLength,
          );
        } else {
          throw new TypeError('input must be an ArrayBuffer or view');
        }
        const stream = Boolean(options.stream);

        if (!this.#doNotFlush) {
          this.#reset();
          this.#bomSeen = false;
        }
        this.#doNotFlush = stream;

        let result = '';
        let i = 0;
        while (i < bytes.length) {
          const byte = bytes[i];
          if (this.#bytesNeeded === 0) {
            i++;
            if (byte <= 0x7F) {
              result = this.#emit(result, byte);
            } else if (byte >= 0xC2 && byte <= 0xDF) {
              this.#bytesNeeded = 1;
              this.#codePoint = byte & 0x1F;
            } else if (byte >= 0xE0 && byte <= 0xEF) {
              if (byte === 0xE0) this.#lowerBoundary = 0xA0;
              if (byte === 0xED) this.#upperBoundary = 0x9F;
              this.#bytesNeeded = 2;
              this.#codePoint = byte & 0x0F;
            } else if (byte >= 0xF0 && byte <= 0xF4) {
              if (byte === 0xF0) this.#lowerBoundary = 0x90;
              if (byte === 0xF4) this.#upperBoundary = 0x8F;
              this.#bytesNeeded = 3;
              this.#codePoint = byte & 0x07;
            } else if (this.#fatal) {
              throw new TypeError('Invalid UTF-8 byte sequence');
            } else {
              result += '\uFFFD';
            }
            continue;
          }
          if (byte < this.#lowerBoundary || byte > this.#upperBoundary) {
            this.#reset();
            if (this.#fatal) {
              throw new TypeError('Invalid UTF-8 byte sequence');
            }
            result += '\uFFFD';
            continue;
          }
          i++;
          this.#lowerBoundary = 0x80;
          this.#upperBoundary = 0xBF;
          this.#codePoint = (this.#codePoint << 6) | (byte & 0x3F);
          this.#bytesSeen++;
          if (this.#bytesSeen === this.#bytesNeeded) {
            const cp = this.#codePoint;
            this.#reset();
            result = this.#emit(result, cp);
          }
        }

        if (!stream && this.#bytesNeeded !== 0) {
          this.#reset();
          if (this.#fatal) {
            throw new TypeError('Invalid UTF-8 byte sequence');
          }
          result += '\uFFFD';
        }

        return result;
      }
    }

    const ownerToken = Symbol('URLSearchParams owner');

    class SafeURLSearchParams {
      #value;
      #readOwner;
      #writeOwner;

      constructor(init = '', token, readOwner, writeOwner) {
        this.#readOwner = token === ownerToken ? readOwner : undefined;
        this.#writeOwner = token === ownerToken ? writeOwner : undefined;
        if (this.#readOwner !== undefined) {
          this.#value = '';
        } else if (init instanceof SafeURLSearchParams) {
          this.#value = init.toString();
        } else if (typeof init === 'string') {
          this.#value = callHost('paramsConstruct', { input: init });
        } else if (init === null || init === undefined) {
          this.#value = '';
        } else {
          this.#value = '';
          const object = Object(init);
          if (typeof object[Symbol.iterator] === 'function') {
            for (const pair of object) {
              if (pair === null || typeof pair[Symbol.iterator] !== 'function') {
                throw new TypeError('Each query pair must be iterable');
              }
              const values = arrayFrom(pair);
              if (values.length !== 2) {
                throw new TypeError('Each query pair must have two items');
              }
              this.append(values[0], values[1]);
            }
          } else {
            for (const key of Object.keys(object)) {
              this.append(key, object[key]);
            }
          }
        }
      }

      #current() {
        return this.#readOwner === undefined
          ? this.#value
          : this.#readOwner();
      }

      #replace(value) {
        if (this.#writeOwner === undefined) this.#value = value;
        else this.#writeOwner(value);
      }

      #read(name, key = '', value, hasValue = false) {
        return callHost('paramsRead', {
          input: this.#current(),
          name,
          key,
          value,
          hasValue,
        });
      }

      #write(name, key = '', value, hasValue = false) {
        this.#replace(callHost('paramsWrite', {
          input: this.#current(),
          name,
          key,
          value,
          hasValue,
        }));
      }

      append(name, value) {
        this.#write('append', toString(name), toString(value), true);
      }

      delete(name, value) {
        this.#write(
          'delete',
          toString(name),
          value === undefined ? '' : toString(value),
          value !== undefined,
        );
      }

      get(name) {
        return this.#read('get', toString(name));
      }

      getAll(name) {
        return this.#read('getAll', toString(name));
      }

      has(name, value) {
        return this.#read(
          'has',
          toString(name),
          value === undefined ? '' : toString(value),
          value !== undefined,
        );
      }

      set(name, value) {
        this.#write('set', toString(name), toString(value), true);
      }

      sort() {
        this.#write('sort');
      }

      entries() {
        return this.#read('entries')[Symbol.iterator]();
      }

      keys() {
        return this.#read('entries').map((entry) => entry[0])[Symbol.iterator]();
      }

      values() {
        return this.#read('entries').map((entry) => entry[1])[Symbol.iterator]();
      }

      forEach(callback, thisArg = undefined) {
        for (const [name, value] of this.#read('entries')) {
          callback.call(thisArg, value, name, this);
        }
      }

      get size() {
        return this.#read('size');
      }

      toString() {
        return this.#current();
      }

      [Symbol.iterator]() {
        return this.entries();
      }
    }

    class SafeURL {
      #parts;
      #searchParams;

      constructor(input, base = undefined) {
        this.#parts = callHost('urlConstruct', {
          input: toString(input),
          base: base === undefined ? null : toString(base),
        });
      }

      #set(name, value) {
        this.#parts = callHost('urlSet', {
          href: this.#parts.href,
          name,
          value: toString(value),
        });
      }

      get href() {
        return this.#parts.href;
      }

      set href(value) {
        this.#set('href', value);
      }

      get origin() {
        return this.#parts.origin;
      }

      get protocol() {
        return this.#parts.protocol;
      }

      set protocol(value) {
        this.#set('protocol', value);
      }

      get username() {
        return this.#parts.username;
      }

      set username(value) {
        this.#set('username', value);
      }

      get password() {
        return this.#parts.password;
      }

      set password(value) {
        this.#set('password', value);
      }

      get host() {
        return this.#parts.host;
      }

      set host(value) {
        this.#set('host', value);
      }

      get hostname() {
        return this.#parts.hostname;
      }

      set hostname(value) {
        this.#set('hostname', value);
      }

      get port() {
        return this.#parts.port;
      }

      set port(value) {
        this.#set('port', value);
      }

      get pathname() {
        return this.#parts.pathname;
      }

      set pathname(value) {
        this.#set('pathname', value);
      }

      get search() {
        return this.#parts.search;
      }

      set search(value) {
        this.#set('search', value);
      }

      get hash() {
        return this.#parts.hash;
      }

      set hash(value) {
        this.#set('hash', value);
      }

      get searchParams() {
        if (this.#searchParams === undefined) {
          this.#searchParams = new SafeURLSearchParams(
            '',
            ownerToken,
            () => this.#parts.search.slice(1),
            (value) => this.#set('search', value === '' ? '' : '?' + value),
          );
        }
        return this.#searchParams;
      }

      toString() {
        return this.#parts.href;
      }

      toJSON() {
        return this.#parts.href;
      }

      static canParse(input, base = undefined) {
        return callHost('urlCanParse', {
          input: toString(input),
          base: base === undefined ? null : toString(base),
        });
      }

      static parse(input, base = undefined) {
        try {
          return new SafeURL(input, base);
        } catch {
          return null;
        }
      }
    }

    for (const [name, value] of [
      ['TextEncoder', SafeTextEncoder],
      ['TextDecoder', SafeTextDecoder],
      ['URL', SafeURL],
      ['URLSearchParams', SafeURLSearchParams],
    ]) {
      Object.defineProperty(globalThis, name, {
        value,
        writable: false,
        configurable: false,
        enumerable: false,
      });
    }
  `,
  ["dispatch"],
  { parsingContext: executionContext },
);
installWebApis(webApiResponse);

const dispatchConsole = (stream: "stdout" | "stderr", args: unknown[]) => {
  try {
    activeCapture?.write(stream, args);
  } catch {
    // Console calls must never affect execution.
  }
};

const installConsole = vm.compileFunction(
  `
    const facade = Object.freeze({
      log: (...args) => dispatch('stdout', args),
      info: (...args) => dispatch('stdout', args),
      debug: (...args) => dispatch('stdout', args),
      warn: (...args) => dispatch('stderr', args),
      error: (...args) => dispatch('stderr', args),
    });
    Object.defineProperty(globalThis, 'console', {
      value: facade,
      writable: false,
      configurable: false,
      enumerable: true,
    });
  `,
  ["dispatch"],
  { parsingContext: executionContext },
);
installConsole(dispatchConsole);

const installProcess = vm.compileFunction(
  `
    const parse = JSON.parse;
    const keys = Object.keys;
    let activeEnv = Object.create(null);
    const facade = Object.create(null);
    Object.defineProperty(facade, 'env', {
      get: () => activeEnv,
      configurable: false,
      enumerable: true,
    });
    Object.freeze(facade);
    Object.defineProperty(globalThis, 'process', {
      value: facade,
      writable: false,
      configurable: false,
      enumerable: true,
    });
    return (serialized) => {
      const source = parse(serialized);
      const next = Object.create(null);
      for (const key of keys(source)) next[key] = source[key];
      activeEnv = next;
    };
  `,
  [],
  { parsingContext: executionContext },
);
const updateContextEnv = installProcess();

type TimerName =
  | "setTimeout"
  | "clearTimeout"
  | "setInterval"
  | "clearInterval";

const TIMER_ERROR_PREFIX = "timer-error:";

const dispatchTimer = (name: TimerName, args: unknown[]) => {
  try {
    const scope = activeTimerScope;
    const env = activeEnv;
    const capture = activeCapture;
    if (scope === undefined) {
      return `${TIMER_ERROR_PREFIX}Timer API called outside an active execution`;
    }

    if (
      (name === "setTimeout" || name === "setInterval") &&
      typeof args[0] === "function"
    ) {
      const callback = args[0] as (...callbackArgs: unknown[]) => unknown;
      args[0] = (...callbackArgs: unknown[]) => {
        const previousScope = activeTimerScope;
        const previousEnv = activeEnv;
        const previousCapture = activeCapture;
        activeTimerScope = scope;
        activeEnv = env;
        activeCapture = capture;
        updateContextEnv(JSON.stringify(env));
        try {
          return callback(...callbackArgs);
        } finally {
          activeTimerScope = previousScope;
          activeEnv = previousEnv;
          activeCapture = previousCapture;
          updateContextEnv(JSON.stringify(previousEnv));
        }
      };
    }

    const operation = scope.globals[name] as (
      ...operationArgs: unknown[]
    ) => unknown;
    return operation(...args);
  } catch (error) {
    return `${TIMER_ERROR_PREFIX}${describeError(error).message}`;
  }
};

const createTimerFacade = vm.compileFunction(
  `
    return Object.freeze((...args) => {
      const result = dispatch(name, args);
      if (typeof result === 'string' && result.startsWith(errorPrefix)) {
        throw new Error(result.slice(errorPrefix.length));
      }
      return result;
    });
  `,
  ["dispatch", "name", "errorPrefix"],
  { parsingContext: executionContext },
);

const timerFacades = Object.fromEntries(
  (
    [
      "setTimeout",
      "clearTimeout",
      "setInterval",
      "clearInterval",
    ] as const
  ).map((name) => [
    name,
    createTimerFacade(dispatchTimer, name, TIMER_ERROR_PREFIX),
  ]),
);

const installTimerFacade = vm.compileFunction(
  `
    Object.defineProperty(globalThis, name, {
      value: facade,
      writable: false,
      configurable: false,
      enumerable: true,
    });
  `,
  ["name", "facade"],
  { parsingContext: executionContext },
);
for (const [name, facade] of Object.entries(timerFacades)) {
  installTimerFacade(name, facade);
}

export function setSandboxEnv(env: Record<string, string>): void {
  sandboxEnv = createEnv(env);
  applyEnv({});
}

function applyEnv(requestEnv: Record<string, string>): void {
  activeEnv = createEnv(sandboxEnv, requestEnv);
  updateContextEnv(JSON.stringify(activeEnv));
}

// Reads a string property without invoking accessors or proxy traps. A hostile
// thrown value could otherwise run an accessor here, outside any vm timeout and
// inside the execution lock, where a looping getter cannot be interrupted.
function readStringProperty(value: object, key: string): string | undefined {
  try {
    const read = readDataProperty(value, key);
    return typeof read.value === "string" ? read.value : undefined;
  } catch {
    return undefined;
  }
}

function describeError(error: unknown): { name: string; message: string } {
  try {
    if (
      error !== null &&
      (typeof error === "object" || typeof error === "function")
    ) {
      return {
        name: readStringProperty(error, "name") ?? "Error",
        message: readStringProperty(error, "message") ?? "[unprintable error]",
      };
    }
    return { name: "Error", message: String(error) };
  } catch {
    return { name: "Error", message: "[unprintable error]" };
  }
}

const adoptResult = vm.compileFunction(
  "return Promise.resolve(value).then(onValue, onError);",
  ["value", "onValue", "onError"],
  { parsingContext: executionContext },
);

const createSettlementCallbacks = vm.compileFunction(
  `
    return [
      (value) => settle('value', value),
      (error) => settle('rejection', error),
    ];
  `,
  ["settle"],
  { parsingContext: executionContext },
);

function isExecutionTimeout(error: unknown): boolean {
  if (
    error === null ||
    (typeof error !== "object" && typeof error !== "function")
  ) {
    return false;
  }
  return readStringProperty(error, "code") === "ERR_SCRIPT_EXECUTION_TIMEOUT";
}

function successOutcome(
  value: unknown,
  capture: ReturnType<typeof createCapture>,
): ExecuteOutcome {
  return {
    ok: true,
    result: serializeResult(value),
    stdout: [...capture.stdout],
    stderr: [...capture.stderr],
  };
}

function errorOutcome(
  kind: "runtime_error" | "compile_error" | "execution_timeout",
  name: string,
  message: string,
  capture?: ReturnType<typeof createCapture>,
): ExecuteOutcome {
  return {
    ok: false,
    kind,
    name,
    message,
    stdout: [...(capture?.stdout ?? [])],
    stderr: [...(capture?.stderr ?? [])],
  };
}

type AsyncSettlement =
  | { kind: "value"; value: unknown }
  | { kind: "rejection"; error: unknown }
  | { kind: "timer_error"; error: unknown }
  | { kind: "timeout" };

async function executeOnce(
  code: string,
  requestEnv: Record<string, string>,
  language: "javascript" | "typescript",
): Promise<ExecuteOutcome> {
  let capture: ReturnType<typeof createCapture> | undefined;
  let timerScope: TimerScope | undefined;
  let phase: "setup" | "compile" | "run" = "setup";
  let acceptsTimerErrors = false;
  let reportTimerError!: (settlement: AsyncSettlement) => void;
  const timerError = new Promise<AsyncSettlement>((resolve) => {
    reportTimerError = resolve;
  });

  try {
    capture = createCapture();
    timerScope = createTimerScope((error) => {
      if (acceptsTimerErrors) {
        reportTimerError({ kind: "timer_error", error });
      }
    });
    activeCapture = capture;
    activeTimerScope = timerScope;
    applyEnv(requestEnv);

    phase = "compile";
    const source = language === "typescript"
      ? await EdgeRuntime.transpile(code, "snippet.ts")
      : code;
    const script = new vm.Script(source);
    phase = "run";
    const raw = script.runInContext(executionContext, {
      timeout: EXECUTION_TIMEOUT_MS,
    });

    let deadline: number | undefined;
    try {
      const guard = new Promise<AsyncSettlement>((resolve) => {
        deadline = setTimeout(
          () => resolve({ kind: "timeout" }),
          EXECUTOR_ASYNC_TIMEOUT_MS,
        );
      });
      const result = new Promise<AsyncSettlement>((resolve) => {
        const settle = (kind: "value" | "rejection", value: unknown) => {
          resolve(
            kind === "value" ? { kind, value } : { kind, error: value },
          );
        };
        const [onValue, onError] = createSettlementCallbacks(settle);
        adoptResult(raw, onValue, onError);
      });
      acceptsTimerErrors = true;
      const settled = await Promise.race([result, guard, timerError]);
      acceptsTimerErrors = false;

      if (settled.kind === "timeout") {
        timerScope.cancelAll();
        return errorOutcome(
          "execution_timeout",
          "Error",
          `Async execution timed out after ${EXECUTOR_ASYNC_TIMEOUT_MS}ms`,
          capture,
        );
      }

      if (settled.kind === "rejection" || settled.kind === "timer_error") {
        const details = describeError(settled.error);
        return errorOutcome(
          "runtime_error",
          details.name,
          details.message,
          capture,
        );
      }

      return successOutcome(settled.value, capture);
    } finally {
      acceptsTimerErrors = false;
      if (deadline !== undefined) clearTimeout(deadline);
    }
  } catch (error) {
    const details = describeError(error);
    const kind = phase === "compile"
      ? "compile_error"
      : phase === "run" && isExecutionTimeout(error)
      ? "execution_timeout"
      : "runtime_error";
    if (kind === "execution_timeout") timerScope?.cancelAll();
    return errorOutcome(kind, details.name, details.message, capture);
  } finally {
    // Release any interval this execution left running. An orphaned interval
    // never self-terminates, so its callback closure would retain this
    // execution's capture buffer and env forever — unbounded growth across
    // executions. One-shot timeouts are left to fire: a successful execution
    // may legitimately schedule one, it self-releases once it fires, and its
    // lifetime is bounded by the worker wall clock. Runs after the awaited
    // result has settled, so it never cancels a timer the result depended on.
    timerScope?.cancelIntervals();
    if (activeCapture === capture) activeCapture = undefined;
    if (activeTimerScope === timerScope) activeTimerScope = undefined;
    applyEnv({});
  }
}

export async function executeInContext(
  code: string,
  requestEnv: Record<string, string> = {},
  language: "javascript" | "typescript" = "javascript",
): Promise<ExecuteOutcome> {
  const previousExecution = executionTail;
  let releaseExecution!: () => void;
  executionTail = new Promise<void>((resolve) => {
    releaseExecution = resolve;
  });

  await previousExecution;
  try {
    return await executeOnce(code, requestEnv, language);
  } finally {
    releaseExecution();
  }
}

export function contextReady(): boolean {
  return vm.isContext(executionContext);
}
