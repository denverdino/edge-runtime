import { core, internals, primordials } from "ext:core/mod.js";

import * as console from "ext:deno_console/01_console.js";
import * as encoding from "ext:deno_web/08_text_encoding.js";
import * as event from "ext:deno_web/02_event.js";
import * as fetch from "ext:deno_fetch/26_fetch.js";
import "ext:deno_fetch/27_eventsource.js";
import * as globalInterfaces from "ext:deno_web/04_global_interfaces.js";
import * as headers from "ext:deno_fetch/20_headers.js";
import * as request from "ext:deno_fetch/23_request.js";
import * as response from "ext:deno_fetch/23_response.js";
import "ext:deno_url/01_urlpattern.js";
import * as timers from "ext:deno_web/02_timers.js";
import * as webidl from "ext:deno_webidl/00_webidl.js";
import "ext:deno_web/10_filereader.js";
import "ext:deno_web/16_image_data.js";
import * as url from "ext:deno_url/00_url.js";
import "ext:deno_websocket/02_websocketstream.js";

import { SUPABASE_ENV } from "ext:env/env.js";
import { installPromiseHook, waitUntil } from "./async_hook.js";
import { denoOverrides, fsVars } from "./denoOverrides.js";
import { registerErrors } from "./errors.js";

const ops = core.ops;
const v8Console = globalThis.console;

const {
  Error,
  ObjectAssign,
  ObjectDefineProperties,
  ObjectDefineProperty,
  ObjectKeys,
  ObjectSetPrototypeOf,
} = primordials;

const nonEnumerable = (value) => ({
  value,
  writable: true,
  enumerable: false,
  configurable: true,
});
const readOnly = (value) => ({
  value,
  enumerable: true,
  writable: false,
  configurable: true,
});
const getterOnly = (getter) => ({
  get: getter,
  set() {},
  enumerable: true,
  configurable: true,
});

let globalThis_;

function dispatchLoadEvent() {
  globalThis_.dispatchEvent(new Event("load"));
}

function dispatchBeforeUnloadEvent(reason) {
  globalThis_.dispatchEvent(
    new CustomEvent("beforeunload", {
      cancelable: true,
      detail: { reason: reason ?? null },
    }),
  );
}

function dispatchUnloadEvent() {
  globalThis_.dispatchEvent(new Event("unload"));
}

function dispatchDrainEvent() {
  internals.drain = true;
  globalThis_.dispatchEvent(new Event("drain"));
}

function processUnhandledPromiseRejection(promise, reason) {
  const rejectionEvent = new event.PromiseRejectionEvent(
    "unhandledrejection",
    {
      cancelable: true,
      promise,
      reason,
    },
  );

  globalThis_.dispatchEvent(rejectionEvent);

  if (
    !rejectionEvent.defaultPrevented &&
    typeof internals.nodeProcessUnhandledRejectionCallback !== "undefined"
  ) {
    internals.nodeProcessUnhandledRejectionCallback(rejectionEvent);
  }

  return rejectionEvent.defaultPrevented;
}

function processRejectionHandled(promise, reason) {
  const rejectionHandledEvent = new event.PromiseRejectionEvent(
    "rejectionhandled",
    { promise, reason },
  );

  globalThis_.dispatchEvent(rejectionHandledEvent);

  if (typeof internals.nodeProcessRejectionHandledCallback !== "undefined") {
    internals.nodeProcessRejectionHandledCallback(rejectionHandledEvent);
  }
}

function makeHardErrorFn(message) {
  return () => {
    throw new Deno.errors.PermissionDenied(message);
  };
}

const deniedDenoFsApis = ObjectKeys(fsVars).reduce((apis, name) => {
  if (fsVars[name] !== void 0) {
    apis[name] = makeHardErrorFn(`Deno.${name} is blocklisted`);
  }
  return apis;
}, {});

function installUserWorkerFsDenials(ctx) {
  const apiOverrides = {
    ...deniedDenoFsApis,

    cwd: true,
    open: true,
    lstat: true,
    stat: true,
    realPath: true,
    create: true,
    remove: true,
    writeFile: true,
    writeTextFile: true,
    readFile: true,
    readTextFile: true,
    mkdir: true,
    makeTempDir: true,
    makeTempFile: true,
    readDir: true,
    copyFile: true,

    lstatSync: "allowIfRuntimeIsInInit",
    statSync: "allowIfRuntimeIsInInit",
    removeSync: "allowIfRuntimeIsInInit",
    writeFileSync: "allowIfRuntimeIsInInit",
    writeTextFileSync: "allowIfRuntimeIsInInit",
    readFileSync: "allowIfRuntimeIsInInit",
    readTextFileSync: "allowIfRuntimeIsInInit",
    mkdirSync: "allowIfRuntimeIsInInit",
    makeTempDirSync: "allowIfRuntimeIsInInit",
    makeTempFileSync: "allowIfRuntimeIsInInit",
    readDirSync: "allowIfRuntimeIsInInit",
    copyFileSync: "allowIfRuntimeIsInInit",
  };

  if (ctx?.useReadSyncFileAPI) {
    apiOverrides.readFileSync = "warnIfRuntimeIsAlreadyInit";
    apiOverrides.readTextFileSync = "warnIfRuntimeIsAlreadyInit";
  }

  for (const name of ObjectKeys(apiOverrides)) {
    const value = apiOverrides[name];
    if (typeof value === "function") {
      Deno[name] = value;
    } else if (value === "allowIfRuntimeIsInInit") {
      const original = Deno[name];
      const blocklisted = makeHardErrorFn(
        `Deno.${name} is blocklisted on the current context`,
      );
      Deno[name] = (...args) => {
        if (ops.op_is_runtime_init()) {
          return original(...args);
        }
        return blocklisted();
      };
    } else if (value === "warnIfRuntimeIsAlreadyInit") {
      const original = Deno[name];
      Deno[name] = (...args) => {
        if (!ops.op_is_runtime_init()) {
          globalThis.console.error(
            `WARNING: Do not use Deno.${name} inside the async callback. ` +
              "This has performance impacts and will be disallowed in the " +
              "future.\nUse the async version instead.",
          );
        }
        return original(...args);
      };
    }
  }
}

function installGlobals() {
  delete globalThis.console;
  ObjectDefineProperties(globalThis, {
    console: nonEnumerable(
      new console.Console((message, level) => core.print(message, level > 1)),
    ),
    clearInterval: { ...nonEnumerable(timers.clearInterval), writable: true },
    clearTimeout: { ...nonEnumerable(timers.clearTimeout), writable: true },
    setInterval: { ...nonEnumerable(timers.setInterval), writable: true },
    setTimeout: { ...nonEnumerable(timers.setTimeout), writable: true },
    TextDecoder: nonEnumerable(encoding.TextDecoder),
    TextEncoder: nonEnumerable(encoding.TextEncoder),
    TextDecoderStream: nonEnumerable(encoding.TextDecoderStream),
    TextEncoderStream: nonEnumerable(encoding.TextEncoderStream),
    URL: nonEnumerable(url.URL),
    URLPattern: nonEnumerable(url.URLPattern),
    URLSearchParams: nonEnumerable(url.URLSearchParams),
    Headers: nonEnumerable(headers.Headers),
    Request: nonEnumerable(request.Request),
    Response: nonEnumerable(response.Response),
    fetch: { ...nonEnumerable(fetch.fetch), writable: true },
    CloseEvent: nonEnumerable(event.CloseEvent),
    CustomEvent: nonEnumerable(event.CustomEvent),
    ErrorEvent: nonEnumerable(event.ErrorEvent),
    Event: nonEnumerable(event.Event),
    EventTarget: nonEnumerable(event.EventTarget),
    PromiseRejectionEvent: nonEnumerable(event.PromiseRejectionEvent),
    Window: globalInterfaces.windowConstructorDescriptor,
    window: getterOnly(() => globalThis),
    self: getterOnly(() => globalThis),
    [webidl.brand]: nonEnumerable(webidl.brand),
  });
}

function runtimeStart(target) {
  core.setWasmStreamingCallback(fetch.handleWasmStreaming);
  core.setBuildInfo(target);
  Error.prepareStackTrace = core.prepareStackTrace;
  registerErrors();
}

installGlobals();

const deno = ObjectAssign({}, denoOverrides);

globalThis.bootstrapSBEdge = (opts, ctx) => {
  globalThis_ = globalThis;

  delete globalThis.__bootstrap;
  delete globalThis.bootstrap;

  ObjectSetPrototypeOf(globalThis, Window.prototype);
  event.setEventTargetData(globalThis);
  event.saveGlobalThisReference(globalThis);

  for (
    const name of [
      "error",
      "load",
      "beforeunload",
      "unload",
      "unhandledrejection",
      "drain",
    ]
  ) {
    event.defineEventHandler(globalThis, name);
  }

  const { kind, target, version } = opts;
  runtimeStart(target);

  ObjectAssign(internals, {
    bootstrapArgs: { opts },
    worker: { kind },
    __ctx: ctx,
  });

  installPromiseHook(kind);

  ObjectDefineProperties(deno, {
    build: readOnly(core.build),
    env: readOnly(SUPABASE_ENV),
    pid: readOnly(globalThis.__pid),
    args: readOnly([]),
    mainModule: getterOnly(() => ops.op_main_module()),
    version: getterOnly(() => ({
      deno: `supabase-edge-runtime-${globalThis.SUPABASE_VERSION} ` +
        `(compatible with Deno v${globalThis.DENO_VERSION})`,
      v8: "11.6.189.12",
      typescript: "5.1.6",
    })),
  });

  ObjectDefineProperty(
    globalThis,
    "SUPABASE_VERSION",
    readOnly(String(version.runtime)),
  );
  ObjectDefineProperty(globalThis, "DENO_VERSION", readOnly(version.deno));
  ObjectDefineProperty(globalThis, "Deno", readOnly(deno));
  ObjectDefineProperty(globalThis, "EdgeRuntime", {
    get() {
      return {
        scheduleTermination: () =>
          ops.op_cancel_drop_token(ctx.terminationRequestToken),
        waitUntil,
        transpile: (source, filename) =>
          ops.op_transpile_ts(source, filename ?? "snippet.ts"),
      };
    },
    configurable: true,
  });

  if (opts.inspector) {
    ObjectDefineProperty(globalThis, "console", nonEnumerable(v8Console));
  } else if (kind === "user") {
    ObjectDefineProperty(
      globalThis,
      "console",
      nonEnumerable(
        new console.Console((message, level) =>
          ops.op_user_worker_log(message, level)
        ),
      ),
    );
  }

  const wasmMemoryCtor = globalThis.WebAssembly.Memory;
  const wasmMemoryPrototypeGrow = wasmMemoryCtor.prototype.grow;

  function patchedWasmMemoryPrototypeGrow(delta) {
    const memory = wasmMemoryPrototypeGrow.call(this, delta);
    ops.op_schedule_mem_check();
    return memory;
  }

  wasmMemoryCtor.prototype.grow = patchedWasmMemoryPrototypeGrow;

  function patchedWasmMemoryCtor(maybeOpts) {
    if (typeof maybeOpts === "object" && maybeOpts["shared"] === true) {
      throw new TypeError("Creating a shared memory is not supported");
    }
    return new wasmMemoryCtor(maybeOpts);
  }

  globalThis.SharedArrayBuffer = globalThis.ArrayBuffer;
  globalThis.WebAssembly.Memory = patchedWasmMemoryCtor;

  if (kind === "user") {
    installUserWorkerFsDenials(ctx);

    const shouldBootstrapMockFnThrowError =
      ctx?.shouldBootstrapMockFnThrowError ?? false;
    for (
      const name of [
        "kill",
        "exit",
        "addSignalListener",
        "removeSignalListener",
      ]
    ) {
      Deno[name] = () => {
        if (shouldBootstrapMockFnThrowError) {
          throw new TypeError("called MOCK_FN");
        }
      };
    }

    Deno.execPath = () => "/bin/edge-runtime";
    Deno.memoryUsage = () => ops.op_runtime_memory_usage();
  }

  if (nodeBootstrap) {
    nodeBootstrap({
      runningOnMainThread: true,
      usesLocalNodeModulesDir: false,
      argv: void 0,
      nodeDebug: Deno.env.get("NODE_DEBUG") ?? "",
    });

    delete globalThis.nodeBootstrap;
  }

  delete globalThis.bootstrapSBEdge;
};

const nodeBootstrap = globalThis.nodeBootstrap;

globalThis.bootstrap = {
  dispatchLoadEvent,
  dispatchUnloadEvent,
  dispatchBeforeUnloadEvent,
  dispatchDrainEvent,
};

core.setUnhandledPromiseRejectionHandler(processUnhandledPromiseRejection);
core.setHandledPromiseRejectionHandler(processRejectionHandled);
nodeBootstrap({ warmup: true });
