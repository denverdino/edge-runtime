use std::cell::RefCell;
use std::rc::Rc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::OnceLock;

use base_mem_check::WorkerHeapStatistics;
use base_rt::DropToken;
use base_rt::RuntimeState;
use base_rt::RuntimeWaker;
use deno_core::error::AnyError;
use deno_core::op2;
use deno_core::v8;
use deno_core::JsRuntime;
use deno_core::OpState;
use deno_core::ResourceId;
use enum_as_inner::EnumAsInner;
use futures::task::AtomicWaker;
use futures::FutureExt;
use log::error;
use serde::Serialize;
use tokio::sync::oneshot;
use tokio::sync::Semaphore;
use tracing::debug;
use tracing::debug_span;

mod upgrade;

pub mod cert;
pub mod conn_sync;
pub mod external_memory;
pub mod ops;
pub mod rate_limit;

pub use rate_limit::RateLimiterOpts;
pub use rate_limit::SharedRateLimitTable;
pub use rate_limit::TraceRateLimitRule;
pub use rate_limit::TraceRateLimiter;
pub use rate_limit::TraceRateLimiterConfig;

pub use ops::bootstrap::runtime_bootstrap;
pub use ops::http::runtime_http;
pub use ops::http_start::runtime_http_start;
pub use ops::net::runtime_net;

pub struct MemCheckWaker(Arc<AtomicWaker>);

impl From<Arc<AtomicWaker>> for MemCheckWaker {
  fn from(value: Arc<AtomicWaker>) -> Self {
    Self(value)
  }
}

#[derive(Debug, Default, Clone)]
pub struct SharedMetricSource {
  active_user_workers: Arc<AtomicUsize>,
  retired_user_workers: Arc<AtomicUsize>,
  received_requests: Arc<AtomicUsize>,
  handled_requests: Arc<AtomicUsize>,
  active_io: Arc<AtomicUsize>,
}

impl SharedMetricSource {
  pub fn active_io(&self) -> usize {
    self.active_io.load(Ordering::Relaxed)
  }

  pub fn received_requests(&self) -> usize {
    self.received_requests.load(Ordering::Relaxed)
  }

  pub fn handled_requests(&self) -> usize {
    self.handled_requests.load(Ordering::Relaxed)
  }

  pub fn incl_active_user_workers(&self) {
    self.active_user_workers.fetch_add(1, Ordering::Relaxed);
  }

  pub fn decl_active_user_workers(&self) {
    self.active_user_workers.fetch_sub(1, Ordering::Relaxed);
  }

  pub fn incl_retired_user_worker(&self) {
    self.retired_user_workers.fetch_add(1, Ordering::Relaxed);
  }

  pub fn incl_received_requests(&self) {
    self.received_requests.fetch_add(1, Ordering::Relaxed);
  }

  pub fn incl_handled_requests(&self) {
    self.handled_requests.fetch_add(1, Ordering::Relaxed);
  }

  pub fn incl_active_io(&self) {
    self.active_io.fetch_add(1, Ordering::Relaxed);
  }

  pub fn decl_active_io(&self) {
    self.active_io.fetch_sub(1, Ordering::Relaxed);
  }

  pub fn reset(&self) {
    self.active_user_workers.store(0, Ordering::Relaxed);
    self.retired_user_workers.store(0, Ordering::Relaxed);
    self.received_requests.store(0, Ordering::Relaxed);
    self.handled_requests.store(0, Ordering::Relaxed);
    self.active_io.store(0, Ordering::Relaxed);
  }
}

#[derive(Debug, Clone, EnumAsInner)]
pub enum MetricSource {
  Worker(WorkerMetricSource),
  Runtime(RuntimeMetricSource),
}

#[derive(Debug, Clone)]
pub struct WorkerMetricSource {
  handle: v8::IsolateHandle,
  waker: Arc<AtomicWaker>,
}

impl From<&mut JsRuntime> for WorkerMetricSource {
  fn from(value: &mut JsRuntime) -> Self {
    Self::from_js_runtime(value)
  }
}

impl WorkerMetricSource {
  pub fn from_js_runtime(runtime: &mut JsRuntime) -> Self {
    let handle = runtime.v8_isolate().thread_safe_handle();
    let waker = {
      let state = runtime.op_state();
      let state_mut = state.borrow_mut();

      state_mut.borrow::<RuntimeWaker>().0.clone()
    };

    Self { handle, waker }
  }
}

#[derive(Debug, Clone)]
pub struct RuntimeMetricSource {
  pub main: WorkerMetricSource,
  pub event: Option<WorkerMetricSource>,
  pub shared: SharedMetricSource,
}

impl RuntimeMetricSource {
  pub fn new(
    main: WorkerMetricSource,
    maybe_event: Option<WorkerMetricSource>,
    maybe_shared: Option<SharedMetricSource>,
  ) -> Self {
    Self {
      main,
      event: maybe_event,
      shared: maybe_shared.unwrap_or_default(),
    }
  }

  async fn get_heap_statistics(&mut self) -> RuntimeHeapStatistics {
    #[repr(C)]
    struct InterruptData {
      heap_tx: oneshot::Sender<WorkerHeapStatistics>,
    }

    extern "C" fn interrupt_fn(
      isolate: &mut v8::Isolate,
      data: *mut std::ffi::c_void,
    ) {
      let arg = unsafe { Box::<InterruptData>::from_raw(data as *mut _) };
      let mut v8_stats = v8::HeapStatistics::default();
      let mut worker_stats = WorkerHeapStatistics::default();

      isolate.get_heap_statistics(&mut v8_stats);

      worker_stats.total_heap_size = v8_stats.total_heap_size();
      worker_stats.total_heap_size_executable =
        v8_stats.total_heap_size_executable();
      worker_stats.total_physical_size = v8_stats.total_physical_size();
      worker_stats.total_available_size = v8_stats.total_available_size();
      worker_stats.total_global_handles_size =
        v8_stats.total_global_handles_size();
      worker_stats.used_global_handles_size =
        v8_stats.used_global_handles_size();
      worker_stats.used_heap_size = v8_stats.used_heap_size();
      worker_stats.malloced_memory = v8_stats.malloced_memory();
      worker_stats.external_memory = v8_stats.external_memory();
      worker_stats.peak_malloced_memory = v8_stats.peak_malloced_memory();

      if let Err(err) = arg.heap_tx.send(worker_stats) {
        error!("failed to send worker heap statistics: {:?}", err);
      }
    }

    let request_heap_statistics_fn = |arg: Option<&mut WorkerMetricSource>| {
      let Some(source) = arg else {
        return async { None::<WorkerHeapStatistics> }.boxed();
      };

      let (tx, rx) = oneshot::channel::<WorkerHeapStatistics>();
      let data_ptr_mut = Box::into_raw(Box::new(InterruptData { heap_tx: tx }));

      if !source
        .handle
        .request_interrupt(interrupt_fn, data_ptr_mut as *mut std::ffi::c_void)
      {
        drop(unsafe { Box::from_raw(data_ptr_mut) });
        return async { None }.boxed();
      }

      let waker = source.waker.clone();

      async move {
        waker.wake();
        rx.await.ok()
      }
      .boxed()
    };

    RuntimeHeapStatistics {
      main_worker_heap_stats: request_heap_statistics_fn(Some(&mut self.main))
        .await
        .unwrap_or_default(),

      event_worker_heap_stats: request_heap_statistics_fn(self.event.as_mut())
        .await,
    }
  }
}

#[derive(Debug, Serialize, Default)]
#[serde(rename_all = "camelCase")]
struct RuntimeHeapStatistics {
  main_worker_heap_stats: WorkerHeapStatistics,
  event_worker_heap_stats: Option<WorkerHeapStatistics>,
}

#[derive(Debug, Serialize, Default)]
#[serde(rename_all = "camelCase")]
struct RuntimeSharedStatistics {
  active_user_workers_count: usize,
  retired_user_workers_count: usize,
  received_requests_count: usize,
  handled_requests_count: usize,
}

impl RuntimeSharedStatistics {
  fn from_shared_metric_src(src: &SharedMetricSource) -> Self {
    Self {
      active_user_workers_count: src
        .active_user_workers
        .load(Ordering::Relaxed),
      retired_user_workers_count: src
        .retired_user_workers
        .load(Ordering::Relaxed),
      received_requests_count: src.received_requests.load(Ordering::Relaxed),
      handled_requests_count: src.handled_requests.load(Ordering::Relaxed),
    }
  }
}

#[derive(Debug, Serialize, Default)]
#[serde(rename_all = "camelCase")]
struct RuntimeMetrics {
  #[serde(flatten)]
  heap_stats: RuntimeHeapStatistics,
  #[serde(flatten)]
  shared_stats: RuntimeSharedStatistics,
}
/*
#[op2(fast)]
fn op_is_terminal(state: &mut OpState, rid: u32) -> Result<bool, AnyError> {
    let handle = state.resource_table.get_handle(rid)?;
    Ok(handle.is_terminal())
}*/

#[op2(fast)]
fn op_is_runtime_init(state: &mut OpState) -> bool {
  state.borrow::<Arc<RuntimeState>>().is_init()
}

#[op2(fast)]
fn op_stdin_set_raw(
  _state: &mut OpState,
  _is_raw: bool,
  _cbreak: bool,
) -> Result<(), AnyError> {
  Ok(())
}

#[op2(fast)]
fn op_console_size(
  _state: &mut OpState,
  #[buffer] _result: &mut [u32],
) -> Result<(), AnyError> {
  Ok(())
}

#[op2(async)]
#[serde]
async fn op_runtime_metrics(
  state: Rc<RefCell<OpState>>,
) -> Result<RuntimeMetrics, AnyError> {
  let mut runtime_metrics = RuntimeMetrics::default();
  let mut runtime_metric_src = {
    let state = state.borrow();
    state.borrow::<RuntimeMetricSource>().clone()
  };

  runtime_metrics.heap_stats = runtime_metric_src.get_heap_statistics().await;
  runtime_metrics.shared_stats =
    RuntimeSharedStatistics::from_shared_metric_src(&runtime_metric_src.shared);

  Ok(runtime_metrics)
}

#[op2(fast)]
fn op_schedule_mem_check(state: &mut OpState) -> Result<(), AnyError> {
  if let Some(waker) = state.try_borrow::<MemCheckWaker>() {
    waker.0.wake();
  }

  Ok(())
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct MemoryUsage {
  rss: usize,
  heap_total: usize,
  heap_used: usize,
  external: usize,
}

#[op2]
#[serde]
fn op_runtime_memory_usage(scope: &mut v8::HandleScope) -> MemoryUsage {
  let mut s = v8::HeapStatistics::default();

  scope.get_heap_statistics(&mut s);

  MemoryUsage {
    // NOTE: Hardcoded for security.
    rss: 0,
    heap_total: s.total_heap_size(),
    heap_used: s.used_heap_size(),
    external: s.external_memory(),
  }
}

#[op2]
#[string]
pub fn op_read_line_prompt(
  #[string] _prompt_text: &str,
  #[string] _default_value: &str,
) -> Result<Option<String>, AnyError> {
  Ok(None)
}

#[op2(fast)]
fn op_set_exit_code(
  _state: &mut OpState,
  #[smi] _code: i32,
) -> Result<(), AnyError> {
  Ok(())
}

#[op2(fast)]
fn op_set_raw(
  _state: &mut OpState,
  _rid: u32,
  _is_raw: bool,
  _cbreak: bool,
) -> Result<(), AnyError> {
  Ok(())
}

#[op2(fast)]
fn op_raise_segfault(_state: &mut OpState) {
  unsafe {
    let ptr: *const i32 = std::ptr::null();
    println!("{}", *ptr);
  }
}

#[derive(Debug, Default, Clone)]
pub struct PromiseMetrics {
  init: Arc<AtomicUsize>,
  resolve: Arc<AtomicUsize>,
}

impl PromiseMetrics {
  pub fn get_init_count(&self) -> usize {
    self.init.load(Ordering::Acquire)
  }

  pub fn get_resolve_count(&self) -> usize {
    self.resolve.load(Ordering::Acquire)
  }

  pub fn have_all_promises_been_resolved(&self) -> bool {
    self.get_init_count() == self.get_resolve_count()
  }
}

#[op2(fast)]
fn op_tap_promise_metrics(state: &mut OpState, #[string] kind: &str) {
  let _span = debug_span!("op_tap_promise_metrics", kind).entered();
  let metrics = if state.has::<PromiseMetrics>() {
    state.borrow_mut::<PromiseMetrics>()
  } else {
    state.put(PromiseMetrics::default());
    state.borrow_mut()
  };

  match kind {
    "init" => {
      metrics.init.fetch_add(1, Ordering::Release);
    }

    "resolve" => {
      metrics.resolve.fetch_add(1, Ordering::Release);
    }

    _ => {}
  }

  debug!(?metrics);
}

#[op2(fast)]
fn op_cancel_drop_token(
  state: &mut OpState,
  #[smi] rid: ResourceId,
) -> Result<(), AnyError> {
  let token = state.resource_table.get::<DropToken>(rid)?;

  token.0.cancel();
  Ok(())
}

#[op2(fast)]
fn op_mi_collect() {
  #[cfg(target_os = "linux")]
  {
    use std::sync::OnceLock;

    type MiCollectFn = unsafe extern "C" fn(force: bool);

    static MI_COLLECT: OnceLock<Option<MiCollectFn>> = OnceLock::new();

    let f = MI_COLLECT.get_or_init(|| unsafe {
      let sym = libc::dlsym(libc::RTLD_DEFAULT, c"mi_collect".as_ptr());
      if sym.is_null() {
        None
      } else {
        Some(std::mem::transmute::<*mut libc::c_void, MiCollectFn>(sym))
      }
    });

    if let Some(mi_collect) = f {
      unsafe { mi_collect(true) };
    }
  }
}

#[op2]
#[serde]
pub fn op_bootstrap_unstable_args(_state: &mut OpState) -> Vec<String> {
  vec![]
}

#[op2(fast)]
/// Returns `u32::MAX` when the request is allowed, or the number of
/// milliseconds until the rate-limit window resets when denied (< u32::MAX).
pub fn op_check_outbound_rate_limit(
  state: &mut OpState,
  #[string] url: &str,
  #[string] key: &str,
  is_traced: bool,
) -> u32 {
  let Some(limiter) = state.try_borrow::<TraceRateLimiter>() else {
    return u32::MAX;
  };
  match limiter.check_and_increment(url, key, is_traced) {
    Ok(()) => u32::MAX,
    Err(retry_after_ms) => retry_after_ms.min(u32::MAX as u64 - 1) as u32,
  }
}

/// The parser is recursive descent with no depth guard, so deeply nested source
/// overflows its stack and aborts the process with
/// `fatal runtime error: stack overflow`. That is not a panic, so `catch_unwind`
/// cannot intercept it, and the crash takes down every sandbox at once.
///
/// Two bounds are needed together, both measured against this parser:
///
///  * A large stack. On the default stack the abort came at roughly 1000-2000
///    nesting levels. Counting brackets does not bound it — `!`, `x=>`, `a?1:`,
///    `a=b=` and `if(a)` chains all abort with zero or constant bracket depth.
///  * A source-size limit. A large stack alone is not sound either, because
///    per-level cost varies by construct: with 512 MiB, 50,000 unary levels
///    (50 KiB) parsed fine while 50,000 arrow levels (150 KiB) and 100,000
///    parens (200 KiB) still aborted. Since the worst case is one recursion
///    level per source byte, the input has to be capped.
///
/// 16 KiB against 512 MiB leaves roughly a 3x margin at the ~10 KiB per level
/// the arrow-chain measurement implies. The stack is reserved lazily, so its
/// cost is virtual address space, not resident memory.
const TRANSPILE_STACK_BYTES: usize = 512 * 1024 * 1024;
const MAX_TRANSPILE_SOURCE_BYTES: usize = 16 * 1024;

/// Whether this isolate may transpile TypeScript.
///
/// Only the trusted E2B executor opts in. The parser runs arbitrary source on a
/// 512 MiB stack, so a user worker that did not request it must not reach the
/// op at all — gating only the namespace would still leave the op callable.
pub struct AllowTranspile(pub bool);

/// One process-wide gate on concurrent parses.
///
/// Each parse reserves a 512 MiB stack, so without a cap enough concurrent
/// callers could reserve that stack many times over. The permit count is the
/// available parallelism: enough to keep the CPUs busy, few enough that the
/// reserved address space stays bounded.
fn transpile_permits() -> &'static Semaphore {
  static PERMITS: OnceLock<Semaphore> = OnceLock::new();
  PERMITS.get_or_init(|| {
    let permits = std::thread::available_parallelism()
      .map(|it| it.get())
      .unwrap_or(1);
    Semaphore::new(permits)
  })
}

#[op2(async)]
#[string]
pub async fn op_transpile_ts(
  state: Rc<RefCell<OpState>>,
  #[string] source: String,
  #[string] filename: String,
) -> Result<String, AnyError> {
  let allowed = state
    .borrow()
    .try_borrow::<AllowTranspile>()
    .map(|it| it.0)
    .unwrap_or(false);

  if !allowed {
    return Err(anyhow::anyhow!("transpile is not enabled for this worker"));
  }

  if source.len() > MAX_TRANSPILE_SOURCE_BYTES {
    return Err(anyhow::anyhow!(
      "TypeScript source is {} bytes, limit is {MAX_TRANSPILE_SOURCE_BYTES}",
      source.len()
    ));
  }

  // Hold one permit for the parse's whole life. Moving it into the parser
  // thread ties its release to that thread ending — whether by a clean return
  // or a caught panic — and a failed spawn drops the closure, and the permit
  // with it. So it is released on every path.
  let permit = transpile_permits().acquire().await?;

  // Parse on a dedicated large-stack thread. Doing it on the caller's thread
  // would let a nested snippet take down the whole runtime.
  let (tx, rx) = oneshot::channel();
  let spawned = std::thread::Builder::new()
    .name("transpile".to_string())
    .stack_size(TRANSPILE_STACK_BYTES)
    .spawn(move || {
      let _permit = permit;
      let result =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
          transpile_ts_inner(source, filename)
        }));
      let _ = tx.send(result);
    });

  // A spawn failure took the permit into the dropped closure already.
  spawned?;

  // Await the parser thread instead of joining it, so a slow parse never
  // blocks the isolate thread.
  match rx.await {
    Ok(Ok(inner)) => inner,
    Ok(Err(_)) => Err(anyhow::anyhow!("transpile panicked")),
    Err(_) => Err(anyhow::anyhow!("transpile thread failed")),
  }
}

fn transpile_ts_inner(
  source: String,
  filename: String,
) -> Result<String, AnyError> {
  let specifier = deno_core::ModuleSpecifier::parse(&format!(
    "file:///{}",
    filename.trim_start_matches('/')
  ))?;

  let parsed = deno_ast::parse_module(deno_ast::ParseParams {
    specifier,
    text: source.into(),
    media_type: deno_ast::MediaType::TypeScript,
    capture_tokens: false,
    scope_analysis: false,
    maybe_syntax: None,
  })?;

  let transpiled = parsed.transpile(
    &deno_ast::TranspileOptions::default(),
    &deno_ast::TranspileModuleOptions::default(),
    &deno_ast::EmitOptions {
      source_map: deno_ast::SourceMapOption::None,
      ..Default::default()
    },
  )?;

  Ok(transpiled.into_source().text)
}

deno_core::extension!(
  runtime_node_compat,
  esm = [
    dir "js",
    "fieldUtils.js",
    "40_process.js",
  ]
);

deno_core::extension!(
  runtime_e2b,
  esm_entry_point = "ext:runtime_e2b/e2b_bootstrap.js",
  esm = [
    dir "js",
    "01_http.js",
    "async_hook.js",
    "denoOverrides.js",
    "e2b_bootstrap.js",
    "errors.js",
    "http.js",
    "request_context.js",
    "permissions.js",
  ]
);

deno_core::extension!(
  runtime,
  ops = [
    // op_is_terminal,
    op_is_runtime_init,
    op_stdin_set_raw,
    op_console_size,
    op_read_line_prompt,
    op_set_exit_code,
    op_runtime_metrics,
    op_schedule_mem_check,
    op_runtime_memory_usage,
    op_set_raw,
    op_bootstrap_unstable_args,
    op_raise_segfault,
    op_tap_promise_metrics,
    op_cancel_drop_token,
    op_check_outbound_rate_limit,
    op_mi_collect,
    op_transpile_ts,
  ],
  esm_entry_point = "ext:runtime/bootstrap.js",
  esm = [
    dir "js",
    "00_serve.js",
    "01_http.js",
    "40_process.js",
    "async_hook.js",
    "bootstrap.js",
    "denoOverrides.js",
    "errors.js",
    "fieldUtils.js",
    "http.js",
    "request_context.js",
    "namespaces.js",
    "navigator.js",
    "permissions.js",
    "promises.js",
  ]
);
