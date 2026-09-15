#![allow(clippy::arc_with_non_send_sync)]
#![allow(clippy::async_yields_async)]

use std::borrow::Cow;
use std::collections::HashMap;
use std::io;
use std::io::BufRead;
use std::io::Cursor;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use async_tungstenite::WebSocketStream;
use base::get_default_permissions;
use base::integration_test;
use base::integration_test_listen_fut;
use base::integration_test_with_server_flag;
use base::server::Builder;
use base::server::RequestIdleTimeout;
use base::server::ServerEvent;
use base::server::ServerFlags;
use base::server::ServerHealth;
use base::server::Tls;
use base::utils::test_utils::create_test_user_worker;
use base::utils::test_utils::ensure_npm_package_installed;
use base::utils::test_utils::test_user_runtime_opts;
use base::utils::test_utils::test_user_worker_pool_policy;
use base::utils::test_utils::TestBed;
use base::utils::test_utils::TestBedBuilder;
use base::worker;
use base::worker::TerminationToken;
use base::WorkerKind;
use deno::deno_telemetry::OtelConfig;
use deno::DenoOptionsBuilder;
use deno_core::error::AnyError;
use deno_core::serde_json::json;
use deno_core::serde_json::{self};
use deno_facade::generate_binary_eszip;
use deno_facade::EmitterFactory;
use deno_facade::EszipPayloadKind;
use deno_facade::Metadata;
use ext_event_worker::events::LogLevel;
use ext_event_worker::events::ShutdownReason;
use ext_event_worker::events::WorkerEventWithMetadata;
use ext_event_worker::events::WorkerEvents;
use ext_runtime::SharedMetricSource;
use ext_workers::context::MainWorkerRuntimeOpts;
use ext_workers::context::WorkerContextInitOpts;
use ext_workers::context::WorkerRequestMsg;
use ext_workers::context::WorkerRuntimeOpts;
use futures_util::future::BoxFuture;
use futures_util::Future;
use futures_util::FutureExt;
use futures_util::SinkExt;
use futures_util::StreamExt;
use http::Method;
use http::Request;
use http::Response as HttpResponse;
use http::StatusCode;
use http_utils::utils::get_upgrade_type;
use http_v02 as http;
use http_v02::HeaderValue;
use hyper::body::to_bytes;
use hyper::Body;
use hyper_v014 as hyper;
use reqwest::header;
use reqwest::multipart::Form;
use reqwest::multipart::Part;
use reqwest::Certificate;
use reqwest::Client;
use reqwest::RequestBuilder;
use reqwest::Response;
use reqwest_v011 as reqwest;
use serde::Deserialize;
use serial_test::serial;
use tokio::fs;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::join;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::time::sleep;
use tokio::time::timeout;
use tokio_rustls::rustls;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::rustls::ClientConfig;
use tokio_rustls::rustls::RootCertStore;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::TlsConnector;
use tokio_util::compat::TokioAsyncReadCompatExt;
use tokio_util::sync::CancellationToken;
use tungstenite::Message;

const MB: usize = 1024 * 1024;
const NON_SECURE_PORT: u16 = 8498;
const SECURE_PORT: u16 = 4433;
const TESTBED_DEADLINE_SEC: u64 = 20;

/// The adapter fails closed when `E2B_API_KEY` is unset, and the main worker
/// inherits the test process's environment. Adapter fixtures run with
/// `crates/base` as their working directory, so they also need an explicit
/// executor path instead of the repository-root deployment default. This runs
/// before any worker boots, which a per-test `set_var` could not guarantee.
///
/// `EDGE_RUNTIME_WORKER_POOL_SIZE` is set here for the same reason: the pool is
/// a `Lazy`, and debug builds default to a single user-worker thread, so
/// cross-sandbox concurrency tests would otherwise measure one shared thread.
#[ctor::ctor]
fn set_test_environment() {
  if std::env::var("E2B_API_KEY").is_err() {
    std::env::set_var("E2B_API_KEY", "test-key");
  }
  std::env::set_var("EXECUTOR_SERVICE_PATH", "../../examples/e2b-executor");
  if std::env::var("EDGE_RUNTIME_WORKER_POOL_SIZE").is_err() {
    std::env::set_var("EDGE_RUNTIME_WORKER_POOL_SIZE", "4");
  }
}

#[tokio::test]
#[serial]
async fn test_main_worker_bundles_a_service_once_and_reuses_it() {
  integration_test!(
    "./test_cases/e2b-bundle-probe-main",
    NON_SECURE_PORT,
    "",
    None,
    None,
    None,
    (|resp| async {
      let body = resp.unwrap().text().await.unwrap();
      let json: serde_json::Value = serde_json::from_str(&body).unwrap();

      assert_eq!(json["ok"], serde_json::json!(true), "got: {body}");
      assert_eq!(
        json["isUint8Array"],
        serde_json::json!(true),
        "bundle must hand JS raw bytes, got: {body}"
      );
      assert!(
        json["byteLength"].as_u64().unwrap() > 0,
        "an empty eszip cannot boot a worker, got: {body}"
      );
    }),
    TerminationToken::new()
  );
}

#[tokio::test]
#[serial]
async fn test_prebuilt_eszip_boots_every_user_worker() {
  integration_test!(
    "./test_cases/e2b-bundle-probe-main",
    NON_SECURE_PORT,
    "?reuse=1",
    None,
    None,
    None,
    (|resp| async {
      let body = resp.unwrap().text().await.unwrap();
      let json: serde_json::Value = serde_json::from_str(&body).unwrap();

      assert_eq!(json["ok"], serde_json::json!(true), "got: {body}");

      // Each worker is a fresh isolate, so both report their own first hit —
      // one shared eszip must not mean one shared context.
      for i in 0..2 {
        let reply: serde_json::Value =
          serde_json::from_str(json["bodies"][i].as_str().unwrap()).unwrap();

        assert_eq!(reply["hits"], serde_json::json!(1), "got: {body}");
        assert_eq!(
          reply["canBundle"],
          serde_json::json!(false),
          "a user worker must not be able to bundle, got: {body}"
        );
      }
    }),
    TerminationToken::new()
  );
}

#[tokio::test]
#[serial]
async fn test_node_vm_allowed_by_context_flag_without_allow_run() {
  integration_test!(
    "./test_cases/e2b-vm-probe-main",
    NON_SECURE_PORT,
    "?vm=1",
    None,
    None,
    None,
    (|resp| async {
      let body = resp.unwrap().text().await.unwrap();
      assert!(
        body.contains(r#""ok":true"#),
        "node:vm should work with allowNodeVm, got: {body}"
      );
      assert!(body.contains(r#""result":2"#), "got: {body}");
    }),
    TerminationToken::new()
  );
}

#[tokio::test]
#[serial]
async fn test_node_vm_denied_without_context_flag() {
  integration_test!(
    "./test_cases/e2b-vm-probe-main",
    NON_SECURE_PORT,
    "?vm=0",
    None,
    None,
    None,
    (|resp| async {
      let body = resp.unwrap().text().await.unwrap();
      assert!(
        body.contains(r#""ok":false"#),
        "node:vm must stay gated without the flag, got: {body}"
      );
    }),
    TerminationToken::new()
  );
}

#[tokio::test]
#[serial]
async fn test_user_worker_terminate_reaps_the_isolate() {
  let tb = TestBedBuilder::new("./test_cases/e2b-terminate-probe")
    .with_per_worker_policy(None)
    .build()
    .await;

  let mut terminate_response = tb
    .request(|b| {
      b.uri("/?timings=1")
        .body(Body::empty())
        .context("can't make request")
    })
    .await
    .unwrap();
  assert_eq!(terminate_response.status(), StatusCode::OK);
  let body = to_bytes(terminate_response.body_mut()).await.unwrap();
  let body = String::from_utf8(body.to_vec()).unwrap();
  let terminate: serde_json::Value = serde_json::from_str(&body).unwrap();

  assert_eq!(
    terminate["terminated"],
    serde_json::json!(true),
    "got: {body}"
  );
  assert_eq!(
    terminate["trackedBefore"],
    serde_json::json!(true),
    "got: {body}"
  );
  assert_eq!(
    terminate["trackedAfter"],
    serde_json::json!(false),
    "got: {body}"
  );
  assert!(
    terminate["timing"]["fresh"]["runtimeInitMs"].is_number(),
    "a fresh worker must report runtime initialization timing, got: {body}"
  );
  assert_eq!(
    terminate["timing"]["reused"]["key"], terminate["timing"]["fresh"]["key"],
    "the second create must reuse the fresh worker, got: {body}"
  );
  assert_eq!(
    terminate["timing"]["reused"]["runtimeInitMs"],
    serde_json::json!(0),
    "a reused worker must not report a fresh runtime timing, got: {body}"
  );
  assert_eq!(
    terminate["timing"]["reused"]["moduleInitMs"],
    serde_json::json!(0),
    "a reused worker must not report a fresh module timing, got: {body}"
  );
  for phase in [
    "loaderVfsMs",
    "resourceLimitsMs",
    "jsRuntimeNewMs",
    "bootstrapMs",
    "bootstrapBlockingRunMs",
    "bootstrapBlockingQueueMs",
    "postSetupBlockingRunMs",
    "postSetupBlockingQueueMs",
  ] {
    assert!(
      terminate["timing"]["fresh"]["runtimeInit"][phase].is_number(),
      "a fresh worker must report {phase}, got: {body}"
    );
    assert_eq!(
      terminate["timing"]["reused"]["runtimeInit"][phase],
      serde_json::json!(0),
      "a reused worker must not report fresh {phase}, got: {body}"
    );
  }
  assert_eq!(
    terminate["terminatedAgain"],
    serde_json::json!(false),
    "got: {body}"
  );

  let mut shutdown_response = timeout(
    Duration::from_secs(5),
    tb.request(|b| {
      b.uri("/shutdown")
        .body(Body::empty())
        .context("can't make request")
    }),
  )
  .await
  .expect("waitForShutdown endpoint timed out")
  .unwrap();
  assert_eq!(shutdown_response.status(), StatusCode::OK);
  let body = to_bytes(shutdown_response.body_mut()).await.unwrap();
  let body = String::from_utf8(body.to_vec()).unwrap();
  let shutdown: serde_json::Value = serde_json::from_str(&body).unwrap();

  assert_eq!(
    shutdown["shutdown"]["reason"],
    serde_json::json!("TerminationRequested"),
    "got: {body}"
  );
  assert!(
    shutdown["shutdown"]["cpuTimeUsed"].is_number(),
    "got: {body}"
  );
  assert!(
    shutdown["shutdown"]["memoryUsed"]["total"]
      .as_u64()
      .unwrap_or_default()
      > 0
      && shutdown["shutdown"]["memoryUsed"]["heap"]
        .as_u64()
        .unwrap_or_default()
        > 0,
    "the final shutdown must include V8 heap telemetry, got: {body}"
  );
  assert!(
    shutdown["shutdown"]["memoryUsed"]["external"].is_number(),
    "got: {body}"
  );

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

/// Asserts that `UserWorker.terminate()` reaps the target isolate rather than
/// merely deregistering it. The `policy_fn` selects which supervisor policy
/// hosts the target worker; every policy must reap it.
async fn assert_user_worker_terminate_shuts_down_the_isolate(
  policy_fn: fn(TestBedBuilder) -> TestBedBuilder,
) {
  let (tx, mut rx) = mpsc::unbounded_channel();
  let tb = policy_fn(TestBedBuilder::new("./test_cases/e2b-terminate-probe"))
    .with_worker_event_sender(Some(tx))
    .build()
    .await;

  let mut resp = tb
    .request(|b| b.uri("/").body(Body::empty()).context("can't make request"))
    .await
    .unwrap();

  assert_eq!(resp.status().as_u16(), StatusCode::OK);

  let body = to_bytes(resp.body_mut()).await.unwrap();
  let body = String::from_utf8(body.to_vec()).unwrap();
  let json: serde_json::Value = serde_json::from_str(&body).unwrap();

  // `TerminationRequested` is also reachable from the driver's drop guard, so
  // pin the reason below to an actual `terminate()` call by the probe.
  assert_eq!(json["terminated"], serde_json::json!(true), "got: {body}");

  let mut shutdown = None;
  while let Ok(Some(ev)) =
    tokio::time::timeout(Duration::from_secs(10), rx.recv()).await
  {
    let is_target = ev.metadata.service_path.as_deref()
      == Some("./test_cases/e2b-terminate-target");

    if let WorkerEvents::Shutdown(ev) = ev.event {
      if is_target {
        shutdown = Some(ev);
        break;
      }
    }
  }

  rx.close();
  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;

  let shutdown =
    shutdown.expect("the target isolate must report a shutdown event");

  assert_eq!(
    shutdown.reason,
    ShutdownReason::TerminationRequested,
    "got: {shutdown:?}"
  );

  // NOTE: The reason alone does not prove the isolate died - a supervisor that
  // returns without dispatching the V8 termination interrupt reports the same
  // reason. Only `v8_handle_termination`, which runs inside the live isolate
  // and calls `terminate_execution`, reads the final heap statistics; when no
  // interrupt is dispatched its sender is dropped and these are all zero.
  assert!(
    shutdown.memory_used.total > 0 && shutdown.memory_used.heap > 0,
    "the V8 termination interrupt must have run in the target isolate, got: {:?}",
    shutdown.memory_used
  );
}

#[tokio::test]
#[serial]
async fn test_user_worker_terminate_shuts_down_the_isolate() {
  assert_user_worker_terminate_shuts_down_the_isolate(|it| {
    it.with_per_worker_policy(None)
  })
  .await;
}

#[tokio::test]
#[serial]
async fn test_user_worker_terminate_shuts_down_the_isolate_per_request() {
  // The per-request strategy has its own `supervise.cancelled()` arm, so it
  // needs its own coverage. `oneshot` cannot host this probe because
  // `forceCreate` is silently ignored under it (`WorkerPool::create_user_worker`),
  // so the target could be an unrelated reused worker.
  assert_user_worker_terminate_shuts_down_the_isolate(|it| {
    it.with_per_request_policy(None)
  })
  .await;
}

const WORKER_OP_PROBE_CHILD_ENV: &str = "E2B_WORKER_OP_PROBE_CHILD";
const WORKER_OP_PROBE_OK: &str = "WORKER_OP_PROBE_OK";

/// A privileged worker-management op called from a user worker must fail closed
/// with a thrown capability error, not abort the process. The message sender it
/// needs is installed only in the main worker, and the mandatory `borrow()` for
/// an absent type is a non-unwinding panic that aborts the whole runtime.
///
/// The probe runs in a re-exec'd child so the pre-fix abort surfaces here as a
/// non-zero child exit (a failed assertion) instead of killing this harness.
#[tokio::test]
#[serial]
async fn test_user_worker_management_op_fails_closed_without_abort() {
  let exe = std::env::current_exe().expect("test executable path");
  let output = std::process::Command::new(exe)
    .args([
      "--ignored",
      "--nocapture",
      "--test-threads=1",
      "worker_management_op_probe_child",
    ])
    .env(WORKER_OP_PROBE_CHILD_ENV, "1")
    .output()
    .expect("failed to run the probe child");

  let stdout = String::from_utf8_lossy(&output.stdout);
  let stderr = String::from_utf8_lossy(&output.stderr);

  assert!(
    output.status.success(),
    "worker-management op aborted the child instead of failing closed: \
     status={:?}\nstdout=\n{stdout}\nstderr=\n{stderr}",
    output.status
  );
  assert!(
    stdout.contains(WORKER_OP_PROBE_OK),
    "child did not confirm a clean capability error:\nstdout=\n{stdout}\n\
     stderr=\n{stderr}"
  );
}

/// Child half of `test_user_worker_management_op_fails_closed_without_abort`.
/// Ignored so it only runs when the parent re-execs it with the guard env set.
/// It drives the probe twice to prove the server keeps answering after the op
/// fails closed.
#[tokio::test]
#[ignore]
async fn worker_management_op_probe_child() {
  if std::env::var(WORKER_OP_PROBE_CHILD_ENV).is_err() {
    return;
  }

  let tb = TestBedBuilder::new("./test_cases/e2b-worker-op-probe-main")
    .build()
    .await;

  for _ in 0..2 {
    let mut resp = tb
      .request(|b| b.uri("/").body(Body::empty()).context("can't make request"))
      .await
      .unwrap();

    assert_eq!(resp.status().as_u16(), StatusCode::OK);

    let body = to_bytes(resp.body_mut()).await.unwrap();
    let body = String::from_utf8(body.to_vec()).unwrap();
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();

    assert_eq!(json["threw"], serde_json::json!(true), "got: {body}");
    assert!(
      json["error"]
        .as_str()
        .unwrap()
        .contains("only available to the main worker"),
      "got: {body}"
    );
  }

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;

  println!("{WORKER_OP_PROBE_OK}");
}

/// A vm-enabled isolate must never be reused to satisfy a create that did not
/// request vm access. The pool reuse key folds in `allowNodeVm`, so the second
/// (non-vm) create for the same service path gets a fresh isolate that denies
/// node:vm.
#[tokio::test]
#[serial]
async fn test_vm_worker_is_not_reused_for_non_vm_create() {
  integration_test!(
    "./test_cases/e2b-vm-reuse-probe-main",
    NON_SECURE_PORT,
    "",
    None,
    None,
    None,
    (
      |(port, _url, _req_builder, _event_rx, _metric_src)| async move {
        let base = format!("http://localhost:{port}");

        // First a vm-enabled create, then a non-vm create for the same service
        // path (neither forces creation), so the second can only differ if the
        // reuse key folds in the vm capability.
        let vm: serde_json::Value = reqwest::get(format!("{base}/?vm=1"))
          .await
          .unwrap()
          .json()
          .await
          .unwrap();
        let plain: serde_json::Value = reqwest::get(format!("{base}/?vm=0"))
          .await
          .unwrap()
          .json()
          .await
          .unwrap();

        assert_eq!(
          vm["vm"]["ok"],
          serde_json::json!(true),
          "the vm create should get node:vm, got: {vm}"
        );
        assert_eq!(
          plain["vm"]["ok"],
          serde_json::json!(false),
          "the non-vm create must not get node:vm, got: {plain}"
        );
        assert_ne!(
          vm["key"], plain["key"],
          "a non-vm create must not reuse the vm isolate: vm={vm} plain={plain}"
        );

        Some(Ok(reqwest::get(format!("{base}/?vm=0")).await.unwrap()))
      },
      |_resp| async {}
    ),
    TerminationToken::new()
  );
}

/// A transpile-enabled isolate must never be reused for a standard create of
/// the same service path.
#[tokio::test]
#[serial]
async fn test_transpile_worker_is_not_reused_for_non_transpile_create() {
  integration_test!(
    "./test_cases/e2b-transpile-reuse-probe-main",
    NON_SECURE_PORT,
    "",
    None,
    None,
    None,
    (
      |(port, _url, _req_builder, _event_rx, _metric_src)| async move {
        let base = format!("http://localhost:{port}");
        let transpile_response =
          reqwest::get(format!("{base}/?transpile=1")).await.unwrap();
        let transpile_status = transpile_response.status();
        let transpile: serde_json::Value =
          transpile_response.json().await.unwrap();
        let plain_response =
          reqwest::get(format!("{base}/?transpile=0")).await.unwrap();
        let plain_status = plain_response.status();
        let plain: serde_json::Value = plain_response.json().await.unwrap();

        assert!(
          transpile_status.is_success(),
          "the transpile create should succeed, got: {transpile}"
        );
        assert_eq!(
          transpile["status"],
          serde_json::json!(200),
          "the transpile worker should respond successfully, got: {transpile}"
        );
        assert_ne!(
          transpile["key"], plain["key"],
          "a non-transpile create must not reuse the transpile isolate: \
           transpile={transpile} plain_status={plain_status} plain={plain}"
        );
        assert!(
          !plain_status.is_success(),
          "the non-transpile worker must preserve its denied response: {plain}"
        );
        assert!(
          !plain["body"]
            .as_str()
            .is_some_and(|body| body.contains("\"stripped\"")),
          "the non-transpile worker must not return a transpile result: {plain}"
        );

        Some(Ok(
          reqwest::get(format!("{base}/?transpile=0")).await.unwrap(),
        ))
      },
      |_resp| async {}
    ),
    TerminationToken::new()
  );
}

/// An E2B executor profile enables node:vm without the legacy context flag,
/// while a standard profile for the same service must not reuse that isolate.
#[tokio::test]
#[serial]
async fn test_e2b_runtime_profile_is_not_reused_for_standard_create() {
  integration_test!(
    "./test_cases/e2b-vm-reuse-probe-main",
    NON_SECURE_PORT,
    "",
    None,
    None,
    None,
    (
      |(port, _url, _req_builder, _event_rx, _metric_src)| async move {
        let base = format!("http://localhost:{port}");

        let e2b: serde_json::Value =
          reqwest::get(format!("{base}/?profile=e2b"))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let standard: serde_json::Value =
          reqwest::get(format!("{base}/?profile=standard"))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

        assert_eq!(
          e2b["vm"]["ok"],
          serde_json::json!(true),
          "the E2B profile should get node:vm, got: {e2b}"
        );
        assert_eq!(
          standard["vm"]["ok"],
          serde_json::json!(false),
          "the standard profile must not get node:vm, got: {standard}"
        );
        assert_ne!(
          e2b["key"], standard["key"],
          "standard profile must not reuse E2B isolate: \
           e2b={e2b}, standard={standard}"
        );

        Some(Ok(
          reqwest::get(format!("{base}/?profile=standard"))
            .await
            .unwrap(),
        ))
      },
      |_resp| async {}
    ),
    TerminationToken::new()
  );
}

#[tokio::test]
#[serial]
async fn test_e2b_runtime_profile_hardens_bootstrap_without_standard_runtime() {
  integration_test!(
    "./test_cases/e2b-bootstrap-hardening-main",
    NON_SECURE_PORT,
    "",
    None,
    None,
    None,
    (|resp| async {
      let body = resp.unwrap().text().await.unwrap();
      let json: serde_json::Value = serde_json::from_str(&body).unwrap();

      assert_eq!(json["vm"]["result"], serde_json::json!(2), "got: {body}");
      assert_eq!(
        json["realPath"]["name"],
        serde_json::json!("PermissionDenied"),
        "got: {body}"
      );

      for name in ["kill", "exit", "addSignalListener", "removeSignalListener"]
      {
        assert_eq!(
          json["mocks"][name]["name"],
          serde_json::json!("TypeError"),
          "{name} did not throw TypeError: {body}"
        );
        assert_eq!(
          json["mocks"][name]["message"],
          serde_json::json!("called MOCK_FN"),
          "{name} had the wrong error message: {body}"
        );
      }

      assert_eq!(
        json["sharedMemory"]["name"],
        serde_json::json!("TypeError"),
        "got: {body}"
      );
      assert_eq!(
        json["sharedMemory"]["message"],
        serde_json::json!("Creating a shared memory is not supported"),
        "got: {body}"
      );
      assert_eq!(
        json["execPath"],
        serde_json::json!("/bin/edge-runtime"),
        "got: {body}"
      );
      assert!(
        json["memoryUsage"]["rss"].is_number(),
        "memoryUsage.rss was not numeric: {body}"
      );
    }),
    TerminationToken::new()
  );
}

#[tokio::test]
#[serial]
async fn test_edge_runtime_transpile_strips_types_and_errors_safely() {
  integration_test!(
    "./test_cases/e2b-transpile-probe-main",
    NON_SECURE_PORT,
    "",
    None,
    None,
    None,
    (|resp| async {
      let body = resp.unwrap().text().await.unwrap();
      let json: serde_json::Value = serde_json::from_str(&body).unwrap();

      let stripped = json["stripped"].as_str().unwrap();
      assert!(stripped.contains("let x = 1"), "got: {stripped}");
      assert!(!stripped.contains(": number"), "got: {stripped}");

      for input in ["malformed", "truncated"] {
        assert!(
          json[input].as_str().unwrap().starts_with("threw:"),
          "{input} input did not throw: {body}"
        );
      }

      let survived = json["survived"].as_str().unwrap();
      assert!(survived.contains("const alive = true"), "got: {survived}");
      assert!(!survived.contains(": boolean"), "got: {survived}");
    }),
    TerminationToken::new()
  );
}

/// A user worker that did not opt into transpile must be denied by the op
/// itself, not merely by the namespace. The parser runs arbitrary source on a
/// 512 MiB stack, so it is a capability reserved for the trusted E2B executor.
#[tokio::test]
#[serial]
async fn test_transpile_denied_without_allow_transpile() {
  integration_test!(
    "./test_cases/e2b-transpile-denied-probe-main",
    NON_SECURE_PORT,
    "",
    None,
    None,
    None,
    (|resp| async {
      let body = resp.unwrap().text().await.unwrap();
      let json: serde_json::Value = serde_json::from_str(&body).unwrap();

      assert_eq!(json["threw"], serde_json::json!(true), "got: {body}");
      assert!(
        json["error"]
          .as_str()
          .unwrap()
          .contains("transpile is not enabled for this worker"),
        "got: {body}"
      );
    }),
    TerminationToken::new()
  );
}

/// Bundling reads and resolves whatever path it is handed, so it must be
/// confined to the bundle root. The shipped executor lives under the root and
/// must still bundle; an absolute path outside it must be refused.
#[tokio::test]
#[serial]
async fn test_bundle_confined_to_root() {
  integration_test!(
    "./test_cases/e2b-bundle-confine-probe-main",
    NON_SECURE_PORT,
    "",
    None,
    None,
    None,
    (|resp| async {
      let body = resp.unwrap().text().await.unwrap();
      let json: serde_json::Value = serde_json::from_str(&body).unwrap();

      assert_eq!(
        json["executor"]["ok"],
        serde_json::json!(true),
        "the shipped executor is under the bundle root, got: {body}"
      );
      assert!(
        json["executor"]["byteLength"].as_u64().unwrap() > 0,
        "an empty eszip cannot boot a worker, got: {body}"
      );

      assert_eq!(
        json["outside"]["ok"],
        serde_json::json!(false),
        "a path outside the bundle root must be refused, got: {body}"
      );
      assert!(
        json["outside"]["error"]
          .as_str()
          .unwrap()
          .contains("outside the bundle root"),
        "got: {body}"
      );
    }),
    TerminationToken::new()
  );
}

async fn e2b_execute(
  tb: &TestBed,
  sandbox: &str,
  code: &str,
) -> serde_json::Value {
  e2b_execute_with_env(tb, sandbox, code, serde_json::json!({})).await
}

async fn e2b_execute_ts(
  tb: &TestBed,
  sandbox: &str,
  code: &str,
) -> serde_json::Value {
  let body = serde_json::json!({ "code": code, "language": "typescript" });
  let mut resp = tb
    .request(|b| {
      b.uri("/internal/execute")
        .method("POST")
        .header("x-sandbox-id", sandbox)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .context("can't make request")
    })
    .await
    .unwrap();

  assert_eq!(resp.status().as_u16(), StatusCode::OK);

  let bytes = to_bytes(resp.body_mut()).await.unwrap();
  serde_json::from_slice(&bytes).unwrap()
}

async fn e2b_execute_with_env(
  tb: &TestBed,
  sandbox: &str,
  code: &str,
  env_vars: serde_json::Value,
) -> serde_json::Value {
  let body = serde_json::json!({
    "code": code,
    "language": "javascript",
    "env_vars": env_vars,
  });
  let mut resp = tb
    .request(|b| {
      b.uri("/internal/execute")
        .method("POST")
        .header("x-sandbox-id", sandbox)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .context("can't make request")
    })
    .await
    .unwrap();

  assert_eq!(resp.status().as_u16(), StatusCode::OK);

  let bytes = to_bytes(resp.body_mut()).await.unwrap();
  serde_json::from_slice(&bytes).unwrap()
}

async fn e2b_execute_with_baseline_env(
  tb: &TestBed,
  sandbox: &str,
  code: &str,
  sandbox_env: &str,
) -> serde_json::Value {
  let body = serde_json::json!({ "code": code, "language": "javascript" });
  let mut resp = tb
    .request(|b| {
      b.uri("/internal/execute")
        .method("POST")
        .header("x-sandbox-id", sandbox)
        .header("x-sandbox-env", sandbox_env)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .context("can't make request")
    })
    .await
    .unwrap();

  assert_eq!(resp.status().as_u16(), StatusCode::OK);

  let bytes = to_bytes(resp.body_mut()).await.unwrap();
  serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
#[serial]
async fn test_e2b_executor_persists_primitive_state() {
  let tb = TestBedBuilder::new("./test_cases/e2b-executor-harness")
    .with_per_worker_policy(None)
    .build()
    .await;

  let first = e2b_execute(&tb, "s1", "let x = 1").await;
  assert_eq!(first["error"], serde_json::Value::Null);

  e2b_execute(&tb, "s1", "x++").await;

  let third = e2b_execute(&tb, "s1", "x").await;
  assert_eq!(third["result"], serde_json::json!(2));
  assert_eq!(third["result_type"], serde_json::json!("number"));

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_e2b_executor_typescript_state_persists() {
  let tb = TestBedBuilder::new("./test_cases/e2b-executor-harness")
    .with_per_worker_policy(None)
    .build()
    .await;

  let first = e2b_execute_ts(&tb, "ts1", "let x: number = 10").await;
  assert_eq!(first["error"], serde_json::Value::Null);
  e2b_execute_ts(&tb, "ts1", "x += 5").await;
  let third = e2b_execute_ts(&tb, "ts1", "x").await;
  assert_eq!(third["result"], serde_json::json!(15));

  let typed = e2b_execute_ts(
    &tb,
    "ts1",
    "interface User { name: string }\nconst user: User = { name: 'alice' }",
  )
  .await;
  assert_eq!(typed["error"], serde_json::Value::Null);
  let name = e2b_execute_ts(&tb, "ts1", "user.name").await;
  assert_eq!(name["result"], serde_json::json!("alice"));
  let erased = e2b_execute(&tb, "ts1", "typeof User").await;
  assert_eq!(erased["result"], serde_json::json!("undefined"));

  let shared_with_js = e2b_execute(&tb, "ts1", "x").await;
  assert_eq!(shared_with_js["result"], serde_json::json!(15));
  e2b_execute(&tb, "ts1", "let fromJavaScript = 4").await;
  let shared_with_ts =
    e2b_execute_ts(&tb, "ts1", "(fromJavaScript as number) + 1").await;
  assert_eq!(shared_with_ts["result"], serde_json::json!(5));

  let promise = e2b_execute_ts(
    &tb,
    "ts1",
    "Promise.resolve(21).then((value: number) => value * 2)",
  )
  .await;
  assert_eq!(promise["result"], serde_json::json!(42));

  let malformed = e2b_execute_ts(&tb, "ts1", "let broken: = ;;;").await;
  assert_eq!(
    malformed["error"]["kind"],
    serde_json::json!("compile_error")
  );
  let survived = e2b_execute_ts(&tb, "ts1", "x").await;
  assert_eq!(survived["result"], serde_json::json!(15));

  for source in [
    "export const unsupported = true",
    "import { unsupported } from './unsupported.ts'; unsupported",
    "await Promise.resolve('unsupported')",
  ] {
    let module_syntax = e2b_execute_ts(&tb, "ts1", source).await;
    assert_eq!(
      module_syntax["error"]["kind"],
      serde_json::json!("compile_error"),
      "source: {source}, response: {module_syntax}",
    );
  }

  let runtime_syntax =
    e2b_execute_ts(&tb, "ts1", "throw new SyntaxError('runtime syntax')").await;
  assert_eq!(
    runtime_syntax["error"]["kind"],
    serde_json::json!("runtime_error")
  );

  let body = serde_json::json!({
    "code": "globalThis.unsupportedLanguageRan = true",
    "language": "python",
  });
  let mut resp = tb
    .request(|b| {
      b.uri("/internal/execute")
        .method("POST")
        .header("x-sandbox-id", "ts1")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .context("can't make request")
    })
    .await
    .unwrap();
  assert_eq!(resp.status().as_u16(), StatusCode::OK);
  let bytes = to_bytes(resp.body_mut()).await.unwrap();
  let rejected: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
  assert_eq!(
    rejected["error"]["kind"],
    serde_json::json!("unsupported_language")
  );
  assert_eq!(rejected["error"]["message"], serde_json::json!("python"));
  assert_eq!(rejected["stdout"], serde_json::json!([]));
  assert_eq!(rejected["stderr"], serde_json::json!([]));
  let did_not_run =
    e2b_execute(&tb, "ts1", "typeof unsupportedLanguageRan").await;
  assert_eq!(did_not_run["result"], serde_json::json!("undefined"));

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_e2b_executor_isolates_sandboxes() {
  let tb = TestBedBuilder::new("./test_cases/e2b-executor-harness")
    .with_per_worker_policy(None)
    .build()
    .await;

  e2b_execute(&tb, "a", "let secret = 'A'").await;
  let probe = e2b_execute(&tb, "b", "typeof secret").await;
  assert_eq!(probe["result"], serde_json::json!("undefined"));

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_e2b_executor_isolates_state_and_secrets() {
  let tb = TestBedBuilder::new("./test_cases/e2b-executor-harness")
    .with_per_worker_policy(None)
    .build()
    .await;

  e2b_execute(&tb, "boundary", "let boundaryState = 1").await;

  for source in [
    "typeof Deno",
    "typeof Deno?.core",
    "typeof EdgeRuntime",
    r#"TextEncoder.constructor("return typeof Deno.core")()"#,
    r#"globalThis.constructor.constructor("return typeof Deno.core")()"#,
    r#"
      ({ then(resolve) {
        globalThis.leaked = resolve.constructor.constructor(
          "return typeof Deno.core",
        )();
        resolve(1);
      } })
    "#,
  ] {
    let probe = e2b_execute(&tb, "boundary", source).await;
    assert!(
      probe["result"] == serde_json::json!("undefined")
        || probe["error"]["kind"] == serde_json::json!("runtime_error"),
      "sandbox escape succeeded for {source}: {probe}",
    );

    let alive = e2b_execute(&tb, "boundary", "boundaryState += 1").await;
    assert_eq!(alive["error"], serde_json::Value::Null);
  }

  let leaked = e2b_execute(&tb, "boundary", "globalThis.leaked").await;
  assert_eq!(leaked["result_type"], serde_json::json!("undefined"));
  assert_eq!(
    e2b_execute(&tb, "boundary", "boundaryState").await["result"],
    serde_json::json!(7)
  );

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_e2b_executor_text_codec_round_trips() {
  let tb = TestBedBuilder::new("./test_cases/e2b-executor-harness")
    .with_per_worker_policy(None)
    .build()
    .await;

  // The in-realm TextEncoder/TextDecoder must round-trip BMP and astral code
  // points, resume a decode split mid-multibyte across `{ stream: true }`
  // calls, match the platform for `encode(undefined)` (empty output), strip a
  // leading BOM by default, and throw on invalid bytes when `fatal` is set.
  let probe = e2b_execute(
    &tb,
    "codec",
    r#"
      const enc = new TextEncoder();
      const round =
        new TextDecoder().decode(enc.encode("héllo 𐍈")) === "héllo 𐍈";
      const astral = enc.encode("𐍈");
      const dec = new TextDecoder();
      const first = dec.decode(astral.subarray(0, 2), { stream: true });
      const second = dec.decode(astral.subarray(2), { stream: true });
      const streamed = first === "" && first + second === "𐍈";
      const undefinedLen = enc.encode(undefined).length;
      const bom = new TextDecoder().decode(
        new Uint8Array([0xEF, 0xBB, 0xBF, 0x41]),
      );
      let fatalThrows = false;
      try {
        new TextDecoder("utf-8", { fatal: true })
          .decode(new Uint8Array([0xFF]));
      } catch (error) {
        fatalThrows = error instanceof TypeError;
      }
      [round, streamed, undefinedLen, bom, fatalThrows]
    "#,
  )
  .await;
  assert_eq!(probe["error"], serde_json::Value::Null);
  assert_eq!(
    probe["result"],
    serde_json::json!([true, true, 0, "A", true])
  );

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_e2b_executor_env_override_is_request_scoped() {
  let tb = TestBedBuilder::new("./test_cases/e2b-executor-harness")
    .with_per_worker_policy(None)
    .build()
    .await;

  let mut resp = tb
    .request(|b| {
      b.uri("/internal/execute")
        .method("POST")
        .header("x-sandbox-id", "envbox")
        .header("x-sandbox-env", r#"{"FOO":"sandbox"}"#)
        .header("content-type", "application/json")
        .body(Body::from(
          r#"{"code":"process.env.FOO","language":"javascript"}"#,
        ))
        .context("can't make request")
    })
    .await
    .unwrap();
  assert_eq!(resp.status().as_u16(), StatusCode::OK);
  let bytes = to_bytes(resp.body_mut()).await.unwrap();
  let first: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
  assert_eq!(first["result"], serde_json::json!("sandbox"));

  let mut resp = tb
    .request(|b| {
      b.uri("/internal/execute")
        .method("POST")
        .header("x-sandbox-id", "envbox")
        .header("content-type", "application/json")
        .body(Body::from(
          r#"{"code":"process.env.FOO","language":"javascript","env_vars":{"FOO":"request"}}"#,
        ))
        .context("can't make request")
    })
    .await
    .unwrap();
  assert_eq!(resp.status().as_u16(), StatusCode::OK);
  let bytes = to_bytes(resp.body_mut()).await.unwrap();
  let second: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
  assert_eq!(second["result"], serde_json::json!("request"));

  let reverted = e2b_execute(&tb, "envbox", "process.env.FOO").await;
  assert_eq!(reverted["result"], serde_json::json!("sandbox"));

  let process_surface = e2b_execute(
    &tb,
    "envbox",
    "[Object.keys(process), typeof process.cwd, typeof process.env.constructor, Object.getPrototypeOf(process) === null]",
  )
  .await;
  assert_eq!(
    process_surface["result"],
    serde_json::json!([["env"], "undefined", "undefined", true])
  );

  let secret = e2b_execute(&tb, "envbox", "process.env.E2B_API_KEY").await;
  assert_eq!(secret["result_type"], serde_json::json!("undefined"));

  let sabotage = e2b_execute(
    &tb,
    "envbox",
    r#"
      process.env.FOO = 'mutated';
      const observed = process.env.FOO;
      let processRedefinition = 'allowed';
      let envRedefinition = 'allowed';
      try {
        Object.defineProperty(globalThis, 'process', {
          value: { env: { FOO: 'poisoned' } },
        });
      } catch {
        processRedefinition = 'blocked';
      }
      try {
        Object.defineProperty(process, 'env', {
          value: { FOO: 'poisoned' },
        });
      } catch {
        envRedefinition = 'blocked';
      }
      Object.freeze(process.env);
      Object.freeze(process);
      [
        observed,
        processRedefinition,
        envRedefinition,
        Object.isFrozen(process),
        Object.isFrozen(process.env),
      ]
    "#,
  )
  .await;
  assert_eq!(
    sabotage["result"],
    serde_json::json!(["mutated", "blocked", "blocked", true, true])
  );

  let after = e2b_execute(&tb, "envbox", "process.env.FOO").await;
  assert_eq!(after["result"], serde_json::json!("sandbox"));

  let mut malformed_later = tb
    .request(|b| {
      b.uri("/internal/execute")
        .method("POST")
        .header("x-sandbox-id", "envbox")
        .header("x-sandbox-env", "{not-json")
        .header("content-type", "application/json")
        .body(Body::from(
          r#"{"code":"process.env.FOO","language":"javascript"}"#,
        ))
        .context("can't make request")
    })
    .await
    .unwrap();
  assert_eq!(malformed_later.status().as_u16(), StatusCode::OK);
  let bytes = to_bytes(malformed_later.body_mut()).await.unwrap();
  let malformed_later: serde_json::Value =
    serde_json::from_slice(&bytes).unwrap();
  assert_eq!(malformed_later["result"], serde_json::json!("sandbox"));

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_e2b_executor_rejects_non_string_env_values() {
  let tb = TestBedBuilder::new("./test_cases/e2b-executor-harness")
    .with_per_worker_policy(None)
    .build()
    .await;

  for invalid_env in [
    serde_json::json!({ "INVALID": { "nested": "value" } }),
    serde_json::json!({ "INVALID": ["value"] }),
    serde_json::json!({ "INVALID": 1 }),
    serde_json::json!({ "INVALID": null }),
    serde_json::json!(["value"]),
    serde_json::json!(1),
    serde_json::Value::Null,
  ] {
    let body = serde_json::json!({
      "code": "globalThis.invalidEnvCodeRan = true",
      "language": "javascript",
      "env_vars": invalid_env,
    });
    let mut resp = tb
      .request(|b| {
        b.uri("/internal/execute")
          .method("POST")
          .header("x-sandbox-id", "invalid-request-env")
          .header("content-type", "application/json")
          .body(Body::from(body.to_string()))
          .context("can't make request")
      })
      .await
      .unwrap();
    assert_eq!(resp.status().as_u16(), StatusCode::BAD_REQUEST);

    let bytes = to_bytes(resp.body_mut()).await.unwrap();
    let error: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
      error,
      serde_json::json!({
        "result": null,
        "result_type": "undefined",
        "stdout": [],
        "stderr": [],
        "error": {
          "kind": "internal_error",
          "name": "TypeError",
          "message": "environment variables must be an object with string values",
        },
      })
    );
  }

  let probe = e2b_execute(
    &tb,
    "invalid-request-env",
    "typeof globalThis.invalidEnvCodeRan",
  )
  .await;
  assert_eq!(probe["result"], serde_json::json!("undefined"));

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_e2b_executor_treats_dangerous_env_keys_as_strings() {
  let tb = TestBedBuilder::new("./test_cases/e2b-executor-harness")
    .with_per_worker_policy(None)
    .build()
    .await;

  let mut resp = tb
    .request(|b| {
      b.uri("/internal/execute")
        .method("POST")
        .header("x-sandbox-id", "dangerous-env-keys")
        .header(
          "x-sandbox-env",
          r#"{"constructor":"sandbox","__proto__":"sandbox-proto"}"#,
        )
        .header("content-type", "application/json")
        .body(Body::from(
          r#"{
            "code":"[process.env.constructor,process.env.__proto__,typeof process.env.constructor,typeof process.env.__proto__,Object.getPrototypeOf(process.env)===null,({}).polluted===undefined]",
            "language":"javascript",
            "env_vars":{"constructor":"request","__proto__":"request-proto"}
          }"#,
        ))
        .context("can't make request")
    })
    .await
    .unwrap();
  assert_eq!(resp.status().as_u16(), StatusCode::OK);

  let bytes = to_bytes(resp.body_mut()).await.unwrap();
  let result: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
  assert_eq!(
    result["result"],
    serde_json::json!([
      "request",
      "request-proto",
      "string",
      "string",
      true,
      true,
    ])
  );

  let baseline = e2b_execute(
    &tb,
    "dangerous-env-keys",
    "[process.env.constructor, process.env.__proto__]",
  )
  .await;
  assert_eq!(
    baseline["result"],
    serde_json::json!(["sandbox", "sandbox-proto"])
  );

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_e2b_executor_retries_after_invalid_baseline_env() {
  let tb = TestBedBuilder::new("./test_cases/e2b-executor-harness")
    .with_per_worker_policy(None)
    .build()
    .await;

  let mut failed = tb
    .request(|b| {
      b.uri("/internal/execute")
        .method("POST")
        .header("x-sandbox-id", "init-retry")
        .header("x-sandbox-env", "{not-json")
        .header("content-type", "application/json")
        .body(Body::from(
          r#"{"code":"globalThis.invalidInitCodeRan=true","language":"javascript"}"#,
        ))
        .context("can't make request")
    })
    .await
    .unwrap();
  assert_eq!(failed.status().as_u16(), StatusCode::BAD_REQUEST);
  let _ = to_bytes(failed.body_mut()).await.unwrap();

  let mut retried = tb
    .request(|b| {
      b.uri("/internal/execute")
        .method("POST")
        .header("x-sandbox-id", "init-retry")
        .header("x-sandbox-env", r#"{"FOO":"valid"}"#)
        .header("content-type", "application/json")
        .body(Body::from(
          r#"{"code":"[process.env.FOO,typeof globalThis.invalidInitCodeRan]","language":"javascript"}"#,
        ))
        .context("can't make request")
    })
    .await
    .unwrap();
  assert_eq!(retried.status().as_u16(), StatusCode::OK);
  let bytes = to_bytes(retried.body_mut()).await.unwrap();
  let result: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
  assert_eq!(result["result"], serde_json::json!(["valid", "undefined"]));

  let mut count = tb
    .request(|b| {
      b.uri("/internal/harness/worker-creation-count")
        .header("x-sandbox-id", "init-retry")
        .body(Body::empty())
        .context("can't make request")
    })
    .await
    .unwrap();
  let bytes = to_bytes(count.body_mut()).await.unwrap();
  let count: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
  assert_eq!(count["count"], serde_json::json!(1));

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_e2b_executor_captures_output_and_serializes_exotics() {
  let tb = TestBedBuilder::new("./test_cases/e2b-executor-harness")
    .with_per_worker_policy(None)
    .build()
    .await;

  let logged = e2b_execute(
    &tb,
    "s1",
    "console.log('hello'); console.error('bad'); 1 + 2",
  )
  .await;
  assert_eq!(logged["result"], serde_json::json!(3));
  assert_eq!(logged["stdout"], serde_json::json!(["hello"]));
  assert_eq!(logged["stderr"], serde_json::json!(["bad"]));

  let big = e2b_execute(&tb, "s1", "123n").await;
  assert_eq!(big["result"], serde_json::Value::Null);
  assert_eq!(big["result_type"], serde_json::json!("bigint"));
  assert_eq!(big["result_repr"], serde_json::json!("123n"));

  // A cycle must not fail the response.
  let cyclic = e2b_execute(&tb, "s1", "const c = {}; c.self = c; c").await;
  assert_eq!(cyclic["error"], serde_json::Value::Null);

  // Cross-realm Map detection: `instanceof` would be false here.
  let map = e2b_execute(&tb, "s1", "new Map([['k', 1]])").await;
  assert_eq!(map["result_type"], serde_json::json!("map"));

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_e2b_executor_bounds_console_output() {
  let tb = TestBedBuilder::new("./test_cases/e2b-executor-harness")
    .with_per_worker_policy(None)
    .build()
    .await;

  let capped = e2b_execute(
    &tb,
    "output-cap",
    "console.log('a'.repeat(65530)); console.error('overflow'); 'done'",
  )
  .await;
  let output_bytes: usize = ["stdout", "stderr"]
    .iter()
    .flat_map(|stream| capped[*stream].as_array().unwrap())
    .map(|line| line.as_str().unwrap().as_bytes().len())
    .sum();
  assert!(output_bytes <= 65536, "captured {output_bytes} bytes");
  assert!(["stdout", "stderr"]
    .iter()
    .flat_map(|stream| capped[*stream].as_array().unwrap())
    .any(|line| line == "[output truncated]"));

  let unicode =
    e2b_execute(&tb, "output-cap", "console.log('é'.repeat(32760)); 'done'")
      .await;
  let unicode_bytes: usize = ["stdout", "stderr"]
    .iter()
    .flat_map(|stream| unicode[*stream].as_array().unwrap())
    .map(|line| line.as_str().unwrap().as_bytes().len())
    .sum();
  assert!(unicode_bytes <= 65536, "captured {unicode_bytes} bytes");

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_e2b_executor_bounds_console_value_rendering() {
  let tb = TestBedBuilder::new("./test_cases/e2b-executor-harness")
    .with_per_worker_policy(None)
    .build()
    .await;

  let many_keys = e2b_execute(
    &tb,
    "bounded-values",
    r#"
      console.log('a'.repeat(65500));
      const value = {};
      for (let i = 0; i < 10000; i++) value[`k${i}`] = i;
      console.log(value);
      10
    "#,
  )
  .await;
  let output_bytes: usize = many_keys["stdout"]
    .as_array()
    .unwrap()
    .iter()
    .map(|line| line.as_str().unwrap().as_bytes().len())
    .sum();
  assert!(output_bytes <= 65536, "captured {output_bytes} bytes");
  assert_eq!(many_keys["result"], serde_json::json!(10));
  assert!(many_keys["stdout"]
    .as_array()
    .unwrap()
    .iter()
    .any(|line| line == "[output truncated]"));

  let huge_bigint =
    e2b_execute(&tb, "bounded-values", "console.log(1n << 1_000_000n); 11")
      .await;
  assert_eq!(huge_bigint["result"], serde_json::json!(11));
  assert_eq!(huge_bigint["stdout"], serde_json::json!(["[bigint]"]));

  let hostile_proxy = e2b_execute(
    &tb,
    "bounded-values",
    r#"
      console.log(new Proxy({}, {
        ownKeys() { throw new Error('ownKeys'); },
      }));
      12
    "#,
  )
  .await;
  assert_eq!(hostile_proxy["result"], serde_json::json!(12));
  assert_eq!(hostile_proxy["stdout"], serde_json::json!(["[object]"]));
  assert_eq!(hostile_proxy["error"], serde_json::Value::Null);

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_e2b_executor_console_is_total_and_immutable() {
  let tb = TestBedBuilder::new("./test_cases/e2b-executor-harness")
    .with_per_worker_policy(None)
    .build()
    .await;

  let hostile = e2b_execute(
    &tb,
    "console-hardening",
    r#"
      const hostile = {
        toJSON() { throw new Error('toJSON'); },
        toString() { throw new Error('toString'); },
      };
      console.log(hostile);
      7
    "#,
  )
  .await;
  assert_eq!(hostile["result"], serde_json::json!(7));
  assert_eq!(hostile["stdout"], serde_json::json!(["[object]"]));

  let benign = e2b_execute(
    &tb,
    "console-hardening",
    "console.log(123n, { a: 1 }, [2]); 8",
  )
  .await;
  assert_eq!(benign["result"], serde_json::json!(8));
  assert_eq!(benign["stdout"], serde_json::json!([r#"123n {"a":1} [2]"#]));

  let attempted = e2b_execute(
    &tb,
    "console-hardening",
    r#"
      Object.defineProperty(globalThis, 'console', {
        configurable: true,
        set() { throw new Error('poisoned console'); },
      })
    "#,
  )
  .await;
  let after =
    e2b_execute(&tb, "console-hardening", "console.log('still captured'); 9")
      .await;
  assert_eq!(
    attempted["error"]["kind"],
    serde_json::json!("runtime_error")
  );
  assert_eq!(after["result"], serde_json::json!(9));
  assert_eq!(after["stdout"], serde_json::json!(["still captured"]));

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_e2b_executor_error_inspection_runs_no_user_code() {
  let tb = TestBedBuilder::new("./test_cases/e2b-executor-harness")
    .with_per_worker_policy(None)
    .build()
    .await;

  e2b_execute(&tb, "errinspect", "globalThis.errorGetterRuns = 0").await;

  // A getter on `name` must never be invoked while classifying the error. The
  // counter is the observable proof: an infinite loop in this getter could not
  // be interrupted, since it would run in host code outside any vm timeout.
  let hostile_getter = e2b_execute(
    &tb,
    "errinspect",
    r#"
      const hostile = { message: 'readable' };
      Object.defineProperty(hostile, 'name', {
        get() {
          globalThis.errorGetterRuns++;
          return 'Attacker';
        },
      });
      throw hostile
    "#,
  )
  .await;
  assert_eq!(
    hostile_getter["error"]["kind"],
    serde_json::json!("runtime_error")
  );
  assert_eq!(hostile_getter["error"]["name"], serde_json::json!("Error"));
  assert_eq!(
    hostile_getter["error"]["message"],
    serde_json::json!("readable")
  );

  // A proxy's getOwnPropertyDescriptor trap is also user code.
  let hostile_proxy = e2b_execute(
    &tb,
    "errinspect",
    r#"
      throw new Proxy({}, {
        get() { globalThis.errorGetterRuns++; return 'trap'; },
        getOwnPropertyDescriptor() {
          globalThis.errorGetterRuns++;
          throw new Error('trap');
        },
      })
    "#,
  )
  .await;
  assert_eq!(
    hostile_proxy["error"]["kind"],
    serde_json::json!("runtime_error")
  );
  assert_eq!(hostile_proxy["error"]["name"], serde_json::json!("Error"));

  let getter_runs =
    e2b_execute(&tb, "errinspect", "globalThis.errorGetterRuns").await;
  assert_eq!(getter_runs["result"], serde_json::json!(0));

  // Ordinary errors still report their real name and message.
  let ordinary =
    e2b_execute(&tb, "errinspect", "throw new TypeError('boom')").await;
  assert_eq!(
    ordinary["error"]["kind"],
    serde_json::json!("runtime_error")
  );
  assert_eq!(ordinary["error"]["name"], serde_json::json!("TypeError"));
  assert_eq!(ordinary["error"]["message"], serde_json::json!("boom"));

  // The sandbox survives every hostile throw above.
  let alive = e2b_execute(&tb, "errinspect", "1 + 1").await;
  assert_eq!(alive["result"], serde_json::json!(2));

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_e2b_executor_survives_nested_ops_from_user_code() {
  let tb = TestBedBuilder::new("./test_cases/e2b-executor-harness")
    .with_per_worker_policy(None)
    .build()
    .await;

  e2b_execute(&tb, "nested", "let alive = 5").await;

  // `new URL(...)` reaches deno_url's op_url_parse, which needs a mutable
  // OpState borrow. While the vm run op held that borrow across user code, this
  // aborted the entire runtime process rather than returning a value.
  let parsed =
    e2b_execute(&tb, "nested", "new URL('https://example.com/path').host")
      .await;
  assert_eq!(parsed["error"], serde_json::Value::Null);
  assert_eq!(parsed["result"], serde_json::json!("example.com"));

  // The sandbox boundary shadows the host `fetch` global, so calling it fails
  // as an ordinary user-code error rather than reaching network access.
  let fetched =
    e2b_execute(&tb, "nested", "fetch('https://example.com')").await;
  assert_eq!(fetched["error"]["kind"], serde_json::json!("runtime_error"));

  // The worker and its state survived both.
  assert_eq!(
    e2b_execute(&tb, "nested", "alive").await["result"],
    serde_json::json!(5)
  );

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_e2b_executor_serialization_budget_is_shared() {
  let tb = TestBedBuilder::new("./test_cases/e2b-executor-harness")
    .with_per_worker_policy(None)
    .build()
    .await;

  // 4 levels of 12 shared references stays inside SERIALIZE_MAX_DEPTH (8) and
  // well under the per-level entry cap (1000), so neither of those can truncate
  // it. Total nodes reached is ~22.6k, so the shared node budget is the only
  // mechanism that can produce a truncated marker here.
  let shared_dag = e2b_execute(
    &tb,
    "budget",
    r#"
      let level = { leaf: true };
      for (let depth = 0; depth < 4; depth++) {
        const next = {};
        for (let index = 0; index < 12; index++) next['k' + index] = level;
        level = next;
      }
      level
    "#,
  )
  .await;
  assert_eq!(shared_dag["error"], serde_json::Value::Null);
  assert_eq!(shared_dag["result_type"], serde_json::json!("object"));
  // Report only the length on failure: an unbounded traversal serializes a tree
  // far too large to put in CI logs.
  let serialized = shared_dag["result"].to_string();
  assert!(
    serialized.contains("[truncated]"),
    "shared budget should truncate a combinatorial DAG; serialized {} chars \
     with no marker",
    serialized.len()
  );

  // A single fully populated level must still serialize completely, so the
  // shared budget does not weaken the documented per-level guarantee.
  let flat = e2b_execute(
    &tb,
    "budget",
    "Array.from({ length: 1000 }, (_unused, index) => index)",
  )
  .await;
  assert_eq!(flat["result_type"], serde_json::json!("array"));
  let flat_items = flat["result"].as_array().expect("array result");
  assert_eq!(flat_items.len(), 1000);
  assert_eq!(flat_items[999], serde_json::json!(999));

  let alive = e2b_execute(&tb, "budget", "1 + 1").await;
  assert_eq!(alive["result"], serde_json::json!(2));

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_e2b_executor_serialization_is_total_and_bounded() {
  let tb = TestBedBuilder::new("./test_cases/e2b-executor-harness")
    .with_per_worker_policy(None)
    .build()
    .await;

  for code in [
    r#"
      const hostile = {};
      Object.defineProperty(hostile, Symbol.toStringTag, {
        get() { throw new Error('brand'); },
      });
      hostile
    "#,
    r#"
      globalThis.serializationGetterRuns = 0;
      const hostileGetter = {};
      Object.defineProperty(hostileGetter, 'value', {
        enumerable: true,
        get() {
          globalThis.serializationGetterRuns++;
          throw new Error('property');
        },
      });
      hostileGetter
    "#,
  ] {
    let hostile = e2b_execute(&tb, "serialization", code).await;
    assert_eq!(hostile["result"], serde_json::Value::Null);
    assert_eq!(hostile["result_type"], serde_json::json!("unserializable"));
    assert_eq!(
      hostile["result_repr"],
      serde_json::json!("[unserializable]")
    );
    assert_eq!(hostile["error"], serde_json::Value::Null);
  }

  let getter_runs =
    e2b_execute(&tb, "serialization", "globalThis.serializationGetterRuns")
      .await;
  assert_eq!(getter_runs["result"], serde_json::json!(0));

  let hostile_proxy = e2b_execute(
    &tb,
    "serialization",
    r#"
      globalThis.serializationProxyTrapRuns = 0;
      new Proxy({}, {
        get(_target, property) {
          if (property === 'then') return undefined;
          globalThis.serializationProxyTrapRuns++;
          throw new Error('get trap');
        },
        ownKeys() {
          globalThis.serializationProxyTrapRuns++;
          throw new Error('ownKeys trap');
        },
        getOwnPropertyDescriptor() {
          globalThis.serializationProxyTrapRuns++;
          throw new Error('descriptor trap');
        },
        getPrototypeOf() {
          globalThis.serializationProxyTrapRuns++;
          throw new Error('prototype trap');
        },
      })
    "#,
  )
  .await;
  assert_eq!(hostile_proxy["result"], serde_json::Value::Null);
  assert_eq!(
    hostile_proxy["result_type"],
    serde_json::json!("unserializable")
  );
  assert_eq!(hostile_proxy["error"], serde_json::Value::Null);
  let proxy_trap_runs = e2b_execute(
    &tb,
    "serialization",
    "globalThis.serializationProxyTrapRuns",
  )
  .await;
  assert_eq!(proxy_trap_runs["result"], serde_json::json!(0));

  let poisoned_array_methods = e2b_execute(
    &tb,
    "array-intrinsics",
    r#"
      Array.prototype.slice = () => [1n];
      Array.prototype.map = () => 1n;
      [1, { nested: 2 }]
    "#,
  )
  .await;
  assert_eq!(
    poisoned_array_methods["result"],
    serde_json::json!([1, { "nested": 2 }])
  );
  assert_eq!(
    poisoned_array_methods["result_type"],
    serde_json::json!("array")
  );
  assert_eq!(poisoned_array_methods["error"], serde_json::Value::Null);

  let hostile_array_proxy = e2b_execute(
    &tb,
    "array-proxy",
    r#"
      new Proxy([], {
        getOwnPropertyDescriptor() {
          throw new Error('descriptor');
        },
      })
    "#,
  )
  .await;
  assert_eq!(hostile_array_proxy["result"], serde_json::Value::Null);
  assert_eq!(
    hostile_array_proxy["result_type"],
    serde_json::json!("unserializable")
  );
  assert_eq!(hostile_array_proxy["error"], serde_json::Value::Null);

  for (code, expected_type, expected_repr) in [
    (
      "function named() {}; named",
      "function",
      "[Function: named]",
    ),
    ("Symbol('value')", "symbol", "Symbol(value)"),
    ("new Error('boom')", "error", "Error: boom"),
    ("new TypeError('typed')", "error", "TypeError: typed"),
    ("new Date(0)", "date", "1970-01-01T00:00:00.000Z"),
    ("new Map([['key', 1]])", "map", "[map size=1]"),
    ("new Set([1, 2])", "set", "[set size=2]"),
  ] {
    let serialized = e2b_execute(&tb, "serialization", code).await;
    assert_eq!(serialized["result_type"], serde_json::json!(expected_type));
    assert_eq!(serialized["result_repr"], serde_json::json!(expected_repr));
  }

  let huge_bigint = e2b_execute(&tb, "serialization", "1n << 1_000_000n").await;
  assert_eq!(huge_bigint["result"], serde_json::Value::Null);
  assert_eq!(huge_bigint["result_type"], serde_json::json!("bigint"));
  assert_eq!(huge_bigint["result_repr"], serde_json::json!("[bigint]"));

  let deep = e2b_execute(
    &tb,
    "serialization",
    r#"
      const root = {};
      let cursor = root;
      for (let i = 0; i < 8; i++) {
        cursor.next = {};
        cursor = cursor.next;
      }
      root
    "#,
  )
  .await;
  let deep_path = "/next".repeat(8);
  assert_eq!(
    deep["result"].pointer(&deep_path),
    Some(&serde_json::json!({
      "value": null,
      "type": "truncated",
      "repr": "[truncated]",
    }))
  );

  let broad = e2b_execute(
    &tb,
    "serialization",
    "Object.fromEntries(Array.from({ length: 100_000 }, (_, i) => [`k${i}`, i]))",
  )
  .await;
  assert_eq!(
    broad["result"]["[truncated]"],
    serde_json::json!({
      "value": null,
      "type": "truncated",
      "repr": "[truncated]",
    })
  );

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_e2b_executor_timeouts_keep_sandbox_usable() {
  let tb = TestBedBuilder::new("./test_cases/e2b-executor-harness")
    .with_per_worker_policy(None)
    .build()
    .await;

  e2b_execute(&tb, "timeouts", "let keep = 7").await;
  // A bystander sandbox: a spin loop must not cost anyone else their state,
  // even though on one worker thread it does stall them until the vm timeout.
  e2b_execute(&tb, "bystander", "let mine = 'intact'").await;

  let async_ok = e2b_execute(&tb, "timeouts", "Promise.resolve(42)").await;
  assert_eq!(async_ok["result"], serde_json::json!(42));

  let awaited_refs = e2b_execute_with_env(
    &tb,
    "timeouts",
    r#"
      new Promise((resolve) => setTimeout(() => {
        console.log(process.env.AWAITED_VALUE);
        resolve(process.env.AWAITED_VALUE);
      }, 10))
    "#,
    serde_json::json!({ "AWAITED_VALUE": "still-active" }),
  )
  .await;
  assert_eq!(awaited_refs["result"], serde_json::json!("still-active"));
  assert_eq!(awaited_refs["stdout"], serde_json::json!(["still-active"]));

  let spin = e2b_execute(&tb, "timeouts", "while (true) {}").await;
  assert_eq!(
    spin["error"]["kind"],
    serde_json::json!("execution_timeout")
  );

  // Recovery, not isolation-in-time: the aborted execution must leave another
  // sandbox able to answer, with its own state.
  let bystander = e2b_execute(&tb, "bystander", "mine").await;
  assert_eq!(bystander["result"], serde_json::json!("intact"));

  let spoofed_timeout = e2b_execute(
    &tb,
    "timeouts",
    "throw new Error('Script execution timed out')",
  )
  .await;
  assert_eq!(
    spoofed_timeout["error"]["kind"],
    serde_json::json!("runtime_error")
  );

  let runtime_syntax = e2b_execute(
    &tb,
    "timeouts",
    "throw new SyntaxError('runtime syntax error')",
  )
  .await;
  assert_eq!(
    runtime_syntax["error"]["kind"],
    serde_json::json!("runtime_error")
  );

  let compile_syntax = e2b_execute(&tb, "timeouts", "let =").await;
  assert_eq!(
    compile_syntax["error"]["kind"],
    serde_json::json!("compile_error")
  );

  let hang = e2b_execute(&tb, "timeouts", "new Promise(() => {})").await;
  assert_eq!(
    hang["error"]["kind"],
    serde_json::json!("execution_timeout")
  );

  let rejected =
    e2b_execute(&tb, "timeouts", "Promise.reject(new Error('promise boom'))")
      .await;
  assert_eq!(
    rejected["error"]["kind"],
    serde_json::json!("runtime_error")
  );
  assert_eq!(
    rejected["error"]["message"],
    serde_json::json!("promise boom")
  );

  for (code, message) in [
    (
      "({ get then() { throw new Error('then getter'); } })",
      "then getter",
    ),
    (
      "({ then() { throw new Error('then call'); } })",
      "then call",
    ),
  ] {
    let hostile = e2b_execute(&tb, "timeouts", code).await;
    assert_eq!(hostile["error"]["kind"], serde_json::json!("runtime_error"));
    assert_eq!(hostile["error"]["message"], serde_json::json!(message));
  }

  let stray =
    e2b_execute(&tb, "timeouts", "Promise.reject(new Error('stray')); 43")
      .await;
  assert_eq!(stray["result"], serde_json::json!(43));

  let survived = e2b_execute(&tb, "timeouts", "keep").await;
  assert_eq!(survived["result"], serde_json::json!(7));

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_e2b_executor_timeout_cancels_only_its_execution_timers() {
  let tb = TestBedBuilder::new("./test_cases/e2b-executor-harness")
    .with_per_worker_policy(None)
    .build()
    .await;

  e2b_execute(
    &tb,
    "timer-ownership",
    "let earlier = 'pending'; let syncAbandoned = 0; let asyncAbandoned = 0",
  )
  .await;
  let scheduled = e2b_execute(
    &tb,
    "timer-ownership",
    "setTimeout(() => earlier = 'fired', 350); 'scheduled'",
  )
  .await;
  assert_eq!(scheduled["result"], serde_json::json!("scheduled"));

  let spin = e2b_execute(
    &tb,
    "timer-ownership",
    "setTimeout(() => syncAbandoned = 1, 150); while (true) {}",
  )
  .await;
  assert_eq!(
    spin["error"]["kind"],
    serde_json::json!("execution_timeout")
  );

  let hang = e2b_execute(
    &tb,
    "timer-ownership",
    r#"
      new Promise((resolve) => setTimeout(() => {
        asyncAbandoned = 1;
        resolve();
      }, 300))
    "#,
  )
  .await;
  assert_eq!(
    hang["error"]["kind"],
    serde_json::json!("execution_timeout")
  );

  let state = e2b_execute(
    &tb,
    "timer-ownership",
    r#"
      new Promise((resolve) => setTimeout(() => resolve({
        earlier,
        syncAbandoned,
        asyncAbandoned,
      }), 150))
    "#,
  )
  .await;
  assert_eq!(
    state["result"],
    serde_json::json!({
      "earlier": "fired",
      "syncAbandoned": 0,
      "asyncAbandoned": 0,
    })
  );

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_e2b_executor_timer_facade_survives_sabotage() {
  let tb = TestBedBuilder::new("./test_cases/e2b-executor-harness")
    .with_per_worker_policy(None)
    .build()
    .await;

  let descriptor = e2b_execute(
    &tb,
    "timer-sabotage",
    r#"
      [
        'setTimeout',
        'clearTimeout',
        'setInterval',
        'clearInterval',
      ].map((name) => {
        const descriptor = Object.getOwnPropertyDescriptor(globalThis, name);
        return [
          Object.hasOwn(globalThis, name),
          descriptor?.writable,
          descriptor?.configurable,
        ];
      })
    "#,
  )
  .await;
  assert_eq!(
    descriptor["result"],
    serde_json::json!([
      [true, false, false],
      [true, false, false],
      [true, false, false],
      [true, false, false],
    ])
  );

  let sabotage = e2b_execute(
    &tb,
    "timer-sabotage",
    r#"
      try { setTimeout = () => { throw new Error('poisoned'); }; } catch (_) {}
      try { clearTimeout = () => { throw new Error('poisoned'); }; } catch (_) {}
      try { setInterval = () => { throw new Error('poisoned'); }; } catch (_) {}
      try { clearInterval = () => { throw new Error('poisoned'); }; } catch (_) {}
      try {
        Object.defineProperty(globalThis, 'setTimeout', {
          value: () => { throw new Error('poisoned'); },
          configurable: false,
        });
      } catch (_) {}
      try { Object.setPrototypeOf(globalThis, null); } catch (_) {}
      'attempted'
    "#,
  )
  .await;
  assert_eq!(sabotage["result"], serde_json::json!("attempted"));

  let after = e2b_execute(
    &tb,
    "timer-sabotage",
    r#"
      new Promise((resolve) => {
        const cancelled = setTimeout(() => resolve('clearTimeout failed'), 50);
        clearTimeout(cancelled);
        const interval = setInterval(() => {
          clearInterval(interval);
          setTimeout(() => resolve('timers-ok'), 10);
        }, 10);
      })
    "#,
  )
  .await;
  assert_eq!(
    after["result"],
    serde_json::json!("timers-ok"),
    "response: {after}",
  );

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_e2b_executor_timer_callback_keeps_owner_state() {
  let tb = TestBedBuilder::new("./test_cases/e2b-executor-harness")
    .with_per_worker_policy(None)
    .build()
    .await;

  let scheduled = e2b_execute_with_env(
    &tb,
    "timer-callback-owner",
    r#"
      globalThis.timerEnv = 'pending';
      setTimeout(() => {
        globalThis.timerEnv = process.env.REQUEST_NAME;
        console.log(`old:${process.env.REQUEST_NAME}`);
      }, 40);
      'scheduled'
    "#,
    serde_json::json!({ "REQUEST_NAME": "origin" }),
  )
  .await;
  assert_eq!(scheduled["result"], serde_json::json!("scheduled"));
  assert_eq!(scheduled["stdout"], serde_json::json!([]));

  let later = e2b_execute_with_env(
    &tb,
    "timer-callback-owner",
    r#"
      new Promise((resolve) => setTimeout(() => {
        console.log(`new:${process.env.REQUEST_NAME}`);
        resolve([process.env.REQUEST_NAME, globalThis.timerEnv]);
      }, 80))
    "#,
    serde_json::json!({ "REQUEST_NAME": "newer" }),
  )
  .await;
  assert_eq!(later["result"], serde_json::json!(["newer", "origin"]));
  assert_eq!(later["stdout"], serde_json::json!(["new:newer"]));

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_e2b_executor_contains_timer_callback_errors() {
  let tb = TestBedBuilder::new("./test_cases/e2b-executor-harness")
    .with_per_worker_policy(None)
    .build()
    .await;

  e2b_execute(
    &tb,
    "timer-errors",
    "let timerErrorState = 0; let lateTimerState = 0",
  )
  .await;

  let awaited = e2b_execute(
    &tb,
    "timer-errors",
    r#"
      new Promise(() => setTimeout(() => {
        timerErrorState = 1;
        throw new Error('awaited timer boom');
      }, 10))
    "#,
  )
  .await;
  assert_eq!(awaited["error"]["kind"], serde_json::json!("runtime_error"));
  assert_eq!(
    awaited["error"]["message"],
    serde_json::json!("awaited timer boom")
  );
  let after_awaited = e2b_execute(&tb, "timer-errors", "timerErrorState").await;
  assert_eq!(after_awaited["result"], serde_json::json!(1));

  let scheduled = e2b_execute(
    &tb,
    "timer-errors",
    r#"
      setTimeout(() => {
        lateTimerState = 2;
        throw new Error('late timer boom');
      }, 10);
      'scheduled'
    "#,
  )
  .await;
  assert_eq!(scheduled["result"], serde_json::json!("scheduled"));

  let after_late = e2b_execute(
    &tb,
    "timer-errors",
    r#"
      new Promise((resolve) => setTimeout(() => {
        resolve([timerErrorState, lateTimerState]);
      }, 40))
    "#,
  )
  .await;
  assert_eq!(after_late["result"], serde_json::json!([1, 2]));
  assert_eq!(after_late["error"], serde_json::Value::Null);

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_e2b_executor_cancels_orphaned_interval_after_success() {
  let tb = TestBedBuilder::new("./test_cases/e2b-executor-harness")
    .with_per_worker_policy(None)
    .build()
    .await;

  // A successful execution starts an interval and returns. The interval's
  // callback closure would otherwise retain this execution's capture buffer and
  // env forever, ticking across every future execution — unbounded growth.
  let started = e2b_execute(
    &tb,
    "orphan-interval",
    "globalThis.ticks = 0; setInterval(() => globalThis.ticks++, 10); 'started'",
  )
  .await;
  assert_eq!(started["result"], serde_json::json!("started"));

  // Read the counter, wait past several interval periods, then read again. If
  // the interval were still live it would keep incrementing between the reads.
  let first = e2b_execute(&tb, "orphan-interval", "globalThis.ticks").await;
  let second = e2b_execute(
    &tb,
    "orphan-interval",
    "new Promise((resolve) => setTimeout(() => resolve(globalThis.ticks), 80))",
  )
  .await;
  assert_eq!(
    first["result"], second["result"],
    "orphaned interval kept ticking after its execution settled: \
     first={first}, second={second}",
  );

  // A one-shot timeout scheduled by a successful execution must still fire.
  let scheduled = e2b_execute(
    &tb,
    "orphan-interval",
    "globalThis.late = 'pending'; setTimeout(() => globalThis.late = 'fired', 20); 'ok'",
  )
  .await;
  assert_eq!(scheduled["result"], serde_json::json!("ok"));
  let observed = e2b_execute(
    &tb,
    "orphan-interval",
    "new Promise((resolve) => setTimeout(() => resolve(globalThis.late), 60))",
  )
  .await;
  assert_eq!(observed["result"], serde_json::json!("fired"));

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_e2b_executor_keeps_async_request_state_scoped() {
  let tb = TestBedBuilder::new("./test_cases/e2b-executor-harness")
    .with_per_worker_policy(None)
    .build()
    .await;

  let (first, second) = tokio::join!(
    e2b_execute_with_env(
      &tb,
      "async-scope",
      r#"
        new Promise((resolve) => setTimeout(() => {
          console.log(process.env.REQUEST_NAME);
          resolve(process.env.REQUEST_NAME);
        }, 40))
      "#,
      serde_json::json!({ "REQUEST_NAME": "first" }),
    ),
    e2b_execute_with_env(
      &tb,
      "async-scope",
      r#"
        new Promise((resolve) => setTimeout(() => {
          console.log(process.env.REQUEST_NAME);
          resolve(process.env.REQUEST_NAME);
        }, 10))
      "#,
      serde_json::json!({ "REQUEST_NAME": "second" }),
    ),
  );

  assert_eq!(first["result"], serde_json::json!("first"));
  assert_eq!(first["stdout"], serde_json::json!(["first"]));
  assert_eq!(second["result"], serde_json::json!("second"));
  assert_eq!(second["stdout"], serde_json::json!(["second"]));

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_e2b_executor_serializes_results_before_releasing_queue() {
  let tb = TestBedBuilder::new("./test_cases/e2b-executor-harness")
    .with_per_worker_policy(None)
    .build()
    .await;

  let (first, second) = tokio::join!(
    e2b_execute(
      &tb,
      "serialized-result",
      r#"
        globalThis.sharedResult = { value: 1 };
        new Promise((resolve) => setTimeout(() => resolve(sharedResult), 40))
      "#,
    ),
    async {
      tokio::time::sleep(Duration::from_millis(10)).await;
      e2b_execute(
        &tb,
        "serialized-result",
        r#"
          sharedResult.value = 2;
          new Promise((resolve) => setTimeout(() => resolve(sharedResult.value), 20))
        "#,
      )
      .await
    },
  );

  assert_eq!(first["result"], serde_json::json!({ "value": 1 }));
  assert_eq!(second["result"], serde_json::json!(2));

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_e2b_executor_queue_recovers_from_rejected_execution() {
  let tb = TestBedBuilder::new("./test_cases/e2b-executor-harness")
    .with_per_worker_policy(None)
    .build()
    .await;

  e2b_execute(&tb, "queued-rejection", "let queuedState = 0").await;
  let (rejected, successor) = tokio::join!(
    e2b_execute(
      &tb,
      "queued-rejection",
      r#"
        new Promise((_, reject) => setTimeout(() => {
          reject(new Error('queued boom'));
        }, 40))
      "#,
    ),
    async {
      tokio::time::sleep(Duration::from_millis(10)).await;
      e2b_execute(&tb, "queued-rejection", "queuedState += 1; queuedState")
        .await
    },
  );

  assert_eq!(
    rejected["error"]["kind"],
    serde_json::json!("runtime_error")
  );
  assert_eq!(
    rejected["error"]["message"],
    serde_json::json!("queued boom")
  );
  assert_eq!(successor["result"], serde_json::json!(1));
  let survived = e2b_execute(&tb, "queued-rejection", "queuedState").await;
  assert_eq!(survived["result"], serde_json::json!(1));

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_e2b_executor_concurrent_first_requests_create_one_worker() {
  let tb = TestBedBuilder::new("./test_cases/e2b-executor-harness")
    .with_per_worker_policy(None)
    .build()
    .await;

  let (first, second) = tokio::join!(
    e2b_execute_with_baseline_env(
      &tb,
      "concurrent",
      "process.env.BASELINE",
      r#"{"BASELINE":"initialized"}"#,
    ),
    e2b_execute_with_baseline_env(
      &tb,
      "concurrent",
      "process.env.BASELINE",
      r#"{"BASELINE":"initialized"}"#,
    ),
  );
  assert_eq!(first["result"], serde_json::json!("initialized"));
  assert_eq!(second["result"], serde_json::json!("initialized"));
  assert_eq!(first["error"], serde_json::Value::Null);
  assert_eq!(second["error"], serde_json::Value::Null);

  let mut resp = tb
    .request(|b| {
      b.uri("/internal/harness/worker-creation-count")
        .header("x-sandbox-id", "concurrent")
        .body(Body::empty())
        .context("can't make request")
    })
    .await
    .unwrap();
  assert_eq!(resp.status().as_u16(), StatusCode::OK);

  let bytes = to_bytes(resp.body_mut()).await.unwrap();
  let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
  assert_eq!(body["count"], serde_json::json!(1));

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

async fn e2b_execute_raw(
  tb: &TestBed,
  sandbox: &str,
  raw_body: &str,
) -> (u16, serde_json::Value) {
  let owned = raw_body.to_string();
  let mut resp = tb
    .request(move |b| {
      b.uri("/internal/execute")
        .method("POST")
        .header("x-sandbox-id", sandbox)
        .header("content-type", "application/json")
        .body(Body::from(owned.clone()))
        .context("can't make request")
    })
    .await
    .unwrap();

  let status = resp.status().as_u16();
  let bytes = to_bytes(resp.body_mut()).await.unwrap();
  let json = if bytes.is_empty() {
    serde_json::Value::Null
  } else {
    serde_json::from_slice(&bytes).unwrap()
  };
  (status, json)
}

#[tokio::test]
#[serial]
async fn test_e2b_executor_rejects_malformed_body() {
  let tb = TestBedBuilder::new("./test_cases/e2b-executor-harness")
    .with_per_worker_policy(None)
    .build()
    .await;

  // Unparseable JSON: an unguarded req.json() would throw and 500. It must be a
  // structured 400 instead.
  let (status, body) = e2b_execute_raw(&tb, "malformed", "not json {").await;
  assert_eq!(status, 400, "got: {body}");
  assert_eq!(body["error"]["kind"], serde_json::json!("invalid_request"));

  // A literal JSON null parses but is not an object; destructuring it would
  // also 500 without the object guard.
  let (status, body) = e2b_execute_raw(&tb, "malformed", "null").await;
  assert_eq!(status, 400, "got: {body}");
  assert_eq!(body["error"]["kind"], serde_json::json!("invalid_request"));

  // The executor is still serving after the bad requests.
  let ok = e2b_execute(&tb, "malformed", "1 + 1").await;
  assert_eq!(ok["result"], serde_json::json!(2));

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_e2b_executor_serializes_proto_key_as_data() {
  let tb = TestBedBuilder::new("./test_cases/e2b-executor-harness")
    .with_per_worker_policy(None)
    .build()
    .await;

  // An own enumerable property literally named "__proto__" must round-trip as a
  // data property. A plain `out[key] = value` would retarget the output
  // object's prototype and drop the value.
  let result = e2b_execute(
    &tb,
    "proto",
    r#"
      const out = {};
      Object.defineProperty(out, "__proto__", {
        value: 42,
        enumerable: true,
        writable: true,
        configurable: true,
      });
      out
    "#,
  )
  .await;
  assert_eq!(
    result["result"]["__proto__"],
    serde_json::json!(42),
    "response: {result}",
  );

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_e2b_executor_falls_back_on_invalid_numeric_config() {
  let tb = TestBedBuilder::new("./test_cases/e2b-executor-bad-config")
    .with_per_worker_policy(None)
    .build()
    .await;

  // EXECUTOR_ASYNC_TIMEOUT_MS is non-numeric. A NaN async timeout would fire
  // immediately and time out this 50ms-resolved promise; the validated default
  // (12000ms) lets it resolve.
  let async_ok = e2b_execute(
    &tb,
    "bad-config",
    "new Promise((resolve) => setTimeout(() => resolve(99), 50))",
  )
  .await;
  assert_eq!(async_ok["result"], serde_json::json!(99), "got: {async_ok}");

  // SERIALIZE_MAX_ENTRIES is non-numeric. A NaN entry cap serializes arrays
  // empty (min(length, NaN) is NaN); the validated default serializes fully.
  let array = e2b_execute(&tb, "bad-config", "[1, 2, 3]").await;
  assert_eq!(
    array["result"],
    serde_json::json!([1, 2, 3]),
    "got: {array}"
  );

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

async fn e2b_adapter_post(
  tb: &TestBed,
  path: &str,
  body: serde_json::Value,
) -> (u16, serde_json::Value) {
  let mut resp = tb
    .request(|b| {
      b.uri(path)
        .method("POST")
        .header("content-type", "application/json")
        .header("x-api-key", "test-key")
        .body(Body::from(body.to_string()))
        .context("can't make request")
    })
    .await
    .unwrap();

  let status = resp.status().as_u16();
  let bytes = to_bytes(resp.body_mut()).await.unwrap();
  let json = if bytes.is_empty() {
    serde_json::Value::Null
  } else {
    serde_json::from_slice(&bytes).unwrap()
  };
  (status, json)
}

async fn e2b_adapter_jupyter_post(
  tb: &TestBed,
  sandbox: &str,
  token: Option<&str>,
  body: serde_json::Value,
) -> (u16, String) {
  let mut response = tb
    .request(|builder| {
      let builder = builder
        .uri(format!("/sandboxes/{sandbox}/jupyter/execute"))
        .method("POST")
        .header("content-type", "application/json");
      let builder = match token {
        Some(token) => builder.header("x-access-token", token),
        None => builder,
      };
      builder
        .body(Body::from(body.to_string()))
        .context("can't make request")
    })
    .await
    .unwrap();
  let status = response.status().as_u16();
  if status == 200 {
    assert_eq!(
      response.headers().get("content-type").unwrap(),
      "application/x-ndjson"
    );
  }
  let text =
    String::from_utf8(to_bytes(response.body_mut()).await.unwrap().to_vec())
      .unwrap();
  (status, text)
}

fn e2b_jupyter_events(body: &str) -> Vec<serde_json::Value> {
  body
    .lines()
    .map(|line| serde_json::from_str(line).unwrap())
    .collect()
}

async fn e2b_adapter_execute(
  tb: &TestBed,
  context_id: &str,
  code: &str,
) -> serde_json::Value {
  let (status, body) = e2b_adapter_post(
    tb,
    "/execute",
    serde_json::json!({
      "code": code,
      "context_id": context_id,
      "language": "javascript",
    }),
  )
  .await;
  assert_eq!(status, 200, "got: {body}");
  body
}

async fn e2b_adapter_get(tb: &TestBed, path: &str) -> (u16, serde_json::Value) {
  let mut resp = tb
    .request(|b| {
      b.uri(path)
        .method("GET")
        .header("x-api-key", "test-key")
        .body(Body::empty())
        .context("can't make request")
    })
    .await
    .unwrap();

  let status = resp.status().as_u16();
  let bytes = to_bytes(resp.body_mut()).await.unwrap();
  let json = if bytes.is_empty() {
    serde_json::Value::Null
  } else {
    serde_json::from_slice(&bytes).unwrap()
  };
  (status, json)
}

async fn e2b_adapter_delete(tb: &TestBed, path: &str) -> u16 {
  tb.request(|b| {
    b.uri(path)
      .method("DELETE")
      .header("x-api-key", "test-key")
      .body(Body::empty())
      .context("can't make request")
  })
  .await
  .unwrap()
  .status()
  .as_u16()
}

#[tokio::test]
#[serial]
async fn test_e2b_adapter_creates_code_interpreter_tokens_without_leaking_them()
{
  let tb = TestBedBuilder::new("./test_cases/e2b-adapter-main")
    .with_per_worker_policy(None)
    .build()
    .await;

  let (first_status, first) = e2b_adapter_post(
    &tb,
    "/sandboxes",
    serde_json::json!({ "templateID": "code-interpreter-v1" }),
  )
  .await;
  let (second_status, second) = e2b_adapter_post(
    &tb,
    "/sandboxes",
    serde_json::json!({ "templateID": "code-interpreter-v1" }),
  )
  .await;

  assert_eq!(first_status, 201, "got: {first}");
  assert_eq!(second_status, 201, "got: {second}");
  let first_token = first["envdAccessToken"].as_str().unwrap();
  let second_token = second["envdAccessToken"].as_str().unwrap();
  assert!(!first_token.is_empty());
  assert_ne!(first_token, second_token);

  let id = first["sandboxID"].as_str().unwrap();
  let (get_status, details) =
    e2b_adapter_get(&tb, &format!("/sandboxes/{id}")).await;
  assert_eq!(get_status, 200);
  assert!(details.get("envdAccessToken").is_none());

  let (list_status, sandboxes) = e2b_adapter_get(&tb, "/sandboxes").await;
  assert_eq!(list_status, 200);
  assert!(sandboxes
    .as_array()
    .unwrap()
    .iter()
    .all(|entry| entry.get("envdAccessToken").is_none()));

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_e2b_adapter_jupyter_preserves_state_and_emits_ndjson_events() {
  let tb = TestBedBuilder::new("./test_cases/e2b-adapter-main")
    .with_per_worker_policy(None)
    .build()
    .await;

  let (status, created) = e2b_adapter_post(
    &tb,
    "/sandboxes",
    serde_json::json!({
      "templateID": "code-interpreter-v1",
      "envVars": { "BASE": "base" },
    }),
  )
  .await;
  assert_eq!(status, 201, "got: {created}");
  let sandbox = created["sandboxID"].as_str().unwrap();
  let token = created["envdAccessToken"].as_str().unwrap();

  let (status, first) = e2b_adapter_jupyter_post(
    &tb,
    sandbox,
    Some(token),
    serde_json::json!({
      "code": "x = 1",
      "context_id": null,
      "language": null,
      "env_vars": null,
    }),
  )
  .await;
  assert_eq!(status, 200, "got: {first}");
  let first_events = e2b_jupyter_events(&first);
  assert_eq!(first_events.len(), 1, "got: {first}");
  assert_eq!(first_events[0]["type"], serde_json::json!("result"));
  assert_eq!(first_events[0]["text"], serde_json::json!("1"));

  let (status, declaration) = e2b_adapter_jupyter_post(
    &tb,
    sandbox,
    Some(token),
    serde_json::json!({ "code": "let y = 1" }),
  )
  .await;
  assert_eq!(status, 200, "got: {declaration}");
  assert_eq!(declaration, "");

  let (status, second) = e2b_adapter_jupyter_post(
    &tb,
    sandbox,
    Some(token),
    serde_json::json!({
      "code": "console.log('out'); console.error('err'); x += 1; x",
      "context_id": null,
      "language": null,
      "env_vars": null,
    }),
  )
  .await;
  assert_eq!(status, 200, "got: {second}");
  assert!(second.ends_with('\n'));
  let events = e2b_jupyter_events(&second);
  assert_eq!(events.len(), 3, "got: {second}");
  assert_eq!(events[0]["type"], serde_json::json!("stdout"));
  assert_eq!(events[0]["text"], serde_json::json!("out"));
  assert!(events[0]["timestamp"].is_u64());
  assert_eq!(events[1]["type"], serde_json::json!("stderr"));
  assert_eq!(events[1]["text"], serde_json::json!("err"));
  assert!(events[1]["timestamp"].is_u64());
  assert_eq!(events[2]["type"], serde_json::json!("result"));
  assert_eq!(events[2]["text"], serde_json::json!("2"));
  assert_eq!(events[2]["json"], serde_json::json!(2));
  assert_eq!(events[2]["is_main_result"], serde_json::json!(true));

  let (status, declaration) = e2b_adapter_jupyter_post(
    &tb,
    sandbox,
    Some(token),
    serde_json::json!({ "code": "let keep = 3" }),
  )
  .await;
  assert_eq!(status, 200, "got: {declaration}");
  assert_eq!(declaration, "");

  let (status, thrown) = e2b_adapter_jupyter_post(
    &tb,
    sandbox,
    Some(token),
    serde_json::json!({ "code": "throw new Error('boom')" }),
  )
  .await;
  assert_eq!(status, 200, "got: {thrown}");
  let thrown_events = e2b_jupyter_events(&thrown);
  assert_eq!(thrown_events.len(), 1, "got: {thrown}");
  assert_eq!(thrown_events[0]["type"], serde_json::json!("error"));
  assert_eq!(thrown_events[0]["name"], serde_json::json!("Error"));
  assert_eq!(thrown_events[0]["value"], serde_json::json!("boom"));
  assert_eq!(
    thrown_events[0]["traceback"],
    serde_json::json!("Error: boom")
  );

  let (status, state) = e2b_adapter_jupyter_post(
    &tb,
    sandbox,
    Some(token),
    serde_json::json!({ "code": "keep" }),
  )
  .await;
  assert_eq!(status, 200, "got: {state}");
  assert_eq!(
    e2b_jupyter_events(&state)[0]["text"],
    serde_json::json!("3")
  );

  let (status, invalid) = e2b_adapter_jupyter_post(
    &tb,
    sandbox,
    Some(token),
    serde_json::json!({ "code": "let broken =" }),
  )
  .await;
  assert_eq!(status, 200, "got: {invalid}");
  let invalid_events = e2b_jupyter_events(&invalid);
  assert_eq!(invalid_events.len(), 1, "got: {invalid}");
  assert_eq!(invalid_events[0]["type"], serde_json::json!("error"));

  let (status, timed_out) = e2b_adapter_jupyter_post(
    &tb,
    sandbox,
    Some(token),
    serde_json::json!({ "code": "while (true) {}" }),
  )
  .await;
  assert_eq!(status, 200, "got: {timed_out}");
  let timeout_events = e2b_jupyter_events(&timed_out);
  assert_eq!(timeout_events.len(), 1, "got: {timed_out}");
  assert_eq!(timeout_events[0]["type"], serde_json::json!("error"));
  assert!(
    timeout_events[0]["value"]
      .as_str()
      .unwrap()
      .to_ascii_lowercase()
      .contains("timed out"),
    "got: {timed_out}"
  );

  let (status, typed) = e2b_adapter_jupyter_post(
    &tb,
    sandbox,
    Some(token),
    serde_json::json!({
      "code": "const typed: number = 4; typed",
      "language": "typescript",
    }),
  )
  .await;
  assert_eq!(status, 200, "got: {typed}");
  assert_eq!(e2b_jupyter_events(&typed)[0]["json"], serde_json::json!(4));

  let (status, overridden) = e2b_adapter_jupyter_post(
    &tb,
    sandbox,
    Some(token),
    serde_json::json!({
      "code": "`${process.env.BASE}:${process.env.ONLY}`",
      "env_vars": { "BASE": "override", "ONLY": "once" },
    }),
  )
  .await;
  assert_eq!(status, 200, "got: {overridden}");
  assert_eq!(
    e2b_jupyter_events(&overridden)[0]["text"],
    serde_json::json!("override:once")
  );

  let (status, restored) = e2b_adapter_jupyter_post(
    &tb,
    sandbox,
    Some(token),
    serde_json::json!({
      "code": "`${process.env.BASE}:${typeof process.env.ONLY}`",
    }),
  )
  .await;
  assert_eq!(status, 200, "got: {restored}");
  assert_eq!(
    e2b_jupyter_events(&restored)[0]["text"],
    serde_json::json!("base:undefined")
  );

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_e2b_adapter_jupyter_authenticates_and_validates_before_execution()
{
  let tb = TestBedBuilder::new("./test_cases/e2b-adapter-main")
    .with_per_worker_policy(None)
    .build()
    .await;

  let (status, unknown) = e2b_adapter_jupyter_post(
    &tb,
    "never-existed",
    None,
    serde_json::json!({ "code": "1" }),
  )
  .await;
  assert_eq!(status, 404, "got: {unknown}");
  assert_eq!(
    serde_json::from_str::<serde_json::Value>(&unknown).unwrap()["error_code"],
    serde_json::json!("sandbox_not_found")
  );

  let (status, created) = e2b_adapter_post(
    &tb,
    "/sandboxes",
    serde_json::json!({ "templateID": "code-interpreter-v1" }),
  )
  .await;
  assert_eq!(status, 201, "got: {created}");
  let sandbox = created["sandboxID"].as_str().unwrap();
  let token = created["envdAccessToken"].as_str().unwrap();

  for candidate in [None, Some(""), Some("wrong"), Some("test-key")] {
    let (status, unauthorized) = e2b_adapter_jupyter_post(
      &tb,
      sandbox,
      candidate,
      serde_json::json!({ "code": "1" }),
    )
    .await;
    assert_eq!(status, 401, "token {candidate:?} got: {unauthorized}");
  }

  let (status, language) = e2b_adapter_jupyter_post(
    &tb,
    sandbox,
    Some(token),
    serde_json::json!({
      "code": "globalThis.ran = true",
      "language": "python",
    }),
  )
  .await;
  assert_eq!(status, 400, "got: {language}");

  let (status, marker) = e2b_adapter_jupyter_post(
    &tb,
    sandbox,
    Some(token),
    serde_json::json!({ "code": "typeof globalThis.ran" }),
  )
  .await;
  assert_eq!(status, 200, "got: {marker}");
  assert_eq!(
    e2b_jupyter_events(&marker)[0]["text"],
    serde_json::json!("undefined")
  );

  let (status, context) = e2b_adapter_jupyter_post(
    &tb,
    sandbox,
    Some(token),
    serde_json::json!({
      "code": "globalThis.ran = true",
      "context_id": "other",
    }),
  )
  .await;
  assert_eq!(status, 400, "got: {context}");

  let (status, marker) = e2b_adapter_jupyter_post(
    &tb,
    sandbox,
    Some(token),
    serde_json::json!({ "code": "typeof globalThis.ran" }),
  )
  .await;
  assert_eq!(status, 200, "got: {marker}");
  assert_eq!(
    e2b_jupyter_events(&marker)[0]["text"],
    serde_json::json!("undefined")
  );

  for body in [
    serde_json::json!({ "language": "javascript" }),
    serde_json::json!({ "code": "1", "env_vars": { "BAD": 1 } }),
    serde_json::json!({ "code": "x".repeat(300_000) }),
    serde_json::json!({
      "code": "1",
      "env_vars": { "PAD": "x".repeat(600_000) },
    }),
  ] {
    let (status, rejected) =
      e2b_adapter_jupyter_post(&tb, sandbox, Some(token), body).await;
    assert_eq!(status, 400, "got: {rejected}");
  }

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_e2b_adapter_jupyter_returns_404_after_kill_or_ttl() {
  let tb = TestBedBuilder::new("./test_cases/e2b-adapter-main")
    .with_per_worker_policy(None)
    .build()
    .await;

  let (status, killed) = e2b_adapter_post(
    &tb,
    "/sandboxes",
    serde_json::json!({ "templateID": "code-interpreter-v1" }),
  )
  .await;
  assert_eq!(status, 201, "got: {killed}");
  let killed_id = killed["sandboxID"].as_str().unwrap();
  let killed_token = killed["envdAccessToken"].as_str().unwrap();
  assert_eq!(
    e2b_adapter_delete(&tb, &format!("/sandboxes/{killed_id}")).await,
    204
  );
  let (status, body) = e2b_adapter_jupyter_post(
    &tb,
    killed_id,
    Some(killed_token),
    serde_json::json!({ "code": "1" }),
  )
  .await;
  assert_eq!(status, 404, "got: {body}");

  let (status, expiring) = e2b_adapter_post(
    &tb,
    "/sandboxes",
    serde_json::json!({
      "templateID": "code-interpreter-v1",
      "timeout": 0.2,
    }),
  )
  .await;
  assert_eq!(status, 201, "got: {expiring}");
  let expiring_id = expiring["sandboxID"].as_str().unwrap();
  let expiring_token = expiring["envdAccessToken"].as_str().unwrap();
  sleep(Duration::from_millis(350)).await;
  let (status, body) = e2b_adapter_jupyter_post(
    &tb,
    expiring_id,
    Some(expiring_token),
    serde_json::json!({ "code": "1" }),
  )
  .await;
  assert_eq!(status, 404, "got: {body}");

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_e2b_adapter_jupyter_reaps_an_unresponsive_executor() {
  let tb = TestBedBuilder::new("./test_cases/e2b-adapter-deadline")
    .with_per_worker_policy(None)
    .build()
    .await;

  let (status, created) = e2b_adapter_post(
    &tb,
    "/sandboxes",
    serde_json::json!({ "templateID": "code-interpreter-v1" }),
  )
  .await;
  assert_eq!(status, 201, "got: {created}");
  let sandbox = created["sandboxID"].as_str().unwrap();
  let token = created["envdAccessToken"].as_str().unwrap();

  let started = std::time::Instant::now();
  let (status, failed) = e2b_adapter_jupyter_post(
    &tb,
    sandbox,
    Some(token),
    serde_json::json!({
      "code": "function f() { Promise.resolve().then(f) } f(); 1",
    }),
  )
  .await;
  let elapsed = started.elapsed();
  assert_eq!(status, 500, "got: {failed}");
  assert_eq!(
    serde_json::from_str::<serde_json::Value>(&failed).unwrap()["error_code"],
    serde_json::json!("internal_server_error")
  );
  assert!(
    elapsed < Duration::from_secs(10),
    "the adapter deadline did not fire; took {elapsed:?}"
  );

  let (status, gone) = e2b_adapter_jupyter_post(
    &tb,
    sandbox,
    Some(token),
    serde_json::json!({ "code": "1" }),
  )
  .await;
  assert_eq!(status, 404, "got: {gone}");

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_e2b_adapter_rejects_oversized_bodies_before_buffering() {
  let tb = TestBedBuilder::new("./test_cases/e2b-adapter-main")
    .with_per_worker_policy(None)
    .build()
    .await;

  let (_status, created) = e2b_adapter_post(
    &tb,
    "/sandboxes",
    serde_json::json!({ "templateID": "base" }),
  )
  .await;
  let sandbox = created["sandboxID"].as_str().unwrap().to_string();

  // Over MAX_REQUEST_BYTES (512KB). The main worker has no memory limit, so
  // buffering this before checking would risk OOM-ing the control plane for
  // every tenant.
  let (status, body) = e2b_adapter_post(
    &tb,
    "/execute",
    serde_json::json!({
      "code": "1",
      "context_id": sandbox,
      "language": "javascript",
      "env_vars": { "PAD": "x".repeat(600_000) },
    }),
  )
  .await;
  assert_eq!(status, 400, "got: {body}");
  assert_eq!(body["error_code"], serde_json::json!("invalid_request"));

  // Ordinary requests still work, and the adapter is still serving.
  assert_eq!(
    e2b_adapter_execute(&tb, &sandbox, "1 + 1").await["result"],
    serde_json::json!(2)
  );

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_e2b_adapter_never_shares_an_isolate() {
  // Under the oneshot policy the pool ignores forceCreate and may hand back an
  // existing worker, which would put two sandboxes in one isolate: state would
  // leak across tenants and deleting either would kill both. Assert the
  // PROPERTY rather than the mechanism — whether the pool avoids reuse or the
  // adapter's duplicate-key guard refuses it, no two live sandboxes may see
  // each other's state.
  let tb = TestBedBuilder::new("./test_cases/e2b-adapter-main")
    .with_oneshot_policy(None)
    .build()
    .await;

  // One execute per sandbox, and the status is inspected rather than asserted:
  // the oneshot pool retires the executor after boot, so every execute here
  // fails. What must hold regardless is that a failure is a loud API error —
  // never another tenant's state. Claiming the marker and reading it back in
  // the same execution is what would expose a shared isolate.
  let mut created_count = 0;
  for index in 0..3 {
    let (status, created) = e2b_adapter_post(
      &tb,
      "/sandboxes",
      serde_json::json!({ "templateID": "base" }),
    )
    .await;
    // 500 is the guard refusing a reused worker, which is also acceptable.
    assert!(status == 201 || status == 500, "got {status}: {created}");
    if status != 201 {
      continue;
    }
    created_count += 1;

    let id = created["sandboxID"].as_str().unwrap();
    let (status, body) = e2b_adapter_post(
      &tb,
      "/execute",
      serde_json::json!({
        "code": format!(
          "if (globalThis.owner === undefined) globalThis.owner = {index}; \
           globalThis.owner"
        ),
        "context_id": id,
        "language": "javascript",
      }),
    )
    .await;

    if status == 200 && body["error"].is_null() {
      assert_eq!(
        body["result"],
        serde_json::json!(index),
        "sandbox {id} shares an isolate with another tenant: {body}"
      );
    } else {
      assert!(
        status == 410 || status == 500,
        "sandbox {id} failed in an unexpected way: {status} {body}"
      );
    }
  }

  assert!(created_count > 0, "no sandbox was created at all");

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_e2b_adapter_duplicate_key_does_not_terminate_active_sandbox() {
  let tb = TestBedBuilder::new("./test_cases/e2b-adapter-duplicate-key")
    .with_per_worker_policy(None)
    .build()
    .await;

  let (status, first) = e2b_adapter_post(
    &tb,
    "/sandboxes",
    serde_json::json!({ "templateID": "base" }),
  )
  .await;
  assert_eq!(status, 201, "got: {first}");
  let sandbox = first["sandboxID"].as_str().expect("sandboxID");

  let (status, duplicate) = e2b_adapter_post(
    &tb,
    "/sandboxes",
    serde_json::json!({ "templateID": "base" }),
  )
  .await;
  assert_eq!(status, 500, "got: {duplicate}");

  let (status, execution) = e2b_adapter_post(
    &tb,
    "/execute",
    serde_json::json!({
      "code": "1 + 2",
      "context_id": sandbox,
      "language": "javascript",
    }),
  )
  .await;
  assert_eq!(status, 200, "got: {execution}");
  assert_eq!(execution["result"], serde_json::json!(3));

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_e2b_adapter_reaps_an_unresponsive_executor() {
  // The fixture pins a 300ms adapter deadline, so the adapter's own deadline is
  // what fires rather than any executor-side timeout.
  let tb = TestBedBuilder::new("./test_cases/e2b-adapter-deadline")
    .with_per_worker_policy(None)
    .build()
    .await;

  let (_status, created) = e2b_adapter_post(
    &tb,
    "/sandboxes",
    serde_json::json!({ "templateID": "base" }),
  )
  .await;
  let sandbox = created["sandboxID"].as_str().unwrap().to_string();

  // An infinite microtask loop uses only intrinsics, so no vm timeout can
  // interrupt it. Without an enforced adapter deadline this request would hold
  // the sandbox lock and a global slot until the cumulative CPU limit fired.
  let started = std::time::Instant::now();
  let (status, body) = e2b_adapter_post(
    &tb,
    "/execute",
    serde_json::json!({
      "code": "function f() { Promise.resolve().then(f) } f(); 1",
      "context_id": sandbox,
      "language": "javascript",
    }),
  )
  .await;
  let elapsed = started.elapsed();

  assert_eq!(status, 410, "got: {body}");
  assert_eq!(body["error_code"], serde_json::json!("sandbox_terminated"));
  assert!(
    elapsed < Duration::from_secs(10),
    "the adapter deadline did not fire; took {elapsed:?}"
  );

  // The dead sandbox is reaped, and the adapter keeps serving.
  let (status, gone) =
    e2b_adapter_get(&tb, &format!("/sandboxes/{sandbox}")).await;
  assert_eq!(status, 410, "got: {gone}");

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_e2b_adapter_bounds_a_hung_terminate() {
  // The fixture's fake worker never settles `terminate()` when the sandbox env
  // asks for "hang-terminate". Before the fix `ExecutorHandle.terminate`
  // awaited that unbounded, so `registry.reap` (and the DELETE awaiting it)
  // hung forever.
  let tb = TestBedBuilder::new("./test_cases/e2b-adapter-worker-fault")
    .with_per_worker_policy(None)
    .build()
    .await;

  let (status, created) = e2b_adapter_post(
    &tb,
    "/sandboxes",
    serde_json::json!({
      "templateID": "base",
      "envVars": { "__fault": "hang-terminate" },
    }),
  )
  .await;
  assert_eq!(status, 201, "got: {created}");
  let sandbox = created["sandboxID"].as_str().unwrap().to_string();

  let started = std::time::Instant::now();
  let status = timeout(
    Duration::from_secs(20),
    e2b_adapter_delete(&tb, &format!("/sandboxes/{sandbox}")),
  )
  .await
  .expect("a hung terminate blocked registry cleanup past its deadline");
  let elapsed = started.elapsed();

  assert_eq!(status, 204);
  assert!(
    elapsed < Duration::from_secs(10),
    "the terminate deadline did not fire; took {elapsed:?}"
  );

  // The adapter keeps serving after bounding the hung terminate.
  let (status, created) = e2b_adapter_post(
    &tb,
    "/sandboxes",
    serde_json::json!({ "templateID": "base" }),
  )
  .await;
  assert_eq!(status, 201, "got: {created}");

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_e2b_adapter_retries_after_a_failed_bundle() {
  // The fixture makes the first `EdgeRuntime.bundle` build reject, then
  // delegates to the real op. Before the fix the rejected promise was cached by
  // `??=` forever, so every future create 500ed until restart. Clearing the
  // cache on rejection must let the retry succeed.
  let tb = TestBedBuilder::new("./test_cases/e2b-adapter-bundle-retry")
    .with_per_worker_policy(None)
    .build()
    .await;

  let (status, body) = e2b_adapter_post(
    &tb,
    "/sandboxes",
    serde_json::json!({ "templateID": "base" }),
  )
  .await;
  assert_eq!(
    status, 500,
    "the injected bundle failure should surface: {body}"
  );
  assert_eq!(
    body["error_code"],
    serde_json::json!("internal_server_error")
  );

  // The second create rebuilds the bundle and succeeds.
  let (status, created) = e2b_adapter_post(
    &tb,
    "/sandboxes",
    serde_json::json!({ "templateID": "base" }),
  )
  .await;
  assert_eq!(
    status, 201,
    "the bundle cache was not cleared; got: {created}"
  );

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_e2b_adapter_isolates_state_and_secrets() {
  let tb = TestBedBuilder::new("./test_cases/e2b-adapter-main")
    .with_per_worker_policy(None)
    .build()
    .await;

  let mut ids = vec![];
  for _ in 0..2 {
    let (_status, created) = e2b_adapter_post(
      &tb,
      "/sandboxes",
      serde_json::json!({
        "templateID": "base",
        "envVars": { "FOO": "sandbox" },
      }),
    )
    .await;
    ids.push(created["sandboxID"].as_str().unwrap().to_string());
  }

  // Bindings do not cross sandboxes.
  e2b_adapter_execute(&tb, &ids[0], "let secret = 'A'").await;
  assert_eq!(
    e2b_adapter_execute(&tb, &ids[1], "typeof secret").await["result"],
    serde_json::json!("undefined")
  );

  // Sandbox env reaches the sandbox; the adapter's own key never does.
  assert_eq!(
    e2b_adapter_execute(&tb, &ids[0], "process.env.FOO").await["result"],
    serde_json::json!("sandbox")
  );
  assert_eq!(
    e2b_adapter_execute(&tb, &ids[0], "process.env.E2B_API_KEY").await
      ["result_type"],
    serde_json::json!("undefined")
  );

  // Network and filesystem stay unreachable through the public API. `fetch`
  // and `Deno` are `undefined` in the vm context (the sandbox boundary shadows
  // all non-allowlisted host globals), so calling them throws a
  // `runtime_error`; the denied permissions and user-worker denylist are a
  // second layer behind that.
  let fetched =
    e2b_adapter_execute(&tb, &ids[0], "fetch('https://example.com')").await;
  assert_eq!(fetched["error"]["kind"], serde_json::json!("runtime_error"));

  let fs =
    e2b_adapter_execute(&tb, &ids[0], "Deno.readTextFile('/etc/passwd')").await;
  assert_eq!(fs["error"]["kind"], serde_json::json!("runtime_error"));

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_e2b_adapter_runs_sandboxes_concurrently() {
  let tb = TestBedBuilder::new("./test_cases/e2b-adapter-main")
    .with_per_worker_policy(None)
    .build()
    .await;

  let mut ids = vec![];
  for _ in 0..2 {
    let (_status, created) = e2b_adapter_post(
      &tb,
      "/sandboxes",
      serde_json::json!({ "templateID": "base" }),
    )
    .await;
    ids.push(created["sandboxID"].as_str().unwrap().to_string());
  }

  // Two sandboxes awaiting timers must overlap. This proves concurrent
  // progress, not thread parallelism, which is not deterministic because
  // thread assignment is not controllable.
  let sleep_code =
    "(async () => { await new Promise(r => setTimeout(r, 400)); return 1 })()";
  let started = std::time::Instant::now();
  let (a, b) = futures_util::future::join(
    e2b_adapter_execute(&tb, &ids[0], sleep_code),
    e2b_adapter_execute(&tb, &ids[1], sleep_code),
  )
  .await;
  let elapsed = started.elapsed();

  assert_eq!(a["result"], serde_json::json!(1));
  assert_eq!(b["result"], serde_json::json!(1));
  // Serialized execution is ~800ms for two 400ms sleeps, so this bound is the
  // discriminator: it can only pass if the sandboxes actually overlapped.
  assert!(
    elapsed < Duration::from_millis(700),
    "expected the sleeps to overlap, took {elapsed:?}"
  );

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_e2b_adapter_separates_user_and_api_errors() {
  let tb = TestBedBuilder::new("./test_cases/e2b-adapter-main")
    .with_per_worker_policy(None)
    .build()
    .await;

  let (_status, created) = e2b_adapter_post(
    &tb,
    "/sandboxes",
    serde_json::json!({ "templateID": "base" }),
  )
  .await;
  let sandbox = created["sandboxID"].as_str().unwrap().to_string();

  // User-code failures are HTTP 200 with an error body, and leave state intact.
  e2b_adapter_execute(&tb, &sandbox, "let keep = 3").await;
  let thrown =
    e2b_adapter_execute(&tb, &sandbox, "throw new Error('boom')").await;
  assert_eq!(thrown["error"]["kind"], serde_json::json!("runtime_error"));
  assert_eq!(thrown["error"]["message"], serde_json::json!("boom"));
  assert_eq!(
    e2b_adapter_execute(&tb, &sandbox, "keep").await["result"],
    serde_json::json!(3)
  );

  let compile = e2b_adapter_execute(&tb, &sandbox, "let bad: = ;;;").await;
  assert_eq!(compile["error"]["kind"], serde_json::json!("compile_error"));

  // API-level failures use the error envelope with a 4xx status.
  for (body, expected_code) in [
    (
      serde_json::json!({
        "code": "1", "context_id": sandbox, "language": "python",
      }),
      "unsupported_language",
    ),
    (
      serde_json::json!({
        "code": "x".repeat(300_000),
        "context_id": sandbox,
        "language": "javascript",
      }),
      "invalid_request",
    ),
    (
      serde_json::json!({
        "code": "1", "context_id": sandbox, "language": "javascript",
        "env_vars": { "NESTED": {} },
      }),
      "invalid_request",
    ),
  ] {
    let (status, rejected) = e2b_adapter_post(&tb, "/execute", body).await;
    assert_eq!(status, 400, "expected 400 for {expected_code}: {rejected}");
    assert_eq!(rejected["error_code"], serde_json::json!(expected_code));
  }

  // An unsupported template must not leak the host path of the executor.
  let (status, template) = e2b_adapter_post(
    &tb,
    "/sandboxes",
    serde_json::json!({ "templateID": "nope" }),
  )
  .await;
  assert_eq!(status, 400, "got: {template}");
  assert_eq!(
    template["error_code"],
    serde_json::json!("unsupported_template")
  );
  let message = template["message"].as_str().unwrap();
  assert!(
    !message.contains('/'),
    "message must not leak host paths: {message}"
  );

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_e2b_adapter_never_revives_a_dead_sandbox() {
  // The fixture pins a 32MB sandbox memory limit.
  let tb = TestBedBuilder::new("./test_cases/e2b-adapter-memory")
    .with_per_worker_policy(None)
    .build()
    .await;

  let (_status, created) = e2b_adapter_post(
    &tb,
    "/sandboxes",
    serde_json::json!({ "templateID": "base" }),
  )
  .await;
  let sandbox = created["sandboxID"].as_str().unwrap().to_string();

  e2b_adapter_execute(&tb, &sandbox, "let marker = 'original'").await;

  // A sandbox that already exists when the other one dies: the memory kill must
  // take only the offending isolate, not its neighbour's state.
  let (_status, bystander_created) = e2b_adapter_post(
    &tb,
    "/sandboxes",
    serde_json::json!({ "templateID": "base" }),
  )
  .await;
  let bystander = bystander_created["sandboxID"].as_str().unwrap().to_string();
  e2b_adapter_execute(&tb, &bystander, "let mine = 'intact'").await;

  // Exhaust the sandbox's memory. Whether this returns 200 with an error or
  // 410 depends on when the supervisor kills the isolate, so accept both.
  let (status, _body) = e2b_adapter_post(
    &tb,
    "/execute",
    serde_json::json!({
      "code": "const a = []; while (true) { a.push(new Array(1e6).fill('x')) }",
      "context_id": sandbox,
      "language": "javascript",
    }),
  )
  .await;
  assert!(status == 200 || status == 410, "unexpected status {status}");

  // The critical invariant: the sandbox must never come back with fresh state.
  // A silently recreated worker would answer 200 with marker === undefined.
  let (status, after) = e2b_adapter_post(
    &tb,
    "/execute",
    serde_json::json!({
      "code": "marker", "context_id": sandbox, "language": "javascript",
    }),
  )
  .await;
  if status == 200 {
    assert_eq!(
      after["result"],
      serde_json::json!("original"),
      "state was silently reset instead of reporting a dead sandbox: {after}"
    );
  } else {
    assert_eq!(status, 410, "got: {after}");
    assert_eq!(after["error_code"], serde_json::json!("sandbox_terminated"));
  }

  // The neighbour sandbox is untouched: only the offending isolate died.
  let survived = e2b_adapter_execute(&tb, &bystander, "mine").await;
  assert_eq!(survived["result"], serde_json::json!("intact"));

  // The adapter itself survives a dead sandbox.
  let (status, fresh) = e2b_adapter_post(
    &tb,
    "/sandboxes",
    serde_json::json!({ "templateID": "base" }),
  )
  .await;
  assert_eq!(status, 201, "adapter must keep serving: {fresh}");

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_e2b_adapter_serializes_one_sandbox() {
  let tb = TestBedBuilder::new("./test_cases/e2b-adapter-main")
    .with_per_worker_policy(None)
    .build()
    .await;

  let (_status, created) = e2b_adapter_post(
    &tb,
    "/sandboxes",
    serde_json::json!({ "templateID": "base" }),
  )
  .await;
  let sandbox = created["sandboxID"].as_str().unwrap().to_string();

  e2b_adapter_execute(&tb, &sandbox, "let x = 0").await;

  // Assert the FINAL state, not the order of the intermediate completion
  // values: `x++` returns the pre-increment value, so which request sees 0 and
  // which sees 1 is not a meaningful contract.
  let (_a, _b) = futures_util::future::join(
    e2b_adapter_execute(&tb, &sandbox, "x++"),
    e2b_adapter_execute(&tb, &sandbox, "x++"),
  )
  .await;

  assert_eq!(
    e2b_adapter_execute(&tb, &sandbox, "x").await["result"],
    serde_json::json!(2),
    "concurrent increments must not interleave"
  );

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_e2b_adapter_keeps_one_sandbox_ordered_under_timeout() {
  // Short lock-wait deadline; the global slot stays generous so requests
  // contend on the per-sandbox lock rather than on the slot.
  let tb = TestBedBuilder::new("./test_cases/e2b-adapter-lock")
    .with_per_worker_policy(None)
    .build()
    .await;

  let (status, created) = e2b_adapter_post(
    &tb,
    "/sandboxes",
    serde_json::json!({ "templateID": "base" }),
  )
  .await;
  assert_eq!(status, 201, "got: {created}");
  let sandbox = created["sandboxID"].as_str().unwrap().to_string();

  // The first execution holds the lock for ~400ms; the other two cannot get it
  // within the 50ms deadline. A waiter that resolved its OWN tail on timeout
  // would hand the lock to the third request while the first was still running,
  // producing two successes instead of one.
  let slow =
    "(async () => { await new Promise(r => setTimeout(r, 400)); return 'A' })()";
  let responses = futures_util::future::join_all(vec![
    e2b_adapter_post(
      &tb,
      "/execute",
      serde_json::json!({
        "code": slow, "context_id": sandbox, "language": "javascript",
      }),
    ),
    e2b_adapter_post(
      &tb,
      "/execute",
      serde_json::json!({
        "code": "1", "context_id": sandbox, "language": "javascript",
      }),
    ),
    e2b_adapter_post(
      &tb,
      "/execute",
      serde_json::json!({
        "code": "1", "context_id": sandbox, "language": "javascript",
      }),
    ),
  ])
  .await;

  let succeeded = responses
    .iter()
    .filter(|(status, _)| *status == 200)
    .count();
  let refused = responses
    .iter()
    .filter(|(status, body)| {
      *status == 429
        && body["error_code"] == serde_json::json!("too_many_requests")
    })
    .count();

  assert_eq!(
    succeeded, 1,
    "only the lock holder may run; got {succeeded} successes: {responses:?}"
  );
  assert_eq!(refused, 2, "got: {responses:?}");

  // The sandbox is still usable once the queue drains.
  assert_eq!(
    e2b_adapter_execute(&tb, &sandbox, "1 + 1").await["result"],
    serde_json::json!(2)
  );

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_e2b_adapter_rejects_when_queue_is_full() {
  // The fixture allows one concurrent execution with a 50ms queue deadline.
  let tb = TestBedBuilder::new("./test_cases/e2b-adapter-queue")
    .with_per_worker_policy(None)
    .build()
    .await;

  let mut sandboxes = vec![];
  // Two is enough: the fixture allows one concurrent execution, so the second
  // request must be refused. Each extra sandbox costs a worker boot, which
  // dominates this test's runtime in debug builds.
  for _ in 0..2 {
    let (status, created) = e2b_adapter_post(
      &tb,
      "/sandboxes",
      serde_json::json!({ "templateID": "base" }),
    )
    .await;
    assert_eq!(status, 201, "got: {created}");
    sandboxes.push(created["sandboxID"].as_str().unwrap().to_string());
  }

  // Different sandboxes do not contend on the per-sandbox lock, so these
  // contend only on the global slot. Each holds it for ~400ms.
  let slow = "(async () => { await new Promise(r => setTimeout(r, 400)); \
              return 1 })()";
  let requests = sandboxes.iter().map(|sandbox| {
    e2b_adapter_post(
      &tb,
      "/execute",
      serde_json::json!({
        "code": slow,
        "context_id": sandbox,
        "language": "javascript",
      }),
    )
  });
  let responses = futures_util::future::join_all(requests).await;

  let rejected = responses
    .iter()
    .filter(|(status, body)| {
      *status == 429
        && body["error_code"] == serde_json::json!("too_many_requests")
    })
    .count();
  assert!(
    rejected >= 1,
    "expected at least one 429 from the global cap, got: {responses:?}"
  );

  // The adapter stays usable once the queue drains.
  assert_eq!(
    e2b_adapter_execute(&tb, &sandboxes[0], "1 + 1").await["result"],
    serde_json::json!(2)
  );

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_e2b_adapter_transfers_a_raced_slot_to_the_next_waiter() {
  // The fixture drives the queue lost-wakeup interleave with a hand-driven
  // clock: a queued waiter is admitted by release() in the same turn its own
  // deadline fires, then times out. With the bug the slot it was handed is
  // stranded and the next waiter never runs though capacity is free. The
  // fixture reports whether that next waiter ran.
  let tb = TestBedBuilder::new("./test_cases/e2b-adapter-queue-race")
    .with_per_worker_policy(None)
    .build()
    .await;

  let (status, body) = e2b_adapter_get(&tb, "/race").await;
  assert_eq!(status, 200, "got: {body}");
  assert_eq!(
    body["secondWaiterRan"],
    serde_json::json!(true),
    "a timed-out-but-admitted waiter stranded its slot: {body}"
  );

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_e2b_adapter_revalidates_after_waiting_for_global_slot() {
  let tb = TestBedBuilder::new("./test_cases/e2b-adapter-stale-slot")
    .with_per_worker_policy(None)
    .build()
    .await;

  let (status, holder) = e2b_adapter_post(
    &tb,
    "/sandboxes",
    serde_json::json!({
      "templateID": "base",
      "envVars": { "__role": "holder" },
    }),
  )
  .await;
  assert_eq!(status, 201, "got: {holder}");
  let holder_id = holder["sandboxID"].as_str().unwrap().to_string();

  let (status, queued) = e2b_adapter_post(
    &tb,
    "/sandboxes",
    serde_json::json!({
      "templateID": "base",
      "envVars": { "__role": "queued" },
    }),
  )
  .await;
  assert_eq!(status, 201, "got: {queued}");
  let queued_id = queued["sandboxID"].as_str().unwrap().to_string();

  let holder_request = e2b_adapter_post(
    &tb,
    "/execute",
    serde_json::json!({
      "code": "hold",
      "context_id": holder_id,
      "language": "javascript",
    }),
  );
  let race = async {
    timeout(Duration::from_secs(5), async {
      loop {
        let (status, body) =
          e2b_adapter_get(&tb, "/__test/holder-started").await;
        assert_eq!(status, 200, "got: {body}");
        if body["holderStarted"] == serde_json::json!(true) {
          break;
        }
        sleep(Duration::from_millis(10)).await;
      }
    })
    .await
    .expect("holder never occupied the global execution slot");

    let queued_request = e2b_adapter_post(
      &tb,
      "/execute",
      serde_json::json!({
        "code": "must not run",
        "context_id": queued_id,
        "language": "javascript",
      }),
    );
    tokio::pin!(queued_request);

    timeout(Duration::from_secs(5), async {
      let pending_path = format!("/__test/lock-pending?sandbox={queued_id}");
      loop {
        let pending_check = e2b_adapter_get(&tb, &pending_path);
        tokio::select! {
          response = &mut queued_request => {
            panic!("queued execution finished before deletion: {response:?}");
          }
          (status, body) = pending_check => {
            assert_eq!(status, 200, "got: {body}");
            if body["pending"] == serde_json::json!(true) {
              break;
            }
          }
        }
        sleep(Duration::from_millis(10)).await;
      }
    })
    .await
    .expect("second execution never queued for the global slot");

    assert_eq!(
      e2b_adapter_delete(&tb, &format!("/sandboxes/{queued_id}")).await,
      204
    );

    let (status, body) = timeout(Duration::from_secs(5), &mut queued_request)
      .await
      .expect("queued execution did not finish after the slot was released");
    assert_eq!(status, 410, "stale execution was dispatched: {body}");
    assert_eq!(body["error_code"], serde_json::json!("sandbox_terminated"));

    let (status, counts) =
      e2b_adapter_get(&tb, "/__test/execution-count?role=queued").await;
    assert_eq!(status, 200, "got: {counts}");
    assert_eq!(
      counts["count"],
      serde_json::json!(0),
      "deleted sandbox code reached its executor: {counts}"
    );
  };

  let (holder_response, ()) =
    futures_util::future::join(holder_request, race).await;
  assert_eq!(holder_response.0, 200, "got: {:?}", holder_response.1);

  assert_eq!(
    e2b_adapter_execute(&tb, &holder_id, "after stale waiter").await["result"],
    serde_json::json!(1),
    "the stale path leaked its acquired global slot"
  );

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_e2b_adapter_refuses_execution_when_ttl_lapses_while_waiting() {
  // Default limits: the lock wait is generous (30s), so a queued execute waits
  // through the holder rather than timing out on the lock.
  let tb = TestBedBuilder::new("./test_cases/e2b-adapter-main")
    .with_per_worker_policy(None)
    .build()
    .await;

  // A 0.6s TTL: long enough for both executes to pass the top-of-handler
  // resolve, short enough to lapse while the second waits on the lock.
  let (status, created) = e2b_adapter_post(
    &tb,
    "/sandboxes",
    serde_json::json!({ "templateID": "base", "timeout": 0.6 }),
  )
  .await;
  assert_eq!(status, 201, "got: {created}");
  let sandbox = created["sandboxID"].as_str().unwrap().to_string();

  // The holder pins the per-sandbox lock for ~2s, well past the TTL. The waiter
  // starts a little later so the holder takes the lock first; the waiter
  // resolves the still-live record, then blocks on the lock. By the time it
  // acquires the lock the TTL has lapsed, so executing then would run stale
  // code on an expired sandbox.
  let hold = "(async () => { await new Promise(r => setTimeout(r, 2000)); \
              return 'A' })()";
  let holder = e2b_adapter_post(
    &tb,
    "/execute",
    serde_json::json!({
      "code": hold, "context_id": sandbox, "language": "javascript",
    }),
  );
  let waiter = async {
    tokio::time::sleep(Duration::from_millis(200)).await;
    e2b_adapter_post(
      &tb,
      "/execute",
      serde_json::json!({
        "code": "1 + 1", "context_id": sandbox, "language": "javascript",
      }),
    )
    .await
  };
  let (holder, waiter) = futures_util::future::join(holder, waiter).await;

  assert_eq!(holder.0, 200, "holder ran before expiry: {:?}", holder.1);
  assert_eq!(
    waiter.0, 404,
    "executed on an expired sandbox instead of refusing: {:?}",
    waiter.1
  );
  assert_eq!(
    waiter.1["error_code"],
    serde_json::json!("sandbox_expired"),
    "got: {:?}",
    waiter.1
  );

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_e2b_adapter_sandbox_lifecycle() {
  let tb = TestBedBuilder::new("./test_cases/e2b-adapter-main")
    .with_per_worker_policy(None)
    .build()
    .await;

  let (_status, created) = e2b_adapter_post(
    &tb,
    "/sandboxes",
    serde_json::json!({ "templateID": "base", "metadata": { "user": "t" } }),
  )
  .await;
  let sandbox = created["sandboxID"].as_str().unwrap().to_string();

  let (status, fetched) =
    e2b_adapter_get(&tb, &format!("/sandboxes/{sandbox}")).await;
  assert_eq!(status, 200, "got: {fetched}");
  assert_eq!(fetched["sandboxID"], serde_json::json!(sandbox));
  assert_eq!(fetched["metadata"]["user"], serde_json::json!("t"));

  for path in ["/sandboxes", "/v2/sandboxes"] {
    let (status, listed) = e2b_adapter_get(&tb, path).await;
    assert_eq!(status, 200, "{path} got: {listed}");
    let items = listed.as_array().expect("array");
    assert!(
      items
        .iter()
        .any(|item| item["sandboxID"] == serde_json::json!(sandbox)),
      "{path} did not list the sandbox: {listed}"
    );
  }

  // Extending within the wall-clock ceiling is allowed.
  let (status, _body) = e2b_adapter_post(
    &tb,
    &format!("/sandboxes/{sandbox}/timeout"),
    serde_json::json!({ "timeout": 600 }),
  )
  .await;
  assert_eq!(status, 204);

  // Beyond the ceiling must be refused, not silently granted: the worker wall
  // clock starts at boot and cannot be extended.
  let (status, refused) = e2b_adapter_post(
    &tb,
    &format!("/sandboxes/{sandbox}/timeout"),
    serde_json::json!({ "timeout": 99_999_999 }),
  )
  .await;
  assert_eq!(status, 400, "got: {refused}");
  assert_eq!(refused["error_code"], serde_json::json!("invalid_request"));

  // Delete releases the worker, and the id is remembered as terminated.
  assert_eq!(
    e2b_adapter_delete(&tb, &format!("/sandboxes/{sandbox}")).await,
    204
  );

  let (status, gone) =
    e2b_adapter_get(&tb, &format!("/sandboxes/{sandbox}")).await;
  assert_eq!(status, 410, "got: {gone}");
  assert_eq!(gone["error_code"], serde_json::json!("sandbox_terminated"));

  let (status, after) = e2b_adapter_post(
    &tb,
    "/execute",
    serde_json::json!({
      "code": "1", "context_id": sandbox, "language": "javascript",
    }),
  )
  .await;
  assert_eq!(status, 410, "execute after delete: {after}");

  // An id we never issued is not found, rather than terminated.
  let (status, unknown) =
    e2b_adapter_get(&tb, "/sandboxes/never-existed").await;
  assert_eq!(status, 404, "got: {unknown}");
  assert_eq!(
    unknown["error_code"],
    serde_json::json!("sandbox_not_found")
  );

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_e2b_adapter_expires_sandboxes() {
  let tb = TestBedBuilder::new("./test_cases/e2b-adapter-main")
    .with_per_worker_policy(None)
    .build()
    .await;

  let (_status, created) = e2b_adapter_post(
    &tb,
    "/sandboxes",
    serde_json::json!({ "templateID": "base", "timeout": 1 }),
  )
  .await;
  let sandbox = created["sandboxID"].as_str().unwrap().to_string();

  assert_eq!(
    e2b_adapter_execute(&tb, &sandbox, "1 + 1").await["result"],
    serde_json::json!(2)
  );

  sleep(Duration::from_millis(1500)).await;

  // Expiry must be distinguishable from an explicit delete, even though both
  // reap the worker.
  let (status, expired) =
    e2b_adapter_get(&tb, &format!("/sandboxes/{sandbox}")).await;
  assert_eq!(status, 404, "got: {expired}");
  assert_eq!(expired["error_code"], serde_json::json!("sandbox_expired"));

  let (status, executed) = e2b_adapter_post(
    &tb,
    "/execute",
    serde_json::json!({
      "code": "1", "context_id": sandbox, "language": "javascript",
    }),
  )
  .await;
  assert_eq!(status, 404, "execute on expired: {executed}");

  let (_status, listed) = e2b_adapter_get(&tb, "/sandboxes").await;
  assert_eq!(
    listed.as_array().unwrap().len(),
    0,
    "expired sandbox must not be listed: {listed}"
  );

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_e2b_adapter_sweeps_expired_sandboxes_concurrently() {
  // The fixture's fake workers take ~700ms each to terminate. A sweep that
  // reaps expired sandboxes one at a time would serialize those terminations
  // (~N * 700ms) while the caller waits; reaping them concurrently bounds the
  // wait near a single terminate. A live sandbox is mixed in to prove sweep
  // never terminates a record whose TTL has not lapsed.
  let tb = TestBedBuilder::new("./test_cases/e2b-adapter-slow-terminate")
    .with_per_worker_policy(None)
    .build()
    .await;

  let (status, live) = e2b_adapter_post(
    &tb,
    "/sandboxes",
    serde_json::json!({ "templateID": "base", "timeout": 600 }),
  )
  .await;
  assert_eq!(status, 201, "got: {live}");
  let live_id = live["sandboxID"].as_str().unwrap().to_string();

  let mut expired_ids = vec![];
  for _ in 0..5 {
    let (status, created) = e2b_adapter_post(
      &tb,
      "/sandboxes",
      serde_json::json!({ "templateID": "base", "timeout": 1 }),
    )
    .await;
    assert_eq!(status, 201, "got: {created}");
    expired_ids.push(created["sandboxID"].as_str().unwrap().to_string());
  }

  // Let the short-TTL sandboxes lapse before triggering a sweep.
  sleep(Duration::from_millis(1200)).await;

  // A single sweep-triggering request. Sequential reaping would take ~3.5s;
  // concurrent reaping stays near one 700ms terminate.
  let started = std::time::Instant::now();
  let (status, listed) = e2b_adapter_get(&tb, "/sandboxes").await;
  let elapsed = started.elapsed();
  assert_eq!(status, 200, "got: {listed}");
  assert!(
    elapsed < Duration::from_secs(2),
    "sweep serialized expired terminations; took {elapsed:?}"
  );

  // Only the live sandbox lists; no expired one is ever shown as running.
  let items = listed.as_array().unwrap();
  assert_eq!(
    items.len(),
    1,
    "only the live sandbox should list: {listed}"
  );
  assert_eq!(items[0]["sandboxID"], serde_json::json!(live_id));

  // Every expired id reports sandbox_expired, never sandbox_terminated.
  for id in &expired_ids {
    let (status, body) =
      e2b_adapter_get(&tb, &format!("/sandboxes/{id}")).await;
    assert_eq!(status, 404, "got: {body}");
    assert_eq!(body["error_code"], serde_json::json!("sandbox_expired"));
  }

  // The live sandbox was not reaped by the sweep.
  let (status, body) =
    e2b_adapter_get(&tb, &format!("/sandboxes/{live_id}")).await;
  assert_eq!(status, 200, "live sandbox was swept: {body}");

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_e2b_adapter_requires_api_key() {
  let tb = TestBedBuilder::new("./test_cases/e2b-adapter-main")
    .with_per_worker_policy(None)
    .build()
    .await;

  let unauthenticated =
    |path: &'static str, method: &'static str, key: Option<&'static str>| {
      tb.request(move |b| {
        let mut builder = b
          .uri(path)
          .method(method)
          .header("content-type", "application/json");
        if let Some(key) = key {
          builder = builder.header("x-api-key", key);
        }
        builder
          .body(Body::from(r#"{"templateID":"base"}"#))
          .context("can't make request")
      })
    };

  for (path, method, key) in [
    ("/sandboxes", "POST", None),
    ("/sandboxes", "POST", Some("wrong")),
    ("/execute", "POST", None),
    ("/internal/metrics", "GET", None),
    // The gate runs before routing, so even an unknown path must not leak
    // whether it exists.
    ("/nope", "GET", None),
  ] {
    let mut resp = unauthenticated(path, method, key).await.unwrap();
    assert_eq!(resp.status().as_u16(), 401, "{method} {path} was not gated");
    let bytes = to_bytes(resp.body_mut()).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["error_code"], serde_json::json!("unauthorized"));
  }

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_e2b_adapter_caps_concurrent_sandbox_creates() {
  // The fixture pins maxConcurrentSandboxes to 2.
  let tb = TestBedBuilder::new("./test_cases/e2b-adapter-cap")
    .with_per_worker_policy(None)
    .build()
    .await;

  // Reading the size and inserting after an await would let every one of these
  // pass the check, overshooting the cap by up to N-1 isolates. Force-created
  // workers bypass the pool's own semaphore, so this counter is the only bound.
  let creates = (0..5).map(|_| {
    e2b_adapter_post(
      &tb,
      "/sandboxes",
      serde_json::json!({ "templateID": "base" }),
    )
  });
  let responses = futures_util::future::join_all(creates).await;

  let created = responses
    .iter()
    .filter(|(status, _)| *status == 201)
    .count();
  let rejected = responses
    .iter()
    .filter(|(status, body)| {
      *status == 429
        && body["error_code"] == serde_json::json!("too_many_sandboxes")
    })
    .count();

  assert!(
    created <= 2,
    "cap exceeded: {created} created, {responses:?}"
  );
  assert_eq!(created + rejected, 5, "unexpected outcomes: {responses:?}");
  assert!(created >= 1, "nothing was created: {responses:?}");

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_e2b_adapter_create_gate_is_fifo_and_reports_waiters() {
  let tb = TestBedBuilder::new("./test_cases/e2b-adapter-create-gate")
    .with_per_worker_policy(None)
    .build()
    .await;

  {
    let first = e2b_adapter_post(
      &tb,
      "/sandboxes",
      serde_json::json!({
        "templateID": "base",
        "envVars": { "__role": "hold" },
      }),
    );
    tokio::pin!(first);

    timeout(Duration::from_secs(5), async {
    loop {
      let state = e2b_adapter_get(&tb, "/__test/create-gate-state");
      tokio::select! {
        response = &mut first => panic!("first create completed before release: {response:?}"),
        (status, body) = state => {
          assert_eq!(status, 200, "got: {body}");
          if body["firstInitStarted"] == serde_json::json!(true) {
            break;
          }
        }
      }
      sleep(Duration::from_millis(10)).await;
    }
  })
  .await
  .expect("first create never reached its controlled worker boot");

    let second = e2b_adapter_post(
      &tb,
      "/sandboxes",
      serde_json::json!({
        "templateID": "base",
        "envVars": { "__role": "second-hold" },
      }),
    );
    tokio::pin!(second);

    timeout(Duration::from_secs(5), async {
      loop {
        let metrics = e2b_adapter_get(&tb, "/internal/metrics");
        tokio::select! {
          response = &mut second => panic!("second create bypassed the gate: {response:?}"),
          (status, body) = metrics => {
            assert_eq!(status, 200, "got: {body}");
            if body["creationGate"]["active"] == serde_json::json!(1)
              && body["creationGate"]["waiting"] == serde_json::json!(1) {
              break;
            }
          }
        }
        sleep(Duration::from_millis(10)).await;
      }
    })
    .await
    .expect("second creator never queued at the create gate");

    let third = e2b_adapter_post(
      &tb,
      "/sandboxes",
      serde_json::json!({
        "templateID": "base",
        "envVars": { "__role": "third" },
      }),
    );
    tokio::pin!(third);

    timeout(Duration::from_secs(5), async {
      loop {
        let metrics = e2b_adapter_get(&tb, "/internal/metrics");
        tokio::select! {
          response = &mut second => panic!("second create bypassed the gate: {response:?}"),
          response = &mut third => panic!("third create bypassed the gate: {response:?}"),
          (status, body) = metrics => {
            assert_eq!(status, 200, "got: {body}");
            if body["creationGate"]["active"] == serde_json::json!(1)
              && body["creationGate"]["waiting"] == serde_json::json!(2) {
              break;
            }
          }
        }
        sleep(Duration::from_millis(10)).await;
      }
    })
    .await
    .expect("third creator never queued at the create gate");

    let (status, body) =
      e2b_adapter_get(&tb, "/__test/release-create-gate").await;
    assert_eq!(status, 204, "got: {body}");

    timeout(Duration::from_secs(5), async {
      loop {
        let state = e2b_adapter_get(&tb, "/__test/create-gate-state");
        tokio::select! {
          response = &mut third => panic!("third create bypassed the second FIFO waiter: {response:?}"),
          (status, body) = state => {
            assert_eq!(status, 200, "got: {body}");
            if body["secondInitStarted"] == serde_json::json!(true) {
              assert_eq!(
                body["initOrder"],
                serde_json::json!(["hold", "second-hold"]),
                "unexpected create admission order: {body}",
              );
              break;
            }
          }
        }
        sleep(Duration::from_millis(10)).await;
      }
    })
    .await
    .expect("second queued creator was not admitted first");

    let (status, metrics) = e2b_adapter_get(&tb, "/internal/metrics").await;
    assert_eq!(status, 200, "got: {metrics}");
    assert_eq!(metrics["creationGate"]["active"], serde_json::json!(1));
    assert_eq!(metrics["creationGate"]["waiting"], serde_json::json!(1));

    let (status, body) =
      e2b_adapter_get(&tb, "/__test/release-second-create-gate").await;
    assert_eq!(status, 204, "got: {body}");
    let (first_response, second_response, third_response) =
      futures_util::future::join3(&mut first, &mut second, &mut third).await;
    assert_eq!(first_response.0, 201, "got: {:?}", first_response.1);
    assert_eq!(second_response.0, 201, "got: {:?}", second_response.1);
    assert_eq!(third_response.0, 201, "got: {:?}", third_response.1);

    let (status, state) =
      e2b_adapter_get(&tb, "/__test/create-gate-state").await;
    assert_eq!(status, 200, "got: {state}");
    assert_eq!(
      state["initOrder"],
      serde_json::json!(["hold", "second-hold", "third"]),
      "unexpected create admission order: {state}",
    );

    let (status, metrics) = e2b_adapter_get(&tb, "/internal/metrics").await;
    assert_eq!(status, 200, "got: {metrics}");
    assert_eq!(metrics["creationGate"]["active"], serde_json::json!(0));
    assert_eq!(metrics["creationGate"]["waiting"], serde_json::json!(0));
    assert!(
      metrics["rollingLifecycleWindow"]["stages"]["create_total"]["ok"]
        ["count"]
        .as_u64()
        .unwrap()
        >= 3
    );
    for stage in [
      "loader_vfs",
      "resource_limits",
      "js_runtime_new",
      "bootstrap",
      "bootstrap_blocking_run",
      "bootstrap_blocking_queue",
      "post_setup_blocking_run",
      "post_setup_blocking_queue",
    ] {
      assert_eq!(
        metrics["rollingLifecycleWindow"]["stages"][stage]["ok"]["count"],
        serde_json::json!(3),
        "missing {stage} timing: {metrics}"
      );
    }
    for (stage, sum_ms) in [
      ("bootstrap_blocking_run", 18),
      ("bootstrap_blocking_queue", 21),
      ("post_setup_blocking_run", 24),
      ("post_setup_blocking_queue", 27),
    ] {
      assert_eq!(
        metrics["rollingLifecycleWindow"]["stages"][stage]["ok"]["sumMs"],
        serde_json::json!(sum_ms),
        "wrong {stage} timing: {metrics}"
      );
    }
  }

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_e2b_adapter_create_gate_timeout_releases_reservation() {
  let tb = TestBedBuilder::new("./test_cases/e2b-adapter-create-gate")
    .with_per_worker_policy(None)
    .build()
    .await;

  {
    let first = e2b_adapter_post(
      &tb,
      "/sandboxes",
      serde_json::json!({
        "templateID": "base",
        "envVars": { "__role": "hold" },
      }),
    );
    tokio::pin!(first);

    timeout(Duration::from_secs(5), async {
    loop {
      let state = e2b_adapter_get(&tb, "/__test/create-gate-state");
      tokio::select! {
        response = &mut first => panic!("first create completed before release: {response:?}"),
        (status, body) = state => {
          assert_eq!(status, 200, "got: {body}");
          if body["firstInitStarted"] == serde_json::json!(true) {
            break;
          }
        }
      }
      sleep(Duration::from_millis(10)).await;
    }
  })
  .await
  .expect("first create never reached its controlled worker boot");

    let (status, body) =
      e2b_adapter_get(&tb, "/__test/shorten-create-gate-timeout").await;
    assert_eq!(status, 204, "got: {body}");

    let (status, timed_out) = timeout(
      Duration::from_secs(5),
      e2b_adapter_post(
        &tb,
        "/sandboxes",
        serde_json::json!({ "templateID": "base" }),
      ),
    )
    .await
    .expect("queued create did not time out");
    assert_eq!(status, 429, "got: {timed_out}");
    assert_eq!(
      timed_out["error_code"],
      serde_json::json!("too_many_requests"),
      "got: {timed_out}",
    );

    let (status, metrics) = e2b_adapter_get(&tb, "/internal/metrics").await;
    assert_eq!(status, 200, "got: {metrics}");
    assert_eq!(metrics["creationGate"]["active"], serde_json::json!(1));
    assert_eq!(metrics["creationGate"]["waiting"], serde_json::json!(0));
    assert_eq!(metrics["capacity"]["reserved"], serde_json::json!(1));

    let (status, body) =
      e2b_adapter_get(&tb, "/__test/release-create-gate").await;
    assert_eq!(status, 204, "got: {body}");
    let first_response = first.await;
    assert_eq!(first_response.0, 201, "got: {:?}", first_response.1);

    let (status, state) =
      e2b_adapter_get(&tb, "/__test/create-gate-state").await;
    assert_eq!(status, 200, "got: {state}");
    assert_eq!(
      state["initOrder"],
      serde_json::json!(["hold"]),
      "timed-out create started after its slot was released: {state}",
    );
  }

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_e2b_adapter_keeps_deleted_workers_draining_until_final_shutdown()
{
  let tb = TestBedBuilder::new("./test_cases/e2b-adapter-draining")
    .with_per_worker_policy(None)
    .build()
    .await;

  let (status, created) = e2b_adapter_post(
    &tb,
    "/sandboxes",
    serde_json::json!({ "templateID": "base" }),
  )
  .await;
  assert_eq!(status, 201, "got: {created}");
  let sandbox = created["sandboxID"].as_str().unwrap();

  let started = std::time::Instant::now();
  assert_eq!(
    e2b_adapter_delete(&tb, &format!("/sandboxes/{sandbox}")).await,
    204,
  );
  assert!(
    started.elapsed() < Duration::from_secs(1),
    "DELETE waited for final shutdown"
  );

  let (status, metrics) = e2b_adapter_get(&tb, "/internal/metrics").await;
  assert_eq!(status, 200, "got: {metrics}");
  assert_eq!(metrics["capacity"]["active"], serde_json::json!(0));
  assert_eq!(metrics["capacity"]["reserved"], serde_json::json!(0));
  assert_eq!(metrics["capacity"]["draining"], serde_json::json!(1));

  let (status, refused) = e2b_adapter_post(
    &tb,
    "/sandboxes",
    serde_json::json!({ "templateID": "base" }),
  )
  .await;
  assert_eq!(status, 429, "got: {refused}");
  assert_eq!(
    refused["error_code"],
    serde_json::json!("too_many_sandboxes")
  );

  let (status, body) =
    e2b_adapter_get(&tb, "/__test/release-final-shutdown").await;
  assert_eq!(status, 204, "got: {body}");
  let metrics = timeout(Duration::from_secs(5), async {
    loop {
      let (status, metrics) = e2b_adapter_get(&tb, "/internal/metrics").await;
      assert_eq!(status, 200, "got: {metrics}");
      if metrics["capacity"]["draining"] == serde_json::json!(0) {
        return metrics;
      }
      sleep(Duration::from_millis(10)).await;
    }
  })
  .await
  .expect("draining capacity was not released after final shutdown");

  assert_eq!(
    metrics["lifetimeFinalWorkerTotals"]["cpuTimeUsed"],
    serde_json::json!(7)
  );
  assert_eq!(
    metrics["lifetimeFinalWorkerTotals"]["v8Heap"]["heap"],
    serde_json::json!(20)
  );
  assert!(metrics.get("sandboxID").is_none());
  assert!(metrics.get("workerID").is_none());

  let (status, replacement) = e2b_adapter_post(
    &tb,
    "/sandboxes",
    serde_json::json!({ "templateID": "base" }),
  )
  .await;
  assert_eq!(status, 201, "got: {replacement}");

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_e2b_adapter_defers_executor_failure_until_execute() {
  let tb = TestBedBuilder::new("./test_cases/e2b-adapter-init-draining")
    .with_per_worker_policy(None)
    .build()
    .await;

  let (status, created) = e2b_adapter_post(
    &tb,
    "/sandboxes",
    serde_json::json!({ "templateID": "base" }),
  )
  .await;
  assert_eq!(status, 201, "got: {created}");
  let sandbox = created["sandboxID"].as_str().expect("sandboxID");

  let (status, failed) = e2b_adapter_post(
    &tb,
    "/execute",
    serde_json::json!({
      "code": "1 + 2",
      "context_id": sandbox,
      "language": "javascript",
    }),
  )
  .await;
  assert_eq!(status, 500, "got: {failed}");

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_e2b_adapter_executes_statefully() {
  let tb = TestBedBuilder::new("./test_cases/e2b-adapter-main")
    .with_per_worker_policy(None)
    .build()
    .await;

  let (status, created) = e2b_adapter_post(
    &tb,
    "/sandboxes",
    serde_json::json!({ "templateID": "base" }),
  )
  .await;
  assert_eq!(status, 201, "got: {created}");
  let sandbox = created["sandboxID"]
    .as_str()
    .expect("sandboxID")
    .to_string();

  e2b_adapter_execute(&tb, &sandbox, "let x = 1").await;
  e2b_adapter_execute(&tb, &sandbox, "x++").await;
  let third = e2b_adapter_execute(&tb, &sandbox, "x").await;
  assert_eq!(third["result"], serde_json::json!(2));
  assert_eq!(third["context_id"], serde_json::json!(sandbox));

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

static OTEL_INIT: std::sync::OnceLock<()> = std::sync::OnceLock::new();

fn init_otel() {
  OTEL_INIT.get_or_init(|| {
    deno::deno_telemetry::init(
      deno::versions::otel_runtime_config(),
      OtelConfig::default(),
    )
    .unwrap();
  });
}

const TLS_LOCALHOST_ROOT_CA: &[u8] =
  include_bytes!("./fixture/tls/root-ca.pem");
const TLS_LOCALHOST_CERT: &[u8] = include_bytes!("./fixture/tls/localhost.pem");
const TLS_LOCALHOST_KEY: &[u8] =
  include_bytes!("./fixture/tls/localhost-key.pem");

#[tokio::test]
#[serial]
async fn test_custom_readable_stream_response() {
  integration_test!(
    "./test_cases/main",
    NON_SECURE_PORT,
    "readable-stream-resp",
    None,
    None,
    None,
    (|resp| async {
      assert_eq!(
        resp.unwrap().text().await.unwrap(),
        "Hello world from streams"
      );
    }),
    TerminationToken::new()
  );
}

#[tokio::test]
#[serial]
async fn test_import_map_inlined() {
  integration_test!(
    "./test_cases/with-import-map",
    NON_SECURE_PORT,
    "",
    None,
    None,
    None,
    (|resp| async {
      let res = resp.unwrap();
      assert!(res.status().as_u16() == 200);

      let body_bytes = res.bytes().await.unwrap();
      assert_eq!(body_bytes, r#"{"message":"ok"}"#);
    }),
    TerminationToken::new()
  );
}

#[tokio::test]
#[serial]
async fn test_import_map_file_path() {
  integration_test!(
    "./test_cases/with-import-map-2",
    NON_SECURE_PORT,
    "",
    None,
    None,
    None,
    (|resp| async {
      let res = resp.unwrap();
      assert!(res.status().as_u16() == 200);

      let body_bytes = res.bytes().await.unwrap();
      assert_eq!(body_bytes, r#"{"message":"ok"}"#);
    }),
    TerminationToken::new()
  );
}

#[cfg(target_os = "linux")]
#[tokio::test]
#[serial]
async fn test_not_trigger_pku_sigsegv_due_to_jit_compilation_cli() {
  integration_test!(
    "./test_cases/main",
    NON_SECURE_PORT,
    "slow_resp",
    None,
    None,
    None,
    (|resp| async {
      assert!(resp.unwrap().text().await.unwrap().starts_with("meow: "));
    }),
    TerminationToken::new()
  );
}

#[cfg(target_os = "linux")]
#[tokio::test]
#[serial]
async fn test_not_trigger_pku_sigsegv_due_to_jit_compilation_non_cli() {
  let pool_termination_token = TerminationToken::new();
  let main_termination_token = TerminationToken::new();

  // create a user worker pool
  let (_, worker_pool_tx) = worker::create_user_worker_pool(
    Arc::default(),
    test_user_worker_pool_policy(),
    None,
    Some(pool_termination_token.clone()),
    vec![],
    None,
  )
  .await
  .unwrap();

  let surface = worker::WorkerSurfaceBuilder::new()
    .init_opts(WorkerContextInitOpts {
      service_path: "./test_cases/slow_resp".into(),
      no_module_cache: false,
      no_npm: None,
      env_vars: HashMap::new(),
      timing: None,
      maybe_eszip: None,
      maybe_entrypoint: None,
      maybe_module_code: None,
      conf: WorkerRuntimeOpts::MainWorker(MainWorkerRuntimeOpts {
        worker_pool_tx,
        shared_metric_src: None,
        event_worker_metric_src: None,
        context: None,
      }),
      static_patterns: vec![],

      maybe_s3_fs_config: None,
      maybe_tmp_fs_config: None,
      maybe_otel_config: None,
    })
    .termination_token(main_termination_token.clone())
    .build()
    .await
    .unwrap();

  let (res_tx, res_rx) =
    oneshot::channel::<Result<HttpResponse<Body>, hyper::Error>>();

  let req = Request::builder()
    .uri("/slow_resp")
    .method("GET")
    .body(Body::empty())
    .unwrap();

  let conn_token = CancellationToken::new();
  let msg = WorkerRequestMsg {
    req,
    res_tx,
    conn_token: Some(conn_token.clone()),
    idle_timed_out: Arc::new(AtomicBool::new(false)),
  };

  let _ = surface.msg_tx.send(msg);

  let res = res_rx.await.unwrap().unwrap();
  assert!(res.status().as_u16() == 200);

  let body_bytes = hyper::body::to_bytes(res.into_body()).await.unwrap();

  assert!(body_bytes.starts_with(b"meow: "));

  conn_token.cancel();
  pool_termination_token.cancel_and_wait().await;
  main_termination_token.cancel_and_wait().await;
}

#[tokio::test]
#[serial]
async fn test_main_worker_options_request() {
  let client = Client::new();
  let req = client
    .request(
      Method::OPTIONS,
      format!("http://localhost:{}/std_user_worker", NON_SECURE_PORT),
    )
    .body(Body::empty())
    .build()
    .unwrap();

  let original = RequestBuilder::from_parts(client, req);

  let request_builder = Some(original);

  integration_test!(
    "./test_cases/main",
    NON_SECURE_PORT,
    "",
    None,
    request_builder,
    None,
    (|resp| async {
      let res = resp.unwrap();
      assert!(res.status().as_u16() == 200);

      assert_eq!(
        res.headers().get("Access-Control-Allow-Origin").unwrap(),
        &"*"
      );
      assert_eq!(
        res.headers().get("Access-Control-Allow-Headers").unwrap(),
        &"authorization, x-client-info, apikey"
      );
    }),
    TerminationToken::new()
  );
}

#[tokio::test]
#[serial]
async fn test_main_worker_post_request() {
  let body_chunk = "{ \"name\": \"bar\"}";

  let content_length = &body_chunk.len();
  let chunks: Vec<Result<_, std::io::Error>> = vec![Ok(body_chunk)];
  let stream = futures_util::stream::iter(chunks);
  let body = Body::wrap_stream(stream);

  let client = Client::new();
  let req = client
    .request(
      Method::POST,
      format!("http://localhost:{}/std_user_worker", NON_SECURE_PORT),
    )
    .body(body)
    .header("Content-Type", "application/json")
    .header("Content-Length", content_length.to_string())
    .build()
    .unwrap();

  let original = RequestBuilder::from_parts(client, req);

  let request_builder = Some(original);

  integration_test!(
    "./test_cases/main",
    NON_SECURE_PORT,
    "",
    None,
    request_builder,
    None,
    (|resp| async {
      let res = resp.unwrap();
      assert!(res.status().as_u16() == 200);

      let body_bytes = res.bytes().await.unwrap();
      assert_eq!(body_bytes, "{\"message\":\"Hello bar from foo!\"}");
    }),
    TerminationToken::new()
  );
}

#[tokio::test]
#[serial]
async fn test_main_worker_boot_error() {
  let pool_termination_token = TerminationToken::new();
  let main_termination_token = TerminationToken::new();

  // create a user worker pool
  let (_, worker_pool_tx) = worker::create_user_worker_pool(
    Arc::default(),
    test_user_worker_pool_policy(),
    None,
    Some(pool_termination_token.clone()),
    vec![],
    None,
  )
  .await
  .unwrap();

  let result = worker::WorkerSurfaceBuilder::new()
    .init_opts(WorkerContextInitOpts {
      service_path: "./test_cases/meow".into(),
      no_module_cache: false,
      no_npm: None,
      env_vars: HashMap::new(),
      timing: None,
      maybe_eszip: None,
      maybe_entrypoint: None,
      maybe_module_code: None,
      conf: WorkerRuntimeOpts::MainWorker(MainWorkerRuntimeOpts {
        worker_pool_tx,
        shared_metric_src: None,
        event_worker_metric_src: None,
        context: None,
      }),
      static_patterns: vec![],

      maybe_s3_fs_config: None,
      maybe_tmp_fs_config: None,
      maybe_otel_config: None,
    })
    .termination_token(main_termination_token.clone())
    .build()
    .await;

  assert!(result.is_err());
  assert!(result
    .unwrap_err()
    .to_string()
    .starts_with("worker boot error"));

  pool_termination_token.cancel_and_wait().await;
  main_termination_token.cancel_and_wait().await;
}

#[tokio::test]
#[serial]
async fn test_main_worker_abort_request() {
  let body_chunk = "{ \"name\": \"bar\"}";

  let content_length = &body_chunk.len();
  let chunks: Vec<Result<_, std::io::Error>> = vec![Ok(body_chunk)];
  let stream = futures_util::stream::iter(chunks);
  let body = Body::wrap_stream(stream);

  let client = Client::new();
  let req = client
    .request(
      Method::POST,
      format!("http://localhost:{}/std_user_worker", NON_SECURE_PORT),
    )
    .body(body)
    .header("Content-Type", "application/json")
    .header("Content-Length", content_length.to_string())
    .build()
    .unwrap();

  let original = RequestBuilder::from_parts(client, req);

  let request_builder = Some(original);

  integration_test!(
    "./test_cases/main_with_abort",
    NON_SECURE_PORT,
    "",
    None,
    request_builder,
    None,
    (|resp| async {
      let res = resp.unwrap();
      assert!(res.status().as_u16() == 500);

      let body_bytes = res.bytes().await.unwrap();
      assert_eq!(
        body_bytes,
        "{\"msg\":\"AbortError: The signal has been aborted\"}"
      );
    }),
    TerminationToken::new()
  );
}

#[tokio::test]
#[serial]
async fn test_main_worker_with_jsx_function() {
  integration_test!(
    "./test_cases/main",
    NON_SECURE_PORT,
    "jsx",
    None,
    None,
    None,
    (|resp: Result<reqwest::Response, reqwest::Error>| async {
      let res = resp.unwrap();
      assert!(res.status().as_u16() == 200);

      let body_bytes = res.bytes().await.unwrap();
      assert_eq!(
        body_bytes,
        r#"{"type":"div","props":{"children":"Hello"},"__k":null,"__":null,"__b":0,"__e":null,"__c":null,"__v":-1,"__i":-1,"__u":0}"#
      );
    }),
    TerminationToken::new()
  );
}

#[tokio::test]
async fn test_main_worker_user_worker_mod_evaluate_exception() {
  let pool_termination_token = TerminationToken::new();
  let main_termination_token = TerminationToken::new();

  // create a user worker pool
  let (_, worker_pool_tx) = worker::create_user_worker_pool(
    Arc::default(),
    test_user_worker_pool_policy(),
    None,
    Some(pool_termination_token.clone()),
    vec![],
    None,
  )
  .await
  .unwrap();

  let surface = worker::WorkerSurfaceBuilder::new()
    .init_opts(WorkerContextInitOpts {
      service_path: "./test_cases/main".into(),
      no_module_cache: false,
      no_npm: None,
      env_vars: HashMap::new(),
      timing: None,
      maybe_eszip: None,
      maybe_entrypoint: None,
      maybe_module_code: None,
      conf: WorkerRuntimeOpts::MainWorker(MainWorkerRuntimeOpts {
        worker_pool_tx,
        shared_metric_src: None,
        event_worker_metric_src: None,
        context: None,
      }),
      static_patterns: vec![],

      maybe_s3_fs_config: None,
      maybe_tmp_fs_config: None,
      maybe_otel_config: None,
    })
    .termination_token(main_termination_token.clone())
    .build()
    .await
    .unwrap();

  let (res_tx, res_rx) =
    oneshot::channel::<Result<HttpResponse<Body>, hyper::Error>>();

  let req = Request::builder()
    .uri("/boot_err_user_worker")
    .method("GET")
    .body(Body::empty())
    .unwrap();

  let conn_token = CancellationToken::new();
  let msg = WorkerRequestMsg {
    req,
    res_tx,
    conn_token: Some(conn_token.clone()),
    idle_timed_out: Arc::new(AtomicBool::new(false)),
  };

  let _ = surface.msg_tx.send(msg);

  let res = res_rx.await.unwrap().unwrap();
  assert!(res.status().as_u16() == 500);

  let body_bytes = to_bytes(res.into_body()).await.unwrap();

  assert!(body_bytes.starts_with(b"{\"msg\":\"InvalidWorkerResponse"));
}

async fn test_main_worker_post_request_with_transfer_encoding(
  maybe_tls: Option<Tls>,
) {
  let chunks: Vec<Result<_, std::io::Error>> =
    vec![Ok("{\"name\":"), Ok("\"bar\"}")];
  let stream = futures_util::stream::iter(chunks);
  let body = Body::wrap_stream(stream);

  let client = maybe_tls.client();
  let req = client
    .request(
      Method::POST,
      format!(
        "{}://localhost:{}/std_user_worker",
        maybe_tls.schema(),
        maybe_tls.port(),
      ),
    )
    .body(body)
    .header("Transfer-Encoding", "chunked")
    .build()
    .unwrap();

  let original = RequestBuilder::from_parts(client, req);
  let request_builder = Some(original);

  integration_test!(
    "./test_cases/main",
    NON_SECURE_PORT,
    "",
    None,
    request_builder,
    maybe_tls,
    (|resp| async {
      let res = resp.unwrap();
      assert!(res.status().as_u16() == 200);

      let body_bytes = res.bytes().await.unwrap();
      assert_eq!(body_bytes, "{\"message\":\"Hello bar from foo!\"}");
    }),
    TerminationToken::new()
  );
}

#[tokio::test]
#[serial]
async fn test_main_worker_post_request_with_transfer_encoding_non_secure() {
  test_main_worker_post_request_with_transfer_encoding(new_localhost_tls(
    false,
  ))
  .await;
}

#[tokio::test]
#[serial]
async fn test_main_worker_post_request_with_transfer_encoding_secure() {
  test_main_worker_post_request_with_transfer_encoding(new_localhost_tls(true))
    .await;
}

#[tokio::test]
#[serial]
async fn test_null_body_with_204_status() {
  integration_test!(
    "./test_cases/empty-response",
    NON_SECURE_PORT,
    "",
    None,
    None,
    None,
    (|resp| async {
      let res = resp.unwrap();
      assert!(res.status().as_u16() == 204);

      let body_bytes = res.bytes().await.unwrap();
      assert_eq!(body_bytes.len(), 0);
    }),
    TerminationToken::new()
  );
}

#[tokio::test]
#[serial]
async fn test_null_body_with_204_status_post() {
  let client = Client::new();
  let req = client
    .request(
      Method::POST,
      format!("http://localhost:{}", NON_SECURE_PORT),
    )
    .body(Body::empty())
    .build()
    .unwrap();

  let original = RequestBuilder::from_parts(client, req);

  let request_builder = Some(original);

  integration_test!(
    "./test_cases/empty-response",
    NON_SECURE_PORT,
    "",
    None,
    request_builder,
    None,
    (|resp| async {
      let res = resp.unwrap();
      assert!(res.status().as_u16() == 204);

      let body_bytes = res.bytes().await.unwrap();
      assert_eq!(body_bytes.len(), 0);
    }),
    TerminationToken::new()
  );
}

#[tokio::test]
#[serial]
async fn test_oak_server() {
  integration_test!(
    "./test_cases/oak",
    NON_SECURE_PORT,
    "oak",
    None,
    None,
    None,
    (|resp| async {
      let res = resp.unwrap();
      assert!(res.status().as_u16() == 200);

      let body_bytes = res.bytes().await.unwrap();
      assert_eq!(
        body_bytes,
        "This is an example Oak server running on Edge Functions!"
      );
    }),
    TerminationToken::new()
  );
}

#[tokio::test]
#[serial]
async fn test_file_upload() {
  let body_chunk = concat!(
    "--TEST\r\n",
    "Content-Disposition: form-data; name=\"file\"; filename=\"test.txt\"\r\n",
    "Content-Type: text/plain\r\n",
    "\r\n",
    "testuser\r\n",
    "--TEST--\r\n"
  );

  let content_length = &body_chunk.len();
  let chunks: Vec<Result<_, std::io::Error>> = vec![Ok(body_chunk)];
  let stream = futures_util::stream::iter(chunks);
  let body = Body::wrap_stream(stream);

  let client = Client::new();
  let req = client
    .request(
      Method::POST,
      format!("http://localhost:{}/file-upload", NON_SECURE_PORT),
    )
    .header("Content-Type", "multipart/form-data; boundary=TEST")
    .header("Content-Length", content_length.to_string())
    .body(body)
    .build()
    .unwrap();

  let original = RequestBuilder::from_parts(client, req);
  let request_builder = Some(original);

  integration_test!(
    "./test_cases/oak-v12-file-upload",
    NON_SECURE_PORT,
    "",
    None,
    request_builder,
    None,
    (|resp| async {
      let res = resp.unwrap();
      assert_eq!(res.status().as_u16(), 201);

      let body_bytes = res.bytes().await.unwrap();
      assert_eq!(body_bytes, "file-type: text/plain");
    }),
    TerminationToken::new()
  );
}

#[tokio::test]
#[serial]
async fn test_file_upload_real_multipart_bytes() {
  test_oak_file_upload(
    Cow::Borrowed("./test_cases/main"),
    (9.98 * MB as f32) as usize, // < 10MB (in binary)
    None,
    |resp| async {
      let res = resp.unwrap();

      assert_eq!(res.status().as_u16(), 201);

      let res = res.text().await;

      assert!(res.is_ok());
      assert_eq!(res.unwrap(), "Success!");
    },
  )
  .await;
}

#[tokio::test]
#[serial]
async fn test_file_upload_size_exceed() {
  test_oak_file_upload(
    Cow::Borrowed("./test_cases/main"),
    10 * MB,
    None,
    |resp| async {
      let res = resp.unwrap();

      assert_eq!(res.status().as_u16(), 500);

      let res = res.text().await;

      assert!(res.is_ok());
      assert_eq!(res.unwrap(), "Error!");
    },
  )
  .await;
}

#[tokio::test]
#[serial]
async fn test_node_server() {
  integration_test!(
    "./test_cases/node-server",
    NON_SECURE_PORT,
    "",
    None,
    None,
    None,
    (|resp| async {
      let res = resp.unwrap();
      assert_eq!(res.status().as_u16(), 200);
      let body_bytes = res.bytes().await.unwrap();
      assert_eq!(
        body_bytes,
        concat!(
          "Look again at that dot. That's here. That's home. That's us. On it everyone you love, ",
          "everyone you know, everyone you ever heard of, every human being who ever was, lived out ",
          "their lives. The aggregate of our joy and suffering, thousands of confident religions, ideologies, ",
          "and economic doctrines, every hunter and forager, every hero and coward, every creator and destroyer of ",
          "civilization, every king and peasant, every young couple in love, every mother and father, hopeful child, ",
          "inventor and explorer, every teacher of morals, every corrupt politician, every 'superstar,' every 'supreme leader,' ",
          "every saint and sinner in the history of our species lived there-on a mote of dust suspended in a sunbeam."
        )
      );
    }),
    TerminationToken::new()
  );
}

#[tokio::test]
#[serial]
async fn test_tls_throw_invalid_data() {
  integration_test!(
    "./test_cases/tls_invalid_data",
    NON_SECURE_PORT,
    "",
    None,
    None,
    None,
    (|resp| async {
      let res = resp.unwrap();
      assert!(res.status().as_u16() == 200);

      let body_bytes = res.bytes().await.unwrap();
      assert_eq!(body_bytes, r#"{"passed":true}"#);
    }),
    TerminationToken::new()
  );
}

#[tokio::test]
#[serial]
async fn test_user_worker_json_imports() {
  integration_test!(
    "./test_cases/json_import",
    NON_SECURE_PORT,
    "",
    None,
    None,
    None,
    (|resp| async {
      let res = resp.unwrap();
      assert!(res.status().as_u16() == 200);

      let body_bytes = res.bytes().await.unwrap();
      assert_eq!(body_bytes, r#"{"version":"1.0.0"}"#);
    }),
    TerminationToken::new()
  );
}

#[tokio::test]
#[serial]
async fn test_user_imports_npm() {
  integration_test!(
    "./test_cases/npm",
    NON_SECURE_PORT,
    "",
    None,
    None,
    None,
    (|resp| async {
      let res = resp.unwrap();
      assert!(res.status().as_u16() == 200);

      let body_bytes = res.bytes().await.unwrap();
      assert_eq!(
        body_bytes,
        r#"{"is_even":true,"hello":"","numbers":{"Uno":1,"Dos":2}}"#
      );
    }),
    TerminationToken::new()
  );
}

#[tokio::test]
#[serial]
async fn test_worker_boot_invalid_imports() {
  let opts = WorkerContextInitOpts {
    service_path: "./test_cases/invalid_imports".into(),
    no_module_cache: false,
    no_npm: None,
    env_vars: HashMap::new(),
    timing: None,
    maybe_eszip: None,
    maybe_entrypoint: None,
    maybe_module_code: None,
    conf: WorkerRuntimeOpts::UserWorker(test_user_runtime_opts()),
    static_patterns: vec![],

    maybe_s3_fs_config: None,
    maybe_tmp_fs_config: None,
    maybe_otel_config: None,
  };

  let result = create_test_user_worker(opts).await;

  assert!(result.is_err());
  assert!(result
    .unwrap_err()
    .to_string()
    .starts_with("worker boot error"));
}

#[tokio::test]
#[serial]
async fn test_worker_boot_with_0_byte_eszip() {
  let opts = WorkerContextInitOpts {
    service_path: "./test_cases/meow".into(),
    no_module_cache: false,
    no_npm: None,
    env_vars: HashMap::new(),
    timing: None,
    maybe_eszip: Some(EszipPayloadKind::VecKind(vec![])),
    maybe_entrypoint: Some("file:///src/index.ts".to_string()),
    maybe_module_code: None,
    conf: WorkerRuntimeOpts::UserWorker(test_user_runtime_opts()),
    static_patterns: vec![],

    maybe_s3_fs_config: None,
    maybe_tmp_fs_config: None,
    maybe_otel_config: None,
  };

  let result = create_test_user_worker(opts).await;

  assert!(result.is_err());
  assert!(format!("{:#}", result.unwrap_err()).starts_with(
    "worker boot error: failed to bootstrap runtime: unexpected end of file"
  ));
}

#[tokio::test]
#[serial]
async fn test_worker_boot_with_invalid_entrypoint() {
  let opts = WorkerContextInitOpts {
    service_path: "./test_cases/meow".into(),
    no_module_cache: false,
    no_npm: None,
    env_vars: HashMap::new(),
    timing: None,
    maybe_eszip: None,
    maybe_entrypoint: Some("file:///meow/mmmmeeeow.ts".to_string()),
    maybe_module_code: None,
    conf: WorkerRuntimeOpts::UserWorker(test_user_runtime_opts()),
    static_patterns: vec![],

    maybe_s3_fs_config: None,
    maybe_tmp_fs_config: None,
    maybe_otel_config: None,
  };

  let result = create_test_user_worker(opts).await;

  assert!(result.is_err());
  assert!(format!("{:#}", result.unwrap_err())
    .starts_with("worker boot error: failed to bootstrap runtime: failed to determine entrypoint"));
}

#[tokio::test]
#[serial]
async fn req_failure_case_timeout() {
  let tb = TestBedBuilder::new("./test_cases/main")
    // NOTE: It should be small enough that the worker pool rejects the
    // request.
    .with_oneshot_policy(Some(10))
    .build()
    .await;

  let req_body_fn = |b: http::request::Builder| {
    b.uri("/slow_resp")
      .method("GET")
      .body(Body::empty())
      .context("can't make request")
  };

  let (res1, res2) = join!(tb.request(req_body_fn), tb.request(req_body_fn));

  let res_iter = vec![res1, res2].into_iter();
  let mut found_timeout = false;

  for res in res_iter {
    let mut res = res.unwrap();

    if !found_timeout {
      let buf = to_bytes(res.body_mut()).await.unwrap();
      let status_500 = res.status() == StatusCode::INTERNAL_SERVER_ERROR;
      let valid_output =
                buf == "{\"msg\":\"InvalidWorkerCreation: worker did not respond in time\"}";

      found_timeout = status_500 && valid_output;
    }
  }

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
  assert!(found_timeout);
}

#[tokio::test]
#[serial]
async fn req_failure_case_cpu_time_exhausted() {
  let tb = TestBedBuilder::new("./test_cases/main_small_cpu_time")
    .with_oneshot_policy(None)
    .build()
    .await;

  let mut res = tb
    .request(|b| {
      b.uri("/slow_resp")
        .method("GET")
        .body(Body::empty())
        .context("can't make request")
    })
    .await
    .unwrap();

  let buf = to_bytes(res.body_mut()).await.unwrap();

  assert_eq!(
    buf,
    "{\"msg\":\"WorkerRequestCancelled: request has been cancelled by supervisor\"}"
  );

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn req_failure_case_cpu_time_exhausted_2() {
  let tb = TestBedBuilder::new("./test_cases/main_small_cpu_time")
    .with_oneshot_policy(None)
    .build()
    .await;

  let mut res = tb
    .request(|b| {
      b.uri("/cpu-sync")
        .method("GET")
        .body(Body::empty())
        .context("can't make request")
    })
    .await
    .unwrap();

  let buf = to_bytes(res.body_mut()).await.unwrap();

  assert_eq!(
    buf,
    "{\"msg\":\"WorkerRequestCancelled: request has been cancelled by supervisor\"}"
  );

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn req_failure_case_wall_clock_reached() {
  let tb = TestBedBuilder::new("./test_cases/main_small_wall_clock")
    .with_oneshot_policy(None)
    .build()
    .await;

  let mut res = tb
    .request(|b| {
      b.uri("/slow_resp")
        .method("GET")
        .body(Body::empty())
        .context("can't make request")
    })
    .await
    .unwrap();

  let buf = to_bytes(res.body_mut()).await.unwrap();

  assert!(
    buf == "{\"msg\":\"InvalidWorkerResponse: user worker failed to respond\"}"
    || buf
      == "{\"msg\":\"WorkerRequestCancelled: request has been cancelled by supervisor\"}"
  );

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn req_failture_case_memory_limit_1() {
  let tb = TestBedBuilder::new("./test_cases/main")
    .with_oneshot_policy(None)
    .build()
    .await;

  let mut res = tb
    .request(|b| {
      b.uri("/array-alloc-sync")
        .method("GET")
        .body(Body::empty())
        .context("can't make request")
    })
    .await
    .unwrap();

  let buf = to_bytes(res.body_mut()).await.unwrap();

  assert_eq!(
    buf,
    "{\"msg\":\"WorkerRequestCancelled: request has been cancelled by supervisor\"}"
  );

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn req_failture_case_memory_limit_2() {
  let tb = TestBedBuilder::new("./test_cases/main")
    .with_oneshot_policy(None)
    .build()
    .await;

  let mut res = tb
    .request(|b| {
      b.uri("/array-alloc")
        .method("GET")
        .body(Body::empty())
        .context("can't make request")
    })
    .await
    .unwrap();

  let buf = to_bytes(res.body_mut()).await.unwrap();

  assert_eq!(
    buf,
    "{\"msg\":\"WorkerRequestCancelled: request has been cancelled by supervisor\"}"
  );

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn req_failure_case_wall_clock_reached_less_than_100ms() {
  // TODO(Nyannyacha): This test seems a little flaky. If running the entire test
  // dozens of times on the local machine, it will fail with a timeout.

  let tb =
    TestBedBuilder::new("./test_cases/main_small_wall_clock_less_than_100ms")
      .with_oneshot_policy(None)
      .build()
      .await;

  let mut res = tb
    .request(|b| {
      b.uri("/slow_resp")
        .method("GET")
        .body(Body::empty())
        .context("can't make request")
    })
    .await
    .unwrap();

  let buf = to_bytes(res.body_mut()).await.unwrap();

  assert!(
    buf == "{\"msg\":\"InvalidWorkerResponse: user worker failed to respond\"}"
    || buf == "{\"msg\":\"InvalidWorkerCreation: worker did not respond in time\"}"
    || buf
      == "{\"msg\":\"WorkerRequestCancelled: request has been cancelled by supervisor\"}"
  );

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

async fn req_failure_case_intentional_peer_reset(maybe_tls: Option<Tls>) {
  let (server_ev_tx, mut server_ev_rx) = mpsc::unbounded_channel();

  integration_test!(
    "./test_cases/main",
    NON_SECURE_PORT,
    "slow_resp",
    None,
    None,
    maybe_tls.clone(),
    (
      |(_, url, _, mut ev, ..)| async move {
        tokio::spawn(async move {
          loop {
            tokio::select! {
              Some(ev) = ev.recv() => {
                let _ = server_ev_tx.send(ev);
                break;
              }

              else => continue
            }
          }
        });

        Some(
          maybe_tls
            .client()
            .get(format!(
              "{}://localhost:{}/{}",
              maybe_tls.schema(),
              maybe_tls.port(),
              url
            ))
            .timeout(Duration::from_millis(100))
            .send()
            .await,
        )
      },
      |_| async {}
    ),
    TerminationToken::new()
  );

  let ev = loop {
    tokio::select! {
      Some(ev) = server_ev_rx.recv() => break ev,
      else => continue,
    }
  };

  assert!(
    matches!(ev, ServerEvent::ConnectionError(e) if e.is_incomplete_message())
  );
}

#[tokio::test]
#[serial]
async fn req_failure_case_intentional_peer_reset_non_secure() {
  req_failure_case_intentional_peer_reset(new_localhost_tls(false)).await;
}

#[tokio::test]
#[serial]
async fn req_failure_case_intentional_peer_reset_secure() {
  req_failure_case_intentional_peer_reset(new_localhost_tls(true)).await;
}

#[tokio::test]
#[serial]
async fn req_failure_case_op_cancel_from_server_due_to_cpu_resource_limit() {
  test_oak_file_upload(
    Cow::Borrowed("./test_cases/main_small_cpu_time"),
    120 * MB,
    None,
    |resp| async {
      assert_op_cancel_from_server_response(resp).await;
    },
  )
  .await;
}

#[tokio::test]
#[serial]
async fn req_failure_case_op_cancel_from_server_due_to_cpu_resource_limit_2() {
  test_oak_file_upload(
    Cow::Borrowed("./test_cases/main_small_cpu_time"),
    10 * MB,
    Some("image/png"),
    |resp| async {
      assert_op_cancel_from_server_response(resp).await;
    },
  )
  .await;
}

/// When the supervisor tears down a user worker that exceeded its CPU limit,
/// two paths race each other and both outcomes are correct:
///
/// 1. The connection token is canceled before the main worker's response is
///    handed back to the server, so the server serves 503 on its own.
/// 2. The main worker's `WorkerRequestCancelled` handler wins the race, and its
///    own 500 payload is relayed to the client untouched.
///
/// Which one wins depends on scheduling alone, so accept either, but keep
/// asserting the shape of the response so that unrelated failures (most
/// notably the request body being detached from its receiver) are still caught.
async fn assert_op_cancel_from_server_response(
  resp: Result<Response, reqwest::Error>,
) {
  let res = resp.unwrap();
  let status = res.status().as_u16();
  let served_by = res
    .headers()
    .get("x-served-by")
    .map(|v| v.to_str().unwrap().to_owned());

  match status {
    503 => {
      assert_eq!(
        served_by.as_deref(),
        Some(concat!(env!("CARGO_PKG_NAME"), "/server"))
      );
    }

    500 => {
      assert_eq!(served_by, None);

      let payload = res.json::<ErrorResponsePayload>().await;

      assert!(payload.is_ok());

      let msg = payload.unwrap().msg;

      assert!(
        !msg.starts_with("TypeError: request body receiver not connected"),
        "unexpected error message: {msg}"
      );
      assert!(
        msg
          == "WorkerRequestCancelled: request has been cancelled by supervisor"
          || msg == "broken pipe",
        "unexpected error message: {msg}"
      );
    }

    _ => panic!("unexpected status code: {status}"),
  }
}

async fn test_oak_file_upload<F, R>(
  main_service: Cow<'static, str>,
  bytes: usize,
  mime: Option<&str>,
  resp_callback: F,
) where
  F: FnOnce(Result<Response, reqwest::Error>) -> R,
  R: Future<Output = ()>,
{
  let client = Client::builder().build().unwrap();
  let req = client
    .request(
      Method::POST,
      format!("http://localhost:{}/oak-file-upload", NON_SECURE_PORT),
    )
    .multipart(
      Form::new().part(
        "meow",
        Part::bytes(vec![0u8; bytes])
          .file_name("meow.bin")
          .mime_str(mime.unwrap_or("application/octet-stream"))
          .unwrap(),
      ),
    )
    .build()
    .unwrap();

  let original = RequestBuilder::from_parts(client, req);
  let request_builder = Some(original);

  integration_test_with_server_flag!(
    ServerFlags {
      request_buffer_size: Some(1024),
      ..Default::default()
    },
    main_service,
    NON_SECURE_PORT,
    "",
    None,
    request_builder,
    None,
    (|resp| async {
      resp_callback(resp).await;
    }),
    TerminationToken::new()
  );
}

async fn test_websocket_upgrade(maybe_tls: Option<Tls>, use_node_ws: bool) {
  let nonce = tungstenite::handshake::client::generate_key();
  let client = maybe_tls.client();
  let req = client
    .request(
      Method::GET,
      format!(
        "{}://localhost:{}/websocket-upgrade{}",
        maybe_tls.schema(),
        maybe_tls.port(),
        if use_node_ws { "-node" } else { "" }
      ),
    )
    .header(header::CONNECTION, "upgrade")
    .header(header::UPGRADE, "websocket")
    .header(header::SEC_WEBSOCKET_KEY, &nonce)
    .header(header::SEC_WEBSOCKET_VERSION, "13")
    .build()
    .unwrap();

  let original = RequestBuilder::from_parts(client, req);
  let request_builder = Some(original);

  integration_test!(
    "./test_cases/main",
    NON_SECURE_PORT,
    "",
    None,
    request_builder,
    maybe_tls,
    (|resp| async {
      let res = resp.unwrap();
      let accepted = get_upgrade_type(res.headers());

      assert!(res.status().as_u16() == 101);
      assert!(accepted.is_some());
      assert_eq!(accepted.as_ref().unwrap(), "websocket");

      let upgraded = res.upgrade().await.unwrap();
      let mut ws = WebSocketStream::from_raw_socket(
        upgraded.compat(),
        tungstenite::protocol::Role::Client,
        None,
      )
      .await;

      assert_eq!(
        ws.next().await.unwrap().unwrap().into_text().unwrap(),
        "meow"
      );

      ws.send(Message::Text("meow!!".into())).await.unwrap();
      assert_eq!(
        ws.next().await.unwrap().unwrap().into_text().unwrap(),
        "meow!!"
      );
    }),
    TerminationToken::new()
  );
}

#[tokio::test]
#[serial]
async fn test_graceful_shutdown() {
  let token = TerminationToken::new();

  let (server_ev_tx, mut server_ev_rx) = mpsc::unbounded_channel();
  let (metric_tx, metric_rx) = oneshot::channel::<SharedMetricSource>();
  let (tx, rx) = oneshot::channel::<()>();

  tokio::spawn({
    let token = token.clone();
    async move {
      let metric_src = metric_rx.await.unwrap();

      while metric_src.active_io() == 0 {
        tokio::task::yield_now().await;
      }

      assert_eq!(metric_src.active_io(), 1);
      token.cancel();

      tokio::select! {
        Some(ServerEvent::Draining) = server_ev_rx.recv() => {
          assert_eq!(metric_src.handled_requests(), 0);
        }

        else => {
          panic!("event sequence does not match != ServerEvent::Draining");
        }
      }

      while metric_src.active_io() > 0 {
        tokio::task::yield_now().await;
      }

      if timeout(Duration::from_secs(10), token.cancel_and_wait())
        .await
        .is_err()
      {
        panic!("failed to terminate server within 10 seconds");
      }

      assert_eq!(metric_src.active_io(), 0);
      assert_eq!(metric_src.handled_requests(), 1);
      assert_eq!(
        metric_src.received_requests(),
        metric_src.handled_requests()
      );

      tx.send(()).unwrap();
    }
  });

  integration_test!(
    "./test_cases/main",
    NON_SECURE_PORT,
    "slow_resp",
    None,
    None,
    None,
    (
      |(.., mut ev, metric_src)| async move {
        metric_tx.send(metric_src).unwrap();
        tokio::spawn(async move {
          while let Some(ev) = ev.recv().await {
            let _ = server_ev_tx.send(ev);
          }
        });

        None
      },
      |resp| async {
        assert_eq!(resp.unwrap().status().as_u16(), 200);
      }
    ),
    #[manual]
    token
  );

  if timeout(Duration::from_secs(10), rx).await.is_err() {
    panic!("failed to check within 10 seconds");
  }
}

#[tokio::test]
#[serial]
async fn test_websocket_upgrade_deno_non_secure() {
  test_websocket_upgrade(new_localhost_tls(false), false).await;
}

#[tokio::test]
#[serial]
async fn test_websocket_upgrade_deno_secure() {
  test_websocket_upgrade(new_localhost_tls(true), false).await;
}

#[tokio::test]
#[serial]
async fn test_websocket_upgrade_node_non_secure() {
  test_websocket_upgrade(new_localhost_tls(false), true).await;
}

#[tokio::test]
#[serial]
async fn test_websocket_upgrade_node_secure() {
  test_websocket_upgrade(new_localhost_tls(true), true).await;
}

async fn test_decorators(suffix: &str) {
  let client = Client::new();
  let req = client
    .request(
      Method::OPTIONS,
      format!("http://localhost:{}/decorator_{}", NON_SECURE_PORT, suffix),
    )
    .build()
    .unwrap();

  integration_test!(
    "./test_cases/main",
    NON_SECURE_PORT,
    "",
    None,
    Some(RequestBuilder::from_parts(client, req)),
    None,
    (|resp| async {
      let resp = resp.unwrap();

      assert_eq!(resp.status(), StatusCode::OK);
      assert_eq!(resp.text().await.unwrap().as_str(), "meow?");
    }),
    TerminationToken::new()
  );
}

#[tokio::test]
#[serial]
async fn test_decorator_parse_tc39() {
  test_decorators("tc39").await;
}

#[tokio::test]
#[serial]
async fn test_decorator_parse_typescript_experimental_with_metadata() {
  test_decorators("typescript_with_metadata").await;
}

#[tokio::test]
#[serial]
async fn send_partial_payload_into_closed_pipe_should_not_be_affected_worker_stability(
) {
  let tb = TestBedBuilder::new("./test_cases/main")
    .with_oneshot_policy(None)
    .build()
    .await;

  let mut resp1 = tb
    .request(|b| {
      b.uri("/chunked-char-1000ms")
        .method("GET")
        .body(Body::empty())
        .context("can't make request")
    })
    .await
    .unwrap();

  assert_eq!(resp1.status().as_u16(), StatusCode::OK);

  let resp1_body = resp1.body_mut();
  let resp1_chunk1 = resp1_body.next().await.unwrap().unwrap();

  assert_eq!(resp1_chunk1, "m");

  drop(resp1);
  sleep(Duration::from_secs(1)).await;

  // NOTE(Nyannyacha): Before dc057b0, the statement below panics with the
  // reason `connection closed before message completed`. This is the result
  // of `Deno.serve` failing to properly handle an exception from a previous
  // request.
  let resp2 = tb
    .request(|b| {
      b.uri("/empty-response")
        .method("GET")
        .body(Body::empty())
        .context("can't make request")
    })
    .await
    .unwrap();

  assert_eq!(resp2.status().as_u16(), StatusCode::NO_CONTENT);

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn oak_with_jsr_specifier() {
  integration_test!(
    "./test_cases/main",
    NON_SECURE_PORT,
    "oak-with-jsr",
    None,
    None,
    None,
    (|resp| async {
      assert_eq!(resp.unwrap().text().await.unwrap(), "meow");
    }),
    TerminationToken::new()
  );
}

async fn test_slowloris<F, R>(
  request_read_timeout_ms: u64,
  maybe_tls: Option<Tls>,
  test_fn: F,
) where
  F: (FnOnce(Box<dyn AsyncReadWrite>) -> R) + Send + 'static,
  R: Future<Output = bool> + Send,
{
  let token = TerminationToken::new();

  let (health_tx, mut health_rx) = mpsc::channel(1);
  let (tx, rx) = oneshot::channel();

  let mut listen_fut = integration_test_listen_fut!(
    NON_SECURE_PORT,
    maybe_tls,
    "./test_cases/main",
    None,
    ServerFlags {
      request_read_timeout_ms: Some(request_read_timeout_ms),
      ..Default::default()
    },
    health_tx,
    Some(token.clone())
  );

  let req_fut = {
    let token = token.clone();
    async move {
      assert!(test_fn(maybe_tls.stream().await).await);

      if timeout(Duration::from_secs(10), token.cancel_and_wait())
        .await
        .is_err()
      {
        panic!("failed to terminate server within 10 seconds");
      }

      tx.send(()).unwrap();
    }
  };

  let join_fut = tokio::spawn(async move {
    loop {
      if let Some(ServerHealth::Listening(..)) = health_rx.recv().await {
        break;
      }
    }

    req_fut.await;
  });

  tokio::select! {
    _ = join_fut => {}
    _ = &mut listen_fut => {}
  };

  if timeout(Duration::from_secs(10), rx).await.is_err() {
    panic!("failed to check within 10 seconds");
  }
}

async fn test_slowloris_no_prompt_timeout(
  maybe_tls: Option<Tls>,
  invert: bool,
) {
  test_slowloris(
    if invert { u64::MAX } else { 5000 },
    maybe_tls,
    move |mut io| async move {
      static HEADER: &[u8] =
        b"GET /oak-with-jsr HTTP/1.1\r\nHost: localhost\r\n\r\n";

      let check_io_kind_fn = move |err: std::io::Error| {
        if invert {
          return true;
        }

        matches!(
          err.kind(),
          io::ErrorKind::BrokenPipe | io::ErrorKind::UnexpectedEof
        )
      };

      // > 5000ms
      sleep(Duration::from_secs(10)).await;

      if let Err(err) = io.write_all(HEADER).await {
        return check_io_kind_fn(err);
      }

      if let Err(err) = io.flush().await {
        return check_io_kind_fn(err);
      }

      let mut buf = vec![0; 1_048_576];

      match io.read(&mut buf).await {
        Ok(nread) => {
          if invert {
            nread > 0
          } else {
            nread == 0
          }
        }

        Err(err) => check_io_kind_fn(err),
      }
    },
  )
  .await;
}

#[tokio::test]
#[serial]
async fn test_slowloris_no_prompt_timeout_non_secure() {
  test_slowloris_no_prompt_timeout(new_localhost_tls(false), false).await;
}

#[tokio::test]
#[serial]
#[ignore = "too slow"]
async fn test_slowloris_no_prompt_timeout_non_secure_inverted() {
  test_slowloris_no_prompt_timeout(new_localhost_tls(false), true).await;
}

#[tokio::test]
#[serial]
async fn test_slowloris_no_prompt_timeout_secure() {
  test_slowloris_no_prompt_timeout(new_localhost_tls(true), false).await;
}

#[tokio::test]
#[serial]
#[ignore = "too slow"]
async fn test_slowloris_no_prompt_timeout_secure_inverted() {
  test_slowloris_no_prompt_timeout(new_localhost_tls(true), true).await;
}

async fn test_slowloris_slow_header_timedout(
  maybe_tls: Option<Tls>,
  invert: bool,
) {
  test_slowloris(
    if invert { u64::MAX } else { 5000 },
    maybe_tls,
    move |mut io| async move {
      static HEADER: &[u8] =
        b"GET /oak-with-jsr HTTP/1.1\r\nHost: localhost\r\n\r\n";

      let check_io_kind_fn = move |err: std::io::Error| {
        if invert {
          return true;
        }

        matches!(
          err.kind(),
          io::ErrorKind::BrokenPipe | io::ErrorKind::UnexpectedEof
        )
      };

      // takes 1000ms per each character (ie. > 5000ms)
      for &b in HEADER {
        if let Err(err) = io.write(&[b]).await {
          return check_io_kind_fn(err);
        }

        if let Err(err) = io.flush().await {
          return check_io_kind_fn(err);
        }

        sleep(Duration::from_secs(1)).await;
      }

      let mut buf = vec![0; 1_048_576];

      match io.read(&mut buf).await {
        Ok(nread) => {
          if invert {
            nread > 0
          } else {
            nread == 0
          }
        }

        Err(err) => check_io_kind_fn(err),
      }
    },
  )
  .await;
}

#[tokio::test]
#[serial]
async fn test_slowloris_slow_header_timedout_non_secure() {
  test_slowloris_slow_header_timedout(new_localhost_tls(false), false).await;
}

#[tokio::test]
#[serial]
#[ignore = "too slow 2x"]
async fn test_slowloris_slow_header_timedout_non_secure_inverted() {
  test_slowloris_slow_header_timedout(new_localhost_tls(false), true).await;
}

#[tokio::test]
#[serial]
async fn test_slowloris_slow_header_timedout_secure() {
  test_slowloris_slow_header_timedout(new_localhost_tls(true), false).await;
}

#[tokio::test]
#[serial]
#[ignore = "too slow 2x"]
async fn test_slowloris_slow_header_timedout_secure_inverted() {
  test_slowloris_slow_header_timedout(new_localhost_tls(true), true).await;
}

async fn test_request_idle_timeout_no_streamed_response(
  maybe_tls: Option<Tls>,
) {
  let client = maybe_tls.client();
  let req = client
    .request(
      Method::GET,
      format!(
        "{}://localhost:{}/sleep-5000ms",
        maybe_tls.schema(),
        maybe_tls.port(),
      ),
    )
    .build()
    .unwrap();

  let original = RequestBuilder::from_parts(client, req);
  let request_builder = Some(original);

  integration_test_with_server_flag!(
    ServerFlags {
      request_idle_timeout: RequestIdleTimeout::from_millis(None, Some(1000)),
      ..Default::default()
    },
    "./test_cases/main",
    NON_SECURE_PORT,
    "",
    None,
    request_builder,
    maybe_tls,
    (|resp| async {
      let resp = resp.unwrap();
      assert_eq!(resp.status().as_u16(), StatusCode::GATEWAY_TIMEOUT);
      let body = resp.bytes().await.unwrap();
      assert!(
        std::str::from_utf8(&body)
          .unwrap()
          .contains("WorkerRequestIdleTimeout"),
        "expected WorkerRequestIdleTimeout in body"
      );
    }),
    TerminationToken::new()
  );
}

#[tokio::test]
#[serial]
async fn test_request_idle_timeout_no_streamed_response_non_secure() {
  test_request_idle_timeout_no_streamed_response(new_localhost_tls(false))
    .await;
}

#[tokio::test]
#[serial]
#[ignore = "running too much tests"]
async fn test_request_idle_timeout_no_streamed_response_non_secure_1000() {
  for _ in 0..1000 {
    test_request_idle_timeout_no_streamed_response(new_localhost_tls(false))
      .await;
  }
}

#[tokio::test]
#[serial]
async fn test_request_idle_timeout_no_streamed_response_secure() {
  test_request_idle_timeout_no_streamed_response(new_localhost_tls(true)).await;
}

async fn test_request_idle_timeout_streamed_response(maybe_tls: Option<Tls>) {
  let client = maybe_tls.client();
  let req = client
    .request(
      Method::GET,
      format!(
        "{}://localhost:{}/chunked-char-variable-delay-max-6000ms",
        maybe_tls.schema(),
        maybe_tls.port(),
      ),
    )
    .build()
    .unwrap();

  let original = RequestBuilder::from_parts(client, req);
  let request_builder = Some(original);

  integration_test_with_server_flag!(
    ServerFlags {
      request_idle_timeout: RequestIdleTimeout::from_millis(None, Some(2000)),
      ..Default::default()
    },
    "./test_cases/main",
    NON_SECURE_PORT,
    "",
    None,
    request_builder,
    maybe_tls,
    (|resp| async {
      let resp = resp.unwrap();

      assert_eq!(resp.status().as_u16(), StatusCode::OK);
      assert!(resp.content_length().is_none());

      let mut buf = Vec::<u8>::new();
      let mut bytes_stream = resp.bytes_stream();

      loop {
        match bytes_stream.next().await {
          Some(Ok(v)) => {
            buf.extend(v);
          }

          Some(Err(_)) => {
            break;
          }

          None => {
            break;
          }
        }
      }

      assert_eq!(&buf, b"meo");
    }),
    TerminationToken::new()
  );
}

#[tokio::test]
#[serial]
async fn test_request_idle_timeout_streamed_response_non_secure() {
  test_request_idle_timeout_streamed_response(new_localhost_tls(false)).await;
}

#[tokio::test]
#[serial]
async fn test_request_idle_timeout_streamed_response_secure() {
  test_request_idle_timeout_streamed_response(new_localhost_tls(true)).await;
}

async fn test_request_idle_timeout_streamed_response_first_chunk_timeout(
  maybe_tls: Option<Tls>,
) {
  let client = maybe_tls.client();
  let req = client
    .request(
      Method::GET,
      format!(
        "{}://localhost:{}/chunked-char-first-6000ms",
        maybe_tls.schema(),
        maybe_tls.port(),
      ),
    )
    .build()
    .unwrap();

  let original = RequestBuilder::from_parts(client, req);
  let request_builder = Some(original);

  integration_test_with_server_flag!(
    ServerFlags {
      request_idle_timeout: RequestIdleTimeout::from_millis(None, Some(1000)),
      ..Default::default()
    },
    "./test_cases/main",
    NON_SECURE_PORT,
    "",
    None,
    request_builder,
    maybe_tls,
    (|resp| async {
      let resp = resp.unwrap();

      assert_eq!(resp.status().as_u16(), StatusCode::OK);
      assert!(resp.content_length().is_none());

      let mut buf = Vec::<u8>::new();
      let mut bytes_stream = resp.bytes_stream();

      loop {
        match bytes_stream.next().await {
          Some(Ok(v)) => {
            buf.extend(v);
          }

          Some(Err(_)) => {
            break;
          }

          None => {
            break;
          }
        }
      }

      assert_eq!(&buf, b"");
    }),
    TerminationToken::new()
  );
}

#[tokio::test]
#[serial]
async fn test_request_idle_timeout_streamed_response_first_chunk_timeout_non_secure(
) {
  test_request_idle_timeout_streamed_response_first_chunk_timeout(
    new_localhost_tls(false),
  )
  .await;
}

#[tokio::test]
#[serial]
async fn test_request_idle_timeout_streamed_response_first_chunk_timeout_secure(
) {
  test_request_idle_timeout_streamed_response_first_chunk_timeout(
    new_localhost_tls(true),
  )
  .await;
}

async fn test_request_idle_timeout_websocket_deno(
  maybe_tls: Option<Tls>,
  use_node_ws: bool,
) {
  let nonce = tungstenite::handshake::client::generate_key();
  let client = maybe_tls.client();
  let req = client
    .request(
      Method::GET,
      format!(
        "{}://localhost:{}/websocket-upgrade-no-send{}",
        maybe_tls.schema(),
        maybe_tls.port(),
        if use_node_ws { "-node" } else { "" }
      ),
    )
    .header(header::CONNECTION, "upgrade")
    .header(header::UPGRADE, "websocket")
    .header(header::SEC_WEBSOCKET_KEY, &nonce)
    .header(header::SEC_WEBSOCKET_VERSION, "13")
    .build()
    .unwrap();

  let original = RequestBuilder::from_parts(client, req);
  let request_builder = Some(original);

  integration_test_with_server_flag!(
    ServerFlags {
      request_idle_timeout: RequestIdleTimeout::from_millis(None, Some(1000)),
      ..Default::default()
    },
    "./test_cases/main",
    NON_SECURE_PORT,
    "",
    None,
    request_builder,
    maybe_tls,
    (|resp| async {
      let res = resp.unwrap();
      let accepted = get_upgrade_type(res.headers());

      assert!(res.status().as_u16() == 101);
      assert!(accepted.is_some());
      assert_eq!(accepted.as_ref().unwrap(), "websocket");

      let upgraded = res.upgrade().await.unwrap();
      let mut ws = WebSocketStream::from_raw_socket(
        upgraded.compat(),
        tungstenite::protocol::Role::Client,
        None,
      )
      .await;

      sleep(Duration::from_secs(3)).await;

      ws.send(Message::Text("meow!!".into())).await.unwrap();

      let err = ws.next().await.unwrap().unwrap_err();

      use tungstenite::error::ProtocolError;
      use tungstenite::Error;

      assert!(matches!(
        err,
        Error::Protocol(ProtocolError::ResetWithoutClosingHandshake)
      ));
    }),
    TerminationToken::new()
  );
}

#[tokio::test]
#[serial]
async fn test_request_idle_timeout_websocket_deno_non_secure() {
  test_request_idle_timeout_websocket_deno(new_localhost_tls(false), false)
    .await;
}

#[tokio::test]
#[serial]
async fn test_request_idle_timeout_websocket_deno_secure() {
  test_request_idle_timeout_websocket_deno(new_localhost_tls(true), false)
    .await;
}

#[tokio::test]
#[serial]
async fn test_request_idle_timeout_websocket_node_non_secure() {
  test_request_idle_timeout_websocket_deno(new_localhost_tls(false), true)
    .await;
}

#[tokio::test]
#[serial]
async fn test_request_idle_timeout_websocket_node_secure() {
  test_request_idle_timeout_websocket_deno(new_localhost_tls(true), true).await;
}

#[tokio::test]
#[serial]
async fn test_should_not_hang_when_forced_redirection_for_specifiers() {
  let (tx, rx) = oneshot::channel::<()>();

  integration_test!(
    "./test_cases/main",
    NON_SECURE_PORT,
    "concurrent-redirect",
    None,
    None,
    None,
    (|resp| async {
      assert_eq!(resp.unwrap().status().as_u16(), 200);
      tx.send(()).unwrap();
    }),
    TerminationToken::new()
  );

  if timeout(Duration::from_secs(10), rx).await.is_err() {
    panic!("failed to check within 10 seconds");
  }
}

async fn test_allow_net<F, R>(
  allow_net: Option<Vec<&str>>,
  url: &str,
  callback: F,
) where
  F: FnOnce(Result<Response, reqwest::Error>) -> R,
  R: Future<Output = ()>,
{
  let payload = serde_json::json!({
      "allowNet": allow_net,
      "url": url
  });

  let client = Client::new();
  let req = client
    .request(
      Method::POST,
      format!("http://localhost:{}/fetch", NON_SECURE_PORT),
    )
    .json(&payload)
    .build()
    .unwrap();

  integration_test!(
    "./test_cases/main_with_allow_net",
    NON_SECURE_PORT,
    "",
    None,
    Some(RequestBuilder::from_parts(client, req)),
    None,
    (|resp| async {
      callback(resp).await;
    }),
    TerminationToken::new()
  );
}

#[tokio::test]
#[serial]
async fn test_allow_net_fetch_google_com() {
  #[derive(Deserialize)]
  struct FetchResponse {
    status: u16,
    body: String,
  }

  // 1. allow only specific hosts
  test_allow_net(
    // because google.com redirects to www.google.com
    Some(vec!["google.com", "www.google.com"]),
    "https://google.com",
    |resp| async move {
      let resp = resp.unwrap();

      assert_eq!(resp.status().as_u16(), StatusCode::OK);

      let payload = resp.json::<FetchResponse>().await.unwrap();

      assert_eq!(payload.status, StatusCode::OK);
      assert!(!payload.body.is_empty());
    },
  )
  .await;

  // 2. allow only specific host (but not considering the redirected host)
  test_allow_net(
      Some(vec!["google.com"]),
      "https://google.com",
      |resp| async move {
        let resp = resp.unwrap();

        assert_eq!(resp.status().as_u16(), StatusCode::INTERNAL_SERVER_ERROR);

        let msg = resp.text().await.unwrap();

        assert_eq!(
          msg.as_str(),
          // google.com redirects to www.google.com, but we didn't allow it
          "NotCapable: Requires net access to \"www.google.com:443\", run again with the --allow-net flag"
        );
      },
    )
    .await;

  // 3. deny all hosts
  test_allow_net(None, "https://google.com", |resp| async move {
    let resp = resp.unwrap();

    assert_eq!(resp.status().as_u16(), StatusCode::INTERNAL_SERVER_ERROR);

    let msg = resp.text().await.unwrap();

    assert_eq!(
      msg.as_str(),
      "NotCapable: Requires net access to \"google.com:443\", run again with the --allow-net flag"
    );
  })
  .await;

  // 4. allow all hosts
  test_allow_net(Some(vec![]), "https://google.com", |resp| async move {
    let resp = resp.unwrap();

    assert_eq!(resp.status().as_u16(), StatusCode::OK);

    let payload = resp.json::<FetchResponse>().await.unwrap();

    assert_eq!(payload.status, StatusCode::OK);
    assert!(!payload.body.is_empty());
  })
  .await;
}

#[tokio::test]
#[serial]
async fn test_fastify_v4_package() {
  integration_test!(
    "./test_cases/main",
    NON_SECURE_PORT,
    "fastify-v4",
    None,
    None,
    None,
    (|resp| async {
      assert_eq!(resp.unwrap().text().await.unwrap(), "meow");
    }),
    TerminationToken::new()
  );
}

#[tokio::test]
#[serial]
async fn test_fastify_latest_package() {
  integration_test!(
    "./test_cases/main",
    NON_SECURE_PORT,
    "fastify-latest",
    None,
    None,
    None,
    (|resp| async {
      assert_eq!(resp.unwrap().text().await.unwrap(), "meow");
    }),
    TerminationToken::new()
  );
}

#[tokio::test]
#[serial]
async fn test_declarative_style_fetch_handler() {
  integration_test!(
    "./test_cases/main",
    NON_SECURE_PORT,
    "serve-declarative-style",
    None,
    None,
    None,
    (|resp| async {
      assert_eq!(resp.unwrap().text().await.unwrap(), "meow");
    }),
    TerminationToken::new()
  );
}

#[tokio::test]
#[serial]
async fn test_fetch_local_file_handler() {
  let (tx, mut rx) = mpsc::unbounded_channel();
  let tb = TestBedBuilder::new("./test_cases/main")
    .with_per_worker_policy(None)
    .with_worker_event_sender(Some(tx))
    .build()
    .await;

  let mut resp = tb
    .request(|b| {
      b.uri("/fetch-local-npm-file")
        .body(Body::empty())
        .context("can't make request")
    })
    .await
    .unwrap();

  let status = resp.status().as_u16();
  let body = to_bytes(resp.body_mut()).await.unwrap();
  let body = String::from_utf8_lossy(&body).to_string();

  if status != 200 || body.is_empty() {
    rx.close();
    tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;

    while let Some(ev) = rx.recv().await {
      if let WorkerEvents::Log(ev) = ev.event {
        eprintln!("[worker-log] {}", ev.msg);
      }
    }
  }

  assert_eq!(status, 200);
  assert!(!body.is_empty());
}

// https://github.com/supabase/edge-runtime/issues/640
#[tokio::test]
#[serial]
async fn test_wasm_module() {
  integration_test!(
    "./test_cases/main",
    NON_SECURE_PORT,
    "wasm-module",
    None,
    None,
    None,
    (|resp| async {
      // Testing mod add(1, 2) === 3;
      assert_eq!(resp.unwrap().text().await.unwrap(), "3");
    }),
    TerminationToken::new()
  );
}

#[tokio::test]
#[serial]
async fn test_issue_208() {
  async fn create_simple_server(
    tls: Option<Tls>,
    token: CancellationToken,
  ) -> Result<(), AnyError> {
    let config = Arc::new(tls.server_config());
    let acceptor = TlsAcceptor::from(config);
    let listener =
      TcpListener::bind(format!("127.0.0.1:{}", tls.port())).await?;
    loop {
      let acceptor = acceptor.clone();
      tokio::select! {
        Ok((stream, _)) = listener.accept() => {
          tokio::spawn(async move {
            if let Ok(tls_stream) = acceptor.accept(stream).await {
              let _ = hyper::server::conn::Http::new().serve_connection(
                tls_stream,
                hyper::service::service_fn(|_req: _| async {
                  Ok::<_, hyper::Error>(hyper::Response::new(Body::from("meow")))
                })
              )
              .await
              .ok();
            }
          });
        }
        _ = token.cancelled() => {
          break;
        }
      }
    }
    Ok(())
  }

  let tls = new_localhost_tls(true);
  let port = tls.port();
  let token = CancellationToken::new();
  let server = tokio::spawn({
    let token = token.clone();
    async move {
      create_simple_server(tls, token).await.unwrap();
    }
  });

  {
    let client = Client::new();
    let builder = client
      .request(
        Method::POST,
        format!("http://localhost:{}/issue-208", NON_SECURE_PORT),
      )
      .header("x-port", port)
      .body(TLS_LOCALHOST_ROOT_CA);

    integration_test!(
      "./test_cases/main",
      NON_SECURE_PORT,
      "",
      None,
      Some(builder),
      None,
      (|resp| async {
        let resp = resp.unwrap();
        assert!(resp.status().as_u16() == 200);
        assert_eq!(resp.text().await.unwrap(), "meow");
      }),
      TerminationToken::new()
    );
  }

  // unknown issuer
  {
    let client = Client::new();
    let builder = client
      .request(
        Method::GET,
        format!("http://localhost:{}/issue-208", NON_SECURE_PORT),
      )
      .header("x-port", port);

    integration_test!(
      "./test_cases/main",
      NON_SECURE_PORT,
      "",
      None,
      Some(builder),
      None,
      (|resp| async {
        let resp = resp.unwrap();
        assert!(resp.status().as_u16() == 500);
        let reason = resp.text().await.unwrap();
        assert!(reason.contains("invalid peer certificate: UnknownIssuer"));
      }),
      TerminationToken::new()
    );
  }

  token.cancel();
  server.await.unwrap();
}

#[tokio::test]
#[serial]
async fn test_issue_420() {
  integration_test!(
    "./test_cases/main",
    NON_SECURE_PORT,
    "issue-420",
    None,
    None,
    None,
    (|resp| async {
      let text = resp.unwrap().text().await.unwrap();

      assert!(text.starts_with("file:///"));
      assert!(text.ends_with(
        "/node_modules/localhost/@imagemagick/magick-wasm/0.0.30/dist/index.js"
      ));
    }),
    TerminationToken::new()
  );
}

#[tokio::test]
#[serial]
async fn test_issue_456() {
  let tb = TestBedBuilder::new("./test_cases/main").build().await;
  let resp = tb
    .request(|b| {
      b.uri("/issue-456")
        .header("x-context-source-map", "true")
        .body(Body::empty())
        .context("can't make request")
    })
    .await
    .unwrap();

  assert_eq!(resp.status().as_u16(), StatusCode::OK);
  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_issue_513() {
  let tb = TestBedBuilder::new("./test_cases/main").build().await;
  let resp = tb
    .request(|b| {
      b.uri("/issue-513")
        .body(Body::empty())
        .context("can't make request")
    })
    .await
    .unwrap();

  assert_eq!(resp.status().as_u16(), StatusCode::OK);
  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
}

#[tokio::test]
#[serial]
async fn test_supabase_issue_29583() {
  integration_test!(
    "./test_cases/main",
    NON_SECURE_PORT,
    "supabase-issue-29583",
    None,
    None,
    None,
    (|resp| async {
      let resp = resp.unwrap();
      assert_eq!(resp.status().as_u16(), StatusCode::OK);
    }),
    TerminationToken::new()
  );
}

#[tokio::test]
#[serial]
async fn test_issue_func_205() {
  let (tx, mut rx) = mpsc::unbounded_channel();
  let tb = TestBedBuilder::new("./test_cases/main")
    .with_per_worker_policy(None)
    .with_worker_event_sender(Some(tx))
    .with_server_flags(ServerFlags {
      beforeunload_wall_clock_pct: Some(90),
      beforeunload_cpu_pct: Some(90),
      beforeunload_memory_pct: Some(90),
      ..Default::default()
    })
    .build()
    .await;

  let resp = tb
    .request(|b| {
      b.uri("/issue-func-205")
        .header("x-cpu-time-soft-limit-ms", HeaderValue::from_static("500"))
        .header("x-cpu-time-hard-limit-ms", HeaderValue::from_static("1000"))
        .header(
          "x-context-use-read-sync-file-api",
          HeaderValue::from_static("true"),
        )
        .body(Body::empty())
        .context("can't make request")
    })
    .await
    .unwrap();

  assert_eq!(resp.status().as_u16(), StatusCode::INTERNAL_SERVER_ERROR);

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;

  while let Some(ev) = rx.recv().await {
    let WorkerEvents::Shutdown(ev) = ev.event else {
      continue;
    };
    assert_eq!(ev.reason, ShutdownReason::CPUTime);
    return;
  }

  unreachable!("test failed");
}

#[tokio::test]
#[serial]
async fn test_issue_func_280() {
  async fn run(func_name: &'static str, reason: ShutdownReason) {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let tb = TestBedBuilder::new("./test_cases/main")
      .with_per_worker_policy(None)
      .with_worker_event_sender(Some(tx))
      .with_server_flags(ServerFlags {
        beforeunload_cpu_pct: Some(90),
        beforeunload_memory_pct: Some(90),
        ..Default::default()
      })
      .build()
      .await;

    let resp = tb
      .request(|b| {
        b.uri("/meow")
          .header("x-cpu-time-soft-limit-ms", HeaderValue::from_static("1000"))
          .header("x-cpu-time-hard-limit-ms", HeaderValue::from_static("2000"))
          .header("x-memory-limit-mb", "30")
          .header("x-service-path", format!("issue-func-280/{}", func_name))
          .body(Body::empty())
          .context("can't make request")
      })
      .await
      .unwrap();

    assert_eq!(resp.status().as_u16(), StatusCode::OK);

    while let Some(ev) = rx.recv().await {
      match ev.event {
        WorkerEvents::Log(ev) => {
          tracing::info!("{}", ev.msg);
          continue;
        }
        WorkerEvents::Shutdown(ev) => {
          tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
          assert_eq!(ev.reason, reason);
          return;
        }
        _ => continue,
      }
    }

    unreachable!("test failed");
  }

  run("cpu", ShutdownReason::CPUTime).await;
  run("mem", ShutdownReason::Memory).await;
}

#[tokio::test]
#[serial]
async fn test_issue_func_284() {
  async fn find_boot_event(
    rx: &mut mpsc::UnboundedReceiver<WorkerEventWithMetadata>,
  ) -> Option<usize> {
    while let Some(ev) = rx.recv().await {
      match ev.event {
        WorkerEvents::Boot(ev) => return Some(ev.boot_time),
        _ => continue,
      }
    }

    None
  }

  let (tx, mut rx) = mpsc::unbounded_channel();
  let tb = TestBedBuilder::new("./test_cases/main")
    .with_per_worker_policy(None)
    .with_worker_event_sender(Some(tx))
    .build()
    .await;

  tokio::spawn({
    let tb = tb.clone();
    async move {
      tb.request(|b| {
        b.uri("/meow")
          .header("x-service-path", "issue-func-284/noisy")
          .body(Body::empty())
          .context("can't make request")
      })
      .await
      .unwrap();
    }
  });

  timeout(Duration::from_secs(1), find_boot_event(&mut rx))
    .await
    .unwrap()
    .unwrap();

  tokio::spawn({
    let tb = tb.clone();
    async move {
      tb.request(|b| {
        b.uri("/meow")
          .header("x-service-path", "issue-func-284/baseline")
          .body(Body::empty())
          .context("can't make request")
      })
      .await
      .unwrap();
    }
  });

  let boot_time = timeout(Duration::from_secs(1), find_boot_event(&mut rx))
    .await
    .unwrap()
    .unwrap();

  assert!(boot_time < 1000);
}

#[tokio::test]
#[serial]
async fn test_should_render_detailed_failed_to_create_graph_error() {
  {
    integration_test!(
      "./test_cases/main",
      NON_SECURE_PORT,
      "graph-error-1",
      None,
      None,
      None,
      (|resp| async {
        let (payload, status) =
          ErrorResponsePayload::assert_error_response(resp).await;

        assert_eq!(status, 500);
        assert!(payload.msg.starts_with(
          "InvalidWorkerCreation: worker boot error: \
          failed to bootstrap runtime: failed to create the graph: \
          Relative import path \"oak\" not prefixed with"
        ));
      }),
      TerminationToken::new()
    );
  }

  {
    integration_test!(
      "./test_cases/main",
      NON_SECURE_PORT,
      "graph-error-2",
      None,
      None,
      None,
      (|resp| async {
        let (payload, status) =
          ErrorResponsePayload::assert_error_response(resp).await;

        assert_eq!(status, 500);
        assert!(payload.msg.starts_with(
          "InvalidWorkerCreation: worker boot error: \
          failed to bootstrap runtime: failed to create the graph: \
          Module not found \"file://"
        ));
      }),
      TerminationToken::new()
    );
  }
}

#[tokio::test]
#[serial]
async fn test_js_entrypoint() {
  {
    integration_test!(
      "./test_cases/main",
      NON_SECURE_PORT,
      "serve-js",
      None,
      None,
      None,
      (|resp| async {
        let resp = resp.unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        let msg = resp.text().await.unwrap();
        assert_eq!(msg, "meow");
      }),
      TerminationToken::new()
    );
  }

  {
    integration_test!(
      "./test_cases/main",
      NON_SECURE_PORT,
      "serve-declarative-style-js",
      None,
      None,
      None,
      (|resp| async {
        let resp = resp.unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        let msg = resp.text().await.unwrap();
        assert_eq!(msg, "meow");
      }),
      TerminationToken::new()
    );
  }
}

#[tokio::test]
#[serial]
async fn test_should_be_able_to_bundle_against_various_exts() {
  let get_eszip_buf = |path: &str| {
    let path = path.to_string();
    let mut emitter_factory = EmitterFactory::new();

    emitter_factory.set_permissions_options(Some(get_default_permissions(
      WorkerKind::UserWorker,
    )));
    emitter_factory.set_deno_options(
      DenoOptionsBuilder::new()
        .entrypoint(PathBuf::from(path))
        .build()
        .unwrap(),
    );

    async {
      let mut metadata = Metadata::default();
      let eszip = generate_binary_eszip(
        &mut metadata,
        Arc::new(emitter_factory),
        None,
        None,
        None,
      )
      .await
      .unwrap();

      eszip.into_bytes()
    }
  };

  {
    let buf =
      get_eszip_buf("./test_cases/eszip-various-ext/npm-supabase/index.js")
        .await;

    let client = Client::new();
    let req = client
      .request(
        Method::POST,
        format!("http://localhost:{}/meow", NON_SECURE_PORT),
      )
      .body(buf);

    integration_test!(
      "./test_cases/main_eszip",
      NON_SECURE_PORT,
      "",
      None,
      Some(req),
      None,
      (|resp| async {
        let resp = resp.unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        let msg = resp.text().await.unwrap();
        assert_eq!(msg, "function");
      }),
      TerminationToken::new()
    );
  }

  let test_serve_simple_fn = |ext: &'static str, expected: &'static [u8]| {
    let ext = ext.to_string();
    let expected = expected.to_vec();

    async move {
      let buf = get_eszip_buf(&format!(
        "./test_cases/eszip-various-ext/serve/index.{}",
        ext
      ))
      .await;

      let client = Client::new();
      let req = client
        .request(
          Method::POST,
          format!("http://localhost:{}/meow", NON_SECURE_PORT),
        )
        .body(buf);

      integration_test!(
        "./test_cases/main_eszip",
        NON_SECURE_PORT,
        "",
        None,
        Some(req),
        None,
        (|resp| async move {
          let resp = resp.unwrap();
          assert_eq!(resp.status().as_u16(), 200);
          let msg = resp.bytes().await.unwrap();
          assert_eq!(msg, expected);
        }),
        TerminationToken::new()
      );
    }
  };

  test_serve_simple_fn("ts", b"meow").await;
  test_serve_simple_fn("js", b"meow").await;
  test_serve_simple_fn("mjs", b"meow").await;

  static REACT_RESULT: &str = r#"{"type":"div","props":{"children":"meow"},"__k":null,"__":null,"__b":0,"__e":null,"__c":null,"__v":-1,"__i":-1,"__u":0}"#;

  test_serve_simple_fn("jsx", REACT_RESULT.as_bytes()).await;
  test_serve_simple_fn("tsx", REACT_RESULT.as_bytes()).await;
}

#[tokio::test]
#[serial]
async fn test_private_npm_package_import() {
  // Required because test_cases/main_with_registry/registry/registry-handler.ts:58
  std::env::set_var("EDGE_RUNTIME_PORT", NON_SECURE_PORT.to_string());
  let _guard = scopeguard::guard((), |_| {
    std::env::remove_var("EDGE_RUNTIME_PORT");
  });

  let client = Client::new();
  let run_server_fn = |main: &'static str, token| async move {
    let (tx, mut rx) = mpsc::channel(1);
    let handle = tokio::task::spawn({
      async move {
        let mut builder = Builder::new(
          SocketAddr::from_str(&format!("127.0.0.1:{NON_SECURE_PORT}"))
            .unwrap(),
          main,
        );

        builder.event_callback(tx).termination_token(token);
        builder.build().await.unwrap().listen().await.unwrap()
      }
    });

    let _ev = loop {
      match rx.recv().await {
        Some(health) => break health.into_listening().unwrap(),
        _ => continue,
      }
    };

    handle
  };

  {
    let token = TerminationToken::new();
    let handle =
      run_server_fn("./test_cases/main_with_registry", token.clone()).await;

    let resp = client
      .request(
        Method::GET,
        format!(
          "http://localhost:{}/private-npm-package-import",
          NON_SECURE_PORT
        ),
      )
      .send()
      .await
      .unwrap();

    assert_eq!(resp.status().as_u16(), 200);

    let body = resp.json::<serde_json::Value>().await.unwrap();
    let body = body.as_object().unwrap();

    assert_eq!(body.len(), 2);
    assert_eq!(body.get("meow"), Some(&json!("function")));
    assert_eq!(body.get("odd"), Some(&json!(true)));

    token.cancel();
    handle.await.unwrap();
  }

  {
    let token = TerminationToken::new();
    let handle =
      run_server_fn("./test_cases/main_with_registry", token.clone()).await;

    let resp = client
      .request(
        Method::GET,
        format!("http://localhost:{}/meow", NON_SECURE_PORT),
      )
      .header("x-service-path", "private-npm-package-import-2/inner")
      .send()
      .await
      .unwrap();

    assert_eq!(resp.status().as_u16(), 200);

    let body = resp.json::<serde_json::Value>().await.unwrap();
    let body = body.as_object().unwrap();

    assert_eq!(body.len(), 2);
    assert_eq!(body.get("meow"), Some(&json!("function")));
    assert_eq!(body.get("odd"), Some(&json!(true)));

    token.cancel();
    handle.await.unwrap();
  }

  {
    let token = TerminationToken::new();
    let handle = run_server_fn("./test_cases/main_eszip", token.clone()).await;

    let buf = {
      let mut emitter_factory = EmitterFactory::new();

      emitter_factory.set_deno_options(
        DenoOptionsBuilder::new()
          .entrypoint(PathBuf::from(
            "./test_cases/private-npm-package-import/index.js",
          ))
          .build()
          .unwrap(),
      );

      let mut metadata = Metadata::default();
      let eszip = generate_binary_eszip(
        &mut metadata,
        Arc::new(emitter_factory),
        None,
        None,
        None,
      )
      .await
      .unwrap();

      eszip.into_bytes()
    };

    let resp = client
      .request(
        Method::POST,
        format!("http://localhost:{}/meow", NON_SECURE_PORT),
      )
      .body(buf)
      .send()
      .await
      .unwrap();

    assert_eq!(resp.status().as_u16(), 200);

    let body = resp.json::<serde_json::Value>().await.unwrap();
    let body = body.as_object().unwrap();

    assert_eq!(body.len(), 2);
    assert_eq!(body.get("meow"), Some(&json!("function")));
    assert_eq!(body.get("odd"), Some(&json!(true)));

    token.cancel();
    handle.await.unwrap();
  }
}

#[tokio::test]
#[serial]
async fn test_tmp_fs_usage() {
  {
    integration_test!(
      "./test_cases/main",
      NON_SECURE_PORT,
      "use-tmp-fs",
      None,
      None,
      None,
      (|resp| async {
        let resp = resp.unwrap();

        assert_eq!(resp.status().as_u16(), 200);

        let body = resp.json::<serde_json::Value>().await.unwrap();
        let body = body.as_object().unwrap();

        assert_eq!(body.len(), 4);
        assert_eq!(body.get("written"), Some(&json!(8)));
        assert_eq!(body.get("content"), Some(&json!("meowmeow")));
        assert_eq!(body.get("deleted"), Some(&json!(true)));

        let steps = body.get("steps").unwrap().as_array().unwrap();

        assert_eq!(&steps[0], &json!(true));
        assert_eq!(&steps[1], &json!(false));
      }),
      TerminationToken::new()
    );
  }

  {
    integration_test!(
      "./test_cases/main",
      NON_SECURE_PORT,
      "use-tmp-fs-2",
      None,
      None,
      None,
      (|resp| async {
        let resp = resp.unwrap();

        assert_eq!(resp.status().as_u16(), 200);

        let body = resp.json::<serde_json::Value>().await.unwrap();
        let body = body.as_object().unwrap();

        assert_eq!(body.len(), 2);
        assert_eq!(body.get("hadExisted"), Some(&json!(true)));

        let path = body.get("path").unwrap().as_str().unwrap();
        let f = fs::read(path).await.unwrap();
        let mut cursor = Cursor::new(&f);

        let client = Client::new();
        let resp2 = client
          .request(Method::GET, "https://httpbin.org/stream/20".to_string())
          .send()
          .await
          .unwrap();

        assert_eq!(resp2.status().as_u16(), 200);

        let body2 = resp2.bytes().await.unwrap();
        let mut cursor2 = Cursor::new(&*body2);
        let mut count = 0;

        loop {
          use serde_json::*;

          let mut buf = String::new();

          cursor.read_line(&mut buf).unwrap();
          let mut msg = from_str::<Map<String, Value>>(&buf).unwrap();

          buf.clear();
          cursor2.read_line(&mut buf).unwrap();
          let mut msg2 = from_str::<Map<String, Value>>(&buf).unwrap();

          assert!(msg.remove("headers").is_some());
          assert!(msg2.remove("headers").is_some());
          assert_eq!(msg, msg2);

          count += 1;
          if count >= 20 {
            break;
          }
        }
      }),
      TerminationToken::new()
    );
  }

  {
    integration_test!(
      "./test_cases/main",
      NON_SECURE_PORT,
      "use-tmp-fs-3",
      None,
      None,
      None,
      (|resp| async {
        let resp = resp.unwrap();
        assert_eq!(resp.status().as_u16(), 200);
      }),
      TerminationToken::new()
    );
  }
}

#[tokio::test]
#[serial]
async fn test_tmp_fs_should_not_be_available_in_import_stmt() {
  // The s3 fs and tmp fs are not currently attached to the module loader, so the import statement
  // should not recognize their prefixes. (But, depending on the case, they may be attached in the
  // future)
  integration_test!(
    "./test_cases/main",
    NON_SECURE_PORT,
    "use-tmp-fs-in-import-stmt",
    None,
    None,
    None,
    (|resp| async {
      let (payload, status) =
        ErrorResponsePayload::assert_error_response(resp).await;

      assert_eq!(status, 500);
      dbg!(&payload.msg);
      assert!(payload.msg.starts_with(
        "InvalidWorkerResponse: event loop error while evaluating the module: \
        TypeError: Module not found: file:///tmp/meowmeow.ts"
      ));
    }),
    TerminationToken::new()
  );
}

#[tokio::test]
#[serial]
async fn test_commonjs() {
  integration_test!(
    "./test_cases/main",
    NON_SECURE_PORT,
    "commonjs",
    None,
    None,
    None,
    (|resp| async {
      let resp = resp.unwrap();
      assert_eq!(resp.status().as_u16(), 200);
      assert_eq!(resp.text().await.unwrap().as_str(), "meow");
    }),
    TerminationToken::new()
  );
}

#[tokio::test]
#[serial]
async fn test_commonjs_require_esm() {
  integration_test!(
    "./test_cases/main",
    NON_SECURE_PORT,
    "commonjs-require-esm",
    None,
    None,
    None,
    (|resp| async {
      let resp = resp.unwrap();
      assert_eq!(resp.status().as_u16(), 200);
      assert_eq!(resp.text().await.unwrap().as_str(), "cjs require esm ok");
    }),
    TerminationToken::new()
  );
}

#[tokio::test]
#[serial]
async fn test_commonjs_no_type_field_in_package_json() {
  integration_test!(
    "./test_cases/main",
    NON_SECURE_PORT,
    "commonjs-no-type-field",
    None,
    None,
    None,
    (|resp| async {
      let resp = resp.unwrap();
      assert_eq!(resp.status().as_u16(), 500);
    }),
    TerminationToken::new()
  );
}

#[tokio::test]
#[serial]
async fn test_commonjs_custom_main() {
  integration_test!(
    "./test_cases/main",
    NON_SECURE_PORT,
    "commonjs-custom-main",
    None,
    None,
    None,
    (|resp| async {
      let resp = resp.unwrap();
      assert_eq!(resp.status().as_u16(), 200);
      assert_eq!(resp.text().await.unwrap().as_str(), "meow");
    }),
    TerminationToken::new()
  );
}

#[tokio::test]
#[serial]
async fn test_commonjs_express() {
  ensure_npm_package_installed("./test_cases/commonjs-express").await;
  integration_test!(
    "./test_cases/main",
    NON_SECURE_PORT,
    "commonjs-express",
    None,
    None,
    None,
    (|resp| async {
      let resp = resp.unwrap();
      assert_eq!(resp.status().as_u16(), 200);
      assert_eq!(resp.text().await.unwrap().as_str(), "meow");
    }),
    TerminationToken::new()
  );
}

#[tokio::test]
#[serial]
async fn test_commonjs_hono() {
  ensure_npm_package_installed("./test_cases/commonjs-hono").await;
  integration_test!(
    "./test_cases/main",
    NON_SECURE_PORT,
    "commonjs-hono",
    None,
    None,
    None,
    (|resp| async {
      let resp = resp.unwrap();
      assert_eq!(resp.status().as_u16(), 200);
      assert_eq!(resp.text().await.unwrap().as_str(), "meow");
    }),
    TerminationToken::new()
  );
}

async fn test_commonjs_websocket(prefix: String) {
  ensure_npm_package_installed(format!(
    "./test_cases/commonjs-{}-websocket",
    prefix
  ))
  .await;
  let nonce = tungstenite::handshake::client::generate_key();
  let client = Client::new();
  let req = client
    .request(
      Method::GET,
      format!(
        "http://localhost:{}/commonjs-{}-websocket",
        NON_SECURE_PORT, prefix
      ),
    )
    .header(header::CONNECTION, "upgrade")
    .header(header::UPGRADE, "websocket")
    .header(header::SEC_WEBSOCKET_KEY, &nonce)
    .header(header::SEC_WEBSOCKET_VERSION, "13")
    .build()
    .unwrap();

  let original = RequestBuilder::from_parts(client, req);
  let request_builder = Some(original);

  integration_test!(
    "./test_cases/main",
    NON_SECURE_PORT,
    "",
    None,
    request_builder,
    None,
    (|resp| async {
      let res = resp.unwrap();
      let accepted = get_upgrade_type(res.headers());

      assert!(res.status().as_u16() == 101);
      assert!(accepted.is_some());
      assert_eq!(accepted.as_ref().unwrap(), "websocket");

      let upgraded = res.upgrade().await.unwrap();
      let mut ws = WebSocketStream::from_raw_socket(
        upgraded.compat(),
        tungstenite::protocol::Role::Client,
        None,
      )
      .await;

      assert_eq!(
        ws.next().await.unwrap().unwrap().into_text().unwrap(),
        "meow"
      );

      ws.send(Message::Text("meow!!".into())).await.unwrap();
      assert_eq!(
        ws.next().await.unwrap().unwrap().into_text().unwrap(),
        "meow!!"
      );
    }),
    TerminationToken::new()
  );
}

#[tokio::test]
#[serial]
async fn test_commonjs_ws_websocket() {
  test_commonjs_websocket(String::from("ws")).await;
}

#[tokio::test]
#[serial]
async fn test_commonjs_hono_websocket() {
  test_commonjs_websocket(String::from("hono")).await;
}

#[tokio::test]
#[serial]
async fn test_commonjs_express_websocket() {
  test_commonjs_websocket(String::from("express")).await;
}

#[tokio::test]
#[serial]
async fn test_commonjs_workspace() {
  ensure_npm_package_installed("./test_cases/commonjs-workspace").await;
  integration_test!(
    "./test_cases/main",
    NON_SECURE_PORT,
    "commonjs-workspace",
    None,
    None,
    None,
    (|resp| async {
      let resp = resp.unwrap();
      assert_eq!(resp.status().as_u16(), 200);

      let body = resp.json::<serde_json::Value>().await.unwrap();
      let body = body.as_object().unwrap();

      assert_eq!(body.len(), 2);
      assert_eq!(body.get("cat"), Some(&json!("meow")));
      assert_eq!(body.get("dog"), Some(&json!("bark")));
    }),
    TerminationToken::new()
  );
}

#[tokio::test]
#[serial]
async fn test_byonm_typescript() {
  ensure_npm_package_installed("./test_cases/byonm-typescript").await;
  integration_test!(
    "./test_cases/main",
    NON_SECURE_PORT,
    "byonm-typescript",
    None,
    None,
    None,
    (|resp| async {
      let resp = resp.unwrap();
      assert_eq!(resp.status().as_u16(), 200);
      assert_eq!(resp.text().await.unwrap().as_str(), "meow");
    }),
    TerminationToken::new()
  );
}

#[tokio::test]
#[serial]
async fn test_deno_workspace() {
  integration_test!(
    "./test_cases/main",
    NON_SECURE_PORT,
    "workspace",
    None,
    None,
    None,
    (|resp| async {
      let resp = resp.unwrap();
      assert_eq!(resp.status().as_u16(), 200);

      let body = resp.json::<serde_json::Value>().await.unwrap();
      let body = body.as_object().unwrap();

      assert_eq!(body.len(), 3);
      assert_eq!(body.get("cat"), Some(&json!("meow")));
      assert_eq!(body.get("dog"), Some(&json!("bark")));
      assert_eq!(body.get("sheep"), Some(&json!("howl")));
    }),
    TerminationToken::new()
  );
}

#[tokio::test]
#[serial]
async fn test_supabase_ai_gte() {
  let tb = TestBedBuilder::new("./test_cases/main")
    .with_per_worker_policy(None)
    .build()
    .await;

  let resp = tb
    .request(|b| {
      b.uri("/supabase-ai")
        .body(Body::empty())
        .context("can't make request")
    })
    .await
    .unwrap();

  assert_eq!(resp.status().as_u16(), StatusCode::OK);
}

// -- ext_ai: ORT base api
#[tokio::test]
#[serial]
async fn test_ort_string_tensor() {
  let base_path = "./test_cases/ai-ort-rust-backend";
  let main_path = format!("{}/main", base_path);

  let tb = TestBedBuilder::new(main_path)
    .with_per_worker_policy(None)
    .build()
    .await;

  let resp = tb
    .request(|b| {
      b.uri("/string-tensor")
        .body(Body::empty())
        .context("can't make request")
    })
    .await
    .unwrap();

  assert_eq!(resp.status().as_u16(), StatusCode::OK);
}

// -- ext_ai: ORT @huggingface/transformers
async fn test_ort_transformers_js(script_path: &str) {
  fn visit_json(value: &mut serde_json::Value) {
    use serde_json::Number;
    use serde_json::Value::*;

    match value {
      Array(vec) => {
        for v in vec {
          visit_json(v);
        }
      }
      Object(map) => {
        for (_, v) in map {
          visit_json(v);
        }
      }
      Number(number) => {
        if let Some(f) = number.as_f64() {
          *number =
            Number::from_f64((f * 1_000_000.0).round() / 1_000_000.0).unwrap();
        }
      }

      _ => {}
    }
  }

  use std::env::consts;

  let base_path = "./test_cases/ai-ort-rust-backend";
  let main_path = format!("{}/main", base_path);
  let script_path = format!("transformers-js/{}", script_path);
  let snapshot_path = PathBuf::from(base_path)
    .join(script_path.as_str())
    .join(format!("__snapshot__/{}_{}.json", consts::OS, consts::ARCH));

  let client = Client::new();
  let body = {
    if snapshot_path.exists() {
      tokio::fs::read(&snapshot_path).await.unwrap()
    } else {
      b"null".to_vec()
    }
  };

  let content_length = body.len();
  let req = client
    .request(
      Method::POST,
      format!(
        "http://localhost:{}/{}",
        NON_SECURE_PORT,
        script_path.as_str(),
      ),
    )
    .body(body)
    .header("Content-Type", "application/json")
    .header("Content-Length", content_length.to_string());

  integration_test!(
    main_path,
    NON_SECURE_PORT,
    "",
    None,
    Some(req),
    None,
    (|resp| async {
      let res = resp.unwrap();
      let status_code = res.status();

      assert!(matches!(status_code, StatusCode::OK | StatusCode::CREATED));

      if status_code == StatusCode::OK {
        return;
      }

      assert_eq!(std::env::var("CI").ok(), None);
      assert!(!snapshot_path.exists());

      tokio::fs::create_dir_all(snapshot_path.parent().unwrap())
        .await
        .unwrap();

      let mut body = res.json::<serde_json::Value>().await.unwrap();
      let mut file = fs::File::create(&snapshot_path).await.unwrap();

      visit_json(&mut body);

      let content = serde_json::to_vec(&body).unwrap();

      file.write_all(&content).await.unwrap();
    }),
    TerminationToken::new()
  );
}

#[tokio::test]
#[serial]
async fn test_ort_nlp_feature_extraction() {
  test_ort_transformers_js("feature-extraction").await;
}

async fn test_runtime_beforeunload_event(kind: &'static str, pct: u8) {
  let (tx, mut rx) = mpsc::unbounded_channel();
  let tb = TestBedBuilder::new("./test_cases/runtime-event")
    .with_per_worker_policy(None)
    .with_worker_event_sender(Some(tx))
    .with_server_flags(ServerFlags {
      beforeunload_wall_clock_pct: Some(pct),
      beforeunload_cpu_pct: Some(pct),
      beforeunload_memory_pct: Some(pct),
      ..Default::default()
    })
    .build()
    .await;

  let resp = tb
    .request(|b| {
      b.uri(format!("/{}", kind))
        .method("GET")
        .body(Body::empty())
        .context("can't make request")
    })
    .await
    .unwrap();

  assert_ne!(resp.status().as_u16(), StatusCode::OK);

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;

  while let Some(ev) = rx.recv().await {
    let WorkerEvents::Log(ev) = ev.event else {
      continue;
    };
    if ev.level != LogLevel::Info {
      continue;
    }
    if ev
      .msg
      .contains(&format!("triggered {}", kind.replace('-', "_")))
    {
      return;
    }
  }

  unreachable!("test failed");
}

#[tokio::test]
#[serial]
async fn test_runtime_event_beforeunload_cpu() {
  test_runtime_beforeunload_event("cpu", 50).await;
}

#[tokio::test]
#[serial]
async fn test_runtime_event_beforeunload_wall_clock() {
  test_runtime_beforeunload_event("wall-clock", 50).await;
}

#[tokio::test]
#[serial]
async fn test_runtime_event_beforeunload_mem() {
  test_runtime_beforeunload_event("mem", 50).await;
}

// NOTE(Nyannyacha): We cannot enable this test unless we clarify the trigger point of the unload
// event.
//
// #[tokio::test]
// #[serial]
// async fn test_runtime_event_unload() {
//     let (tx, mut rx) = mpsc::unbounded_channel();
//     let tb = TestBedBuilder::new("./test_cases/runtime-event")
//         .with_per_worker_policy(None)
//         .with_worker_event_sender(Some(tx))
//         .build()
//         .await;
//
//     let resp = tb
//         .request(|b| {
//             b.uri("/unload")
//                 .method("GET")
//                 .body(Body::empty())
//                 .context("can't make request")
//         })
//         .await
//         .unwrap();
//
//     assert_eq!(resp.status().as_u16(), StatusCode::OK);
//
//     sleep(Duration::from_secs(8)).await;
//     tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
//
//     while let Some(ev) = rx.recv().await {
//         let WorkerEvents::Log(ev) = ev.event else {
//             continue;
//         };
//         if ev.level != LogLevel::Info {
//             continue;
//         }
//         if ev.msg.contains("triggered unload") {
//             break;
//         }
//     }
//
//     unreachable!("test failed");
// }

#[tokio::test]
#[serial]
async fn test_should_wait_for_background_tests() {
  let (tx, mut rx) = mpsc::unbounded_channel();
  let tb = TestBedBuilder::new("./test_cases/main")
    // only the `per_worker` policy allows waiting for background tasks.
    .with_per_worker_policy(None)
    .with_worker_event_sender(Some(tx))
    .build()
    .await;

  let resp = tb
    .request(|b| {
      b.uri("/mark-background-task")
        .header("x-cpu-time-soft-limit-ms", HeaderValue::from_static("100"))
        .body(Body::empty())
        .context("can't make request")
    })
    .await
    .unwrap();

  assert_eq!(resp.status().as_u16(), StatusCode::OK);

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;

  while let Some(ev) = rx.recv().await {
    let WorkerEvents::Log(ev) = ev.event else {
      continue;
    };
    if ev.level != LogLevel::Info {
      continue;
    }
    if ev.msg.contains("meow") {
      return;
    }
  }

  unreachable!("test failed");
}

#[tokio::test]
#[serial]
async fn test_should_not_wait_for_background_tests() {
  let (tx, mut rx) = mpsc::unbounded_channel();
  let tb = TestBedBuilder::new("./test_cases/main")
    // only the `per_worker` policy allows waiting for background tasks.
    .with_per_worker_policy(None)
    .with_worker_event_sender(Some(tx))
    .build()
    .await;

  let resp = tb
    .request(|b| {
      b.uri("/mark-background-task-2")
        .header("x-cpu-time-soft-limit-ms", HeaderValue::from_static("100"))
        .body(Body::empty())
        .context("can't make request")
    })
    .await
    .unwrap();

  assert_eq!(resp.status().as_u16(), StatusCode::OK);

  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;

  while let Some(ev) = rx.recv().await {
    let WorkerEvents::Log(ev) = ev.event else {
      continue;
    };
    if ev.level != LogLevel::Info {
      continue;
    }
    if ev.msg.contains("meow") {
      unreachable!("test failed");
    }
  }
}

#[tokio::test]
#[serial]
async fn test_should_be_able_to_trigger_early_drop_with_wall_clock() {
  let (tx, mut rx) = mpsc::unbounded_channel();
  let tb = TestBedBuilder::new("./test_cases/main")
    .with_per_worker_policy(None)
    .with_worker_event_sender(Some(tx))
    .build()
    .await;

  let resp = tb
    .request(|b| {
      b.uri("/early-drop-wall-clock")
        .header("x-worker-timeout-ms", HeaderValue::from_static("3000"))
        .body(Body::empty())
        .context("can't make request")
    })
    .await
    .unwrap();

  assert_eq!(resp.status().as_u16(), StatusCode::OK);

  sleep(Duration::from_secs(2)).await;
  rx.close();
  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;

  while let Some(ev) = rx.recv().await {
    let WorkerEvents::Log(ev) = ev.event else {
      continue;
    };
    if ev.level != LogLevel::Info {
      continue;
    }
    if ev.msg.contains("early_drop") {
      return;
    }
  }

  unreachable!("test failed");
}

#[tokio::test]
#[serial]
async fn test_should_be_able_to_trigger_early_drop_with_mem() {
  let (tx, mut rx) = mpsc::unbounded_channel();
  let tb = TestBedBuilder::new("./test_cases/main")
    .with_per_worker_policy(None)
    .with_worker_event_sender(Some(tx))
    .build()
    .await;

  let resp = tb
    .request(|b| {
      b.uri("/early-drop-mem")
        .header("x-memory-limit-mb", HeaderValue::from_static("30"))
        .body(Body::empty())
        .context("can't make request")
    })
    .await
    .unwrap();

  assert_eq!(resp.status().as_u16(), StatusCode::OK);

  sleep(Duration::from_secs(2)).await;
  rx.close();
  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;

  while let Some(ev) = rx.recv().await {
    let WorkerEvents::Log(ev) = ev.event else {
      continue;
    };
    if ev.level != LogLevel::Info {
      continue;
    }
    if ev.msg.contains("early_drop") {
      return;
    }
  }

  unreachable!("test failed");
}

#[tokio::test]
#[serial]
async fn test_eszip_wasm_import() {
  integration_test!(
    "./test_cases/main",
    NON_SECURE_PORT,
    "eszip-wasm",
    None,
    None,
    None,
    (|resp| async {
      let resp = resp.unwrap();
      assert_eq!(resp.status().as_u16(), 200);
      assert_eq!(resp.text().await.unwrap().as_str(), "meow");
    }),
    TerminationToken::new()
  );
}

#[tokio::test]
#[serial]
async fn test_request_absent_timeout() {
  let (tx, mut rx) = mpsc::unbounded_channel();
  let tb = TestBedBuilder::new("./test_cases/main")
    .with_per_worker_policy(None)
    .with_worker_event_sender(Some(tx))
    .build()
    .await;

  let resp = tb
    .request(|b| {
      b.uri("/sleep-5000ms")
        .header("x-worker-timeout-ms", HeaderValue::from_static("3600000"))
        .header(
          "x-context-supervisor-request-absent-timeout-ms",
          HeaderValue::from_static("1000"),
        )
        .body(Body::empty())
        .context("can't make request")
    })
    .await
    .unwrap();

  assert_eq!(resp.status().as_u16(), StatusCode::OK);

  sleep(Duration::from_secs(3)).await;
  rx.close();
  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;

  while let Some(ev) = rx.recv().await {
    let WorkerEvents::Shutdown(ev) = ev.event else {
      continue;
    };
    if ev.reason != ShutdownReason::EarlyDrop {
      break;
    }
    return;
  }

  unreachable!("test failed");
}

#[tokio::test]
#[serial]
async fn test_user_workers_cleanup_idle_workers() {
  let (tx, mut rx) = mpsc::unbounded_channel();
  let tb = TestBedBuilder::new("./test_cases/main")
    .with_per_worker_policy(None)
    .with_worker_event_sender(Some(tx))
    .build()
    .await;

  let resp = tb
    .request(|b| {
      b.uri("/sleep-5000ms")
        .header("x-worker-timeout-ms", HeaderValue::from_static("3600000"))
        .body(Body::empty())
        .context("can't make request")
    })
    .await
    .unwrap();

  assert_eq!(resp.status().as_u16(), StatusCode::OK);

  let mut resp = tb
    .request(|b| {
      b.uri("/_internal/cleanup-idle-workers")
        .body(Body::empty())
        .context("can't make request")
    })
    .await
    .unwrap();

  assert_eq!(resp.status().as_u16(), StatusCode::OK);

  let bytes = hyper_v014::body::HttpBody::collect(resp.body_mut())
    .await
    .unwrap()
    .to_bytes();

  let payload = serde_json::from_slice::<serde_json::Value>(&bytes).unwrap();
  let count = payload.get("count").unwrap().as_u64().unwrap();

  assert_eq!(count, 1);

  sleep(Duration::from_secs(3)).await;

  rx.close();
  tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
  while let Some(ev) = rx.recv().await {
    let WorkerEvents::Shutdown(ev) = ev.event else {
      continue;
    };
    if ev.reason != ShutdownReason::EarlyDrop {
      break;
    }
    return;
  }

  unreachable!("test failed");
}

#[tokio::test]
#[serial]
async fn test_no_npm() {
  async fn send_it(
    no_npm: bool,
    tx: mpsc::UnboundedSender<WorkerEventWithMetadata>,
  ) -> (TestBed, u16) {
    let tb = TestBedBuilder::new("./test_cases/main")
      .with_per_worker_policy(None)
      .with_worker_event_sender(Some(tx))
      .build()
      .await;

    let resp = tb
      .request(|mut b| {
        b = b.uri("/npm-import-with-package-json");
        if no_npm {
          b = b.header("x-no-npm", HeaderValue::from_static("1"))
        }
        b.body(Body::empty()).context("can't make request")
      })
      .await
      .unwrap();

    (tb, resp.status().as_u16())
  }

  {
    // Since `noNpm` is configured, it will not discover package.json, and
    // `npm:is-even` will resolve normally using Deno's original module
    // resolution method.
    let (tx, mut rx) = mpsc::unbounded_channel();
    let (tb, status_code) = send_it(true, tx).await;

    assert_eq!(status_code, StatusCode::OK);

    rx.close();
    tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;
  }
  {
    // Note that `noNpm` is not set this time. In this case, it will try to
    // discover package.json and will eventually find it.
    //
    // This causes it to switch to Byonm mode and try to resolve modules from
    // the adjacent node_modules/.
    //
    // However, since node_modules/ does not exist anywhere, the attempt to
    // resolve `npm:is-even` will fail.
    let (tx, mut rx) = mpsc::unbounded_channel();
    let (tb, status_code) = send_it(false, tx).await;

    assert_eq!(status_code, StatusCode::INTERNAL_SERVER_ERROR);

    rx.close();
    tb.exit(Duration::from_secs(TESTBED_DEADLINE_SEC)).await;

    while let Some(ev) = rx.recv().await {
      let WorkerEvents::BootFailure(ev) = ev.event else {
        continue;
      };

      assert!(ev.msg.starts_with(
        "worker boot error: failed to bootstrap runtime: failed to create the \
graph: Could not find a matching package for 'npm:is-even' in the node_modules \
directory."
      ));

      return;
    }

    unreachable!("test failed");
  }
}

#[tokio::test]
#[serial]
async fn test_user_worker_with_import_map() {
  let assert_fn = |resp: Result<Response, reqwest::Error>| async {
    let res = resp.unwrap();
    let status = res.status().as_u16();

    let body_bytes = res.bytes().await.unwrap();
    let body_str = String::from_utf8_lossy(&body_bytes);

    assert_eq!(
      status, 200,
      "Expected 200, got {} with body: {}",
      status, body_str
    );

    assert!(
      body_str.contains("import map works!"),
      "Expected import map works!, got: {}",
      body_str
    );
  };
  {
    integration_test!(
      "./test_cases/user-worker-with-import-map",
      NON_SECURE_PORT,
      "import_map",
      None,
      None,
      None,
      (assert_fn),
      TerminationToken::new()
    );
  }
  {
    integration_test!(
      "./test_cases/user-worker-with-import-map",
      NON_SECURE_PORT,
      "inline_import_map",
      None,
      None,
      None,
      (assert_fn),
      TerminationToken::new()
    );
  }
}

#[tokio::test]
#[serial]
async fn test_pin_package_version_correctly() {
  integration_test!(
    "./test_cases/pin-package",
    NON_SECURE_PORT,
    "",
    None,
    None,
    None,
    (|resp| async {
      let res = resp.unwrap();
      assert!(res.status().as_u16() == 200);

      let body_bytes = res.bytes().await.unwrap();
      assert_eq!(body_bytes, r#"3.0.0"#);
    }),
    TerminationToken::new()
  );
}

#[tokio::test]
#[serial]
async fn test_drop_socket_when_http_handler_returns_an_invalid_value() {
  {
    integration_test!(
      "./test_cases/main",
      NON_SECURE_PORT,
      "return-invalid-resp",
      None,
      None,
      None,
      (|resp| async {
        let res = resp.unwrap();
        assert!(res.status().as_u16() == 502);
      }),
      TerminationToken::new()
    );
  }
  {
    integration_test!(
      "./test_cases/main",
      NON_SECURE_PORT,
      "return-invalid-resp-2",
      None,
      None,
      None,
      (|resp| async {
        let res = resp.unwrap();
        assert!(res.status().as_u16() == 502);
      }),
      TerminationToken::new()
    );
  }
}

#[tokio::test]
#[serial]
async fn test_brotli_async() {
  integration_test!(
    "./test_cases/main",
    NON_SECURE_PORT,
    "brotli-async",
    None,
    None,
    None,
    (|resp| async {
      let resp = resp.unwrap();
      assert_eq!(resp.status().as_u16(), 200);
      assert_eq!(resp.text().await.unwrap().as_str(), "meow");
    }),
    TerminationToken::new()
  );
}

async fn assert_rate_limit_error(
  resp: Result<reqwest::Response, reqwest::Error>,
) {
  let res = resp.unwrap();
  assert_eq!(
    res.status().as_u16(),
    StatusCode::INTERNAL_SERVER_ERROR,
    "expected the chain to be rate-limited"
  );
  let body = res.text().await.unwrap();
  assert!(
    body.contains("RateLimitError"),
    "expected RateLimitError in body, got: {body}"
  );
}

/// Verifies that the local (traced) budget cuts off A→B→A circular chains.
async fn test_outbound_rate_limit_circular(http_mode: &'static str) {
  const TRACEPARENT: &str =
    "00-12345678901234567890123456789012-1234567890123456-01";

  init_otel();

  integration_test_with_server_flag!(
    ServerFlags {
      rate_limit_cleanup_interval_sec: 60,
      request_wait_timeout_ms: Some(30_000),
      ..Default::default()
    },
    "./test_cases/rate-limit-main",
    NON_SECURE_PORT,
    "rate-limit-a",
    None,
    Some(
      reqwest::Client::new()
        .get(format!("http://localhost:{}/rate-limit-a", NON_SECURE_PORT))
        .header("traceparent", TRACEPARENT)
        .header("x-http-mode", http_mode)
        .header(
          "x-test-server-url",
          format!("http://localhost:{}", NON_SECURE_PORT),
        )
    ),
    None::<Tls>,
    (|resp| async { assert_rate_limit_error(resp).await }),
    TerminationToken::new()
  );
}

#[tokio::test]
#[serial]
async fn test_outbound_rate_limit_circular_fetch() {
  test_outbound_rate_limit_circular("fetch").await;
}

#[tokio::test]
#[serial]
async fn test_outbound_rate_limit_circular_node_http() {
  test_outbound_rate_limit_circular("node").await;
}

/// Verifies that the global (untraced) budget cuts off a self-calling worker.
async fn test_outbound_rate_limit_global(http_mode: &'static str) {
  init_otel();

  integration_test_with_server_flag!(
    ServerFlags {
      rate_limit_cleanup_interval_sec: 60,
      request_wait_timeout_ms: Some(30_000),
      ..Default::default()
    },
    "./test_cases/rate-limit-main",
    NON_SECURE_PORT,
    "rate-limit-untraced",
    None,
    Some(
      reqwest::Client::new()
        .get(format!(
          "http://localhost:{}/rate-limit-untraced",
          NON_SECURE_PORT
        ))
        .header("x-http-mode", http_mode)
        .header(
          "x-test-server-url",
          format!("http://localhost:{}", NON_SECURE_PORT),
        )
    ),
    None::<Tls>,
    (|resp| async { assert_rate_limit_error(resp).await }),
    TerminationToken::new()
  );
}

#[tokio::test]
#[serial]
async fn test_outbound_rate_limit_global_budget_fetch() {
  test_outbound_rate_limit_global("fetch").await;
}

#[tokio::test]
#[serial]
async fn test_outbound_rate_limit_global_budget_node_http() {
  test_outbound_rate_limit_global("node").await;
}

/// Verifies that `AsyncVariable` isolates trace IDs per request.
///
/// Sends N concurrent requests to the `rate-limit-echo` worker, each carrying
/// a distinct `traceparent`.  The worker reads the `AsyncVariable` set by
/// `http.js` and echoes the trace ID back.  If any response contains a trace
/// ID that does not match the one sent in that request, context has leaked.
#[tokio::test]
#[serial]
async fn test_request_trace_id_isolation() {
  const N: usize = 20;

  init_otel();

  integration_test_with_server_flag!(
    ServerFlags {
      rate_limit_cleanup_interval_sec: 60,
      request_wait_timeout_ms: Some(30_000),
      ..Default::default()
    },
    "./test_cases/rate-limit-main",
    NON_SECURE_PORT,
    "rate-limit-echo",
    None,
    None::<reqwest::RequestBuilder>,
    None::<Tls>,
    (
      |(port, _url, _req_builder, _event_rx, _metric_src)| async move {
        let client = std::sync::Arc::new(reqwest::Client::new());
        let base = std::sync::Arc::new(format!(
          "http://localhost:{}/rate-limit-echo",
          port
        ));

        // Build N futures, each with a unique trace ID.
        let futs: Vec<_> = (0..N)
          .map(|i| {
            let client = client.clone();
            let base = base.clone();
            // Each trace ID is a 32-hex-char string unique to this request.
            let trace_id = format!("{:032x}", i);
            let traceparent = format!("00-{}-1234567890123456-01", trace_id);
            async move {
              let resp = client
                .get(base.as_str())
                .header("traceparent", &traceparent)
                .send()
                .await
                .unwrap();
              assert!(
                resp.status().is_success(),
                "request {i} failed with status {}",
                resp.status()
              );
              let body: serde_json::Value = resp.json().await.unwrap();
              let returned = body["traceId"].as_str().unwrap_or("").to_string();
              (trace_id, returned)
            }
          })
          .collect();

        // Run all requests concurrently.
        let results = futures_util::future::join_all(futs).await;

        for (expected, got) in &results {
          assert_eq!(
            expected, got,
            "AsyncVariable context leaked: expected trace_id={expected} but worker saw {got}"
          );
        }

        // Return the last response to satisfy the macro's type requirement.
        Some(Ok(
          reqwest::Client::new()
            .get(base.as_str())
            .send()
            .await
            .unwrap(),
        ))
      },
      |_resp| async {}
    ),
    TerminationToken::new()
  );
}

/// Verifies that `RateLimitError.retryAfterMs` is a positive number when the
/// global (untraced) budget is exhausted.
#[tokio::test]
#[serial]
async fn test_rate_limit_retry_after_ms() {
  init_otel();

  // The budget in rate-limit-main is 10.  Send 11 requests; the last one
  // should come back as 429 with a positive retryAfterMs value.
  const BUDGET: usize = 10;

  integration_test_with_server_flag!(
    ServerFlags {
      rate_limit_cleanup_interval_sec: 60,
      request_wait_timeout_ms: Some(30_000),
      ..Default::default()
    },
    "./test_cases/rate-limit-main",
    NON_SECURE_PORT,
    "rate-limit-retry-after",
    None,
    None::<reqwest::RequestBuilder>,
    None::<Tls>,
    (
      |(port, _url, _req_builder, _event_rx, _metric_src)| async move {
        let client = reqwest::Client::new();
        let url = format!("http://localhost:{}/rate-limit-retry-after", port);
        let server_url = format!("http://localhost:{}", port);

        // Exhaust the budget.
        for _ in 0..BUDGET {
          let resp = client
            .get(&url)
            .header("x-test-server-url", &server_url)
            .send()
            .await
            .unwrap();
          // Each of these may itself trigger an inner fetch that is counted
          // against the budget; we only care about the final one below.
          let _ = resp;
        }

        // This request should be rate-limited.
        let resp = client
          .get(&url)
          .header("x-test-server-url", &server_url)
          .send()
          .await;

        Some(resp)
      },
      |resp| async move {
        let res = resp.unwrap();
        assert_eq!(
          res.status().as_u16(),
          429,
          "expected 429 from rate-limited request"
        );
        let body: serde_json::Value = res.json().await.unwrap();
        assert_eq!(
          body["name"].as_str().unwrap_or(""),
          "RateLimitError",
          "expected RateLimitError in body, got: {body}"
        );
        let retry_after_ms = body["retryAfterMs"]
          .as_u64()
          .expect("retryAfterMs should be a non-null number");
        assert!(
          retry_after_ms > 0,
          "retryAfterMs should be positive, got {retry_after_ms}"
        );
      }
    ),
    TerminationToken::new()
  );
}

#[derive(Deserialize)]
struct ErrorResponsePayload {
  msg: String,
}

impl ErrorResponsePayload {
  async fn assert_error_response(
    resp: Result<Response, reqwest::Error>,
  ) -> (Self, u16) {
    let res = resp.unwrap();
    let status = res.status().as_u16();
    let res = res.json::<Self>().await;

    assert!(res.is_ok());

    (res.unwrap(), status)
  }
}

trait AsyncReadWrite: AsyncRead + AsyncWrite + Send + Unpin {}

impl<T> AsyncReadWrite for T where T: AsyncRead + AsyncWrite + Send + Unpin {}

trait TlsExt {
  fn client(&self) -> Client;
  fn schema(&self) -> &'static str;
  fn sock_addr(&self) -> SocketAddr;
  fn port(&self) -> u16;
  fn stream(&self) -> BoxFuture<'static, Box<dyn AsyncReadWrite>>;
  fn server_config(&self) -> rustls::ServerConfig;
}

impl TlsExt for Option<Tls> {
  fn client(&self) -> Client {
    if self.is_some() {
      Client::builder()
        .add_root_certificate(
          Certificate::from_pem(TLS_LOCALHOST_ROOT_CA).unwrap(),
        )
        .build()
        .unwrap()
    } else {
      Client::new()
    }
  }

  fn schema(&self) -> &'static str {
    if self.is_some() {
      "https"
    } else {
      "http"
    }
  }

  fn sock_addr(&self) -> SocketAddr {
    const SOCK_ADDR_SECURE: SocketAddr =
      SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), SECURE_PORT);

    const SOCK_ADDR_NON_SECURE: SocketAddr =
      SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), NON_SECURE_PORT);

    if self.is_some() {
      SOCK_ADDR_SECURE
    } else {
      SOCK_ADDR_NON_SECURE
    }
  }

  fn port(&self) -> u16 {
    if self.is_some() {
      SECURE_PORT
    } else {
      NON_SECURE_PORT
    }
  }

  fn stream(&self) -> BoxFuture<'static, Box<dyn AsyncReadWrite>> {
    let use_tls = self.is_some();
    let sock_addr = self.sock_addr();

    async move {
      if use_tls {
        let mut cursor = Cursor::new(Vec::from(TLS_LOCALHOST_ROOT_CA));
        let certs = rustls_pemfile::certs(&mut cursor)
          .collect::<Result<Vec<_>, _>>()
          .unwrap();

        let mut root_cert_store = RootCertStore::empty();
        let _ = root_cert_store.add_parsable_certificates(certs);

        let config = ClientConfig::builder()
          .with_root_certificates(root_cert_store)
          .with_no_client_auth();

        let connector = TlsConnector::from(Arc::new(config));
        let dnsname = ServerName::try_from("localhost").unwrap();

        let stream = TcpStream::connect(sock_addr).await.unwrap();
        let stream = connector.connect(dnsname, stream).await.unwrap();

        Box::new(stream) as Box<dyn AsyncReadWrite>
      } else {
        let stream = TcpStream::connect(sock_addr).await.unwrap();

        Box::new(stream) as Box<dyn AsyncReadWrite>
      }
    }
    .boxed()
  }

  fn server_config(&self) -> rustls::ServerConfig {
    assert!(self.is_some());
    let certs =
      rustls_pemfile::certs(&mut std::io::BufReader::new(TLS_LOCALHOST_CERT))
        .flatten()
        .collect();
    let key = rustls_pemfile::private_key(&mut std::io::BufReader::new(
      TLS_LOCALHOST_KEY,
    ))
    .into_iter()
    .flatten()
    .next()
    .unwrap();

    rustls::ServerConfig::builder()
      .with_no_client_auth()
      .with_single_cert(certs, key)
      .unwrap()
  }
}

fn new_localhost_tls(secure: bool) -> Option<Tls> {
  secure.then(|| {
    Tls::new(SECURE_PORT, TLS_LOCALHOST_KEY, TLS_LOCALHOST_CERT).unwrap()
  })
}

/// Every `User-Agent` the echo server has been sent, in arrival order. Requests
/// that never get a response of their own (a websocket handshake, say) are only
/// observable this way.
type SeenUserAgents = Arc<std::sync::Mutex<Vec<String>>>;

/// Starts a server that answers every request with the `User-Agent` it arrived
/// with, until the token is cancelled.
async fn start_echo_user_agent_server(
  token: CancellationToken,
) -> (String, SeenUserAgents, tokio::task::JoinHandle<()>) {
  let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
  let url = format!("http://{}", listener.local_addr().unwrap());
  let seen = SeenUserAgents::default();
  let server = tokio::spawn({
    let seen = seen.clone();
    async move {
      loop {
        tokio::select! {
          Ok((stream, _)) = listener.accept() => {
            let seen = seen.clone();
            tokio::spawn(async move {
              hyper::server::conn::Http::new()
                .serve_connection(
                  stream,
                  hyper::service::service_fn(move |req: Request<Body>| {
                    let seen = seen.clone();
                    async move {
                      let user_agent = req
                        .headers()
                        .get(header::USER_AGENT)
                        .and_then(|it| it.to_str().ok())
                        .unwrap_or_default()
                        .to_string();

                      seen.lock().unwrap().push(user_agent.clone());
                      Ok::<_, hyper::Error>(
                        HttpResponse::new(Body::from(user_agent)),
                      )
                    }
                  }),
                )
                .await
                .ok();
            });
          }
          _ = token.cancelled() => break,
        }
      }
    }
  });

  (url, seen, server)
}

/// Serves `path` from `main_with_project_ref` and asserts the function's
/// outbound request reached the echo server with `expected` as `User-Agent`.
async fn assert_echoed_user_agent(
  path: &str,
  echo_url: &str,
  project_ref: Option<&str>,
  user_agent: Option<&str>,
  expected: String,
) {
  let mut builder = Client::new()
    .request(
      Method::POST,
      format!("http://localhost:{}/{}", NON_SECURE_PORT, path),
    )
    .header("x-echo-url", echo_url);

  if let Some(project_ref) = project_ref {
    builder = builder.header("x-project-ref", project_ref);
  }
  if let Some(user_agent) = user_agent {
    builder = builder.header("x-set-user-agent", user_agent);
  }

  integration_test!(
    "./test_cases/main_with_project_ref",
    NON_SECURE_PORT,
    "",
    None,
    Some(builder),
    None,
    (|resp| async move {
      let resp = resp.unwrap();
      assert_eq!(resp.status().as_u16(), StatusCode::OK);
      assert_eq!(resp.text().await.unwrap(), expected);
    }),
    TerminationToken::new()
  );
}

#[tokio::test]
#[serial]
async fn test_outbound_user_agent_is_stamped_with_project_ref() {
  const PROJECT_REF: &str = "abcdefghijklmnopqrst";

  let token = CancellationToken::new();
  let (echo_url, _, server) = start_echo_user_agent_server(token.clone()).await;
  let ref_comment = deno::versions::user_agent_comment(Some(PROJECT_REF));

  // A function that sends no `User-Agent` of its own gets the runtime's, and
  // the project ref is part of it.
  assert_echoed_user_agent(
    "user-agent",
    &echo_url,
    Some(PROJECT_REF),
    None,
    deno::versions::user_agent(Some(PROJECT_REF)),
  )
  .await;

  // A function that sets its own `User-Agent` keeps it, but cannot drop the
  // project ref.
  assert_echoed_user_agent(
    "user-agent",
    &echo_url,
    Some(PROJECT_REF),
    Some("curl/8.7.1"),
    format!("curl/8.7.1 {ref_comment}"),
  )
  .await;

  // Without a project ref, nothing is stamped.
  assert_echoed_user_agent(
    "user-agent",
    &echo_url,
    None,
    Some("curl/8.7.1"),
    "curl/8.7.1".into(),
  )
  .await;

  token.cancel();
  server.await.unwrap();
}

#[tokio::test]
#[serial]
async fn test_outbound_node_http_user_agent_is_stamped_with_project_ref() {
  const PROJECT_REF: &str = "abcdefghijklmnopqrst";

  let token = CancellationToken::new();
  let (echo_url, _, server) = start_echo_user_agent_server(token.clone()).await;
  let ref_comment = deno::versions::user_agent_comment(Some(PROJECT_REF));

  // `node:http` sends no `User-Agent` of its own, so it gets the runtime's.
  assert_echoed_user_agent(
    "user-agent-node",
    &echo_url,
    Some(PROJECT_REF),
    None,
    deno::versions::user_agent(Some(PROJECT_REF)),
  )
  .await;

  // One the caller set is kept, with the project appended to it.
  assert_echoed_user_agent(
    "user-agent-node",
    &echo_url,
    Some(PROJECT_REF),
    Some("curl/8.7.1"),
    format!("curl/8.7.1 {ref_comment}"),
  )
  .await;

  token.cancel();
  server.await.unwrap();
}

#[tokio::test]
#[serial]
async fn test_outbound_node_http2_user_agent_is_stamped_with_project_ref() {
  const PROJECT_REF: &str = "abcdefghijklmnopqrst";

  let token = CancellationToken::new();
  let (echo_url, _, server) = start_echo_user_agent_server(token.clone()).await;
  let ref_comment = deno::versions::user_agent_comment(Some(PROJECT_REF));

  // `node:http2` sends no `User-Agent` of its own, so it gets the runtime's.
  assert_echoed_user_agent(
    "user-agent-http2",
    &echo_url,
    Some(PROJECT_REF),
    None,
    deno::versions::user_agent(Some(PROJECT_REF)),
  )
  .await;

  // One the caller set is kept, with the project appended to it.
  assert_echoed_user_agent(
    "user-agent-http2",
    &echo_url,
    Some(PROJECT_REF),
    Some("curl/8.7.1"),
    format!("curl/8.7.1 {ref_comment}"),
  )
  .await;

  token.cancel();
  server.await.unwrap();
}

#[tokio::test]
#[serial]
async fn test_websocket_handshake_user_agent_is_stamped_with_project_ref() {
  const PROJECT_REF: &str = "abcdefghijklmnopqrst";

  let token = CancellationToken::new();
  let (echo_url, seen, server) =
    start_echo_user_agent_server(token.clone()).await;

  let builder = Client::new()
    .request(
      Method::POST,
      format!("http://localhost:{}/user-agent-websocket", NON_SECURE_PORT),
    )
    .header("x-project-ref", PROJECT_REF)
    .header("x-echo-url", &echo_url);

  integration_test!(
    "./test_cases/main_with_project_ref",
    NON_SECURE_PORT,
    "",
    None,
    Some(builder),
    None,
    (|resp| async move {
      assert_eq!(resp.unwrap().status().as_u16(), StatusCode::OK);
    }),
    TerminationToken::new()
  );

  token.cancel();
  server.await.unwrap();

  assert_eq!(
    seen.lock().unwrap().as_slice(),
    [deno::versions::user_agent(Some(PROJECT_REF))]
  );
}
