use std::cell::RefCell;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;

use anyhow::bail;
use anyhow::Context;
use deno::DenoOptionsBuilder;
use deno_core::error::AnyError;
use deno_core::op2;
use deno_core::OpState;
use deno_core::ToJsBuffer;
use deno_facade::generate_binary_eszip;
use deno_facade::EmitterFactory;
use deno_facade::Metadata;

use crate::runtime::permissions::get_default_permissions;
use crate::WorkerKind;

/// Whether this isolate may turn a path on the host filesystem into an eszip.
///
/// Only the main worker gets this. A user worker with it could resolve and read
/// arbitrary module graphs, which is precisely what its sandbox denies.
pub struct AllowBundle(pub bool);

/// The directory that bundle entrypoints are confined to.
///
/// Installed only on the main worker. Bundling reads and resolves whatever the
/// entrypoint points at, so a canonical path that escapes this root is refused.
pub struct BundleRoot(pub PathBuf);

/// The directory bundle entrypoints are confined to.
///
/// In production the runtime starts at the repository root, so this is just the
/// current directory. `cargo test` runs from `crates/base`, though, and the
/// shipped `examples/e2b-executor` it must bundle lives above that — so walk up
/// to the workspace root (the ancestor holding `deno.json`) to keep both the
/// tests and production inside one root. Falls back to the current directory if
/// no marker is found, and to `None` only when the current directory cannot be
/// resolved, in which case bundling is left unconfined.
pub fn bundle_root() -> Option<PathBuf> {
  let cwd = std::env::current_dir().ok()?.canonicalize().ok()?;
  let mut dir = cwd.clone();
  loop {
    if dir.join("deno.json").is_file() {
      return Some(dir);
    }
    if !dir.pop() {
      return Some(cwd);
    }
  }
}

/// Bundles a service entrypoint into eszip bytes.
///
/// The bytes are meant to be handed back as `maybeEszip` when creating user
/// workers: without it every `create` re-resolves the module graph, transpiles
/// it, and walks the npm vfs, which dominates worker boot time.
#[op2(async)]
#[serde]
#[allow(clippy::arc_with_non_send_sync)]
pub async fn op_bundle_service(
  state: Rc<RefCell<OpState>>,
  #[string] entrypoint: String,
) -> Result<ToJsBuffer, AnyError> {
  // Scoped so the borrow is gone before the first await.
  let permitted = state
    .borrow()
    .try_borrow::<AllowBundle>()
    .map(|it| it.0)
    .unwrap_or(false);

  if !permitted {
    bail!("bundling is only available to the main worker");
  }

  let bundle_root = state
    .borrow()
    .try_borrow::<BundleRoot>()
    .map(|it| it.0.clone());

  let path = PathBuf::from(&entrypoint);
  if !path.is_file() {
    bail!("entrypoint is not a file ({entrypoint})");
  }

  let path = path
    .canonicalize()
    .with_context(|| format!("failed to resolve entrypoint ({entrypoint})"))?;

  // Both sides are canonical, so a symlink cannot smuggle the entrypoint back
  // out of the root.
  if let Some(root) = &bundle_root {
    if !path.starts_with(root) {
      bail!("entrypoint is outside the bundle root");
    }
  }

  let mut emitter_factory = EmitterFactory::new();

  emitter_factory.set_permissions_options(Some(get_default_permissions(
    WorkerKind::MainWorker,
  )));
  emitter_factory
    .set_deno_options(DenoOptionsBuilder::new().entrypoint(path).build()?);

  let mut metadata = Metadata::default();
  let eszip = generate_binary_eszip(
    &mut metadata,
    Arc::new(emitter_factory),
    None,
    None,
    None,
  )
  .await?;

  Ok(eszip.into_bytes().into())
}

deno_core::extension!(base_runtime_bundle, ops = [op_bundle_service],);
