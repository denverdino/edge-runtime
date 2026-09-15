pub static CLI_SNAPSHOT: &[u8] =
  include_bytes!(concat!(env!("OUT_DIR"), "/RUNTIME_SNAPSHOT.bin"));
pub static E2B_SNAPSHOT: &[u8] =
  include_bytes!(concat!(env!("OUT_DIR"), "/RUNTIME_SNAPSHOT_E2B.bin"));

include!(concat!(env!("OUT_DIR"), "/e2b_snapshot_extensions.rs"));

pub fn snapshot() -> Option<&'static [u8]> {
  let data = CLI_SNAPSHOT;
  Some(data)
}
