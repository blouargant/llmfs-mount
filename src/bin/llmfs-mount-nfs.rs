use tracing::{error, info};

use llmfs_mount::setup::setup;

fn main() {
    let s = setup(true);
    let mut daemon_guard = llmfs_mount::daemon::DaemonGuard::from_env();

    if let Err(e) = s.runtime.block_on(llmfs_mount::nfs::mount_nfs(
        s.virtual_fs,
        &s.mount_point,
        s.metadata_ttl_ms,
        s.read_only,
        daemon_guard.as_mut(),
    )) {
        error!("NFS mount failed: {}", e);
        std::process::exit(1);
    }

    info!("Unmounted cleanly");
}
