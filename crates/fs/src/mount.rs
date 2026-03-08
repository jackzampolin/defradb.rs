/// Mount/unmount lifecycle for the DefraDB FUSE filesystem.
use std::path::Path;
use std::sync::Arc;

use storage::corekv::Store;

use crate::ops::DefraFs;

/// Handle to a mounted DefraDB filesystem.
///
/// The filesystem is unmounted when this handle is dropped.
pub struct MountHandle {
    session: fuser::BackgroundSession,
}

impl MountHandle {
    /// Unmount the filesystem.
    pub fn unmount(self) {
        drop(self.session);
    }
}

/// Mount a DefraDB instance as a FUSE filesystem.
///
/// Returns a `MountHandle` that keeps the filesystem mounted.
/// The filesystem is unmounted when the handle is dropped.
///
/// # Arguments
///
/// * `db` - The DefraDB instance to expose
/// * `mountpoint` - Directory to mount on (must exist)
/// * `rt` - Tokio runtime handle for async DB operations
pub fn mount<S: Store + 'static>(
    db: Arc<db::DB<S>>,
    mountpoint: &Path,
    rt: tokio::runtime::Handle,
) -> Result<MountHandle, crate::Error> {
    let fs = DefraFs::new(db, rt);

    let options = &[
        fuser::MountOption::RW,
        fuser::MountOption::FSName("defradb".into()),
        fuser::MountOption::AutoUnmount,
        fuser::MountOption::AllowOther,
    ];

    let session = fuser::spawn_mount2(fs, mountpoint, options)?;

    Ok(MountHandle { session })
}
