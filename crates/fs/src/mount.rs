/// Mount/unmount lifecycle for the DefraDB FUSE filesystem.
use std::path::Path;
use std::sync::Arc;

use storage::corekv::Store;

use crate::ops::DefraFs;

/// Options for mounting the DefraDB filesystem.
pub struct MountOptions {
    /// Mount as read-only (disables write/create/unlink).
    /// When true, all mutating operations return EROFS.
    pub read_only: bool,
}

impl Default for MountOptions {
    fn default() -> Self {
        Self { read_only: false }
    }
}

impl MountOptions {
    pub fn read_only() -> Self {
        Self { read_only: true }
    }
}

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
pub fn mount<S: Store + 'static>(
    db: Arc<db::DB<S>>,
    mountpoint: &Path,
    rt: tokio::runtime::Handle,
    options: MountOptions,
) -> Result<MountHandle, crate::Error> {
    let fs = DefraFs::new(db, rt, options.read_only);

    let mut fuse_options = vec![
        fuser::MountOption::FSName("defradb".into()),
        fuser::MountOption::AutoUnmount,
        fuser::MountOption::AllowOther,
    ];

    if options.read_only {
        fuse_options.push(fuser::MountOption::RO);
    } else {
        fuse_options.push(fuser::MountOption::RW);
    }

    let session = fuser::spawn_mount2(fs, mountpoint, &fuse_options)?;

    Ok(MountHandle { session })
}
