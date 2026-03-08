/// FUSE filesystem implementation backed by DefraDB.
///
/// Maps FUSE operations to DefraDB collection/document CRUD:
/// - readdir(root) → list_collections()
/// - readdir(collection) → collection.get_all()
/// - lookup/read(doc) → collection.get()
/// - write/create → collection.create() or collection.save()
/// - unlink → collection.delete()
/// - rmdir → ENOTEMPTY (collections cannot be dropped via fs)
use std::ffi::OsStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fuser::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, ReplyWrite, Request,
};
use parking_lot::Mutex;
use storage::corekv::Store;
use tokio::runtime::Handle;

use crate::errno::db_err_to_errno;
use crate::inode::{InodeTable, InodeTarget, ROOT_INO};

/// TTL for filesystem attributes (short, since DB can change).
const TTL: Duration = Duration::from_secs(1);

/// Epoch time used for all file timestamps.
const EPOCH: SystemTime = UNIX_EPOCH;

/// FUSE filesystem backed by a DefraDB instance.
pub struct DefraFs<S: Store> {
    db: Arc<db::DB<S>>,
    inodes: Mutex<InodeTable>,
    rt: Handle,
    read_only: bool,
    /// Per-inode write buffers for accumulating write() data before flush.
    write_buffers: Mutex<std::collections::HashMap<u64, Vec<u8>>>,
}

impl<S: Store> DefraFs<S> {
    pub fn new(db: Arc<db::DB<S>>, rt: Handle, read_only: bool) -> Self {
        Self {
            db,
            inodes: Mutex::new(InodeTable::new()),
            rt,
            read_only,
            write_buffers: Mutex::new(std::collections::HashMap::new()),
        }
    }

    fn dir_attr(ino: u64, perm: u16) -> FileAttr {
        FileAttr {
            ino,
            size: 0,
            blocks: 0,
            atime: EPOCH,
            mtime: EPOCH,
            ctime: EPOCH,
            crtime: EPOCH,
            kind: FileType::Directory,
            perm,
            nlink: 2,
            uid: 0,
            gid: 0,
            rdev: 0,
            blksize: 512,
            flags: 0,
        }
    }

    fn file_attr(ino: u64, size: u64, perm: u16) -> FileAttr {
        FileAttr {
            ino,
            size,
            blocks: (size + 511) / 512,
            atime: EPOCH,
            mtime: EPOCH,
            ctime: EPOCH,
            crtime: EPOCH,
            kind: FileType::RegularFile,
            perm,
            nlink: 1,
            uid: 0,
            gid: 0,
            rdev: 0,
            blksize: 512,
            flags: 0,
        }
    }

    fn dir_perm(&self) -> u16 {
        if self.read_only { 0o555 } else { 0o755 }
    }

    fn file_perm(&self) -> u16 {
        if self.read_only { 0o444 } else { 0o644 }
    }

    /// Fetch document JSON bytes for a given collection + doc_id.
    /// Returns Ok(bytes) on success, Err(errno) on failure.
    fn fetch_doc_json(&self, collection_name: &str, doc_id_str: &str) -> Result<Vec<u8>, i32> {
        let db = self.db.clone();
        let collection_name = collection_name.to_string();
        let doc_id_str = doc_id_str.to_string();

        self.rt.block_on(async move {
            let col = db
                .get_collection(&collection_name)
                .map_err(|e| db_err_to_errno(&e))?
                .ok_or(libc::ENOENT)?;
            let doc_id = document::DocID::from_string(&doc_id_str).map_err(|_| libc::EINVAL)?;
            let txn = db.new_txn(true).await.map_err(|e| db_err_to_errno(&e))?;
            let doc = col
                .get(&txn, &doc_id)
                .await
                .map_err(|e| db_err_to_errno(&e))?
                .ok_or(libc::ENOENT)?;
            txn.commit().await.map_err(|e| db_err_to_errno(&e))?;
            let map = doc.to_map().map_err(|_| libc::EIO)?;
            serde_json::to_vec_pretty(&map).map_err(|_| libc::EIO)
        })
    }

    /// List all doc IDs in a collection.
    fn list_doc_ids(&self, collection_name: &str) -> Vec<String> {
        let db = self.db.clone();
        let collection_name = collection_name.to_string();

        self.rt.block_on(async move {
            let col = match db.get_collection(&collection_name) {
                Ok(Some(c)) => c,
                _ => return vec![],
            };
            let txn = match db.new_txn(true).await {
                Ok(t) => t,
                _ => return vec![],
            };
            let docs = match col.get_all(&txn).await {
                Ok(d) => d,
                _ => return vec![],
            };
            let _ = txn.commit().await;
            docs.iter()
                .filter_map(|d| d.id().map(|id| id.to_string()))
                .collect()
        })
    }

    /// Validate that a filename represents a valid doc ID.
    /// Strips .json suffix and checks the bae-xxx format.
    fn parse_doc_id_from_filename(name: &str) -> Result<&str, i32> {
        let doc_id_str = name.strip_suffix(".json").unwrap_or(name);
        if !doc_id_str.starts_with("bae-") {
            return Err(libc::EINVAL);
        }
        // Validate it parses as a DocID
        document::DocID::from_string(doc_id_str).map_err(|_| libc::EINVAL)?;
        Ok(doc_id_str)
    }
}

impl<S: Store + 'static> Filesystem for DefraFs<S> {
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name_str = match name.to_str() {
            Some(n) => n,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let mut inodes = self.inodes.lock();
        let target = inodes.get(parent).cloned();

        match target {
            Some(InodeTarget::Root) => {
                match self.db.get_collection(name_str) {
                    Ok(Some(_)) => {
                        let ino = inodes.collection_ino(name_str);
                        reply.entry(&TTL, &Self::dir_attr(ino, self.dir_perm()), 0);
                    }
                    _ => reply.error(libc::ENOENT),
                }
            }
            Some(InodeTarget::Collection { name: col_name }) => {
                let doc_id_str = match Self::parse_doc_id_from_filename(name_str) {
                    Ok(id) => id.to_string(),
                    Err(errno) => {
                        reply.error(errno);
                        return;
                    }
                };
                match self.fetch_doc_json(&col_name, &doc_id_str) {
                    Ok(json) => {
                        let ino = inodes.doc_ino(&col_name, &doc_id_str);
                        let attr = Self::file_attr(ino, json.len() as u64, self.file_perm());
                        reply.entry(&TTL, &attr, 0);
                    }
                    Err(errno) => reply.error(errno),
                }
            }
            _ => reply.error(libc::ENOENT),
        }
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        let inodes = self.inodes.lock();
        match inodes.get(ino) {
            Some(InodeTarget::Root) | Some(InodeTarget::Collection { .. }) => {
                reply.attr(&TTL, &Self::dir_attr(ino, self.dir_perm()));
            }
            Some(InodeTarget::Document {
                collection, doc_id, ..
            }) => {
                let col = collection.clone();
                let did = doc_id.clone();
                drop(inodes);
                match self.fetch_doc_json(&col, &did) {
                    Ok(json) => {
                        let attr = Self::file_attr(ino, json.len() as u64, self.file_perm());
                        reply.attr(&TTL, &attr);
                    }
                    Err(errno) => reply.error(errno),
                }
            }
            None => reply.error(libc::ENOENT),
        }
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let mut inodes = self.inodes.lock();
        let target = inodes.get(ino).cloned();

        match target {
            Some(InodeTarget::Root) => {
                let collections = self.db.list_collections().unwrap_or_default();
                let mut entries: Vec<(u64, FileType, String)> = vec![
                    (ROOT_INO, FileType::Directory, ".".into()),
                    (ROOT_INO, FileType::Directory, "..".into()),
                ];
                for name in &collections {
                    let child_ino = inodes.collection_ino(name);
                    entries.push((child_ino, FileType::Directory, name.clone()));
                }
                for (i, (ino, kind, name)) in entries.iter().enumerate().skip(offset as usize) {
                    if reply.add(*ino, (i + 1) as i64, *kind, name) {
                        break;
                    }
                }
                reply.ok();
            }
            Some(InodeTarget::Collection { name: col_name }) => {
                let col_ino = ino;
                let doc_ids = self.list_doc_ids(&col_name);
                let mut entries: Vec<(u64, FileType, String)> = vec![
                    (col_ino, FileType::Directory, ".".into()),
                    (ROOT_INO, FileType::Directory, "..".into()),
                ];
                for doc_id in &doc_ids {
                    let child_ino = inodes.doc_ino(&col_name, doc_id);
                    entries.push((
                        child_ino,
                        FileType::RegularFile,
                        format!("{}.json", doc_id),
                    ));
                }
                for (i, (ino, kind, name)) in entries.iter().enumerate().skip(offset as usize) {
                    if reply.add(*ino, (i + 1) as i64, *kind, name) {
                        break;
                    }
                }
                reply.ok();
            }
            _ => reply.error(libc::ENOTDIR),
        }
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        let inodes = self.inodes.lock();
        match inodes.get(ino).cloned() {
            Some(InodeTarget::Document { collection, doc_id }) => {
                drop(inodes);
                match self.fetch_doc_json(&collection, &doc_id) {
                    Ok(json) => {
                        let start = offset as usize;
                        let end = (start + size as usize).min(json.len());
                        if start >= json.len() {
                            reply.data(&[]);
                        } else {
                            reply.data(&json[start..end]);
                        }
                    }
                    Err(errno) => reply.error(errno),
                }
            }
            _ => reply.error(libc::EISDIR),
        }
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        if self.read_only {
            reply.error(libc::EROFS);
            return;
        }

        let inodes = self.inodes.lock();
        match inodes.get(ino).cloned() {
            Some(InodeTarget::Document { .. }) => {
                drop(inodes);
                let mut buffers = self.write_buffers.lock();
                let buf = buffers.entry(ino).or_default();
                let end = offset as usize + data.len();
                if buf.len() < end {
                    buf.resize(end, 0);
                }
                buf[offset as usize..end].copy_from_slice(data);
                reply.written(data.len() as u32);
            }
            _ => reply.error(libc::EBADF),
        }
    }

    fn flush(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        _lock_owner: u64,
        reply: ReplyEmpty,
    ) {
        let data = self.write_buffers.lock().remove(&ino);
        let Some(buf) = data else {
            reply.ok();
            return;
        };

        let inodes = self.inodes.lock();
        let target = inodes.get(ino).cloned();
        drop(inodes);

        match target {
            Some(InodeTarget::Document { collection, doc_id }) => {
                let db = self.db.clone();
                let col_name = collection.clone();
                let result = self.rt.block_on(async move {
                    let col = db
                        .get_collection(&collection)
                        .map_err(|e| db_err_to_errno(&e))?
                        .ok_or(libc::ENOENT)?;
                    let doc_id_parsed =
                        document::DocID::from_string(&doc_id).map_err(|_| libc::EINVAL)?;

                    let mut doc =
                        document::Document::from_json(&buf).map_err(|_| libc::EINVAL)?;
                    doc.set_id(doc_id_parsed);
                    doc.set_collection(col.schema().clone());

                    let txn = db.new_txn(false).await.map_err(|e| db_err_to_errno(&e))?;
                    col.save(&txn, &doc)
                        .await
                        .map_err(|e| db_err_to_errno(&e))?;
                    txn.commit().await.map_err(|e| db_err_to_errno(&e))?;
                    Ok::<(), i32>(())
                });

                match result {
                    Ok(()) => {
                        self.inodes.lock().invalidate(&col_name);
                        reply.ok();
                    }
                    Err(errno) => reply.error(errno),
                }
            }
            _ => reply.ok(),
        }
    }

    fn create(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        if self.read_only {
            reply.error(libc::EROFS);
            return;
        }

        let name_str = match name.to_str() {
            Some(n) => n,
            None => {
                reply.error(libc::EINVAL);
                return;
            }
        };

        let doc_id_str = match Self::parse_doc_id_from_filename(name_str) {
            Ok(id) => id.to_string(),
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };

        let mut inodes = self.inodes.lock();
        let target = inodes.get(parent).cloned();

        match target {
            Some(InodeTarget::Collection { name: col_name }) => {
                let ino = inodes.doc_ino(&col_name, &doc_id_str);
                inodes.invalidate(&col_name);
                drop(inodes);
                let attr = Self::file_attr(ino, 0, self.file_perm());
                reply.created(&TTL, &attr, 0, 0, 0);
            }
            _ => reply.error(libc::ENOTDIR),
        }
    }

    fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        if self.read_only {
            reply.error(libc::EROFS);
            return;
        }

        let name_str = match name.to_str() {
            Some(n) => n,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let doc_id_str = match Self::parse_doc_id_from_filename(name_str) {
            Ok(id) => id.to_string(),
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };

        let mut inodes = self.inodes.lock();
        let target = inodes.get(parent).cloned();

        match target {
            Some(InodeTarget::Collection { name: col_name }) => {
                let db = self.db.clone();
                let col_name_clone = col_name.clone();
                let doc_id_string = doc_id_str.clone();

                drop(inodes);

                let result = self.rt.block_on(async move {
                    let col = db
                        .get_collection(&col_name_clone)
                        .map_err(|e| db_err_to_errno(&e))?
                        .ok_or(libc::ENOENT)?;
                    let doc_id =
                        document::DocID::from_string(&doc_id_string).map_err(|_| libc::EINVAL)?;
                    let txn = db.new_txn(false).await.map_err(|e| db_err_to_errno(&e))?;
                    let deleted = col
                        .delete(&txn, &doc_id)
                        .await
                        .map_err(|e| db_err_to_errno(&e))?;
                    txn.commit().await.map_err(|e| db_err_to_errno(&e))?;
                    if deleted {
                        Ok(())
                    } else {
                        Err(libc::ENOENT)
                    }
                });

                match result {
                    Ok(()) => {
                        let mut inodes = self.inodes.lock();
                        inodes.remove_doc(&col_name, &doc_id_str);
                        inodes.invalidate(&col_name);
                        reply.ok();
                    }
                    Err(errno) => reply.error(errno),
                }
            }
            _ => reply.error(libc::ENOTDIR),
        }
    }

    /// rmdir on a collection directory is not allowed.
    /// Collections contain schema and data — dropping them via `rm -r` would be destructive.
    fn rmdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let name_str = match name.to_str() {
            Some(n) => n,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let inodes = self.inodes.lock();
        match inodes.get(parent) {
            Some(InodeTarget::Root) => {
                match self.db.get_collection(name_str) {
                    Ok(Some(_)) => reply.error(libc::EPERM),
                    _ => reply.error(libc::ENOENT),
                }
            }
            _ => reply.error(libc::ENOTDIR),
        }
    }

    fn setattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<fuser::TimeOrNow>,
        _mtime: Option<fuser::TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        if size.is_some() && self.read_only {
            reply.error(libc::EROFS);
            return;
        }

        // Support truncate (size=0) for write-then-flush pattern.
        if let Some(new_size) = size {
            if new_size == 0 {
                self.write_buffers.lock().remove(&ino);
            }
        }

        let inodes = self.inodes.lock();
        match inodes.get(ino) {
            Some(InodeTarget::Root) | Some(InodeTarget::Collection { .. }) => {
                reply.attr(&TTL, &Self::dir_attr(ino, self.dir_perm()));
            }
            Some(InodeTarget::Document { .. }) => {
                let buffers = self.write_buffers.lock();
                let buf_size = buffers.get(&ino).map(|b| b.len() as u64).unwrap_or(0);
                let actual_size = size.unwrap_or(buf_size);
                reply.attr(&TTL, &Self::file_attr(ino, actual_size, self.file_perm()));
            }
            None => reply.error(libc::ENOENT),
        }
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, _flags: i32, reply: fuser::ReplyOpen) {
        let inodes = self.inodes.lock();
        match inodes.get(ino) {
            Some(InodeTarget::Document { .. }) => reply.opened(0, 0),
            _ => reply.error(libc::EISDIR),
        }
    }

    fn opendir(&mut self, _req: &Request<'_>, ino: u64, _flags: i32, reply: fuser::ReplyOpen) {
        let inodes = self.inodes.lock();
        match inodes.get(ino) {
            Some(InodeTarget::Root) | Some(InodeTarget::Collection { .. }) => {
                reply.opened(0, 0);
            }
            _ => reply.error(libc::ENOTDIR),
        }
    }
}
