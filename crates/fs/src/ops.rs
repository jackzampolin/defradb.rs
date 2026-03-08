/// FUSE filesystem implementation backed by DefraDB.
///
/// Maps FUSE operations to DefraDB collection/document CRUD:
/// - readdir(root) → list_collections()
/// - readdir(collection) → collection.get_all()
/// - lookup/read(doc) → collection.get()
/// - write/create → collection.save()
/// - unlink → collection.delete() (soft delete — data preserved in CRDT DAG)
/// - rmdir → EPERM (collections cannot be dropped via fs)
///
/// # Filename Convention
///
/// Documents with a `_name` field use that value as the filename:
///   `{_name}.json`  (e.g. `alice.json`)
///
/// Documents without `_name` use the raw docID:
///   `{bae-xxx}.json`
///
/// When creating a file with a non-docID name (e.g. `touch notes.json`),
/// the `_name` field is set automatically in the document.
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
use crate::inode::{InodeTable, InodeTarget, NAME_FIELD, ROOT_INO};

const TTL: Duration = Duration::from_secs(1);
const EPOCH: SystemTime = UNIX_EPOCH;

/// FUSE filesystem backed by a DefraDB instance.
pub struct DefraFs<S: Store> {
    db: Arc<db::DB<S>>,
    inodes: Mutex<InodeTable>,
    rt: Handle,
    /// Per-inode write buffers for accumulating write() data before flush.
    write_buffers: Mutex<std::collections::HashMap<u64, Vec<u8>>>,
}

impl<S: Store> DefraFs<S> {
    pub fn new(db: Arc<db::DB<S>>, rt: Handle) -> Self {
        Self {
            db,
            inodes: Mutex::new(InodeTable::new()),
            rt,
            write_buffers: Mutex::new(std::collections::HashMap::new()),
        }
    }

    fn dir_attr(ino: u64) -> FileAttr {
        FileAttr {
            ino,
            size: 0,
            blocks: 0,
            atime: EPOCH,
            mtime: EPOCH,
            ctime: EPOCH,
            crtime: EPOCH,
            kind: FileType::Directory,
            perm: 0o755,
            nlink: 2,
            uid: 0,
            gid: 0,
            rdev: 0,
            blksize: 512,
            flags: 0,
        }
    }

    fn file_attr(ino: u64, size: u64) -> FileAttr {
        FileAttr {
            ino,
            size,
            blocks: (size + 511) / 512,
            atime: EPOCH,
            mtime: EPOCH,
            ctime: EPOCH,
            crtime: EPOCH,
            kind: FileType::RegularFile,
            perm: 0o644,
            nlink: 1,
            uid: 0,
            gid: 0,
            rdev: 0,
            blksize: 512,
            flags: 0,
        }
    }

    /// Fetch document JSON bytes by doc_id.
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

    /// List all documents in a collection, returning (doc_id, display_name) pairs.
    /// display_name is the `_name` field value if present, otherwise the doc_id.
    fn list_docs(&self, collection_name: &str) -> Vec<(String, String)> {
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
                .filter_map(|d| {
                    let doc_id = d.id()?.to_string();
                    let display_name = extract_name_field(d).unwrap_or_else(|| doc_id.clone());
                    Some((doc_id, display_name))
                })
                .collect()
        })
    }

    /// Resolve a filename (without .json) to a doc_id.
    /// First checks the inode cache, then falls back to scanning the collection.
    fn resolve_filename(
        &self,
        inodes: &mut InodeTable,
        collection: &str,
        filename: &str,
    ) -> Result<String, i32> {
        // If it's already a valid docID, use it directly
        if filename.starts_with("bae-") {
            if document::DocID::from_string(filename).is_ok() {
                return Ok(filename.to_string());
            }
        }

        // Check inode cache
        if let Some(doc_id) = inodes.resolve_name(collection, filename) {
            return Ok(doc_id.to_string());
        }

        // Cache miss — scan collection for a doc with _name matching
        let docs = self.list_docs(collection);
        for (doc_id, display_name) in &docs {
            inodes.doc_ino(collection, doc_id, display_name);
        }

        // Try again after populating
        inodes
            .resolve_name(collection, filename)
            .map(|s| s.to_string())
            .ok_or(libc::ENOENT)
    }
}

/// Extract the `_name` field from a document if present.
fn extract_name_field(doc: &document::Document) -> Option<String> {
    let val = doc.get(NAME_FIELD)?;
    match val {
        document::NormalValue::String(s) => Some(s.clone()),
        _ => None,
    }
}

/// Strip the .json suffix from a filename.
fn strip_json_suffix(name: &str) -> &str {
    name.strip_suffix(".json").unwrap_or(name)
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
            Some(InodeTarget::Root) => match self.db.get_collection(name_str) {
                Ok(Some(_)) => {
                    let ino = inodes.collection_ino(name_str);
                    reply.entry(&TTL, &Self::dir_attr(ino), 0);
                }
                _ => reply.error(libc::ENOENT),
            },
            Some(InodeTarget::Collection { name: col_name }) => {
                let filename = strip_json_suffix(name_str);
                let doc_id = match self.resolve_filename(&mut inodes, &col_name, filename) {
                    Ok(id) => id,
                    Err(errno) => {
                        reply.error(errno);
                        return;
                    }
                };
                match self.fetch_doc_json(&col_name, &doc_id) {
                    Ok(json) => {
                        let ino = inodes.doc_ino(&col_name, &doc_id, filename);
                        reply.entry(&TTL, &Self::file_attr(ino, json.len() as u64), 0);
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
                reply.attr(&TTL, &Self::dir_attr(ino));
            }
            Some(InodeTarget::Document {
                collection, doc_id, ..
            }) => {
                let col = collection.clone();
                let did = doc_id.clone();
                drop(inodes);
                match self.fetch_doc_json(&col, &did) {
                    Ok(json) => reply.attr(&TTL, &Self::file_attr(ino, json.len() as u64)),
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
                let docs = self.list_docs(&col_name);
                let mut entries: Vec<(u64, FileType, String)> = vec![
                    (col_ino, FileType::Directory, ".".into()),
                    (ROOT_INO, FileType::Directory, "..".into()),
                ];
                for (doc_id, display_name) in &docs {
                    let child_ino = inodes.doc_ino(&col_name, doc_id, display_name);
                    entries.push((
                        child_ino,
                        FileType::RegularFile,
                        format!("{}.json", display_name),
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
            Some(InodeTarget::Document { collection, doc_id, .. }) => {
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
            Some(InodeTarget::Document {
                collection,
                doc_id,
                display_name,
            }) => {
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

                    // Preserve _name field if the file was created with a friendly name
                    if display_name != doc_id && !doc.has_field(NAME_FIELD) {
                        doc.set(NAME_FIELD, document::NormalValue::String(display_name));
                    }

                    let txn = db.new_txn(false).await.map_err(|e| db_err_to_errno(&e))?;
                    col.save(&txn, &doc)
                        .await
                        .map_err(|e| db_err_to_errno(&e))?;
                    txn.commit().await.map_err(|e| db_err_to_errno(&e))?;
                    Ok::<(), i32>(())
                });

                match result {
                    Ok(()) => {
                        self.inodes.lock().invalidate_collection(&col_name);
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
        let name_str = match name.to_str() {
            Some(n) => n,
            None => {
                reply.error(libc::EINVAL);
                return;
            }
        };

        let display_name = strip_json_suffix(name_str);

        let mut inodes = self.inodes.lock();
        let target = inodes.get(parent).cloned();

        match target {
            Some(InodeTarget::Collection { name: col_name }) => {
                // For friendly names, we need a placeholder doc_id.
                // Generate one by creating and immediately saving an empty doc
                // with the _name field set, so the docID is content-addressed.
                let doc_id = if display_name.starts_with("bae-") {
                    match document::DocID::from_string(display_name) {
                        Ok(_) => display_name.to_string(),
                        Err(_) => {
                            reply.error(libc::EINVAL);
                            return;
                        }
                    }
                } else {
                    // Create a doc with _name to get a deterministic docID
                    let db = self.db.clone();
                    let col_name_clone = col_name.clone();
                    let display = display_name.to_string();
                    let result = self.rt.block_on(async move {
                        let col = db
                            .get_collection(&col_name_clone)
                            .map_err(|e| db_err_to_errno(&e))?
                            .ok_or(libc::ENOENT)?;
                        let mut doc = document::Document::new();
                        doc.set_collection(col.schema().clone());
                        doc.set(NAME_FIELD, document::NormalValue::String(display));
                        doc.generate_and_set_doc_id()
                            .map_err(|_| libc::EIO)?;
                        Ok::<String, i32>(doc.id().unwrap().to_string())
                    });
                    match result {
                        Ok(id) => id,
                        Err(errno) => {
                            reply.error(errno);
                            return;
                        }
                    }
                };

                let ino = inodes.doc_ino(&col_name, &doc_id, display_name);
                inodes.invalidate_collection(&col_name);
                drop(inodes);
                reply.created(&TTL, &Self::file_attr(ino, 0), 0, 0, 0);
            }
            _ => reply.error(libc::ENOTDIR),
        }
    }

    fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let name_str = match name.to_str() {
            Some(n) => n,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let filename = strip_json_suffix(name_str);

        let mut inodes = self.inodes.lock();
        let target = inodes.get(parent).cloned();

        match target {
            Some(InodeTarget::Collection { name: col_name }) => {
                let doc_id = match self.resolve_filename(&mut inodes, &col_name, filename) {
                    Ok(id) => id,
                    Err(errno) => {
                        reply.error(errno);
                        return;
                    }
                };

                let db = self.db.clone();
                let col_name_clone = col_name.clone();
                let doc_id_clone = doc_id.clone();

                drop(inodes);

                let result = self.rt.block_on(async move {
                    let col = db
                        .get_collection(&col_name_clone)
                        .map_err(|e| db_err_to_errno(&e))?
                        .ok_or(libc::ENOENT)?;
                    let doc_id_parsed =
                        document::DocID::from_string(&doc_id_clone).map_err(|_| libc::EINVAL)?;
                    let txn = db.new_txn(false).await.map_err(|e| db_err_to_errno(&e))?;
                    let deleted = col
                        .delete(&txn, &doc_id_parsed)
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
                        inodes.remove_doc(&col_name, &doc_id);
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
            Some(InodeTarget::Root) => match self.db.get_collection(name_str) {
                Ok(Some(_)) => reply.error(libc::EPERM),
                _ => reply.error(libc::ENOENT),
            },
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
        // Support truncate (size=0) for write-then-flush pattern.
        if let Some(new_size) = size {
            if new_size == 0 {
                self.write_buffers.lock().remove(&ino);
            }
        }

        let inodes = self.inodes.lock();
        match inodes.get(ino) {
            Some(InodeTarget::Root) | Some(InodeTarget::Collection { .. }) => {
                reply.attr(&TTL, &Self::dir_attr(ino));
            }
            Some(InodeTarget::Document { .. }) => {
                let buffers = self.write_buffers.lock();
                let buf_size = buffers.get(&ino).map(|b| b.len() as u64).unwrap_or(0);
                let actual_size = size.unwrap_or(buf_size);
                reply.attr(&TTL, &Self::file_attr(ino, actual_size));
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
