//! OPFS-backed environment for rusty-leveldb (WASM only).
//!
//! Implements the `Env` trait using an in-memory filesystem that persists to
//! the browser's Origin Private File System (OPFS).
//!
//! The Env trait is synchronous, but OPFS is async. We bridge this by:
//! 1. Maintaining a complete in-memory copy of all files
//! 2. All Env operations work against the in-memory copy (synchronous)
//! 3. Async `load()` and `persist()` methods handle OPFS I/O
//!
//! Lifecycle:
//! - `OpfsEnv::new(db_name)` creates an empty in-memory env
//! - `env.load().await` populates from OPFS (call before opening LevelDB)
//! - LevelDB operates against the in-memory copy (fast, synchronous)
//! - `env.persist().await` writes dirty files back to OPFS
//! - Call persist on transaction commits and on close

use rusty_leveldb::env::{Env, FileLock, Logger, RandomAccess};
use rusty_leveldb::{Result, Status, StatusCode};

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::rc::Rc;

use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;

// ---------------------------------------------------------------------------
// In-memory filesystem (synchronous operations)
// ---------------------------------------------------------------------------

/// A file's data buffer, shared between readers/writers.
type FileData = Rc<RefCell<Vec<u8>>>;

/// Entry in the in-memory filesystem.
struct FileEntry {
    data: FileData,
    locked: bool,
}

/// In-memory filesystem with change tracking for OPFS persistence.
struct InMemoryFS {
    files: HashMap<String, FileEntry>,
    dirty: Rc<RefCell<HashSet<String>>>,
    deleted: HashSet<String>,
}

impl InMemoryFS {
    fn new() -> Self {
        Self {
            files: HashMap::new(),
            dirty: Rc::new(RefCell::new(HashSet::new())),
            deleted: HashSet::new(),
        }
    }

    /// Insert file data loaded from OPFS (not marked dirty).
    fn insert_loaded(&mut self, path: String, data: Vec<u8>) {
        self.files.insert(
            path,
            FileEntry {
                data: Rc::new(RefCell::new(data)),
                locked: false,
            },
        );
    }

    fn open(&mut self, path: &str, create: bool) -> Result<FileData> {
        if let Some(entry) = self.files.get(path) {
            return Ok(entry.data.clone());
        }
        if !create {
            return Err(Status::new(
                StatusCode::NotFound,
                &format!("file not found: {}", path),
            ));
        }
        let data = Rc::new(RefCell::new(Vec::new()));
        self.files.insert(
            path.to_string(),
            FileEntry {
                data: data.clone(),
                locked: false,
            },
        );
        self.dirty.borrow_mut().insert(path.to_string());
        self.deleted.remove(path);
        Ok(data)
    }

    fn open_writable(
        &mut self,
        path: &str,
        append: bool,
        truncate: bool,
    ) -> Result<Box<dyn Write>> {
        let data = self.open(path, true)?;
        if truncate {
            data.borrow_mut().clear();
        }
        let offset = if append { data.borrow().len() } else { 0 };
        self.dirty.borrow_mut().insert(path.to_string());
        Ok(Box::new(MemFileWriter {
            data,
            offset,
            path: path.to_string(),
            dirty: Rc::clone(&self.dirty),
        }))
    }

    fn exists(&self, path: &str) -> bool {
        self.files.contains_key(path)
    }

    fn children(&self, path: &str) -> Vec<PathBuf> {
        let prefix = if path.ends_with('/') {
            path.to_string()
        } else {
            format!("{}/", path)
        };
        let mut result = Vec::new();
        for key in self.files.keys() {
            if let Some(rest) = key.strip_prefix(&prefix) {
                result.push(PathBuf::from(rest));
            }
        }
        result
    }

    fn size_of(&self, path: &str) -> Result<usize> {
        match self.files.get(path) {
            Some(entry) => Ok(entry.data.borrow().len()),
            None => Err(Status::new(
                StatusCode::NotFound,
                &format!("file not found: {}", path),
            )),
        }
    }

    fn delete(&mut self, path: &str) -> Result<()> {
        if self.files.remove(path).is_some() {
            self.dirty.borrow_mut().remove(path);
            self.deleted.insert(path.to_string());
            Ok(())
        } else {
            Err(Status::new(
                StatusCode::NotFound,
                &format!("file not found: {}", path),
            ))
        }
    }

    fn rename(&mut self, from: &str, to: &str) -> Result<()> {
        if let Some(entry) = self.files.remove(from) {
            self.files.insert(to.to_string(), entry);
            self.dirty.borrow_mut().remove(from);
            self.deleted.insert(from.to_string());
            self.dirty.borrow_mut().insert(to.to_string());
            Ok(())
        } else {
            Err(Status::new(
                StatusCode::NotFound,
                &format!("file not found: {}", from),
            ))
        }
    }

    fn lock(&mut self, path: &str) -> Result<FileLock> {
        match self.files.get_mut(path) {
            Some(entry) => {
                if entry.locked {
                    return Err(Status::new(
                        StatusCode::LockError,
                        &format!("already locked: {}", path),
                    ));
                }
                entry.locked = true;
                Ok(FileLock {
                    id: path.to_string(),
                })
            }
            None => {
                // Create lock file on demand
                self.files.insert(
                    path.to_string(),
                    FileEntry {
                        data: Rc::new(RefCell::new(Vec::new())),
                        locked: true,
                    },
                );
                Ok(FileLock {
                    id: path.to_string(),
                })
            }
        }
    }

    fn unlock(&mut self, id: &str) -> Result<()> {
        match self.files.get_mut(id) {
            Some(entry) => {
                if !entry.locked {
                    return Err(Status::new(
                        StatusCode::LockError,
                        &format!("not locked: {}", id),
                    ));
                }
                entry.locked = false;
                Ok(())
            }
            None => Err(Status::new(
                StatusCode::NotFound,
                &format!("file not found: {}", id),
            )),
        }
    }
}

// ---------------------------------------------------------------------------
// Read/Write/RandomAccess implementations
// ---------------------------------------------------------------------------

/// Sequential reader over in-memory file data.
struct MemFileReader {
    data: FileData,
    offset: usize,
}

impl Read for MemFileReader {
    fn read(&mut self, dst: &mut [u8]) -> io::Result<usize> {
        let buf = self.data.borrow();
        if self.offset >= buf.len() {
            return Ok(0);
        }
        let remaining = buf.len() - self.offset;
        let to_read = dst.len().min(remaining);
        dst[..to_read].copy_from_slice(&buf[self.offset..self.offset + to_read]);
        self.offset += to_read;
        Ok(to_read)
    }
}

/// Random access reader over in-memory file data.
struct MemRandomAccess {
    data: FileData,
}

impl RandomAccess for MemRandomAccess {
    fn read_at(&self, off: usize, dst: &mut [u8]) -> Result<usize> {
        let buf = self.data.borrow();
        if off > buf.len() {
            return Ok(0);
        }
        let remaining = buf.len() - off;
        let to_read = dst.len().min(remaining);
        dst[..to_read].copy_from_slice(&buf[off..off + to_read]);
        Ok(to_read)
    }
}

/// Writer to in-memory file data.
struct MemFileWriter {
    data: FileData,
    offset: usize,
    path: String,
    dirty: Rc<RefCell<HashSet<String>>>,
}

impl Write for MemFileWriter {
    fn write(&mut self, src: &[u8]) -> io::Result<usize> {
        // XXX Mark on every write, not just at open: this writer outlives any
        // number of persist() calls, each of which clears the set.
        self.dirty.borrow_mut().insert(self.path.clone());
        let mut buf = self.data.borrow_mut();
        if self.offset == buf.len() {
            buf.extend_from_slice(src);
        } else {
            let remaining = buf.len() - self.offset;
            if src.len() <= remaining {
                buf[self.offset..self.offset + src.len()].copy_from_slice(src);
            } else {
                buf[self.offset..self.offset + remaining].copy_from_slice(&src[..remaining]);
                buf.extend_from_slice(&src[remaining..]);
            }
        }
        self.offset += src.len();
        Ok(src.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// OpfsEnv — Env trait implementation with OPFS persistence
// ---------------------------------------------------------------------------

/// OPFS-backed environment for rusty-leveldb.
///
/// Wraps an in-memory filesystem for synchronous `Env` trait compliance,
/// with async methods to load from and persist to the browser's OPFS.
///
/// Uses `Rc<RefCell<...>>` internally so clones share the same state.
/// This allows both rusty-leveldb and `LevelDbStore` to reference the
/// same environment for read/write operations and OPFS persistence.
#[derive(Clone)]
pub struct OpfsEnv {
    fs: Rc<RefCell<InMemoryFS>>,
    db_name: String,
}

impl OpfsEnv {
    /// Create a new OpfsEnv for the given database name.
    ///
    /// The database name determines the OPFS directory used for persistence.
    /// Call `load().await` before opening LevelDB to populate from OPFS.
    pub fn new(db_name: &str) -> Self {
        Self {
            fs: Rc::new(RefCell::new(InMemoryFS::new())),
            db_name: db_name.to_string(),
        }
    }

    /// Load all files from OPFS into the in-memory filesystem.
    ///
    /// Call this before opening LevelDB. If the OPFS directory doesn't exist
    /// yet (fresh database), this is a no-op.
    pub async fn load(&self) -> std::result::Result<(), JsValue> {
        let root = opfs_get_root().await?;
        let db_dir = match opfs_get_directory(&root, &self.db_name, false).await {
            Ok(dir) => dir,
            Err(_) => return Ok(()), // Directory doesn't exist — fresh database
        };

        let entries = opfs_list_files(&db_dir).await?;

        // Read all files before borrowing the FS (OPFS reads are async)
        let mut loaded = Vec::with_capacity(entries.len());
        for (filename, file_handle) in entries {
            let data = opfs_read_file(&file_handle).await?;
            let path = format!("{}/{}", self.db_name, filename);
            loaded.push((path, data));
        }

        let mut fs = self.fs.borrow_mut();
        for (path, data) in loaded {
            fs.insert_loaded(path, data);
        }

        Ok(())
    }

    /// Persist dirty files to OPFS and remove deleted files.
    ///
    /// Call this after LevelDB transaction commits and on close.
    pub async fn persist(&self) -> std::result::Result<(), JsValue> {
        let dirty: Vec<String>;
        let deleted: Vec<String>;
        let file_data: Vec<(String, Vec<u8>)>;

        {
            let fs = self.fs.borrow();
            dirty = fs.dirty.borrow().iter().cloned().collect();
            deleted = fs.deleted.iter().cloned().collect();

            // Collect data for dirty files
            file_data = dirty
                .iter()
                .filter_map(|path| {
                    fs.files
                        .get(path)
                        .map(|entry| (path.clone(), entry.data.borrow().clone()))
                })
                .collect();
        }

        // Skip if nothing to persist
        if file_data.is_empty() && deleted.is_empty() {
            return Ok(());
        }

        let root = opfs_get_root().await?;
        let db_dir = opfs_get_directory(&root, &self.db_name, true).await?;
        let prefix = format!("{}/", self.db_name);

        // Write dirty files
        for (path, data) in &file_data {
            let filename = path.strip_prefix(&prefix).unwrap_or(path);
            opfs_write_file(&db_dir, filename, data).await?;
        }

        // Delete removed files
        for path in &deleted {
            let filename = path.strip_prefix(&prefix).unwrap_or(path);
            // Ignore errors — file may not exist in OPFS yet
            let _ = opfs_remove_entry(&db_dir, filename).await;
        }

        // Clear tracking sets
        let mut fs = self.fs.borrow_mut();
        fs.dirty.borrow_mut().clear();
        fs.deleted.clear();

        Ok(())
    }

    /// Check if there are unpersisted changes.
    pub fn has_pending_changes(&self) -> bool {
        let fs = self.fs.borrow();
        !fs.dirty.borrow().is_empty() || !fs.deleted.is_empty()
    }
}

fn path_to_string(p: &Path) -> String {
    p.to_str().map(String::from).unwrap_or_default()
}

impl Env for OpfsEnv {
    fn open_sequential_file(&self, p: &Path) -> Result<Box<dyn Read>> {
        let path = path_to_string(p);
        let data = self.fs.borrow_mut().open(&path, false)?;
        Ok(Box::new(MemFileReader { data, offset: 0 }))
    }

    fn open_random_access_file(&self, p: &Path) -> Result<Box<dyn RandomAccess>> {
        let path = path_to_string(p);
        let data = self.fs.borrow_mut().open(&path, false)?;
        Ok(Box::new(MemRandomAccess { data }))
    }

    fn open_writable_file(&self, p: &Path) -> Result<Box<dyn Write>> {
        let path = path_to_string(p);
        self.fs.borrow_mut().open_writable(&path, true, true)
    }

    fn open_appendable_file(&self, p: &Path) -> Result<Box<dyn Write>> {
        let path = path_to_string(p);
        self.fs.borrow_mut().open_writable(&path, true, false)
    }

    fn exists(&self, p: &Path) -> Result<bool> {
        let path = path_to_string(p);
        Ok(self.fs.borrow().exists(&path))
    }

    fn children(&self, p: &Path) -> Result<Vec<PathBuf>> {
        let path = path_to_string(p);
        Ok(self.fs.borrow().children(&path))
    }

    fn size_of(&self, p: &Path) -> Result<usize> {
        let path = path_to_string(p);
        self.fs.borrow().size_of(&path)
    }

    fn delete(&self, p: &Path) -> Result<()> {
        let path = path_to_string(p);
        self.fs.borrow_mut().delete(&path)
    }

    fn mkdir(&self, p: &Path) -> Result<()> {
        let path = path_to_string(p);
        if self.fs.borrow().exists(&path) {
            Err(Status::new(StatusCode::AlreadyExists, ""))
        } else {
            Ok(())
        }
    }

    fn rmdir(&self, p: &Path) -> Result<()> {
        let path = path_to_string(p);
        if !self.fs.borrow().exists(&path) {
            Err(Status::new(StatusCode::NotFound, ""))
        } else {
            Ok(())
        }
    }

    fn rename(&self, old: &Path, new: &Path) -> Result<()> {
        let old_path = path_to_string(old);
        let new_path = path_to_string(new);
        self.fs.borrow_mut().rename(&old_path, &new_path)
    }

    fn lock(&self, p: &Path) -> Result<FileLock> {
        let path = path_to_string(p);
        self.fs.borrow_mut().lock(&path)
    }

    fn unlock(&self, l: FileLock) -> Result<()> {
        self.fs.borrow_mut().unlock(&l.id)
    }

    fn new_logger(&self, p: &Path) -> Result<Logger> {
        self.open_appendable_file(p).map(Logger::new)
    }

    fn micros(&self) -> u64 {
        // js_sys::Date::now() returns milliseconds since epoch as f64
        (js_sys::Date::now() * 1000.0) as u64
    }

    fn sleep_for(&self, _micros: u32) {
        // No-op on WASM — single-threaded, cannot block
    }
}

// ---------------------------------------------------------------------------
// OPFS async helpers (JS interop via js_sys::Reflect)
// ---------------------------------------------------------------------------

/// Get the OPFS root directory handle.
async fn opfs_get_root() -> std::result::Result<JsValue, JsValue> {
    let global = js_sys::global();
    let navigator = js_sys::Reflect::get(&global, &"navigator".into())?;
    let storage = js_sys::Reflect::get(&navigator, &"storage".into())?;
    let get_directory = js_sys::Reflect::get(&storage, &"getDirectory".into())?;
    let func: js_sys::Function = get_directory.unchecked_into();
    let promise: js_sys::Promise = func.call0(&storage)?.unchecked_into();
    JsFuture::from(promise).await
}

/// Get a subdirectory handle, optionally creating it.
async fn opfs_get_directory(
    parent: &JsValue,
    name: &str,
    create: bool,
) -> std::result::Result<JsValue, JsValue> {
    let options = js_sys::Object::new();
    js_sys::Reflect::set(&options, &"create".into(), &create.into())?;

    let method = js_sys::Reflect::get(parent, &"getDirectoryHandle".into())?;
    let func: js_sys::Function = method.unchecked_into();
    let promise: js_sys::Promise = func
        .call2(parent, &name.into(), &options.into())?
        .unchecked_into();
    JsFuture::from(promise).await
}

/// List all file entries in a directory (non-recursive).
///
/// Returns pairs of (filename, FileSystemFileHandle).
async fn opfs_list_files(dir: &JsValue) -> std::result::Result<Vec<(String, JsValue)>, JsValue> {
    // Call dir.values() to get an async iterator of handles
    let values_method = js_sys::Reflect::get(dir, &"values".into())?;
    let values_fn: js_sys::Function = values_method.unchecked_into();
    let iterator = values_fn.call0(dir)?;

    let mut files = Vec::new();
    loop {
        // Call iterator.next() which returns a Promise<{done, value}>
        let next_method = js_sys::Reflect::get(&iterator, &"next".into())?;
        let next_fn: js_sys::Function = next_method.unchecked_into();
        let promise: js_sys::Promise = next_fn.call0(&iterator)?.unchecked_into();
        let result = JsFuture::from(promise).await?;

        let done = js_sys::Reflect::get(&result, &"done".into())?;
        if done.as_bool().unwrap_or(true) {
            break;
        }

        let handle = js_sys::Reflect::get(&result, &"value".into())?;
        let kind = js_sys::Reflect::get(&handle, &"kind".into())?;
        let name = js_sys::Reflect::get(&handle, &"name".into())?;

        // Only include file entries (skip subdirectories)
        if kind.as_string().as_deref() == Some("file") {
            if let Some(name_str) = name.as_string() {
                files.push((name_str, handle));
            }
        }
    }

    Ok(files)
}

/// Read the full contents of an OPFS file.
async fn opfs_read_file(file_handle: &JsValue) -> std::result::Result<Vec<u8>, JsValue> {
    // file_handle.getFile() -> Promise<File>
    let get_file = js_sys::Reflect::get(file_handle, &"getFile".into())?;
    let func: js_sys::Function = get_file.unchecked_into();
    let promise: js_sys::Promise = func.call0(file_handle)?.unchecked_into();
    let file = JsFuture::from(promise).await?;

    // file.arrayBuffer() -> Promise<ArrayBuffer>
    let array_buffer_method = js_sys::Reflect::get(&file, &"arrayBuffer".into())?;
    let func: js_sys::Function = array_buffer_method.unchecked_into();
    let promise: js_sys::Promise = func.call0(&file)?.unchecked_into();
    let array_buffer = JsFuture::from(promise).await?;

    let uint8_array = js_sys::Uint8Array::new(&array_buffer);
    Ok(uint8_array.to_vec())
}

/// Write data to an OPFS file (creates or overwrites).
async fn opfs_write_file(
    dir: &JsValue,
    filename: &str,
    data: &[u8],
) -> std::result::Result<(), JsValue> {
    // dir.getFileHandle(filename, {create: true}) -> Promise<FileSystemFileHandle>
    let options = js_sys::Object::new();
    js_sys::Reflect::set(&options, &"create".into(), &true.into())?;

    let method = js_sys::Reflect::get(dir, &"getFileHandle".into())?;
    let func: js_sys::Function = method.unchecked_into();
    let promise: js_sys::Promise = func
        .call2(dir, &filename.into(), &options.into())?
        .unchecked_into();
    let file_handle = JsFuture::from(promise).await?;

    // file_handle.createWritable() -> Promise<FileSystemWritableFileStream>
    let create_writable = js_sys::Reflect::get(&file_handle, &"createWritable".into())?;
    let func: js_sys::Function = create_writable.unchecked_into();
    let promise: js_sys::Promise = func.call0(&file_handle)?.unchecked_into();
    let writable = JsFuture::from(promise).await?;

    // writable.write(data) -> Promise<undefined>
    let uint8_array = js_sys::Uint8Array::from(data);
    let write_method = js_sys::Reflect::get(&writable, &"write".into())?;
    let func: js_sys::Function = write_method.unchecked_into();
    let promise: js_sys::Promise = func.call1(&writable, &uint8_array)?.unchecked_into();
    JsFuture::from(promise).await?;

    // writable.close() -> Promise<undefined>
    let close_method = js_sys::Reflect::get(&writable, &"close".into())?;
    let func: js_sys::Function = close_method.unchecked_into();
    let promise: js_sys::Promise = func.call0(&writable)?.unchecked_into();
    JsFuture::from(promise).await?;

    Ok(())
}

/// Remove a file entry from an OPFS directory.
async fn opfs_remove_entry(dir: &JsValue, filename: &str) -> std::result::Result<(), JsValue> {
    let method = js_sys::Reflect::get(dir, &"removeEntry".into())?;
    let func: js_sys::Function = method.unchecked_into();
    let promise: js_sys::Promise = func.call1(dir, &filename.into())?.unchecked_into();
    JsFuture::from(promise).await?;
    Ok(())
}
