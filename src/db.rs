use bedrock_leveldb_sys as sys;
use std::path::Path;
use std::ptr;

use crate::options::{Options, ReadOptions, WriteOptions};
use crate::util::{error_message, to_cstring};

/// A handle to a LevelDB database.
///
/// This struct provides a safe Rust interface to LevelDB operations including
/// CRUD operations, iteration, and database maintenance.
///
/// The database is automatically closed when the `DB` instance is dropped.
///
/// # Examples
///
/// ```no_run
/// use bedrock_leveldb::DB;
/// use bedrock_leveldb::options::Options;
///
/// let options = Options::default();
/// let db = DB::open("path/to/database", &options).unwrap();
///
/// // Perform database operations...
/// db.put(b"key", b"value", &Default::default()).unwrap();
/// let value = db.get(b"key", &Default::default()).unwrap();
/// ```
///
/// # Thread Safety
///
/// `DB` implements `Send` and `Sync`, meaning it can be safely shared between threads.
/// However, individual operations should be synchronized externally if needed.
///
/// # SAFETY
/// The underlying LevelDB implementation is thread-safe for concurrent reads,
/// but writes should be synchronized. The Rust wrapper ensures proper synchronization
/// through external synchronization requirements.
pub struct DB {
    raw: *mut sys::leveldb_t,
}

unsafe impl Send for DB {}
unsafe impl Sync for DB {}

impl DB {
    /// Opens a database at the given path with the specified options.
    ///
    /// # Arguments
    ///
    /// * `path` - The filesystem path where the database should be stored
    /// * `options` - Configuration options for the database
    ///
    /// # Returns
    ///
    /// * `Ok(DB)` - If the database was successfully opened
    /// * `Err(String)` - If the database could not be opened, containing an error message
    ///
    /// # Errors
    ///
    /// This function will return an error in the following situations:
    /// * The path contains null bytes
    /// * The database cannot be created or opened (permissions, disk space, etc.)
    /// * The database is corrupted and cannot be repaired
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use bedrock_leveldb::DB;
    /// use bedrock_leveldb::options::Options;
    ///
    /// let options = Options::default();
    /// options.create_if_missing(true);
    ///
    /// match DB::open("test_db", &options) {
    ///     Ok(db) => println!("Database opened successfully"),
    ///     Err(e) => eprintln!("Failed to open database: {}", e),
    /// }
    /// ```
    pub fn open(path: impl AsRef<Path>, options: &Options) -> Result<Self, String> {
        let cpath = to_cstring(path.as_ref().to_string_lossy().as_ref())
            .ok_or("invalid path: contains null byte")?;

        let mut err = ptr::null_mut();

        let db = unsafe { sys::leveldb_open(options.raw(), cpath.as_ptr(), &mut err) };

        if !err.is_null() {
            Err(error_message(err as *mut _))
        } else if db.is_null() {
            Err("failed to open database".to_string())
        } else {
            Ok(Self { raw: db })
        }
    }

    /// Retrieve a value for a given key.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to look up in the database
    /// * `options` - Read options controlling the behavior of the read operation
    ///
    /// # Returns
    ///
    /// * `Ok(Some(Vec<u8>))` - If the key was found, containing the value
    /// * `Ok(None)` - If the key was not found in the database
    /// * `Err(String)` - If an error occurred during the read operation
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// * The database is corrupted
    /// * An I/O error occurs during the read
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use bedrock_leveldb::DB;
    /// # use bedrock_leveldb::options::{Options, ReadOptions};
    /// # let options = Options::default();
    /// # let db = DB::open("test_db", &options).unwrap();
    /// // Store a value
    /// db.put(b"my_key", b"my_value", &Default::default()).unwrap();
    ///
    /// // Retrieve the value
    /// match db.get(b"my_key", &ReadOptions::default()) {
    ///     Ok(Some(value)) => println!("Found value: {:?}", value),
    ///     Ok(None) => println!("Key not found"),
    ///     Err(e) => eprintln!("Error reading key: {}", e),
    /// }
    /// ```
    pub fn get(&self, key: &[u8], options: &ReadOptions) -> Result<Option<Vec<u8>>, String> {
        unsafe {
            let mut err = ptr::null_mut();
            let mut val_len: usize = 0;
            let val_ptr = sys::leveldb_get(
                self.raw,
                options.raw(),
                key.as_ptr() as *const _,
                key.len(),
                &mut val_len,
                &mut err,
            );

            if !err.is_null() {
                return Err(error_message(err as *mut _));
            }

            if val_ptr.is_null() {
                return Ok(None);
            }

            let slice = std::slice::from_raw_parts(val_ptr as *const _, val_len);
            let result = slice.to_vec();
            sys::leveldb_free(val_ptr as *mut _);
            Ok(Some(result))
        }
    }

    /// Insert or overwrite a key-value pair.
    ///
    /// If the key already exists in the database, its value will be overwritten.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to insert or update
    /// * `value` - The value to associate with the key
    /// * `options` - Write options controlling the behavior of the write operation
    ///
    /// # Returns
    ///
    /// * `Ok(())` - If the write operation completed successfully
    /// * `Err(String)` - If an error occurred during the write operation
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// * The database is read-only
    /// * An I/O error occurs during the write
    /// * The write would exceed disk space limits
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use bedrock_leveldb::DB;
    /// # use bedrock_leveldb::options::{Options, WriteOptions};
    /// # let options = Options::default();
    /// # let db = DB::open("test_db", &options).unwrap();
    /// // Write a key-value pair with sync enabled for durability
    /// let write_options = WriteOptions::default();
    /// write_options.sync(true);
    ///
    /// db.put(b"important_key", b"important_data", &write_options)
    ///    .expect("Failed to write to database");
    /// ```
    pub fn put(&self, key: &[u8], value: &[u8], options: &WriteOptions) -> Result<(), String> {
        unsafe {
            let mut err = ptr::null_mut();
            sys::leveldb_put(
                self.raw,
                options.raw(),
                key.as_ptr() as *const _,
                key.len(),
                value.as_ptr() as *const _,
                value.len(),
                &mut err,
            );
            if !err.is_null() {
                return Err(error_message(err as *mut _));
            }
        }
        Ok(())
    }

    /// Delete a key from the database.
    ///
    /// If the key does not exist in the database, this operation is a no-op and succeeds.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to delete from the database
    /// * `options` - Write options controlling the behavior of the delete operation
    ///
    /// # Returns
    ///
    /// * `Ok(())` - If the delete operation completed successfully
    /// * `Err(String)` - If an error occurred during the delete operation
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// * The database is read-only
    /// * An I/O error occurs during the deletion
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use bedrock_leveldb::DB;
    /// # use bedrock_leveldb::options::Options;
    /// # let options = Options::default();
    /// # let db = DB::open("test_db", &options).unwrap();
    /// // Delete a key from the database
    /// db.delete(b"unwanted_key", &Default::default())
    ///    .expect("Failed to delete key");
    /// ```
    pub fn delete(&self, key: &[u8], options: &WriteOptions) -> Result<(), String> {
        unsafe {
            let mut err = ptr::null_mut();
            sys::leveldb_delete(
                self.raw,
                options.raw(),
                key.as_ptr() as *const _,
                key.len(),
                &mut err,
            );
            if !err.is_null() {
                return Err(error_message(err as *mut _));
            }
        }
        Ok(())
    }

    /// Compact the database over the given key range.
    ///
    /// Compaction reorganizes the database files to reduce disk space usage
    /// and improve read performance. This operation is automatically performed
    /// by LevelDB in the background, but can be manually triggered for specific
    /// key ranges.
    ///
    /// # Arguments
    ///
    /// * `start` - The start key of the range to compact (inclusive). If `None`, starts from the beginning.
    /// * `limit` - The limit key of the range to compact (exclusive). If `None`, continues to the end.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use bedrock_leveldb::DB;
    /// # use bedrock_leveldb::options::Options;
    /// # let options = Options::default();
    /// # let db = DB::open("test_db", &options).unwrap();
    /// // Compact the entire database
    /// db.compact_range(None, None);
    ///
    /// // Compact only keys from "a" to "m"
    /// db.compact_range(Some(b"a"), Some(b"m"));
    /// ```
    pub fn compact_range(&self, start: Option<&[u8]>, limit: Option<&[u8]>) {
        unsafe {
            let (start_ptr, start_len) = match start {
                Some(s) => (s.as_ptr() as *const _, s.len()),
                None => (ptr::null(), 0),
            };
            let (limit_ptr, limit_len) = match limit {
                Some(s) => (s.as_ptr() as *const _, s.len()),
                None => (ptr::null(), 0),
            };
            sys::leveldb_compact_range(self.raw, start_ptr, start_len, limit_ptr, limit_len);
        }
    }

    /// Synchronize the database to disk.
    ///
    /// This method forces all pending writes to be flushed to disk.
    /// In LevelDB, this is achieved by performing a full compaction,
    /// which ensures all data is written to persistent storage.
    ///
    /// # Note
    ///
    /// This operation may be expensive for large databases as it involves
    /// rewriting the entire database contents.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use bedrock_leveldb::DB;
    /// # use bedrock_leveldb::options::Options;
    /// # let options = Options::default();
    /// # let db = DB::open("test_db", &options).unwrap();
    /// // Force all data to disk
    /// db.flush();
    /// ```
    pub fn flush(&self) {
        // In LevelDB, explicit flush isn't exposed, but compact_range(None, None)
        // effectively forces all data to disk.
        self.compact_range(None, None);
    }

    /// Create a new iterator over the database contents.
    ///
    /// The iterator provides sequential access to all key-value pairs in the database.
    /// The order of iteration is determined by the key comparator.
    ///
    /// # Arguments
    ///
    /// * `options` - Read options controlling the behavior of the iterator
    ///
    /// # Returns
    ///
    /// A `DBIterator` that can be used to traverse the database contents.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use bedrock_leveldb::DB;
    /// # use bedrock_leveldb::options::Options;
    /// # let options = Options::default();
    /// # let db = DB::open("test_db", &options).unwrap();
    /// let iter = db.iter(&Default::default());
    ///
    /// for (key, value) in iter {
    ///     println!("Key: {:?}, Value: {:?}", key, value);
    /// }
    /// ```
    pub fn iter(&'_ self, options: &ReadOptions) -> crate::iterator::DBIterator<'_> {
        crate::iterator::DBIterator::new(self, options)
    }

    /// Return the raw pointer to the underlying LevelDB database.
    ///
    /// # Safety
    ///
    /// This method is marked as `pub(crate)` because it's intended for internal use
    /// within the crate. The returned pointer should not be stored or used outside
    /// the lifetime of this `DB` instance.
    ///
    /// # Returns
    ///
    /// A raw pointer to the underlying `leveldb_t` database handle.
    pub(crate) fn raw(&self) -> *mut sys::leveldb_t {
        self.raw
    }
}

impl Drop for DB {
    /// Close the database and release all associated resources.
    ///
    /// This method is automatically called when the `DB` instance goes out of scope.
    /// It ensures that all database files are properly closed and any pending
    /// operations are completed.
    fn drop(&mut self) {
        unsafe { sys::leveldb_close(self.raw) };
    }
}
