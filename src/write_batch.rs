use bleveldb_sys as sys;
use std::ptr;

use crate::DB;
use crate::options::WriteOptions;
use crate::util::error_message;

/// A batch of write operations (put/delete) that can be committed atomically.
///
/// `WriteBatch` allows you to group multiple write operations (puts and deletes)
/// into a single atomic unit. Either all operations in the batch succeed, or none
/// are applied. This provides strong consistency guarantees and can significantly
/// improve write performance by reducing I/O overhead.
///
/// # Atomicity Guarantees
///
/// When a write batch is committed, LevelDB ensures that:
/// - All operations in the batch are applied atomically
/// - No partial updates are visible to other readers
/// - The batch is durable once the write operation completes successfully
///
/// # Performance Benefits
///
/// Using write batches can provide significant performance improvements:
/// - Reduced I/O overhead by batching multiple operations
/// - Fewer sync operations when using synchronous writes
/// - Better write amplification characteristics
///
/// # Examples
///
/// ## Basic batch operations
/// ```no_run
/// use bleveldb::{DB, WriteBatch, options::Options};
///
/// # let options = Options::default();
/// # let db = DB::open("test_db", &options).unwrap();
/// let mut batch = WriteBatch::new();
///
/// // Add multiple operations to the batch
/// batch.put(b"key1", b"value1");
/// batch.put(b"key2", b"value2");
/// batch.delete(b"old_key");
///
/// // Commit all operations atomically
/// batch.write(&db, &Default::default()).unwrap();
/// ```
///
/// ## Batch with error handling
/// ```no_run
/// use bleveldb::{DB, WriteBatch, options::Options};
///
/// # let options = Options::default();
/// # let db = DB::open("test_db", &options).unwrap();
/// let mut batch = WriteBatch::new();
/// batch.put(b"data", b"important information");
///
/// match batch.write(&db, &Default::default()) {
///     Ok(()) => println!("Batch committed successfully"),
///     Err(e) => eprintln!("Failed to write batch: {}", e),
/// }
/// ```
pub struct WriteBatch {
    raw: *mut sys::leveldb_writebatch_t,
}

impl WriteBatch {
    /// Create a new, empty write batch.
    ///
    /// The batch starts with no operations. Use `put()` and `delete()` methods
    /// to add operations to the batch, then call `write()` to commit them.
    ///
    /// # Returns
    ///
    /// A new empty `WriteBatch` instance.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use bleveldb::WriteBatch;
    ///
    /// let batch = WriteBatch::new();
    /// ```
    pub fn new() -> Self {
        Self {
            raw: unsafe { sys::leveldb_writebatch_create() },
        }
    }

    /// Add a `put` operation to the batch.
    ///
    /// This operation will insert or update the key-value pair in the database
    /// when the batch is committed. If the key already exists, its value will
    /// be overwritten.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to insert or update
    /// * `value` - The value to associate with the key
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use bleveldb::WriteBatch;
    ///
    /// let mut batch = WriteBatch::new();
    /// batch.put(b"user:123", b"John Doe");
    /// batch.put(b"user:456", b"Jane Smith");
    /// ```
    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        unsafe {
            sys::leveldb_writebatch_put(
                self.raw,
                key.as_ptr() as *const _,
                key.len(),
                value.as_ptr() as *const _,
                value.len(),
            );
        }
    }

    /// Add a `delete` operation to the batch.
    ///
    /// This operation will remove the key from the database when the batch is
    /// committed. If the key does not exist, this operation is a no-op.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to delete from the database
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use bleveldb::WriteBatch;
    ///
    /// let mut batch = WriteBatch::new();
    /// batch.put(b"new_data", b"fresh value");
    /// batch.delete(b"old_data"); // Remove outdated data
    /// ```
    pub fn delete(&mut self, key: &[u8]) {
        unsafe {
            sys::leveldb_writebatch_delete(self.raw, key.as_ptr() as *const _, key.len());
        }
    }

    /// Clear all operations from this batch.
    ///
    /// This method removes all put and delete operations that have been added
    /// to the batch, returning it to an empty state.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use bleveldb::WriteBatch;
    ///
    /// let mut batch = WriteBatch::new();
    /// batch.put(b"key1", b"value1");
    /// batch.put(b"key2", b"value2");
    ///
    /// // Change mind, clear all operations
    /// batch.clear();
    ///
    /// // Batch is now empty and ready for new operations
    /// batch.put(b"key3", b"value3");
    /// ```
    pub fn clear(&mut self) {
        unsafe {
            sys::leveldb_writebatch_clear(self.raw);
        }
    }

    /// Write this batch to the database atomically.
    ///
    /// This method commits all operations in the batch to the database in a
    /// single atomic transaction. Either all operations succeed, or none are
    /// applied.
    ///
    /// # Arguments
    ///
    /// * `db` - The database to write the batch to
    /// * `options` - Write options controlling the behavior of the write operation
    ///
    /// # Returns
    ///
    /// * `Ok(())` - If all operations in the batch were committed successfully
    /// * `Err(String)` - If an error occurred during the write operation
    ///
    /// # Errors
    ///
    /// This method may return an error in the following situations:
    /// * The database is read-only
    /// * Disk space is exhausted
    /// * An I/O error occurs during the write
    /// * The database is corrupted
    ///
    /// # Atomicity
    ///
    /// If this method returns `Ok(())`, all operations in the batch have been
    /// applied atomically. If it returns `Err`, no operations from the batch
    /// have been applied.
    ///
    /// # Examples
    ///
    /// ## Basic batch write
    /// ```no_run
    /// use bleveldb::{DB, WriteBatch, options::Options};
    ///
    /// # let options = Options::default();
    /// # let db = DB::open("test_db", &options).unwrap();
    /// let mut batch = WriteBatch::new();
    /// batch.put(b"account:balance", b"1000");
    /// batch.put(b"account:name", b"Checking");
    ///
    /// batch.write(&db, &Default::default()).unwrap();
    /// ```
    ///
    /// ## Batch write with synchronous options
    /// ```no_run
    /// use bleveldb::{DB, WriteBatch, options::{Options, WriteOptions}};
    ///
    /// # let options = Options::default();
    /// # let db = DB::open("test_db", &options).unwrap();
    /// let mut batch = WriteBatch::new();
    /// batch.put(b"critical:data", b"must be durable");
    ///
    /// let mut write_options = WriteOptions::new();
    /// write_options.sync(true); // Ensure data is durable
    ///
    /// batch.write(&db, &write_options).unwrap();
    /// ```
    ///
    /// ## Error handling
    /// ```no_run
    /// use bleveldb::{DB, WriteBatch, options::Options};
    ///
    /// # let options = Options::default();
    /// # let db = DB::open("test_db", &options).unwrap();
    /// let mut batch = WriteBatch::new();
    /// batch.put(b"data", b"value");
    ///
    /// if let Err(e) = batch.write(&db, &Default::default()) {
    ///     eprintln!("Failed to commit batch: {}", e);
    ///     // Batch was not applied - database state is unchanged
    /// }
    /// ```
    pub fn write(&self, db: &DB, options: &WriteOptions) -> Result<(), String> {
        unsafe {
            let mut err = ptr::null_mut();
            sys::leveldb_write(db.raw(), options.raw(), self.raw, &mut err);
            if !err.is_null() {
                return Err(error_message(err as *mut _));
            }
        }
        Ok(())
    }
}

impl Default for WriteBatch {
    /// Create a default write batch.
    ///
    /// This is equivalent to calling `WriteBatch::new()`.
    ///
    /// # Returns
    ///
    /// A new empty `WriteBatch` instance.
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for WriteBatch {
    /// Clean up the write batch resources.
    ///
    /// This method is automatically called when the `WriteBatch` instance goes
    /// out of scope. It ensures that all internal LevelDB write batch resources
    /// are properly released.
    ///
    /// # Note
    ///
    /// If a batch is dropped without being written, all operations in the batch
    /// are lost. To persist the operations, call `write()` before the batch is dropped.
    fn drop(&mut self) {
        unsafe {
            sys::leveldb_writebatch_destroy(self.raw);
        }
    }
}
