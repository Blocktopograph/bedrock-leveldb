use bleveldb_sys::{self as leveldb_sys};

pub type Compression = leveldb_sys::Compression;

/// Configuration options for opening or creating a LevelDB database.
///
/// This struct allows you to customize various aspects of database behavior
/// including creation policies, performance tuning, and compression settings.
///
/// # Examples
///
/// ## Basic usage with defaults
/// ```no_run
/// use bleveldb::options::Options;
///
/// let options = Options::default();
/// ```
///
/// ## Custom configuration
/// ```no_run
/// use bleveldb::options::Options;
///
/// let options = Options::new();
/// options.create_if_missing(true);
/// options.paranoid_checks(true);
/// ```
pub struct Options {
    raw: *mut leveldb_sys::leveldb_options_t,
}

impl Options {
    /// Create a new `Options` instance with default values.
    ///
    /// The default options are:
    /// - `create_if_missing`: false
    /// - `error_if_exists`: false
    /// - `paranoid_checks`: false
    /// - `compression`: No compression
    ///
    /// # Returns
    ///
    /// A new `Options` instance with default settings.
    ///
    pub fn new() -> Self {
        Self {
            raw: unsafe { leveldb_sys::leveldb_options_create() },
        }
    }

    /// Configure whether to create the database if it doesn't exist.
    ///
    /// # Arguments
    ///
    /// * `value` - If `true`, the database will be created if it is missing.
    ///             If `false`, attempting to open a non-existent database will fail.
    ///
    pub fn create_if_missing(&self, value: bool) {
        unsafe { leveldb_sys::leveldb_options_set_create_if_missing(self.raw, value as u8) };
    }

    /// Configure whether to raise an error if the database already exists.
    ///
    /// # Arguments
    ///
    /// * `value` - If `true`, an error is raised if the database already exists.
    ///             If `false`, existing databases are opened normally.
    ///
    pub fn error_if_exists(&self, value: bool) {
        unsafe { leveldb_sys::leveldb_options_set_error_if_exists(self.raw, value as u8) };
    }

    /// Enable or disable paranoid checks for data integrity.
    ///
    /// When enabled, the implementation will do aggressive checking of the data
    /// and will stop early if it detects any corruption. This provides better
    /// data integrity guarantees but may impact performance.
    ///
    /// # Arguments
    ///
    /// * `value` - If `true`, enable paranoid data checks.
    ///             If `false`, use normal checking level.
    ///
    pub fn paranoid_checks(&self, value: bool) {
        unsafe { leveldb_sys::leveldb_options_set_paranoid_checks(self.raw, value as u8) };
    }

    /// Set the compression algorithm for stored data.
    ///
    /// Compression reduces disk space usage at the cost of CPU overhead during
    /// compression and decompression operations.
    ///
    /// # Arguments
    ///
    /// * `compression_type` - The compression algorithm to use:
    ///   - `Compression::No` - No compression (fastest)
    ///   - `Compression::Snappy` - Snappy compression (good balance of speed and ratio)
    ///   - `Compression::Zstd` - Zstd compression (good compression ratio)
    ///   - `Compression::ZlibRaw` - Zlib raw compression (high compression ratio, slower) (*Minecraft Bedrock uses this*)
    ///
    pub fn compression(&self, compression_type: Compression) {
        unsafe {
            leveldb_sys::leveldb_options_set_compression(self.raw, compression_type);
        }
    }

    /// Get the raw pointer to the underlying LevelDB options.
    ///
    /// # Safety
    ///
    /// This method is for internal use only. The returned pointer should not be
    /// stored or used outside the lifetime of this `Options` instance.
    ///
    /// # Returns
    ///
    /// A raw pointer to the underlying `leveldb_options_t`.
    pub(crate) fn raw(&self) -> *mut leveldb_sys::leveldb_options_t {
        self.raw
    }
}

impl Default for Options {
    /// Create a default `Options` instance.
    ///
    /// This is equivalent to calling `Options::new()`.
    ///
    /// # Returns
    ///
    /// A new `Options` instance with default settings.
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for Options {
    /// Clean up the options resources.
    ///
    /// This method is automatically called when the `Options` instance goes out of scope.
    /// It ensures that all internal LevelDB options resources are properly released.
    fn drop(&mut self) {
        unsafe { leveldb_sys::leveldb_options_destroy(self.raw) }
    }
}

//
// READ OPTIONS
//

/// Trait for types that can provide a LevelDB snapshot pointer.
///
/// This trait allows different types to be used as snapshots in read operations,
/// enabling consistent reads across multiple operations.
///
/// # Implementors
///
/// - `DB`: Creates a new snapshot from the database
/// - `*const leveldb_snapshot_t`: Uses an existing snapshot pointer directly
pub trait AsSnapshot {
    /// Convert the implementor to a raw snapshot pointer.
    ///
    /// # Returns
    ///
    /// A raw pointer to a LevelDB snapshot.
    fn as_snapshot_ptr(&self) -> *const leveldb_sys::leveldb_snapshot_t;
}

impl AsSnapshot for crate::DB {
    fn as_snapshot_ptr(&self) -> *const leveldb_sys::leveldb_snapshot_t {
        unsafe { leveldb_sys::leveldb_create_snapshot(self.raw()) }
    }
}

impl AsSnapshot for *const leveldb_sys::leveldb_snapshot_t {
    fn as_snapshot_ptr(&self) -> *const leveldb_sys::leveldb_snapshot_t {
        *self
    }
}

/// Options for reading from the database.
///
/// This struct allows you to customize read behavior including checksum verification,
/// cache usage, and snapshot consistency.
///
/// # Examples
///
/// ## Basic usage with defaults
/// ```no_run
/// # use bleveldb::options::ReadOptions;
///
/// let read_options = ReadOptions::default();
/// ```
///
/// ## Custom configuration
/// ```no_run
/// # use bleveldb::options::ReadOptions;
///
/// let read_options = ReadOptions::new();
/// read_options.verify_checksums(true);
/// read_options.fill_cache(false);
/// ```
pub struct ReadOptions {
    raw: *mut leveldb_sys::leveldb_readoptions_t,
}

impl ReadOptions {
    /// Create a new `ReadOptions` instance with default values.
    ///
    /// The default read options are:
    /// - `verify_checksums`: false
    /// - `fill_cache`: true
    /// - No snapshot (reads see the latest data)
    ///
    /// # Returns
    ///
    /// A new `ReadOptions` instance with default settings.
    ///
    pub fn new() -> Self {
        Self {
            raw: unsafe { leveldb_sys::leveldb_readoptions_create() },
        }
    }

    /// Configure whether to verify checksums during reads.
    ///
    /// When enabled, all data read from underlying storage will be verified
    /// against its checksum. This provides better data integrity guarantees
    /// but may impact read performance.
    ///
    /// # Arguments
    ///
    /// * `value` - If `true`, verify checksums on all reads.
    ///             If `false`, skip checksum verification.
    ///
    pub fn verify_checksums(&self, value: bool) {
        unsafe { leveldb_sys::leveldb_readoptions_set_verify_checksums(self.raw, value as u8) };
    }

    /// Configure whether reads should populate the cache.
    ///
    /// When enabled, read operations will store data in the cache for faster
    /// subsequent access. Disabling this may be useful for one-off reads of
    /// large amounts of data that are unlikely to be accessed again.
    ///
    /// # Arguments
    ///
    /// * `value` - If `true`, reads will fill the cache.
    ///             If `false`, reads will not affect the cache.
    ///
    pub fn fill_cache(&self, value: bool) {
        unsafe { leveldb_sys::leveldb_readoptions_set_fill_cache(self.raw, value as u8) };
    }

    /// Set a snapshot for consistent reads.
    ///
    /// When a snapshot is set, all read operations will see a consistent
    /// view of the database as it existed when the snapshot was created.
    ///
    /// # Arguments
    ///
    /// * `snapshot` - A snapshot instance that implements `AsSnapshot`
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use bleveldb::{DB, options::{Options, ReadOptions}};
    ///
    /// # let db = DB::open("test_db", &Options::default()).unwrap();
    /// // Create a snapshot for consistent reads
    /// let mut read_options = ReadOptions::new();
    /// read_options.snapshot(&db);
    ///
    /// // All reads using these options will see the same database state
    /// let value1 = db.get(b"key1", &read_options).unwrap();
    /// let value2 = db.get(b"key2", &read_options).unwrap();
    /// ```
    pub fn snapshot<Snapshot: AsSnapshot>(&self, snapshot: &Snapshot) {
        let snapshot_ptr = snapshot.as_snapshot_ptr();
        unsafe {
            leveldb_sys::leveldb_readoptions_set_snapshot(self.raw, snapshot_ptr);
        }
    }

    /// Get the raw pointer to the underlying LevelDB read options.
    ///
    /// # Safety
    ///
    /// This method is for internal use only. The returned pointer should not be
    /// stored or used outside the lifetime of this `ReadOptions` instance.
    ///
    /// # Returns
    ///
    /// A raw pointer to the underlying `leveldb_readoptions_t`.
    pub(crate) fn raw(&self) -> *mut leveldb_sys::leveldb_readoptions_t {
        self.raw
    }
}

impl Default for ReadOptions {
    /// Create a default `ReadOptions` instance.
    ///
    /// This is equivalent to calling `ReadOptions::new()`.
    ///
    /// # Returns
    ///
    /// A new `ReadOptions` instance with default settings.
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for ReadOptions {
    /// Clean up the read options resources.
    ///
    /// This method is automatically called when the `ReadOptions` instance goes out of scope.
    /// It ensures that all internal LevelDB read options resources are properly released.
    fn drop(&mut self) {
        unsafe { leveldb_sys::leveldb_readoptions_destroy(self.raw) }
    }
}

//
// WRITE OPTIONS
//

/// Options for writing to the database.
///
/// This struct allows you to customize write behavior including durability
/// guarantees through sync operations.
///
/// # Examples
///
/// ## Basic usage with defaults
/// ```no_run
/// use bleveldb::options::WriteOptions;
///
/// let write_options = WriteOptions::default();
/// ```
///
/// ## Custom configuration for durable writes
/// ```no_run
/// use bleveldb::options::WriteOptions;
///
/// let write_options = WriteOptions::new();
/// write_options.sync(true);
/// ```
pub struct WriteOptions {
    raw: *mut leveldb_sys::leveldb_writeoptions_t,
}

impl WriteOptions {
    /// Create a new `WriteOptions` instance with default values.
    ///
    /// The default write options are:
    /// - `sync`: false (writes are asynchronous)
    ///
    /// # Returns
    ///
    /// A new `WriteOptions` instance with default settings.
    pub fn new() -> Self {
        Self {
            raw: unsafe { leveldb_sys::leveldb_writeoptions_create() },
        }
    }

    /// Configure whether writes should be synchronized to disk.
    ///
    /// When enabled, each write will be flushed from the operating system
    /// buffer cache to disk before the write operation is considered complete.
    /// This provides durability guarantees but significantly impacts write performance.
    ///
    /// # Arguments
    ///
    /// * `value` - If `true`, writes are synchronized to disk.
    ///             If `false`, writes are asynchronous (faster but less durable).
    /// ```
    pub fn sync(&self, value: bool) {
        unsafe { leveldb_sys::leveldb_writeoptions_set_sync(self.raw, value as u8) };
    }

    /// Get the raw pointer to the underlying LevelDB write options.
    ///
    /// # Safety
    ///
    /// This method is for internal use only. The returned pointer should not be
    /// stored or used outside the lifetime of this `WriteOptions` instance.
    ///
    /// # Returns
    ///
    /// A raw pointer to the underlying `leveldb_writeoptions_t`.
    pub(crate) fn raw(&self) -> *mut leveldb_sys::leveldb_writeoptions_t {
        self.raw
    }
}

impl Default for WriteOptions {
    /// Create a default `WriteOptions` instance.
    ///
    /// This is equivalent to calling `WriteOptions::new()`.
    ///
    /// # Returns
    ///
    /// A new `WriteOptions` instance with default settings.
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for WriteOptions {
    /// Clean up the write options resources.
    ///
    /// This method is automatically called when the `WriteOptions` instance goes out of scope.
    /// It ensures that all internal LevelDB write options resources are properly released.
    fn drop(&mut self) {
        unsafe { leveldb_sys::leveldb_writeoptions_destroy(self.raw) }
    }
}
