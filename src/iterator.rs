use bedrock_leveldb_sys as sys;
use std::marker::PhantomData;
use std::slice;

use crate::DB;
use crate::options::ReadOptions;

/// A safe iterator over key-value pairs in a LevelDB database.
///
/// This iterator provides sequential access to all key-value pairs in the database.
/// The iteration order is determined by the key comparator configured in the database options.
///
/// The iterator yields `(Vec<u8>, Vec<u8>)` pairs representing (key, value).
///
/// # Examples
///
/// ## Basic iteration
/// ```no_run
/// # use bedrock_leveldb::{DB, options::Options};
/// # let options = Options::default();
/// # let db = DB::open("test_db", &options).unwrap();
/// let mut iter = db.iter(&Default::default());
///
/// // Iterate through all key-value pairs
/// for result in iter {
///     let (key, value) = result;
///     println!("Key: {:?}, Value: {:?}", key, value);
/// }
/// ```
///
/// ## Seeking to a specific position
/// ```no_run
/// # use bedrock_leveldb::{DB, options::Options};
/// # let options = Options::default();
/// # let db = DB::open("test_db", &options).unwrap();
/// let mut iter = db.iter(&Default::default());
///
/// // Start from a specific key
/// iter.seek(b"prefix_");
/// while iter.valid() {
///     if let (Some(key), Some(value)) = (iter.key(), iter.value()) {
///         println!("Found: {:?} => {:?}", key, value);
///     }
///     iter.next_native();
/// }
/// ```
///
/// # Performance Notes
///
/// - The iterator is efficient for sequential access but may be slower for random access
/// - Using `seek()` is optimized for prefix-based access patterns
/// - The iterator maintains internal resources that are automatically cleaned up when dropped
pub struct DBIterator<'db> {
    raw: *mut sys::leveldb_iterator_t,
    _db: PhantomData<&'db DB>,
}

impl<'db> DBIterator<'db> {
    /// Create a new iterator from a database.
    ///
    /// This is marked as `pub(crate)` because iterators should be created through
    /// the `DB::iter()` method rather than directly.
    ///
    /// # Arguments
    ///
    /// * `db` - Reference to the database to iterate over
    /// * `options` - Read options controlling the iterator behavior
    ///
    /// # Returns
    ///
    /// A new iterator positioned before the first key in the database.
    pub(crate) fn new(db: &'db DB, options: &ReadOptions) -> Self {
        let iter = unsafe { sys::leveldb_create_iterator(db.raw(), options.raw()) };
        Self {
            raw: iter,
            _db: PhantomData,
        }
    }

    /// Move iterator to the first key in the database.
    ///
    /// After calling this method, if the database is not empty, `valid()` will return `true`
    /// and `key()`/`value()` will return the first key-value pair.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use bedrock_leveldb::{DB, options::Options};
    /// # let options = Options::default();
    /// # let db = DB::open("test_db", &options).unwrap();
    /// let mut iter = db.iter(&Default::default());
    /// iter.seek_to_first();
    ///
    /// if iter.valid() {
    ///     println!("First key: {:?}", iter.key().unwrap());
    /// }
    /// ```
    pub fn seek_to_first(&mut self) {
        unsafe { sys::leveldb_iter_seek_to_first(self.raw) };
    }

    /// Move iterator to the last key in the database.
    ///
    /// After calling this method, if the database is not empty, `valid()` will return `true`
    /// and `key()`/`value()` will return the last key-value pair.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use bedrock_leveldb::{DB, options::Options};
    /// # let options = Options::default();
    /// # let db = DB::open("test_db", &options).unwrap();
    /// let mut iter = db.iter(&Default::default());
    /// iter.seek_to_last();
    ///
    /// if iter.valid() {
    ///     println!("Last key: {:?}", iter.key().unwrap());
    /// }
    /// ```
    pub fn seek_to_last(&mut self) {
        unsafe { sys::leveldb_iter_seek_to_last(self.raw) };
    }

    /// Move iterator to the first key greater than or equal to the given key.
    ///
    /// This method is efficient for starting iteration from a specific key or prefix.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to seek to. The iterator will position itself at the first key
    ///           that is greater than or equal to this key according to the comparator.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use bedrock_leveldb::{DB, options::Options};
    /// # let options = Options::default();
    /// # let db = DB::open("test_db", &options).unwrap();
    /// let mut iter = db.iter(&Default::default());
    ///
    /// // Seek to keys starting with "user_"
    /// iter.seek(b"user_");
    ///
    /// while iter.valid() {
    ///     let key = iter.key().unwrap();
    ///     if !key.starts_with(b"user_") {
    ///         break;
    ///     }
    ///     println!("User key: {:?}", key);
    ///     iter.next_native();
    /// }
    /// ```
    pub fn seek(&mut self, key: &[u8]) {
        unsafe {
            sys::leveldb_iter_seek(self.raw, key.as_ptr() as *const _, key.len());
        }
    }

    /// Move to the next key in the database.
    ///
    /// This is the low-level method that only advances the iterator without returning data.
    /// For most use cases, prefer using the `Iterator` trait implementation which combines
    /// advancement with data retrieval.
    ///
    /// # Note
    ///
    /// After calling this method, you should check `valid()` before accessing `key()` or `value()`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use bedrock_leveldb::{DB, options::Options};
    /// # let options = Options::default();
    /// # let db = DB::open("test_db", &options).unwrap();
    /// let mut iter = db.iter(&Default::default());
    /// iter.seek_to_first();
    ///
    /// while iter.valid() {
    ///     if let (Some(key), Some(value)) = (iter.key(), iter.value()) {
    ///         println!("Key: {:?}", key);
    ///     }
    ///     iter.next_native();
    /// }
    /// ```
    pub fn next_native(&mut self) {
        unsafe { sys::leveldb_iter_next(self.raw) };
    }

    /// Move to the previous key in the database.
    ///
    /// This is the low-level method that only moves the iterator backward without returning data.
    ///
    /// # Note
    ///
    /// After calling this method, you should check `valid()` before accessing `key()` or `value()`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use bedrock_leveldb::{DB, options::Options};
    /// # let options = Options::default();
    /// # let db = DB::open("test_db", &options).unwrap();
    /// let mut iter = db.iter(&Default::default());
    /// iter.seek_to_last();
    ///
    /// // Iterate backwards
    /// while iter.valid() {
    ///     if let (Some(key), Some(value)) = (iter.key(), iter.value()) {
    ///         println!("Key: {:?}", key);
    ///     }
    ///     iter.prev_native();
    /// }
    /// ```
    pub fn prev_native(&mut self) {
        unsafe { sys::leveldb_iter_prev(self.raw) };
    }

    /// Move to the previous key and return the current key-value pair.
    ///
    /// This is a convenience method that combines moving backward with data retrieval.
    ///
    /// # Returns
    ///
    /// * `Some((key, value))` - If the iterator was valid before moving and is still valid after moving
    /// * `None` - If the iterator was not valid or reached the beginning
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use bedrock_leveldb::{DB, options::Options};
    /// # let options = Options::default();
    /// # let db = DB::open("test_db", &options).unwrap();
    /// let mut iter = db.iter(&Default::default());
    /// iter.seek_to_last();
    ///
    /// // Get the last item and move backward
    /// while let Some((key, value)) = iter.prev() {
    ///     println!("Previous key: {:?}", key);
    /// }
    /// ```
    pub fn prev(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        if !self.valid() {
            None
        } else {
            self.prev_native();

            let key = self.key()?;
            let value = self.value()?;
            Some((key, value))
        }
    }

    /// Check if the iterator is currently positioned at a valid key-value pair.
    ///
    /// Returns `true` if the iterator is positioned at a valid entry, `false` if the iterator
    /// has reached the end (or beginning when iterating backwards) of the database.
    ///
    /// # Returns
    ///
    /// `true` if the iterator is valid, `false` otherwise.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use bedrock_leveldb::{DB, options::Options};
    /// # let options = Options::default();
    /// # let db = DB::open("test_db", &options).unwrap();
    /// let mut iter = db.iter(&Default::default());
    /// iter.seek_to_first();
    ///
    /// if iter.valid() {
    ///     println!("Database is not empty");
    /// } else {
    ///     println!("Database is empty");
    /// }
    /// ```
    pub fn valid(&self) -> bool {
        unsafe { sys::leveldb_iter_valid(self.raw) != 0 }
    }

    /// Get the current key at the iterator position.
    ///
    /// # Returns
    ///
    /// * `Some(Vec<u8>)` - The current key if the iterator is valid
    /// * `None` - If the iterator is not positioned at a valid entry
    ///
    /// # Note
    ///
    /// The returned vector is a copy of the key data. For performance-sensitive code,
    /// consider whether you need to copy the data or can work with references.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use bedrock_leveldb::{DB, options::Options};
    /// # let options = Options::default();
    /// # let db = DB::open("test_db", &options).unwrap();
    /// let mut iter = db.iter(&Default::default());
    /// iter.seek_to_first();
    ///
    /// if let Some(key) = iter.key() {
    ///     println!("First key: {:?}", key);
    /// }
    /// ```
    pub fn key(&self) -> Option<Vec<u8>> {
        unsafe {
            if self.valid() {
                let mut klen: usize = 0;
                let ptr = sys::leveldb_iter_key(self.raw, &mut klen);
                Some(slice::from_raw_parts(ptr as *const u8, klen).to_vec())
            } else {
                None
            }
        }
    }

    /// Get the current value at the iterator position.
    ///
    /// # Returns
    ///
    /// * `Some(Vec<u8>)` - The current value if the iterator is valid
    /// * `None` - If the iterator is not positioned at a valid entry
    ///
    /// # Note
    ///
    /// The returned vector is a copy of the value data. For performance-sensitive code,
    /// consider whether you need to copy the data or can work with references.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use bedrock_leveldb::{DB, options::Options};
    /// # let options = Options::default();
    /// # let db = DB::open("test_db", &options).unwrap();
    /// let mut iter = db.iter(&Default::default());
    /// iter.seek_to_first();
    ///
    /// if let Some(value) = iter.value() {
    ///     println!("First value: {:?}", value);
    /// }
    /// ```
    pub fn value(&self) -> Option<Vec<u8>> {
        unsafe {
            if self.valid() {
                let mut vlen: usize = 0;
                let ptr = sys::leveldb_iter_value(self.raw, &mut vlen);
                Some(slice::from_raw_parts(ptr as *const u8, vlen).to_vec())
            } else {
                None
            }
        }
    }
}

impl<'db> Iterator for DBIterator<'db> {
    type Item = (Vec<u8>, Vec<u8>);

    /// Advance the iterator and return the next key-value pair.
    ///
    /// This method is part of the `Iterator` trait implementation, allowing the
    /// `DBIterator` to be used with Rust's iterator patterns.
    ///
    /// # Returns
    ///
    /// * `Some((key, value))` - The next key-value pair if the iterator is valid
    /// * `None` - If the iterator has reached the end of the database
    ///
    /// # Behavior
    ///
    /// - The iterator starts before the first element
    /// - The first call to `next()` returns the first key-value pair
    /// - Subsequent calls advance through the database in order
    /// - Returns `None` when all elements have been visited
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use bedrock_leveldb::{DB, options::Options};
    /// # let options = Options::default();
    /// # let db = DB::open("test_db", &options).unwrap();
    /// let iter = db.iter(&Default::default());
    ///
    /// // Use iterator with for loop
    /// for (key, value) in iter {
    ///     println!("Key: {:?}, Value: {:?}", key, value);
    /// }
    ///
    /// // Use iterator with collect
    /// let iter2 = db.iter(&Default::default());
    /// let all_data: Vec<(Vec<u8>, Vec<u8>)> = iter2.collect();
    /// ```
    fn next(&mut self) -> Option<Self::Item> {
        if !self.valid() {
            return None;
        }

        let key = self.key()?;
        let value = self.value()?;
        self.next_native();
        Some((key, value))
    }
}

impl<'db> Drop for DBIterator<'db> {
    /// Clean up the iterator resources.
    ///
    /// This method is automatically called when the iterator goes out of scope.
    /// It ensures that all internal LevelDB iterator resources are properly released.
    fn drop(&mut self) {
        unsafe {
            sys::leveldb_iter_destroy(self.raw);
        }
    }
}
