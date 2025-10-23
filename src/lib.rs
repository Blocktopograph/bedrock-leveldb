//! # bedrock-leveldb
//! Safe, ergonomic Rust bindings for Minecraft Bedrock's LevelDB database.
//!
//! This crate wraps the raw FFI in `bedrock-leveldb-sys` and exposes a
//! high-level, memory-safe API for reading and writing Bedrock world data.
//!
//! ## Example
//! ```no_run
//! use bedrock_leveldb::{DB, Options, ReadOptions, WriteOptions};
//!
//! let options = Options::new();
//! options.create_if_missing(true);
//!
//! let db = DB::open("test_db", &options).unwrap();
//! db.put(b"key", b"value", &WriteOptions::new()).unwrap();
//! let value = db.get(b"key", &ReadOptions::new()).unwrap();
//! assert_eq!(value.unwrap(), b"value");
//! ```

pub mod db;
pub mod iterator;
pub mod options;
pub mod write_batch;

#[cfg(test)]
mod tests;

#[cfg(feature = "error")]
mod error;

pub use db::DB;
pub use iterator::DBIterator;
pub use options::Options;
pub use options::ReadOptions;
pub use options::WriteOptions;
pub use write_batch::WriteBatch;

#[cfg(feature = "error")]
pub use error::Error;

/// Internal utility functions (not public API)
pub(crate) mod util;

/// Version of the crate at runtime
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
