use crate::DB;
use crate::options::{Options, ReadOptions, WriteOptions};
use tempfile::TempDir;

// Test utilities
fn setup_test_db(name: &str) -> (DB, TempDir) {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join(name);

    let options = Options::new();
    options.create_if_missing(true);

    let db = DB::open(&db_path, &options).expect("Failed to open database");
    (db, temp_dir)
}

#[test]
fn test_db_open_success() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_open");

    let options = Options::new();
    options.create_if_missing(true);

    let db = DB::open(&db_path, &options);
    assert!(db.is_ok());

    let db = db.unwrap();
    // Database should be usable
    let read_opts = ReadOptions::new();
    let result = db.get(b"test_key", &read_opts);
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

#[test]
fn test_db_open_failure() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_open_fail");

    let opts = Options::new();
    // Don't set create_if_missing

    let db = DB::open(&db_path, &opts);
    assert!(db.is_err());

    let error = db.err().unwrap();
    assert!(!error.is_empty());
    assert!(error.to_lowercase().contains("not exist") || error.to_lowercase().contains("error"));
}

#[test]
fn test_db_put_and_get() {
    let (db, _temp_dir) = setup_test_db("test_put_get");
    let write_opts = WriteOptions::new();
    let read_opts = ReadOptions::new();

    // Test basic put and get
    let key = b"test_key";
    let value = b"test_value";

    db.put(key, value, &write_opts).expect("Put failed");

    let result = db.get(key, &read_opts).expect("Get failed");
    assert_eq!(result, Some(value.to_vec()));
}

#[test]
fn test_db_put_multiple() {
    let (db, _temp_dir) = setup_test_db("test_put_multiple");
    let write_opts = WriteOptions::new();
    let read_opts = ReadOptions::new();

    let test_data = vec![
        (b"key1", b"value1"),
        (b"key2", b"value2"),
        (b"key3", b"value3"),
    ];

    // Put all data
    for &(key, value) in &test_data {
        db.put(key, value, &write_opts).expect("Put failed");
    }

    // Verify all data
    for &(key, expected_value) in &test_data {
        let result = db.get(key, &read_opts).expect("Get failed");
        assert_eq!(result, Some(expected_value.to_vec()));
    }
}

#[test]
fn test_db_get_nonexistent() {
    let (db, _temp_dir) = setup_test_db("test_get_nonexistent");
    let read_opts = ReadOptions::new();

    let result = db.get(b"nonexistent_key", &read_opts).expect("Get failed");
    assert_eq!(result, None);
}

#[test]
fn test_db_delete() {
    let (db, _temp_dir) = setup_test_db("test_delete");
    let write_opts = WriteOptions::new();
    let read_opts = ReadOptions::new();

    let key = b"key_to_delete";
    let value = b"value_to_delete";

    // Put then delete
    db.put(key, value, &write_opts).expect("Put failed");

    let result = db.get(key, &read_opts).expect("Get failed");
    assert_eq!(result, Some(value.to_vec()));

    db.delete(key, &write_opts).expect("Delete failed");

    let result = db.get(key, &read_opts).expect("Get failed");
    assert_eq!(result, None);
}

#[test]
fn test_db_delete_nonexistent() {
    let (db, _temp_dir) = setup_test_db("test_delete_nonexistent");
    let write_opts = WriteOptions::new();

    // Deleting non-existent key should not error
    db.delete(b"nonexistent_key", &write_opts)
        .expect("Delete failed");
}

#[test]
fn test_db_overwrite() {
    let (db, _temp_dir) = setup_test_db("test_overwrite");
    let write_opts = WriteOptions::new();
    let read_opts = ReadOptions::new();

    let key = b"same_key";
    let value1 = b"first_value";
    let value2 = b"second_value";

    db.put(key, value1, &write_opts).expect("First put failed");
    let result1 = db.get(key, &read_opts).expect("First get failed");
    assert_eq!(result1, Some(value1.to_vec()));

    db.put(key, value2, &write_opts).expect("Second put failed");
    let result2 = db.get(key, &read_opts).expect("Second get failed");
    assert_eq!(result2, Some(value2.to_vec()));
}

#[test]
fn test_db_empty_key_value() {
    let (db, _temp_dir) = setup_test_db("test_empty");
    let write_opts = WriteOptions::new();
    let read_opts = ReadOptions::new();

    // Test empty key
    db.put(b"", b"empty_key_value", &write_opts)
        .expect("Put with empty key failed");
    let result = db.get(b"", &read_opts).expect("Get with empty key failed");
    assert_eq!(result, Some(b"empty_key_value".to_vec()));

    // Test empty value
    db.put(b"empty_value_key", b"", &write_opts)
        .expect("Put with empty value failed");
    let result = db
        .get(b"empty_value_key", &read_opts)
        .expect("Get with empty value failed");
    assert_eq!(result, Some(b"".to_vec()));
}

#[test]
fn test_db_large_value() {
    let (db, _temp_dir) = setup_test_db("test_large_value");
    let write_opts = WriteOptions::new();
    let read_opts = ReadOptions::new();

    let key = b"large_value_key";
    let value = vec![0xAB; 1024 * 1024]; // 1MB value

    db.put(key, &value, &write_opts)
        .expect("Put large value failed");
    let result = db.get(key, &read_opts).expect("Get large value failed");
    assert_eq!(result, Some(value));
}

#[test]
fn test_db_binary_data() {
    let (db, _temp_dir) = setup_test_db("test_binary_data");
    let write_opts = WriteOptions::new();
    let read_opts = ReadOptions::new();

    let key = b"binary_key\x00\x01\x02";
    let value = vec![0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD];

    db.put(key, &value, &write_opts)
        .expect("Put binary data failed");
    let result = db.get(key, &read_opts).expect("Get binary data failed");
    assert_eq!(result, Some(value));
}

#[test]
fn test_db_compact_range() {
    let (db, _temp_dir) = setup_test_db("test_compact");
    let write_opts = WriteOptions::new();

    // Add some data
    for i in 0..100 {
        let key = format!("key_{:03}", i);
        let value = format!("value_{:03}", i);
        db.put(key.as_bytes(), value.as_bytes(), &write_opts)
            .expect("Put failed");
    }

    // Compact specific range
    db.compact_range(Some(b"key_010"), Some(b"key_090"));

    // Compact entire database
    db.compact_range(None, None);

    // Database should still work after compaction
    let read_opts = ReadOptions::new();
    let result = db
        .get(b"key_050", &read_opts)
        .expect("Get after compaction failed");
    assert_eq!(result, Some(b"value_050".to_vec()));
}

#[test]
fn test_db_flush() {
    let (db, _temp_dir) = setup_test_db("test_flush");
    let write_opts = WriteOptions::new();
    let read_opts = ReadOptions::new();

    // Add data
    db.put(b"flush_key", b"flush_value", &write_opts)
        .expect("Put before flush failed");

    // Flush (compact_range with None, None)
    db.flush();

    // Data should still be accessible
    let result = db
        .get(b"flush_key", &read_opts)
        .expect("Get after flush failed");
    assert_eq!(result, Some(b"flush_value".to_vec()));
}

#[test]
fn test_db_sync_write() {
    let (db, _temp_dir) = setup_test_db("test_sync_write");
    let write_opts = WriteOptions::new();
    write_opts.sync(true); // Enable sync writes

    let read_opts = ReadOptions::new();

    db.put(b"sync_key", b"sync_value", &write_opts)
        .expect("Sync put failed");

    let result = db
        .get(b"sync_key", &read_opts)
        .expect("Get after sync failed");
    assert_eq!(result, Some(b"sync_value".to_vec()));
}

#[test]
fn test_db_concurrent_access() {
    let (db, _temp_dir) = setup_test_db("test_concurrent");
    let write_opts = WriteOptions::new();
    let read_opts = ReadOptions::new();

    // Test that multiple operations work sequentially
    db.put(b"key1", b"value1", &write_opts)
        .expect("Put 1 failed");
    db.put(b"key2", b"value2", &write_opts)
        .expect("Put 2 failed");

    let result1 = db.get(b"key1", &read_opts).expect("Get 1 failed");
    let result2 = db.get(b"key2", &read_opts).expect("Get 2 failed");

    db.delete(b"key1", &write_opts).expect("Delete failed");
    let result3 = db.get(b"key1", &read_opts).expect("Get 3 failed");

    assert_eq!(result1, Some(b"value1".to_vec()));
    assert_eq!(result2, Some(b"value2".to_vec()));
    assert_eq!(result3, None);
}

#[test]
fn test_db_drop() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_drop");

    let options = Options::new();
    options.create_if_missing(true);

    {
        let db = DB::open(&db_path, &options).expect("Failed to open database");
        let write_opts = WriteOptions::new();
        db.put(b"test_key", b"test_value", &write_opts)
            .expect("Put before drop failed");
    } // db is dropped here

    // Reopen database to verify data persisted
    let db = DB::open(&db_path, &options).expect("Failed to reopen database");
    let read_opts = ReadOptions::new();
    let result = db
        .get(b"test_key", &read_opts)
        .expect("Get after reopen failed");
    assert_eq!(result, Some(b"test_value".to_vec()));
}

#[test]
fn test_db_error_messages() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_errors");

    let options = Options::new();
    // Don't set create_if_missing to force error

    let result = DB::open(&db_path, &options);
    assert!(result.is_err());

    let error = result.err().unwrap();
    assert!(!error.is_empty());
    // Error message should be descriptive
    println!("Error message: {}", error);
}
