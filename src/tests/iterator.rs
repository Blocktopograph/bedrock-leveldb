use crate::DB;
use crate::options::{Options, ReadOptions, WriteOptions};
use tempfile::TempDir;

fn setup_test_db_with_data(name: &str, data: &[(&[u8], &[u8])]) -> (DB, TempDir) {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join(name);

    let options = Options::new();
    options.create_if_missing(true);

    let db = DB::open(&db_path, &options).expect("Failed to open database");
    let write_opts = WriteOptions::new();

    // Insert test data
    for (key, value) in data {
        db.put(key, value, &write_opts)
            .expect("Failed to put test data");
    }

    (db, temp_dir)
}

#[test]
fn test_iterator_creation() {
    let (db, _temp_dir) = setup_test_db_with_data("test_iter_creation", &[]);
    let read_opts = ReadOptions::new();

    let iter = db.iter(&read_opts);
    assert!(!iter.valid()); // Iterator should be invalid initially
}

#[test]
fn test_iterator_seek_to_first() {
    let test_data: Vec<(&'static [u8], &'static [u8])> =
        vec![(b"a", b"1"), (b"b", b"2"), (b"c", b"3")];
    let (db, _temp_dir) = setup_test_db_with_data("test_seek_first", &test_data);
    let read_opts = ReadOptions::new();

    let mut iter = db.iter(&read_opts);
    iter.seek_to_first();

    assert!(iter.valid());
    let key = iter.key().expect("Should have key");
    let value = iter.value().expect("Should have value");
    assert_eq!(key, b"a");
    assert_eq!(value, b"1");
}

#[test]
fn test_iterator_seek_to_last() {
    let test_data: Vec<(&'static [u8], &'static [u8])> =
        vec![(b"a", b"1"), (b"b", b"2"), (b"c", b"3")];
    let (db, _temp_dir) = setup_test_db_with_data("test_seek_last", &test_data);
    let read_opts = ReadOptions::new();

    let mut iter = db.iter(&read_opts);
    iter.seek_to_last();

    assert!(iter.valid());
    let key = iter.key().expect("Should have key");
    let value = iter.value().expect("Should have value");
    assert_eq!(key, b"c");
    assert_eq!(value, b"3");
}

#[test]
fn test_iterator_seek() {
    let test_data: Vec<(&'static [u8], &'static [u8])> = vec![
        (b"apple", b"fruit"),
        (b"banana", b"fruit"),
        (b"cherry", b"fruit"),
        (b"date", b"fruit"),
    ];
    let (db, _temp_dir) = setup_test_db_with_data("test_seek", &test_data);
    let read_opts = ReadOptions::new();

    let mut iter = db.iter(&read_opts);

    // Seek to exact key
    iter.seek(b"banana");
    assert!(iter.valid());
    assert_eq!(iter.key().unwrap(), b"banana");
    assert_eq!(iter.value().unwrap(), b"fruit");

    // Seek to non-existent key (should go to next)
    iter.seek(b"carrot");
    assert!(iter.valid());
    assert_eq!(iter.key().unwrap(), b"cherry");

    // Seek beyond last key
    iter.seek(b"zucchini");
    assert!(!iter.valid());
}

#[test]
fn test_iterator_forward_iteration() {
    let test_data: Vec<(&'static [u8], &'static [u8])> =
        vec![(b"a", b"1"), (b"b", b"2"), (b"c", b"3")];
    let (db, _temp_dir) = setup_test_db_with_data("test_forward", &test_data);
    let read_opts = ReadOptions::new();

    let mut iter = db.iter(&read_opts);
    iter.seek_to_first();

    let mut collected = Vec::new();
    while iter.valid() {
        if let (Some(key), Some(value)) = (iter.key(), iter.value()) {
            collected.push((key, value));
        }
        iter.next();
    }

    assert_eq!(collected.len(), 3);
    assert_eq!(collected[0], (b"a".to_vec(), b"1".to_vec()));
    assert_eq!(collected[1], (b"b".to_vec(), b"2".to_vec()));
    assert_eq!(collected[2], (b"c".to_vec(), b"3".to_vec()));
}

#[test]
fn test_iterator_backward_iteration() {
    let test_data: Vec<(&'static [u8], &'static [u8])> =
        vec![(b"a", b"1"), (b"b", b"2"), (b"c", b"3")];
    let (db, _temp_dir) = setup_test_db_with_data("test_backward", &test_data);
    let read_opts = ReadOptions::new();

    let mut iter = db.iter(&read_opts);
    iter.seek_to_last();

    let mut collected = Vec::new();
    while iter.valid() {
        if let (Some(key), Some(value)) = (iter.key(), iter.value()) {
            collected.push((key, value));
        }
        iter.prev();
    }

    // Backward iteration should give keys in reverse order
    assert_eq!(collected.len(), 3);
    assert_eq!(collected[0], (b"c".to_vec(), b"3".to_vec()));
    assert_eq!(collected[1], (b"b".to_vec(), b"2".to_vec()));
    assert_eq!(collected[2], (b"a".to_vec(), b"1".to_vec()));
}

#[test]
fn test_iterator_trait_implementation() {
    let test_data: Vec<(&'static [u8], &'static [u8])> = vec![
        (b"key1", b"value1"),
        (b"key2", b"value2"),
        (b"key3", b"value3"),
    ];
    let (db, _temp_dir) = setup_test_db_with_data("test_iter_trait", &test_data);
    let read_opts = ReadOptions::new();

    let mut iter = db.iter(&read_opts);
    iter.seek_to_first();

    // Use Iterator trait methods
    let mut collected = Vec::new();
    while let Some((key, value)) = iter.next() {
        collected.push((key, value));
    }

    assert_eq!(collected.len(), 3);
    assert_eq!(collected[0], (b"key1".to_vec(), b"value1".to_vec()));
    assert_eq!(collected[1], (b"key2".to_vec(), b"value2".to_vec()));
    assert_eq!(collected[2], (b"key3".to_vec(), b"value3".to_vec()));

    // Iterator should be exhausted
    assert!(!iter.valid());
    assert!(iter.next().is_none());
}

#[test]
fn test_iterator_empty_database() {
    let (db, _temp_dir) = setup_test_db_with_data("test_empty_iter", &[]);
    let read_opts = ReadOptions::new();

    let mut iter = db.iter(&read_opts);

    // All operations should not panic on empty DB
    iter.seek_to_first();
    assert!(!iter.valid());
    assert!(iter.key().is_none());
    assert!(iter.value().is_none());

    iter.seek_to_last();
    assert!(!iter.valid());

    iter.seek(b"any_key");
    assert!(!iter.valid());

    iter.next();
    assert!(!iter.valid());

    iter.prev();
    assert!(!iter.valid());
}

#[test]
fn test_iterator_key_value_invalid() {
    let test_data: Vec<(&'static [u8], &'static [u8])> = vec![(b"single", b"value")];
    let (db, _temp_dir) = setup_test_db_with_data("test_invalid", &test_data);
    let read_opts = ReadOptions::new();

    let mut iter = db.iter(&read_opts);

    // Key/value should return None when iterator is invalid
    assert!(iter.key().is_none());
    assert!(iter.value().is_none());

    // Move to valid position
    iter.seek_to_first();
    assert!(iter.key().is_some());
    assert!(iter.value().is_some());

    // Move beyond data
    iter.next();
    assert!(!iter.valid());
    assert!(iter.key().is_none());
    assert!(iter.value().is_none());
}

#[test]
fn test_iterator_with_snapshot() {
    let test_data: Vec<(&'static [u8], &'static [u8])> = vec![(b"a", b"1"), (b"b", b"2")];
    let (db, _temp_dir) = setup_test_db_with_data("test_snapshot_iter", &test_data);

    // Create snapshot
    let read_opts = ReadOptions::new();
    read_opts.snapshot(&db);

    let mut iter = db.iter(&read_opts);
    iter.seek_to_first();

    // Verify initial data
    assert_eq!(iter.key().unwrap(), b"a");
    iter.next();
    assert_eq!(iter.key().unwrap(), b"b");

    // Add more data while iterator is active
    let write_opts = WriteOptions::new();
    db.put(b"c", b"3", &write_opts).expect("Put failed");

    // Iterator with snapshot should not see new data
    iter.seek_to_first();
    let mut count = 0;
    while iter.valid() {
        count += 1;
        iter.next();
    }
    assert_eq!(count, 2); // Should only see a and b, not c
}

#[test]
fn test_iterator_binary_data() {
    let test_data: Vec<(&'static [u8], &'static [u8])> = vec![
        (b"key\x00\x01", b"value\xFF\xFE"),
        (b"key\x02\x03", b"value\xFD\xFC"),
    ];
    let (db, _temp_dir) = setup_test_db_with_data("test_binary_iter", &test_data);
    let read_opts = ReadOptions::new();

    let mut iter = db.iter(&read_opts);
    iter.seek_to_first();

    let mut collected = Vec::new();
    while let Some((key, value)) = iter.next() {
        collected.push((key, value));
    }

    assert_eq!(collected.len(), 2);
    assert_eq!(collected[0].0, b"key\x00\x01");
    assert_eq!(collected[0].1, b"value\xFF\xFE");
    assert_eq!(collected[1].0, b"key\x02\x03");
    assert_eq!(collected[1].1, b"value\xFD\xFC");
}

#[test]
fn test_iterator_large_dataset() {
    let mut keys = Vec::new();

    let mut values = Vec::new();

    let mut test_data = Vec::new();
    for i in 0..100 {
        keys.push(format!("key_{:03}", i).into_bytes());
        values.push(format!("value_{:03}", i).into_bytes());
    }

    for i in 0..100 {
        test_data.push((keys[i].as_slice(), values[i].as_slice()));
    }

    let (db, _temp_dir) = setup_test_db_with_data("test_large_iter", &test_data);
    let read_opts = ReadOptions::new();

    let mut iter = db.iter(&read_opts);
    iter.seek_to_first();

    let mut count = 0;
    let mut last_key = None;
    while iter.valid() {
        if let Some(key) = iter.key() {
            last_key = Some(key.clone());
            count += 1;
        }
        iter.next();
    }

    assert_eq!(count, 100);
    assert_eq!(last_key.unwrap(), b"key_099");
}

#[test]
fn test_iterator_drop_cleanup() {
    let test_data: Vec<(&'static [u8], &'static [u8])> = vec![(b"test", b"data")];
    let (db, _temp_dir) = setup_test_db_with_data("test_drop_iter", &test_data);
    let read_opts = ReadOptions::new();

    // Create multiple iterators to test resource cleanup
    {
        let _ = db.iter(&read_opts);
        let _ = db.iter(&read_opts);
        // Iterators should be dropped here without issues
    }

    // Database should still work after iterators are dropped
    let write_opts = WriteOptions::new();
    db.put(b"new_key", b"new_value", &write_opts)
        .expect("Put after iterator drop failed");

    let result = db
        .get(b"new_key", &read_opts)
        .expect("Get after iterator drop failed");
    assert_eq!(result, Some(b"new_value".to_vec()));
}

#[test]
fn test_iterator_mixed_operations() {
    let test_data: Vec<(&'static [u8], &'static [u8])> = vec![
        (b"a", b"1"),
        (b"b", b"2"),
        (b"c", b"3"),
        (b"d", b"4"),
        (b"e", b"5"),
    ];
    let (db, _temp_dir) = setup_test_db_with_data("test_mixed_ops", &test_data);
    let read_opts = ReadOptions::new();

    let mut iter = db.iter(&read_opts);

    // Test mixed seek and navigation
    iter.seek(b"c");
    assert_eq!(iter.key().unwrap(), b"c");

    iter.prev();
    assert_eq!(iter.key().unwrap(), b"b");

    iter.next();
    assert_eq!(iter.key().unwrap(), b"c");

    iter.next();
    assert_eq!(iter.key().unwrap(), b"d");

    iter.seek_to_first();
    assert_eq!(iter.key().unwrap(), b"a");

    iter.seek_to_last();
    assert_eq!(iter.key().unwrap(), b"e");
}

#[test]
fn test_iterator_range_scan() {
    let test_data: Vec<(&'static [u8], &'static [u8])> = vec![
        (b"apple", b"red"),
        (b"banana", b"yellow"),
        (b"cherry", b"red"),
        (b"date", b"brown"),
        (b"elderberry", b"purple"),
    ];
    let (db, _temp_dir) = setup_test_db_with_data("test_range_scan", &test_data);
    let read_opts = ReadOptions::new();

    let mut iter = db.iter(&read_opts);

    // Scan range from "b" to "d"
    iter.seek(b"b");
    let mut fruits_in_range = Vec::new();
    while iter.valid() {
        if let Some(key) = iter.key() {
            if key.starts_with(b"b") || key.starts_with(b"c") {
                fruits_in_range.push(key);
            } else if key.starts_with(b"d") {
                // Include 'date' but stop after
                fruits_in_range.push(key);
                break;
            } else {
                break;
            }
        }
        iter.next();
    }

    assert_eq!(fruits_in_range.len(), 3);
    assert_eq!(fruits_in_range[0], b"banana");
    assert_eq!(fruits_in_range[1], b"cherry");
    assert_eq!(fruits_in_range[2], b"date");
}
