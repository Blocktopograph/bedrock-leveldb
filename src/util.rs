use std::ffi::{CStr, CString};

/// Convert a LevelDB error pointer into a Rust `String`.
///
/// If `err_ptr` is null, returns `"unknown error"`.
pub(crate) fn error_message(err_ptr: *mut i8) -> String {
    if err_ptr.is_null() {
        return "unknown error".to_string();
    }
    let c_str = unsafe { CStr::from_ptr(err_ptr as *const _) };
    let msg = c_str.to_string_lossy().into_owned();
    // LevelDB allocates the error string with malloc → must be freed
    unsafe { bedrock_leveldb_sys::leveldb_free(err_ptr as *mut _) };
    msg
}

/// Convert a Rust &str into a C-compatible `CString`.
///
/// Returns `None` if the string contains null bytes.
pub(crate) fn to_cstring(s: &str) -> Option<CString> {
    CString::new(s).ok()
}
