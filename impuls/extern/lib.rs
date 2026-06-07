// © Copyright 2026 Mikołaj Kuranowski
// SPDX-License-Identifier: GPL-3.0-or-later

use std::ffi::{CStr, c_char, c_int};
use std::path::Path;
use std::str;

mod db;
mod error;
mod gtfs;
mod logging;

#[unsafe(no_mangle)]
pub unsafe extern "C" fn set_log_handler(handler: Option<logging::LogHandler>) {
    logging::set_global_handler(handler);
}

#[repr(C)]
pub struct FileHeader {
    file_name: *const c_char,
    fields: *const *const c_char,
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn load_gtfs(
    db_path: *const c_char,
    gtfs_dir_path: *const c_char,
    extra_fields: bool,
    extra_files: *const *const c_char,
) -> c_int {
    return 1;
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn save_gtfs(
    db_path: *const c_char,
    gtfs_dir_path: *const c_char,
    headers_ptr: *const FileHeader,
    headers_len: c_int,
    emit_empty_calendars: bool,
    ensure_order: bool,
) -> c_int {
    let files: Vec<_> = (0..headers_len)
        .map(|i| {
            let f = unsafe { headers_ptr.offset(i as isize).read() };
            (cstr_to_str(f.file_name), collect_cstr_arr(f.fields))
        })
        .collect();

    let options = gtfs::SaveOptions { ensure_order };

    let result = gtfs::save(
        cstr_to_path(db_path),
        cstr_to_path(gtfs_dir_path),
        files
            .iter()
            .map(|(file_name, header)| (file_name.as_ref(), header.as_ref())),
        options,
    );

    match result {
        Ok(_) => 0,
        Err(_) => 1,
    }
}

cfg_select! {
    any(unix, target_os = "wasi") => {
        use std::ffi::OsStr;
        #[cfg(unix)]
        use std::os::unix::ffi::OsStrExt;
        #[cfg(target_os = "wasi")]
        use std::os::wasi::ffi::OsStrExt;

        /// Borrows a C-style, NULL-terminated string into a Rust-friendly &Path.
        ///
        /// This implementation contains a slight optimization on Unix and WASI platforms,
        /// where paths are "bags of bytes", and thus the string can be invalid UTF-8.
        pub fn cstr_to_path<'a>(ptr: *const c_char) -> &'a Path {
            assert!(!ptr.is_null());
            let c = unsafe { CStr::from_ptr(ptr) };
            let os = OsStr::from_bytes(c.to_bytes());
            Path::new(os)
        }
    }

    _ => {
        /// Borrows a C-tyle, NULL terminated string into a Rust-friendly &Path.
        ///
        /// This implementation goes through [cstr_to_str], requiring the string to contain
        /// valid UTF-8. This is used on platforms where paths are not "bags of bytes", rather
        /// a proper string - most likely Windows.
        pub fn cstr_to_path<'a>(ptr: *const c_char) -> &'a Path {
            // No OsStr::from_bytes - the only other solution is to go through &str
            Path::new(cstr_to_str(ptr))
        }
    }
}

/// Borrows a C-style, NULL-terminated string into a Rust-friendly &str.
///
/// Panics if the provided pointer is NULL or the string is not valid UTF-8.
pub fn cstr_to_str<'a>(ptr: *const c_char) -> &'a str {
    assert!(!ptr.is_null());
    let cstr = unsafe { CStr::from_ptr(ptr) };
    str::from_utf8(cstr.to_bytes()).expect("strings must be valid utf-8")
}

/// Collects a C-style, NULL-terminated array of strings into a Rust-friendly
/// vector of borrowed strings.
///
/// The provided pointer can be null, in which case an empty vector is returned.
///
/// Panics if any string is not valid UTF-8.
pub fn collect_cstr_arr<'a>(mut ptr: *const *const c_char) -> Vec<&'a str> {
    if !ptr.is_null() {
        let mut s = vec![];
        unsafe {
            while !(*ptr).is_null() {
                s.push(cstr_to_str(*ptr));
                ptr = ptr.add(1);
            }
        }
        s
    } else {
        vec![]
    }
}

#[cfg(test)]
mod tests {
    use std::ptr::null;

    use super::*;

    #[test]
    fn test_cstr() {
        assert_eq!(cstr_to_str(c"Hello, 世界!".as_ptr()), "Hello, 世界!");
        assert_eq!(
            cstr_to_path(c"/foo/bar/baz.txt".as_ptr()),
            "/foo/bar/baz.txt"
        );

        let c_string_arr = &[
            c"Hello, 世界!".as_ptr(),
            c"Lorem ipsum dolor sit amet consectetur".as_ptr(),
            null(),
        ];

        assert_eq!(
            collect_cstr_arr(c_string_arr.as_ptr()),
            &["Hello, 世界!", "Lorem ipsum dolor sit amet consectetur"]
        );
        assert!(collect_cstr_arr(c_string_arr[2..].as_ptr()).is_empty());
        assert!(collect_cstr_arr(null()).is_empty());
    }
}
