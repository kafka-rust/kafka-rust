extern crate pkg_config;

use std::fs;
use std::path::Path;
use std::env;

fn main() {
    configure_snappy();
}

fn configure_snappy() {
    // try pkg_config first
    if pkg_config::find_library("snappy").is_ok() {
        return;
    }
    let mode = if env::var_os("SNAPPY_STATIC").is_some() {
        "static"
    } else {
        "dylib"
    };
    println!("cargo:rustc-link-lib={}=snappy", mode);
    println!("cargo:rustc-flags=-l c++");

    for f in vec!["/usr/lib","/usr/local/lib"] {
        if is_file_in("libsnappy.a", Path::new(f)) {
            println!("cargo:rustc-link-search={}", f);
        }
    }
}

fn is_file_in(file: &str, folder: &Path) -> bool {
    let full = folder.join(file);
    match fs::metadata(full) {
        Ok(ref found) if found.is_file() => true,
        _ => false
    }

}
