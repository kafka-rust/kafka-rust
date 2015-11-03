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

    match env::var_os("SNAPPY_STATIC") {
        Some(_) => {
            println!("cargo:rustc-link-lib=static=snappy");
            // From: https://github.com/alexcrichton/gcc-rs/blob/master/src/lib.rs
            let target = env::var("TARGET").unwrap();
            let cpp = if target.contains("darwin") {
                "c++"
            } else {
                "stdc++"
            };
            println!("cargo:rustc-flags=-l {}", cpp);
        },
        None => {
            println!("cargo:rustc-link-lib=dylib=snappy");
        }
    };

    if let Some(path) = first_path_with_file("libsnappy.a") {
        println!("cargo:rustc-link-search=native={}", path);
    }
}

fn first_path_with_file(file: &str) -> Option<String> {
    // we want to look in LD_LIBRARY_PATH and then some default folders
    if let Some(ld_path) = env::var_os("LD_LIBRARY_PATH") {
        for p in env::split_paths(&ld_path) {
            if is_file_in(file, &p) {
                return p.to_str().map(|s| String::from(s))
            }
        }
    }
    for p in vec!["/usr/lib","/usr/local/lib"] {
        if is_file_in(file, &Path::new(p)) {
            return Some(String::from(p))
        }
    }
    return None
}

fn is_file_in(file: &str, folder: &Path) -> bool {
    let full = folder.join(file);
    match fs::metadata(full) {
        Ok(ref found) if found.is_file() => true,
        _ => false
    }

}
