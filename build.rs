use std::fs;
use std::path::Path;

fn main() {
    configure_snappy();
}

fn configure_snappy() {
    // link the static library to simplify distribution
    println!("cargo:rustc-link-lib=static=snappy");
    println!("cargo:rustc-flags=-l c++");
    // For now just look in /usr/local/lib since this is the path where brew install snappy.
    if is_file_in("libsnappy.a", Path::new("/usr/local/lib")) {
        println!("cargo:rustc-link-search={}", "/usr/local/lib/");
    }
}

fn is_file_in(file: &str, folder: &Path) -> bool {
    let full = folder.join(file);
    match fs::metadata(full) {
        Ok(ref found) if found.is_file() => true,
        _ => false
    }

}
