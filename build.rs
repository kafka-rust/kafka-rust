#![allow(clippy::unusual_byte_groupings)]
use std::env;

fn main() {
    if let Ok(v) = env::var("DEP_OPENSSL_VERSION_NUMBER") {
        let version = u64::from_str_radix(&v, 16).unwrap();

        if version >= 0x1_01_00_00_0 {
            println!("cargo:rustc-cfg=openssl110");
        }
    }
}
