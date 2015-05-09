#![allow(unused_variables)]
#![allow(dead_code)]
#![allow(unused_parens)]
#![allow(unused_imports)]
#![allow(unused_must_use)]
#![allow(unused_mut)]
extern crate num;
extern crate byteorder;

mod error;
mod utils;
mod crc32;
mod snappy;
mod codecs;
mod connection;
mod protocol;
pub mod client;
