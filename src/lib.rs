#![cfg_attr(feature = "nightly", feature(test))]

extern crate byteorder;
extern crate flate2;
extern crate crc;

#[macro_use]
extern crate log;

#[cfg(all(test, feature = "nightly"))]
extern crate test;

pub mod error;
pub mod utils;
pub mod client;
pub mod consumer;
mod codecs;
mod connection;
mod protocol;
mod compression;
