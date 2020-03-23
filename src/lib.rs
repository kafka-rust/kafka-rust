//! Clients for comunicating with a [Kafka](http://kafka.apache.org/)
//! cluster.  These are:
//!
//! - `kafka::producer::Producer` - for sending message to Kafka
//! - `kafka::consumer::Consumer` - for retrieving/consuming messages from Kafka
//! - `kafka::client::KafkaClient` - a lower-level, general purpose client leaving
//!   you with more power but also more resposibility
//!
//! See module level documentation corresponding to each client individually.
#![recursion_limit = "128"]
#![cfg_attr(feature = "nightly", feature(test))]

extern crate byteorder;
extern crate crc;
extern crate fnv;
extern crate ref_slice;
extern crate twox_hash;

#[macro_use]
extern crate error_chain;

#[macro_use]
extern crate log;

#[cfg(feature = "security")]
extern crate rustls;
#[cfg(feature = "security")]
extern crate webpki;

#[cfg(feature = "gzip")]
extern crate flate2;

#[cfg(feature = "snappy")]
extern crate snap;

#[cfg(all(test, feature = "nightly"))]
extern crate test;

pub mod client;
mod codecs;
mod compression;
pub mod consumer;
pub mod error;
pub mod producer;
mod protocol;
mod utils;

pub use self::error::{Error, Result};
