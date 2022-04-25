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

#[macro_use]
extern crate error_chain;

#[macro_use]
extern crate log;

#[cfg(feature = "snappy")]
extern crate snap;

#[cfg(feature = "zstandard")]
extern crate zstd;

#[cfg(all(test, feature = "nightly"))]
extern crate test;

pub mod client;
mod client_internals;
mod codecs;
pub mod consumer;
pub mod error;
pub mod producer;
mod protocol;

pub mod compression;
mod utils;

pub use self::error::{Error, Result};
