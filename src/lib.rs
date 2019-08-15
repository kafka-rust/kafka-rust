//! Clients for comunicating with a [Kafka](http://kafka.apache.org/)
//! cluster.  These are:
//!
//! - `kafka::producer::Producer` - for sending message to Kafka
//! - `kafka::consumer::Consumer` - for retrieving/consuming messages from Kafka
//! - `kafka::client::KafkaClient` - a lower-level, general purpose client leaving
//!   you with more power but also more resposibility
//!
//! See module level documentation corresponding to each client individually.
#![recursion_limit="128"]
#![cfg_attr(feature = "nightly", feature(test))]
#![feature(async_await)]


#[macro_use]
extern crate error_chain;

#[macro_use]
extern crate log;

pub mod error;
pub mod client;
mod client_internals;
pub mod consumer;
pub mod producer;
mod utils;
mod codecs;
mod protocol;
mod compression;

pub use self::error::{Error, Result};
