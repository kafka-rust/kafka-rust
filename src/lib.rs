//! Clients for comunicating with a [Kafka](http://kafka.apache.org/)
//! cluster.  These are:
//!
//! - `kafka::producer::Producer` - for sending message to Kafka
//! - `kafka::consumer::Consumer` - for retrieving/consuming messages from Kafka
//! - `kafka::client::KafkaClient` - a lower-level, general purpose client leaving
//!   you with more power but also more resposibility
//!
//! See module level documentation corresponding to each client individually.

#![cfg_attr(feature = "nightly", feature(test))]

extern crate byteorder;
extern crate crc;
extern crate ref_slice;
extern crate fnv;

#[macro_use]
extern crate log;

#[cfg(feature = "security")]
extern crate openssl;

#[cfg(feature = "gzip")]
extern crate flate2;

#[cfg(feature = "snappy")]
extern crate snappy;

#[cfg(all(test, feature = "nightly"))]
extern crate test;

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

trait KafkaClientInternals {
    fn internal_produce_messages<'a, 'b, I, J>(&mut self,
                                               ack: client::RequiredAcks,
                                               ack_timeout: i32,
                                               messages: I)
                                               -> Result<Vec<client::TopicPartitionOffset>>
        where J: AsRef<client::ProduceMessage<'a, 'b>>,
              I: IntoIterator<Item = J>;
}
