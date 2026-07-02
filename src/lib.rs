//! Clients for communicating with a [Kafka](http://kafka.apache.org/)
//! cluster.  These are:
//!
//! - `kafka::producer::Producer` - for sending message to Kafka
//! - `kafka::consumer::Consumer` - for retrieving/consuming messages from Kafka
//! - `kafka::client::KafkaClient` - a lower-level, general purpose client leaving
//!   you with more power but also more responsibility
//!
//! See module level documentation corresponding to each client individually.
#![recursion_limit = "128"]
#![deny(clippy::all)]
#![warn(clippy::pedantic)]
#![allow(deprecated)]
// Allow deprecated items (e.g., OpenSSL backend)
// Allow pedantic lints that would require extensive refactoring
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::cast_lossless)]
#![allow(clippy::used_underscore_binding)]
#![allow(clippy::used_underscore_items)]
#![allow(clippy::missing_transmute_annotations)]
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::struct_field_names)]
#![allow(clippy::needless_pass_by_value)] // Already fixed for new code
#![allow(clippy::similar_names)] // Already fixed for new code
#![allow(clippy::doc_lazy_continuation)]
#![allow(clippy::match_like_matches_macro)]
#![allow(clippy::default_trait_access)]
#![allow(clippy::manual_let_else)]
#![allow(clippy::manual_assert)]
#![allow(clippy::legacy_numeric_constants)]
#![allow(clippy::question_mark)]

pub mod client;
mod client_internals;
mod codecs;
mod compression;
pub mod consumer;
pub mod error;
pub mod producer;
mod protocol;
mod utils;

#[cfg(any(feature = "security-rustls", feature = "security-openssl"))]
mod tls;

pub use self::error::{Error, Result};
