//! Some utility structures
//!
//! This module is _not_ exposed to the public directly.

use error::Result;

/// A retrieved offset for a particular partition in the context of an
/// already known topic.
#[derive(Debug)]
pub struct PartitionOffset {
    pub offset: Result<i64>,
    pub partition: i32,
}

/// A retrieved offset of a particular topic partition.
#[derive(Debug)]
pub struct TopicPartitionOffset {
    pub offset: Result<i64>,
    pub topic: String,
    pub partition: i32,
}
