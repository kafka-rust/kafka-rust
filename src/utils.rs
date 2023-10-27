//! Some utility structures
//!
//! This module is _not_ exposed to the public directly.

/// A retrieved offset for a particular partition in the context of an
/// already known topic.
#[derive(Debug, Hash, PartialEq, Eq)]
pub struct PartitionOffset {
    pub offset: i64,
    pub partition: i32,
}

/// A retrieved offset for a particular partition in the context of an
/// already known topic, specific to a timestamp.
#[derive(Debug, Hash, PartialEq, Eq)]
pub struct TimestampedPartitionOffset {
    pub offset: i64,
    pub partition: i32,
    pub time: i64
}
