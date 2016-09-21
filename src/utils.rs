//! Some utility structures
//!
//! This module is _not_ exposed to the public directly.

/// A retrieved offset for a particular partition in the context of an
/// already known topic.
#[derive(Debug)]
pub struct PartitionOffset {
    pub offset: i64,
    pub partition: i32,
}
