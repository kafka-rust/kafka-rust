//! Some utility structures

use error::{Result,Error};

#[derive(Debug)]
pub struct PartitionOffset {
    pub offset: Result<i64>,
    pub partition: i32,
}

// XXX rework to `offset: Result<i64>`, similar to `PartitionOffset`
#[derive(Debug)]
pub struct TopicPartitionOffsetError {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub error: Option<Error>
}
