//! Some utility structures

use error::{Result,Error};

// XXX move to protocol module
#[derive(Clone, Debug)]
pub struct OffsetMessage {
    pub offset: i64,
    pub message: Vec<u8>
}

#[derive(Clone, Debug)]
pub struct TopicMessage {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub error: Option<Error>,
    pub message: Vec<u8>
}

#[derive(Clone, Debug)]
pub struct ProduceMessage {
    pub topic: String,
    pub message: Vec<u8>
}

#[derive(Debug)]
pub struct PartitionOffset {
    pub partition: i32,
    pub offset: Result<i64>,
}

#[derive(Debug)]
pub struct TopicPartitionOffset<'a> {
    pub topic: &'a str,
    pub partition: i32,
    pub offset: i64
}

impl<'a> TopicPartitionOffset<'a> {
    pub fn new(topic: &'a str, partition: i32, offset: i64) -> TopicPartitionOffset<'a> {
        TopicPartitionOffset {
            topic: topic,
            partition: partition,
            offset: offset,
        }
    }
}

impl<'a> AsRef<TopicPartitionOffset<'a>> for TopicPartitionOffset<'a> {
    fn as_ref(&self) -> &Self {
        &self
    }
}

#[derive(Debug)]
pub struct TopicPartitionOffsetError {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub error: Option<Error>
}

#[derive(Debug)]
pub struct TopicPartitions {
    pub topic: String,
    pub partitions: Vec<i32>
}

#[derive(Debug)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: i32
}
