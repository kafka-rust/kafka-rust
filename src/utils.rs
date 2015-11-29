//! Some utility structures

use error::{Result,Error};

#[derive(Clone, Debug)]
pub struct OffsetMessage {
    pub offset: i64,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct TopicMessage {
    pub topic: String,
    pub partition: i32,
    pub message: Result<OffsetMessage>,
}

#[derive(Debug)]
pub struct ProduceMessage<'a, 'b> {
    pub topic: &'a str,
    pub message: &'b [u8],
}

impl<'a, 'b> AsRef<ProduceMessage<'a, 'b>> for ProduceMessage<'a, 'b> {
    fn as_ref(&self) -> &Self {
        &self
    }
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
pub struct TopicPartition<'a> {
    pub topic: &'a str,
    pub partition: i32
}

impl<'a> AsRef<TopicPartition<'a>> for TopicPartition<'a> {
    fn as_ref(&self) -> &Self {
        &self
    }
}
