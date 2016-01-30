//! Some utility structures

use error::{Result,Error};

// XXX move to kafka::client module
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
    pub offset: Result<i64>,
    pub partition: i32,
}

#[derive(Debug)]
pub struct TopicPartitionOffset<'a> {
    pub topic: &'a str,
    pub offset: i64,
    pub partition: i32,
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
pub struct TopicPartition<'a> {
    pub topic: &'a str,
    pub partition: i32
}

impl<'a> TopicPartition<'a> {
    #[inline]
    pub fn new(topic: &str, partition: i32) -> TopicPartition {
        TopicPartition { topic: topic, partition: partition }
    }
}

impl<'a> AsRef<TopicPartition<'a>> for TopicPartition<'a> {
    fn as_ref(&self) -> &Self {
        &self
    }
}
