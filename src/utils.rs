use error::Error;
use num::traits::FromPrimitive;

#[derive(Clone, Debug)]
pub struct OffsetMessage {
    pub offset: i64,
    pub message: Vec<u8>
}

#[derive(Debug)]
pub struct TopicMessage {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub error: Option<Error>,
    pub message: Vec<u8>
}

impl Clone for TopicMessage {
    fn clone(&self) -> TopicMessage {
        TopicMessage{
            topic: self.topic.clone(),
            partition: self.partition.clone(),
            offset: self.offset.clone(),
            error: Error::from_i16(0),
            message: self.message.clone()
        }
    }
}


#[derive(Clone, Debug)]
pub struct ProduceMessage {
    pub topic: String,
    pub message: Vec<u8>
}


#[derive(Debug)]
pub struct PartitionOffset {
    pub partition: i32,
    pub offset: i64
}

#[derive(Debug)]
pub struct TopicPartitionOffset {
    pub topic: String,
    pub partition: i32,
    pub offset: i64
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
