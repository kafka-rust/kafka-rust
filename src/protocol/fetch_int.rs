//! A representation of the requests for fetching messages.  This code
//! is kept apart from the corresponding "responses" module, since the
//! latter is publicly exported by this crate but we don't want to
//! expose the "request" structures.

use error::Result;
use std::io::Write;

use codecs::ToByte;
use super::HeaderRequest;
use super::{API_KEY_FETCH, API_VERSION};


#[derive(Debug)]
pub struct FetchRequest<'a, 'b> {
    pub header: HeaderRequest<'a>,
    pub replica: i32,
    pub max_wait_time: i32,
    pub min_bytes: i32,
    pub topic_partitions: Vec<TopicPartitionFetchRequest<'b>>
}

#[derive(Debug)]
pub struct TopicPartitionFetchRequest<'a> {
    pub topic: &'a str,
    pub partitions: Vec<PartitionFetchRequest>
}

#[derive(Debug)]
pub struct PartitionFetchRequest {
    pub offset: i64,
    pub partition: i32,
    pub max_bytes: i32
}

impl<'a, 'b> FetchRequest<'a, 'b> {

    pub fn new(correlation_id: i32, client_id: &'a str, max_wait_time: i32, min_bytes: i32)
               -> FetchRequest<'a, 'b> {
        FetchRequest {
            header: HeaderRequest::new(
                API_KEY_FETCH, API_VERSION, correlation_id, client_id),
            replica: -1,
            max_wait_time: max_wait_time,
            min_bytes: min_bytes,
            topic_partitions: vec!()
        }
    }

    pub fn add(&mut self, topic: &'b str, partition: i32, offset: i64, max_bytes: i32) {
        for tp in &mut self.topic_partitions {
            if tp.topic == topic {
                tp.add(partition, offset, max_bytes);
                return;
            }
        }
        let mut tp = TopicPartitionFetchRequest::new(topic);
        tp.add(partition, offset, max_bytes);
        self.topic_partitions.push(tp);
    }
}

impl<'a> TopicPartitionFetchRequest<'a> {
    pub fn new(topic: &'a str) -> TopicPartitionFetchRequest<'a> {
        TopicPartitionFetchRequest {
            topic: topic,
            partitions: vec!()
        }
    }

    pub fn add(&mut self, partition: i32, offset: i64, max_bytes: i32) {
        self.partitions.push(PartitionFetchRequest:: new(partition, offset, max_bytes))
    }
}

impl PartitionFetchRequest {
    pub fn new(partition: i32, offset: i64, max_bytes: i32) -> PartitionFetchRequest {
        PartitionFetchRequest {
            partition: partition,
            offset: offset,
            max_bytes: max_bytes,
        }
    }
}

impl<'a, 'b> ToByte for FetchRequest<'a, 'b> {
    fn encode<W: Write>(&self, buffer: &mut W) -> Result<()> {
        try_multi!(
            self.header.encode(buffer),
            self.replica.encode(buffer),
            self.max_wait_time.encode(buffer),
            self.min_bytes.encode(buffer),
            self.topic_partitions.encode(buffer)
        )
    }
}

impl<'a> ToByte for TopicPartitionFetchRequest<'a> {
    fn encode<W: Write>(&self, buffer: &mut W) -> Result<()> {
        try_multi!(
            self.topic.encode(buffer),
            self.partitions.encode(buffer)
        )
    }
}

impl ToByte for PartitionFetchRequest {
    fn encode<T:Write>(&self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.partition.encode(buffer),
            self.offset.encode(buffer),
            self.max_bytes.encode(buffer)
        )
    }
}
