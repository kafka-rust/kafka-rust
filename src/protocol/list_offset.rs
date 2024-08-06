use std::io::{Read, Write};

use crate::codecs::{FromByte, ToByte};
use crate::error::{KafkaCode, Result};
use crate::utils::TimestampedPartitionOffset;

use super::{HeaderRequest, HeaderResponse, API_KEY_OFFSET};

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ListOffsetVersion {
    // currently only support 1
    V1 = 1,
}


/// https://kafka.apache.org/protocol.html#The_Messages_ListOffsets
#[derive(Debug)]
pub struct ListOffsetsRequest<'a> {
    pub header: HeaderRequest<'a>,
    pub replica: i32,
    pub topics: Vec<TopicListOffsetsRequest<'a>>,
}

#[derive(Debug)]
pub struct TopicListOffsetsRequest<'a> {
    pub topic: &'a str,
    pub partitions: Vec<PartitionListOffsetsRequest>,
}

#[derive(Default, Debug)]
pub struct PartitionListOffsetsRequest {
    pub partition: i32,
    pub time: i64,
}

impl<'a> ListOffsetsRequest<'a> {
    pub fn new(
        correlation_id: i32,
        version: ListOffsetVersion,
        client_id: &'a str,
    ) -> ListOffsetsRequest<'a> {
        ListOffsetsRequest {
            header: HeaderRequest::new(API_KEY_OFFSET, version as i16, correlation_id, client_id),
            replica: -1,
            topics: vec![],
        }
    }

    pub fn add(&mut self, topic: &'a str, partition: i32, time: i64) {
        for tp in &mut self.topics {
            if tp.topic == topic {
                tp.add(partition, time);
                return;
            }
        }
        let mut tp = TopicListOffsetsRequest::new(topic);
        tp.add(partition, time);
        self.topics.push(tp);
    }
}

impl<'a> TopicListOffsetsRequest<'a> {
    fn new(topic: &'a str) -> TopicListOffsetsRequest<'a> {
        TopicListOffsetsRequest {
            topic,
            partitions: vec![],
        }
    }
    fn add(&mut self, partition: i32, time: i64) {
        self.partitions
            .push(PartitionListOffsetsRequest::new(partition, time));
    }
}

impl PartitionListOffsetsRequest {
    fn new(partition: i32, time: i64) -> PartitionListOffsetsRequest {
        PartitionListOffsetsRequest { partition, time }
    }
}

impl<'a> ToByte for ListOffsetsRequest<'a> {
    fn encode<T: Write>(&self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.header.encode(buffer),
            self.replica.encode(buffer),
            self.topics.encode(buffer)
        )
    }
}

impl<'a> ToByte for TopicListOffsetsRequest<'a> {
    fn encode<T: Write>(&self, buffer: &mut T) -> Result<()> {
        try_multi!(self.topic.encode(buffer), self.partitions.encode(buffer))
    }
}

impl ToByte for PartitionListOffsetsRequest {
    fn encode<T: Write>(&self, buffer: &mut T) -> Result<()> {
        try_multi!(self.partition.encode(buffer), self.time.encode(buffer))
    }
}

// -------------------------------------

#[derive(Default, Debug)]
pub struct ListOffsetsResponse {
    pub header: HeaderResponse,
    pub topics: Vec<TopicListOffsetsResponse>,
}

#[derive(Default, Debug)]
pub struct TopicListOffsetsResponse {
    pub topic: String,
    pub partitions: Vec<TimestampedPartitionOffsetListOffsetsResponse>,
}

#[derive(Default, Debug)]
pub struct TimestampedPartitionOffsetListOffsetsResponse {
    pub partition: i32,
    pub error_code: i16,
    pub timestamp: i64,
    pub offset: i64,
}

impl TimestampedPartitionOffsetListOffsetsResponse {
    pub fn to_offset(&self) -> std::result::Result<TimestampedPartitionOffset, KafkaCode> {
        match KafkaCode::from_protocol(self.error_code) {
            Some(code) => Err(code),
            None => Ok(TimestampedPartitionOffset {
                partition: self.partition,
                offset: self.offset,
                time: self.timestamp,
            }),
        }
    }
}

impl FromByte for ListOffsetsResponse {
    type R = ListOffsetsResponse;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try_multi!(self.header.decode(buffer), self.topics.decode(buffer))
    }
}

impl FromByte for TopicListOffsetsResponse {
    type R = TopicListOffsetsResponse;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try_multi!(self.topic.decode(buffer), self.partitions.decode(buffer))
    }
}

impl FromByte for TimestampedPartitionOffsetListOffsetsResponse {
    type R = TimestampedPartitionOffsetListOffsetsResponse;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.partition.decode(buffer),
            self.error_code.decode(buffer),
            self.timestamp.decode(buffer),
            self.offset.decode(buffer)
        )
    }
}
