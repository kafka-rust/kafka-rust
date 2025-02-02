use std::io::{Read, Write};

use crate::codecs::{self, FromByte, ToByte};
use crate::error::{self, Error, KafkaCode, Result};
use crate::utils::PartitionOffset;

use super::{HeaderRequest, HeaderResponse};
use super::{API_KEY_GROUP_COORDINATOR, API_KEY_OFFSET_COMMIT, API_KEY_OFFSET_FETCH, API_VERSION};

// --------------------------------------------------------------------

#[derive(Debug)]
pub struct GroupCoordinatorRequest<'a, 'b> {
    pub header: HeaderRequest<'a>,
    pub group: &'b str,
}

impl<'a, 'b> GroupCoordinatorRequest<'a, 'b> {
    pub fn new(
        group: &'b str,
        correlation_id: i32,
        client_id: &'a str,
    ) -> GroupCoordinatorRequest<'a, 'b> {
        GroupCoordinatorRequest {
            header: HeaderRequest::new(
                API_KEY_GROUP_COORDINATOR,
                API_VERSION,
                correlation_id,
                client_id,
            ),
            group,
        }
    }
}

impl<'a, 'b> ToByte for GroupCoordinatorRequest<'a, 'b> {
    fn encode<W: Write>(&self, buffer: &mut W) -> Result<()> {
        try_multi!(self.header.encode(buffer), self.group.encode(buffer))
    }
}

#[derive(Debug, Default)]
pub struct GroupCoordinatorResponse {
    pub header: HeaderResponse,
    pub error: i16,
    pub broker_id: i32,
    pub port: i32,
    pub host: String,
}

impl GroupCoordinatorResponse {
    pub fn into_result(self) -> Result<Self> {
        match Error::from_protocol(self.error) {
            Some(e) => Err(e),
            None => Ok(self),
        }
    }
}

impl FromByte for GroupCoordinatorResponse {
    type R = GroupCoordinatorResponse;

    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.header.decode(buffer),
            self.error.decode(buffer),
            self.broker_id.decode(buffer),
            self.host.decode(buffer),
            self.port.decode(buffer)
        )
    }
}

// --------------------------------------------------------------------

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum OffsetFetchVersion {
    /// causes the retrieval of the offsets from zookeeper
    V0 = 0,
    /// supported as of kafka 0.8.2, causes the retrieval of the
    /// offsets from kafka itself
    V1 = 1,
}

#[derive(Debug)]
pub struct OffsetFetchRequest<'a, 'b, 'c> {
    pub header: HeaderRequest<'a>,
    pub group: &'b str,
    pub topic_partitions: Vec<TopicPartitionOffsetFetchRequest<'c>>,
}

#[derive(Debug)]
pub struct TopicPartitionOffsetFetchRequest<'a> {
    pub topic: &'a str,
    pub partitions: Vec<PartitionOffsetFetchRequest>,
}

#[derive(Debug)]
pub struct PartitionOffsetFetchRequest {
    pub partition: i32,
}

impl<'a, 'b, 'c> OffsetFetchRequest<'a, 'b, 'c> {
    pub fn new(
        group: &'b str,
        version: OffsetFetchVersion,
        correlation_id: i32,
        client_id: &'a str,
    ) -> OffsetFetchRequest<'a, 'b, 'c> {
        OffsetFetchRequest {
            header: HeaderRequest::new(
                API_KEY_OFFSET_FETCH,
                version as i16,
                correlation_id,
                client_id,
            ),
            group,
            topic_partitions: vec![],
        }
    }

    pub fn add(&mut self, topic: &'c str, partition: i32) {
        for tp in &mut self.topic_partitions {
            if tp.topic == topic {
                tp.add(partition);
                return;
            }
        }
        let mut tp = TopicPartitionOffsetFetchRequest::new(topic);
        tp.add(partition);
        self.topic_partitions.push(tp);
    }
}

impl<'a> TopicPartitionOffsetFetchRequest<'a> {
    pub fn new(topic: &'a str) -> TopicPartitionOffsetFetchRequest<'a> {
        TopicPartitionOffsetFetchRequest {
            topic,
            partitions: vec![],
        }
    }

    pub fn add(&mut self, partition: i32) {
        self.partitions
            .push(PartitionOffsetFetchRequest::new(partition));
    }
}

impl PartitionOffsetFetchRequest {
    pub fn new(partition: i32) -> PartitionOffsetFetchRequest {
        PartitionOffsetFetchRequest { partition }
    }
}

impl<'a, 'b, 'c> ToByte for OffsetFetchRequest<'a, 'b, 'c> {
    fn encode<W: Write>(&self, buffer: &mut W) -> Result<()> {
        try_multi!(
            self.header.encode(buffer),
            self.group.encode(buffer),
            self.topic_partitions.encode(buffer)
        )
    }
}

impl<'a> ToByte for TopicPartitionOffsetFetchRequest<'a> {
    fn encode<W: Write>(&self, buffer: &mut W) -> Result<()> {
        try_multi!(self.topic.encode(buffer), self.partitions.encode(buffer))
    }
}

impl ToByte for PartitionOffsetFetchRequest {
    fn encode<W: Write>(&self, buffer: &mut W) -> Result<()> {
        self.partition.encode(buffer)
    }
}

// --------------------------------------------------------------------

#[derive(Default, Debug)]
pub struct OffsetFetchResponse {
    pub header: HeaderResponse,
    pub topic_partitions: Vec<TopicPartitionOffsetFetchResponse>,
}

#[derive(Default, Debug)]
pub struct TopicPartitionOffsetFetchResponse {
    pub topic: String,
    pub partitions: Vec<PartitionOffsetFetchResponse>,
}

#[derive(Default, Debug)]
pub struct PartitionOffsetFetchResponse {
    pub partition: i32,
    pub offset: i64,
    pub metadata: String,
    pub error: i16,
}

impl PartitionOffsetFetchResponse {
    pub fn get_offsets(&self) -> Result<PartitionOffset> {
        match Error::from_protocol(self.error) {
            Some(Error::Kafka(KafkaCode::UnknownTopicOrPartition)) => {
                // ~ occurs only on protocol v0 when no offset available
                // for the group in question; we'll align the behavior
                // with protocol v1.
                Ok(PartitionOffset {
                    partition: self.partition,
                    offset: -1,
                })
            }
            Some(e) => Err(e),
            None => Ok(PartitionOffset {
                partition: self.partition,
                offset: self.offset,
            }),
        }
    }
}

impl FromByte for OffsetFetchResponse {
    type R = OffsetFetchResponse;

    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.header.decode(buffer),
            self.topic_partitions.decode(buffer)
        )
    }
}

impl FromByte for TopicPartitionOffsetFetchResponse {
    type R = TopicPartitionOffsetFetchResponse;

    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try_multi!(self.topic.decode(buffer), self.partitions.decode(buffer))
    }
}

impl FromByte for PartitionOffsetFetchResponse {
    type R = PartitionOffsetFetchResponse;

    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.partition.decode(buffer),
            self.offset.decode(buffer),
            self.metadata.decode(buffer),
            self.error.decode(buffer)
        )
    }
}

// --------------------------------------------------------------------

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum OffsetCommitVersion {
    /// causes offset to be stored in zookeeper
    V0 = 0,
    /// supported as of kafka 0.8.2, causes offsets to be stored
    /// directly in kafka
    V1 = 1,
    /// supported as of kafka 0.9.0, causes offsets to be stored
    /// directly in kafka
    V2 = 2,
}

impl OffsetCommitVersion {
    fn from_protocol(n: i16) -> Self {
        match n {
            0 => Self::V0,
            1 => Self::V1,
            2 => Self::V2,
            _ => panic!("Unknown offset commit version code: {n}"),
        }
    }
}

#[derive(Debug)]
pub struct OffsetCommitRequest<'a, 'b> {
    pub header: HeaderRequest<'a>,
    pub group: &'b str,
    pub topic_partitions: Vec<TopicPartitionOffsetCommitRequest<'b>>,
}

#[derive(Debug)]
pub struct TopicPartitionOffsetCommitRequest<'a> {
    pub topic: &'a str,
    pub partitions: Vec<PartitionOffsetCommitRequest<'a>>,
}

#[derive(Debug)]
pub struct PartitionOffsetCommitRequest<'a> {
    pub partition: i32,
    pub offset: i64,
    pub metadata: &'a str,
}

impl<'a, 'b> OffsetCommitRequest<'a, 'b> {
    pub fn new(
        group: &'b str,
        version: OffsetCommitVersion,
        correlation_id: i32,
        client_id: &'a str,
    ) -> OffsetCommitRequest<'a, 'b> {
        OffsetCommitRequest {
            header: HeaderRequest::new(
                API_KEY_OFFSET_COMMIT,
                version as i16,
                correlation_id,
                client_id,
            ),
            group,
            topic_partitions: vec![],
        }
    }

    pub fn add(&mut self, topic: &'b str, partition: i32, offset: i64, metadata: &'b str) {
        for tp in &mut self.topic_partitions {
            if tp.topic == topic {
                tp.add(partition, offset, metadata);
                return;
            }
        }
        let mut tp = TopicPartitionOffsetCommitRequest::new(topic);
        tp.add(partition, offset, metadata);
        self.topic_partitions.push(tp);
    }
}

impl<'a> TopicPartitionOffsetCommitRequest<'a> {
    pub fn new(topic: &'a str) -> TopicPartitionOffsetCommitRequest<'a> {
        TopicPartitionOffsetCommitRequest {
            topic,
            partitions: vec![],
        }
    }

    pub fn add(&mut self, partition: i32, offset: i64, metadata: &'a str) {
        self.partitions.push(PartitionOffsetCommitRequest::new(
            partition, offset, metadata,
        ));
    }
}

impl<'a> PartitionOffsetCommitRequest<'a> {
    pub fn new(partition: i32, offset: i64, metadata: &'a str) -> PartitionOffsetCommitRequest<'a> {
        PartitionOffsetCommitRequest {
            partition,
            offset,
            metadata,
        }
    }
}

impl<'a, 'b> ToByte for OffsetCommitRequest<'a, 'b> {
    fn encode<W: Write>(&self, buffer: &mut W) -> Result<()> {
        let v = OffsetCommitVersion::from_protocol(self.header.api_version);
        self.header.encode(buffer)?;
        self.group.encode(buffer)?;
        match v {
            OffsetCommitVersion::V1 => {
                (-1i32).encode(buffer)?;
                "".encode(buffer)?;
            }
            OffsetCommitVersion::V2 => {
                (-1i32).encode(buffer)?;
                "".encode(buffer)?;
                (-1i64).encode(buffer)?;
            }
            OffsetCommitVersion::V0 => {
                // nothing to do
            }
        }
        codecs::encode_as_array(buffer, &self.topic_partitions, |buffer, tp| {
            try_multi!(
                tp.topic.encode(buffer),
                codecs::encode_as_array(buffer, &tp.partitions, |buffer, p| {
                    p.partition.encode(buffer)?;
                    p.offset.encode(buffer)?;
                    if v == OffsetCommitVersion::V1 {
                        (-1i64).encode(buffer)?;
                    }
                    p.metadata.encode(buffer)
                })
            )
        })
    }
}

// --------------------------------------------------------------------

#[derive(Default, Debug)]
pub struct OffsetCommitResponse {
    pub header: HeaderResponse,
    pub topic_partitions: Vec<TopicPartitionOffsetCommitResponse>,
}

impl FromByte for OffsetCommitResponse {
    type R = OffsetCommitResponse;

    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.header.decode(buffer),
            self.topic_partitions.decode(buffer)
        )
    }
}

#[derive(Default, Debug)]
pub struct TopicPartitionOffsetCommitResponse {
    pub topic: String,
    pub partitions: Vec<PartitionOffsetCommitResponse>,
}

impl FromByte for TopicPartitionOffsetCommitResponse {
    type R = TopicPartitionOffsetCommitResponse;

    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try_multi!(self.topic.decode(buffer), self.partitions.decode(buffer))
    }
}

#[derive(Default, Debug)]
pub struct PartitionOffsetCommitResponse {
    pub partition: i32,
    pub error: i16,
}

impl PartitionOffsetCommitResponse {
    pub fn to_error(&self) -> Option<error::KafkaCode> {
        error::KafkaCode::from_protocol(self.error)
    }
}

impl FromByte for PartitionOffsetCommitResponse {
    type R = PartitionOffsetCommitResponse;

    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try_multi!(self.partition.decode(buffer), self.error.decode(buffer))
    }
}
