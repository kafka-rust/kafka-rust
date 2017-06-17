use std::io::{Read, Write};

use error::Result;
use codecs::{AsStrings, ToByte, FromByte};

use super::{HeaderRequest, HeaderResponse};
use super::{API_KEY_METADATA, API_VERSION};

#[derive(Debug)]
pub struct MetadataRequest<'a, T: 'a> {
    pub header: HeaderRequest<'a>,
    pub topics: &'a [T],
}

impl<'a, T: AsRef<str>> MetadataRequest<'a, T> {
    pub fn new(correlation_id: i32, client_id: &'a str, topics: &'a [T]) -> MetadataRequest<'a, T> {
        MetadataRequest {
            header: HeaderRequest::new(API_KEY_METADATA, API_VERSION, correlation_id, client_id),
            topics: topics,
        }
    }
}

impl<'a, T: AsRef<str> + 'a> ToByte for MetadataRequest<'a, T> {
    fn encode<W: Write>(&self, buffer: &mut W) -> Result<()> {
        try_multi!(self.header.encode(buffer), AsStrings(self.topics).encode(buffer))
    }
}

// --------------------------------------------------------------------

#[derive(Default, Debug)]
pub struct MetadataResponse {
    pub header: HeaderResponse,
    pub brokers: Vec<BrokerMetadata>,
    pub topics: Vec<TopicMetadata>,
}

#[derive(Default, Debug)]
pub struct BrokerMetadata {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
}

#[derive(Default, Debug)]
pub struct TopicMetadata {
    pub error: i16,
    pub topic: String,
    pub partitions: Vec<PartitionMetadata>,
}

#[derive(Default, Debug)]
pub struct PartitionMetadata {
    pub error: i16,
    pub id: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
}

impl FromByte for MetadataResponse {
    type R = MetadataResponse;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.header.decode(buffer),
            self.brokers.decode(buffer),
            self.topics.decode(buffer)
        )
    }
}

impl FromByte for BrokerMetadata {
    type R = BrokerMetadata;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try_multi!(self.node_id.decode(buffer), self.host.decode(buffer), self.port.decode(buffer))
    }
}

impl FromByte for TopicMetadata {
    type R = TopicMetadata;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.error.decode(buffer),
            self.topic.decode(buffer),
            self.partitions.decode(buffer)
        )
    }
}

impl FromByte for PartitionMetadata {
    type R = PartitionMetadata;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.error.decode(buffer),
            self.id.decode(buffer),
            self.leader.decode(buffer),
            self.replicas.decode(buffer),
            self.isr.decode(buffer)
        )
    }
}
