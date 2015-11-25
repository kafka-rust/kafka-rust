use std::io::{Read, Write};

use num::traits::FromPrimitive;

use codecs::{ToByte, FromByte};
use error::{Error, Result};
use utils::TopicPartitionOffsetError;

use super::{HeaderRequest, HeaderResponse};
use super::{API_KEY_OFFSET_FETCH, API_KEY_OFFSET_COMMIT, API_VERSION};

// #[derive(Debug)]
// pub struct ConsumerMetadataRequest<'a> {
//     pub header: HeaderRequest<'a>,
//     pub group: &'a str,
// }

// impl<'a> ToByte for ConsumerMetadataRequest<'a> {
//     fn encode<T: Write>(&self, buffer: &mut T) -> Result<()> {
//         try_multi!(
//             self.header.encode(buffer),
//             self.group.encode(buffer)
//         )
//     }
// }

// // --------------------------------------------------------------------

// #[derive(Default, Debug)]
// pub struct ConsumerMetadataResponse {
//     pub header: HeaderResponse,
//     pub error: i16,
//     pub id: i32,
//     pub host: String,
//     pub port: i32
// }

// impl FromByte for ConsumerMetadataResponse {
//     type R = ConsumerMetadataResponse;

//     #[allow(unused_must_use)]
//     fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
//         try_multi!(
//             self.header.decode(buffer),
//             self.error.decode(buffer),
//             self.id.decode(buffer),
//             self.host.decode(buffer),
//             self.port.decode(buffer)
//         )
//     }
// }

// --------------------------------------------------------------------

#[derive(Debug)]
pub struct OffsetFetchRequest<'a, 'b, 'c> {
    pub header: HeaderRequest<'a>,
    pub group: &'b str,
    pub topic_partitions: Vec<TopicPartitionOffsetFetchRequest<'c>>
}

#[derive(Debug)]
pub struct TopicPartitionOffsetFetchRequest<'a> {
    pub topic: &'a str,
    pub partitions: Vec<PartitionOffsetFetchRequest>
}

#[derive(Debug)]
pub struct PartitionOffsetFetchRequest {
    pub partition: i32
}

impl<'a, 'b, 'c> OffsetFetchRequest<'a, 'b, 'c> {
    pub fn new(group: &'b str, correlation_id: i32, client_id: &'a str) -> OffsetFetchRequest<'a, 'b, 'c> {
        OffsetFetchRequest {
            header: HeaderRequest::new(
                API_KEY_OFFSET_FETCH, API_VERSION, correlation_id, client_id),
            group: group,
            topic_partitions: vec!()
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
        TopicPartitionOffsetFetchRequest{topic: topic, partitions: vec!()}
    }

    pub fn add(&mut self, partition: i32) {
        self.partitions.push(PartitionOffsetFetchRequest::new(partition));
    }
}

impl PartitionOffsetFetchRequest {
    pub fn new(partition: i32) -> PartitionOffsetFetchRequest {
        PartitionOffsetFetchRequest{partition: partition}
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
        try_multi!(
            self.topic.encode(buffer),
            self.partitions.encode(buffer)
        )
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
    pub topic_partitions: Vec<TopicPartitionOffsetFetchResponse>
}

#[derive(Default, Debug)]
pub struct TopicPartitionOffsetFetchResponse {
    pub topic: String,
    pub partitions: Vec<PartitionOffsetFetchResponse>
}

#[derive(Default, Debug)]
pub struct PartitionOffsetFetchResponse {
    pub partition: i32,
    pub offset: i64,
    pub metadata: String,
    pub error: i16
}

impl OffsetFetchResponse {
    pub fn get_offsets(&self) -> Vec<TopicPartitionOffsetError> {
        self.topic_partitions
            .iter()
            .flat_map(|ref tp| tp.get_offsets(tp.topic.clone()))
            .collect()
    }
}

impl TopicPartitionOffsetFetchResponse {
    pub fn get_offsets(&self, topic: String) -> Vec<TopicPartitionOffsetError>{
        self.partitions
            .iter()
            .map(|ref p| p.get_offsets(topic.clone()))
            .collect()
    }
}

impl PartitionOffsetFetchResponse {
    pub fn get_offsets(&self, topic: String) -> TopicPartitionOffsetError{
        TopicPartitionOffsetError{
            topic: topic,
            partition: self.partition,
            offset: self.offset,
            error: Error::from_i16(self.error)
        }
    }
}

impl FromByte for OffsetFetchResponse {
    type R = OffsetFetchResponse;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.header.decode(buffer),
            self.topic_partitions.decode(buffer)
        )
    }
}

impl FromByte for TopicPartitionOffsetFetchResponse {
    type R = TopicPartitionOffsetFetchResponse;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.topic.decode(buffer),
            self.partitions.decode(buffer)
        )
    }
}

impl FromByte for PartitionOffsetFetchResponse {
    type R = PartitionOffsetFetchResponse;

    #[allow(unused_must_use)]
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

#[derive(Debug)]
pub struct OffsetCommitRequest<'a, 'b> {
    pub header: HeaderRequest<'a>,
    pub group: &'b str,
    pub topic_partitions: Vec<TopicPartitionOffsetCommitRequest<'b>>
}

#[derive(Debug)]
pub struct TopicPartitionOffsetCommitRequest<'a> {
    pub topic: &'a str,
    pub partitions: Vec<PartitionOffsetCommitRequest<'a>>
}

#[derive(Debug)]
pub struct PartitionOffsetCommitRequest<'a> {
    pub partition: i32,
    pub offset: i64,
    pub metadata: &'a str,
}

impl<'a, 'b> OffsetCommitRequest<'a, 'b> {
    pub fn new(group: &'b str, correlation_id: i32, client_id: &'a str) -> OffsetCommitRequest<'a, 'b> {
        OffsetCommitRequest{
            header: HeaderRequest::new(
                API_KEY_OFFSET_COMMIT, API_VERSION, correlation_id, client_id),
            group: group,
            topic_partitions: vec!()
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
            topic: topic,
            partitions: vec!()
        }
    }

    pub fn add(&mut self, partition: i32, offset: i64, metadata: &'a str) {
        self.partitions.push(PartitionOffsetCommitRequest::new(partition, offset, metadata))
    }
}

impl<'a> PartitionOffsetCommitRequest<'a> {
    pub fn new(partition: i32, offset: i64, metadata: &'a str) -> PartitionOffsetCommitRequest<'a> {
        PartitionOffsetCommitRequest{
            partition: partition,
            offset: offset,
            metadata: metadata
        }
    }
}

impl<'a, 'b> ToByte for OffsetCommitRequest<'a, 'b> {
    fn encode<W: Write>(&self, buffer: &mut W) -> Result<()> {
        try_multi!(
            self.header.encode(buffer),
            self.group.encode(buffer),
            self.topic_partitions.encode(buffer)
        )
    }
}

impl<'a> ToByte for TopicPartitionOffsetCommitRequest<'a> {
    fn encode<W: Write>(&self, buffer: &mut W) -> Result<()> {
        try_multi!(
            self.topic.encode(buffer),
            self.partitions.encode(buffer)
        )
    }
}

impl<'a> ToByte for PartitionOffsetCommitRequest<'a> {
    fn encode<W: Write>(&self, buffer: &mut W) -> Result<()> {
        try_multi!(
            self.partition.encode(buffer),
            self.offset.encode(buffer),
            self.metadata.encode(buffer)
        )
    }
}


// --------------------------------------------------------------------

#[derive(Default, Debug)]
pub struct OffsetCommitResponse {
    pub header: HeaderResponse,
    pub topic_partitions: Vec<TopicPartitionOffsetCommitResponse>
}

#[derive(Default, Debug)]
pub struct TopicPartitionOffsetCommitResponse {
    pub topic: String,
    pub partitions: Vec<PartitionOffsetCommitResponse>
}

#[derive(Default, Debug)]
pub struct PartitionOffsetCommitResponse {
    pub partition: i32,
    pub error: i16
}

impl FromByte for OffsetCommitResponse {
    type R = OffsetCommitResponse;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.header.decode(buffer),
            self.topic_partitions.decode(buffer)
        )
    }
}

impl FromByte for TopicPartitionOffsetCommitResponse {
    type R = TopicPartitionOffsetCommitResponse;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.topic.decode(buffer),
            self.partitions.decode(buffer)
        )
    }
}

impl FromByte for PartitionOffsetCommitResponse {
    type R = PartitionOffsetCommitResponse;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.partition.decode(buffer),
            self.error.decode(buffer)
        )
    }
}
