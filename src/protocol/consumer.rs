use std::io::{Read, Write};
use std::rc::Rc;

use num::traits::FromPrimitive;

use codecs::{ToByte, FromByte};
use error::{Error, Result};
use utils::TopicPartitionOffsetError;

use super::{HeaderRequest, HeaderResponse};
use super::{OFFSET_FETCH_KEY, OFFSET_COMMIT_KEY, API_VERSION};

// #[derive(Debug)]
// pub struct ConsumerMetadataRequest {
//     pub header: HeaderRequest,
//     pub group: String
// }

// impl ToByte for ConsumerMetadataRequest {
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

#[derive(Default, Debug, Clone)]
pub struct OffsetFetchRequest {
    pub header: HeaderRequest,
    pub group: String,
    pub topic_partitions: Vec<TopicPartitionOffsetFetchRequest>
}

#[derive(Default, Debug, Clone)]
pub struct TopicPartitionOffsetFetchRequest {
    pub topic: String,
    pub partitions: Vec<PartitionOffsetFetchRequest>
}

#[derive(Default, Debug, Clone)]
pub struct PartitionOffsetFetchRequest {
    pub partition: i32
}

impl OffsetFetchRequest {
    pub fn new(group: String, correlation: i32, clientid: Rc<String>) -> OffsetFetchRequest {
        OffsetFetchRequest{
            header: HeaderRequest{key: OFFSET_FETCH_KEY, correlation: correlation,
                                  clientid: clientid, version: API_VERSION},
            group: group,
            topic_partitions: vec!()}
    }

    pub fn add(&mut self, topic: String, partition: i32) {
        for tp in &mut self.topic_partitions {
            if tp.topic == topic {
                tp.add(partition);
                return;
            }
        }
        let mut tp = TopicPartitionOffsetFetchRequest::new(topic.clone());
        tp.add(partition);
        self.topic_partitions.push(tp);
    }
}

impl TopicPartitionOffsetFetchRequest {
    pub fn new(topic: String) -> TopicPartitionOffsetFetchRequest {
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

impl ToByte for OffsetFetchRequest {
    fn encode<T:Write>(&self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.header.encode(buffer),
            self.group.encode(buffer),
            self.topic_partitions.encode(buffer)
        )
    }
}

impl ToByte for TopicPartitionOffsetFetchRequest {
    fn encode<T:Write>(&self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.topic.encode(buffer),
            self.partitions.encode(buffer)
        )
    }
}

impl ToByte for PartitionOffsetFetchRequest {
    fn encode<T:Write>(&self, buffer: &mut T) -> Result<()> {
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
pub struct OffsetCommitRequest {
    pub header: HeaderRequest,
    pub group: String,
    pub topic_partitions: Vec<TopicPartitionOffsetCommitRequest>
}

#[derive(Debug)]
pub struct TopicPartitionOffsetCommitRequest {
    pub topic: String,
    pub partitions: Vec<PartitionOffsetCommitRequest>
}

#[derive(Debug)]
pub struct PartitionOffsetCommitRequest {
    pub partition: i32,
    pub offset: i64,
    pub metadata: String
}

impl OffsetCommitRequest {
    pub fn new(group: String, correlation: i32, clientid: Rc<String>) -> OffsetCommitRequest {
        OffsetCommitRequest{
            header: HeaderRequest{key: OFFSET_COMMIT_KEY, correlation: correlation,
                                  clientid: clientid, version: API_VERSION},
            group: group,
            topic_partitions: vec!()
            }
    }

    pub fn add(&mut self, topic: String, partition: i32, offset: i64, metadata: String) {
        for tp in &mut self.topic_partitions {
            if tp.topic == topic {
                tp.add(partition, offset, metadata);
                return;
            }
        }
        let mut tp = TopicPartitionOffsetCommitRequest::new(topic.clone());
        tp.add(partition, offset, metadata);
        self.topic_partitions.push(tp);
    }
}

impl TopicPartitionOffsetCommitRequest {
    pub fn new(topic: String) -> TopicPartitionOffsetCommitRequest{
        TopicPartitionOffsetCommitRequest {
            topic: topic,
            partitions: vec!()
        }
    }

    pub fn add(&mut self, partition: i32, offset: i64, metadata: String) {
        self.partitions.push(PartitionOffsetCommitRequest::new(partition, offset, metadata))
    }
}

impl PartitionOffsetCommitRequest {
    pub fn new(partition: i32, offset: i64, metadata: String) -> PartitionOffsetCommitRequest {

        PartitionOffsetCommitRequest{
            partition: partition,
            offset: offset,
            metadata: metadata
        }
    }
}

impl ToByte for OffsetCommitRequest {
    fn encode<T:Write>(&self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.header.encode(buffer),
            self.group.encode(buffer),
            self.topic_partitions.encode(buffer)
        )
    }
}

impl ToByte for TopicPartitionOffsetCommitRequest {
    fn encode<T:Write>(&self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.topic.encode(buffer),
            self.partitions.encode(buffer)
        )
    }
}

impl ToByte for PartitionOffsetCommitRequest {
    fn encode<T:Write>(&self, buffer: &mut T) -> Result<()> {
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
