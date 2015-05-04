
use super::codecs::*;
use num::traits::ToPrimitive;
use byteorder::{ByteOrder, BigEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Read, Write};
use std::fmt;

const PRODUCE_KEY: i16 = 0;
const FETCH_KEY: i16 = 1;
const OFFSET_KEY: i16 = 2;
const METADATA_KEY: i16 = 3;
const OFFSET_COMMIT_KEY: i16 = 8;
const OFFSET_FETCH_KEY: i16 = 9;

const VERSION: i16 = 0;

#[derive(Default)]
#[derive(Debug)]
#[derive(Clone)]
pub struct HeaderRequest {
    pub key: i16,
    pub version: i16,
    pub correlation: i32,
    pub clientid: String
}

#[derive(Default)]
#[derive(Debug)]
#[derive(Clone)]
pub struct HeaderResponse {
    pub correlation: i32
}

#[derive(Default)]
#[derive(Debug)]
#[derive(Clone)]
pub struct MetadataRequest {
    pub header: HeaderRequest,
    pub topics: Vec<String>
}

#[derive(Default)]
#[derive(Debug)]
#[derive(Clone)]
pub struct MetadataResponse {
    pub header: HeaderResponse,
    pub brokers: Vec<BrokerMetadata>,
    pub topics: Vec<TopicMetadata>
}

#[derive(Default)]
#[derive(Debug)]
#[derive(Clone)]
pub struct ProduceRequest {
    pub header: HeaderRequest,
    pub required_acks: i16,
    pub timeout: i32,
    pub topic: String,
    pub partition: i32,
    pub messageset_size: i32,
    pub messageset: Vec<MessageSet>
}

#[derive(Default)]
#[derive(Debug)]
#[derive(Clone)]
pub struct ProduceResponse {
    pub header: HeaderResponse,
    pub topic: String,
    pub partition: i32,
    pub error: i16,
    pub offset: i64
}

#[derive(Default)]
#[derive(Debug)]
#[derive(Clone)]
pub struct OffsetRequest {
    pub header: HeaderRequest,
    pub replica: i32,
    pub topic_partitions: Vec<TopicPartitionRequest>
}

#[derive(Default)]
#[derive(Debug)]
#[derive(Clone)]
pub struct TopicPartitionRequest {
    pub topic: String,
    pub partitions: Vec<PartitionRequest>
}

#[derive(Default)]
#[derive(Debug)]
#[derive(Clone)]
pub struct PartitionRequest {
    pub partition: i32,
    pub time: i64,
    pub max_offsets: i32
}


#[derive(Default)]
#[derive(Debug)]
#[derive(Clone)]
pub struct OffsetResponse {
    pub header: HeaderResponse,
    pub topic_partitions: Vec<TopicPartitionResponse>,
}

#[derive(Default)]
#[derive(Debug)]
#[derive(Clone)]
pub struct TopicPartitionResponse {
    pub topic: String,
    pub partitions: Vec<PartitionResponse>,
}

#[derive(Default)]
#[derive(Debug)]
#[derive(Clone)]
pub struct PartitionResponse {
    pub partition: i32,
    pub error: i16,
    pub offset: Vec<i64>
}


#[derive(Default)]
#[derive(Debug)]
#[derive(Clone)]
pub struct FetchRequest {
    pub header: HeaderRequest,
    pub replica: i32,
    pub max_wait_time: i32,
    pub min_bytes: i32,
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub max_bytes: i32
}

#[derive(Default)]
#[derive(Debug)]
#[derive(Clone)]
pub struct FetchResponse {
    pub header: HeaderResponse,
    pub topic: String,
    pub partition: i32,
    pub error: i16,
    pub offset: i64,
    pub messageset_size: i32,
    pub messageset: Vec<MessageSet>
}

// Helper Structs

#[derive(Default)]
#[derive(Debug)]
#[derive(Clone)]
pub struct BrokerMetadata {
    pub nodeid: i32,
    pub host: String,
    pub port: i32
}

#[derive(Default)]
#[derive(Debug)]
#[derive(Clone)]
pub struct TopicMetadata {
    pub error: i16,
    pub topic: String,
    pub partitions: Vec<PartitionMetadata>
}

#[derive(Default)]
#[derive(Debug)]
#[derive(Clone)]
pub struct PartitionMetadata {
    pub error: i16,
    pub id: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>
}

#[derive(Default)]
#[derive(Debug)]
#[derive(Clone)]
pub struct MessageSet {
    pub offset: i64,
    pub messagesize: i32,
    pub message: Message
}

#[derive(Default)]
#[derive(Debug)]
#[derive(Clone)]
pub struct Message {
    pub crc: i32,
    pub magic: i8,
    pub attributes: i8,
    pub key: Vec<u8>,
    pub value: Vec<u8>
}

// Constructors for Requests
impl MetadataRequest {
    pub fn new(correlation: i32, clientid: &String, topics: Vec<String>) -> MetadataRequest{
        MetadataRequest{
            header: HeaderRequest{key: METADATA_KEY, correlation: correlation,
                                  clientid: clientid.clone(), version: VERSION},
            topics: topics.to_vec()
        }
    }
}

impl OffsetRequest {
    pub fn new(topic_partitions: &Vec<(String, Vec<i32>)>, time: i64,
               correlation: i32, clientid: &String) -> OffsetRequest{
        OffsetRequest{
            header: HeaderRequest{key: OFFSET_KEY, correlation: correlation,
                                  clientid: clientid.clone(), version: VERSION},
            replica: -1,
            topic_partitions: topic_partitions.iter()
                                .map(|&(ref topic, ref partitions)|
                                    TopicPartitionRequest::new(topic, partitions.to_vec(), &time))
                                .collect()
        }
    }
    pub fn new_latest(topic_partitions: &Vec<(String, Vec<i32>)>,
                      correlation: i32, clientid: &String) -> OffsetRequest{
        OffsetRequest::new(topic_partitions, -1, correlation, clientid)
    }

    pub fn new_earliest(topic_partitions: &Vec<(String, Vec<i32>)>,
                      correlation: i32, clientid: &String) -> OffsetRequest{
        OffsetRequest::new(topic_partitions, -2, correlation, clientid)
    }
}

impl TopicPartitionRequest {
    pub fn new(topic: &String, partitions: Vec<i32>, time: &i64) -> TopicPartitionRequest{
        TopicPartitionRequest {
            topic: topic.clone(),
            partitions: partitions.iter()
                                  .map(|&partition| PartitionRequest:: new(partition, time))
                                  .collect()
        }
    }
}

impl PartitionRequest {
    pub fn new(partition: i32, time: &i64) -> PartitionRequest {
        PartitionRequest{
            partition: partition,
            time: *time,
            max_offsets: 1
        }
    }
}
// Encoder and Decoder implementations
impl ToByte for HeaderRequest {
    fn encode<T:Write>(&self, buffer: &mut T) {
        self.key.encode(buffer);
        self.version.encode(buffer);
        self.correlation.encode(buffer);
        self.clientid.encode(buffer);
    }
}

impl ToByte for MetadataRequest {
    fn encode<T:Write>(&self, buffer: &mut T) {
        self.header.encode(buffer);
        self.topics.encode(buffer);
    }
}

impl ToByte for OffsetRequest {
    fn encode<T:Write>(&self, buffer: &mut T) {
        self.header.encode(buffer);
        self.replica.encode(buffer);
        self.topic_partitions.encode(buffer);
    }
}

impl ToByte for FetchRequest {
    fn encode<T:Write>(&self, buffer: &mut T) {
        self.header.encode(buffer);
        self.replica.encode(buffer);
        self.max_wait_time.encode(buffer);
        self.min_bytes.encode(buffer);
        self.topic.encode(buffer);
        self.partition.encode(buffer);
        self.offset.encode(buffer);
        self.max_bytes.encode(buffer);
    }
}


impl FromByte for HeaderResponse {
    type R = HeaderResponse;

    fn decode<T: Read>(&mut self, buffer: &mut T) {
        self.correlation.decode(buffer);
    }
}

impl FromByte for MetadataResponse {
    type R = MetadataResponse;

    fn decode<T: Read>(&mut self, buffer: &mut T) {
        self.header.decode(buffer);
        self.brokers.decode(buffer);
        self.topics.decode(buffer);
    }
}

impl FromByte for OffsetResponse {
    type R = OffsetResponse;

    fn decode<T: Read>(&mut self, buffer: &mut T) {
        self.header.decode(buffer);
        self.topic_partitions.decode(buffer);
    }
}

impl FromByte for TopicPartitionResponse {
    type R = TopicPartitionResponse;

    fn decode<T: Read>(&mut self, buffer: &mut T) {
        self.topic.decode(buffer);
        self.partitions.decode(buffer);
    }
}

impl FromByte for PartitionResponse {
    type R = PartitionResponse;

    fn decode<T: Read>(&mut self, buffer: &mut T) {
        self.partition.decode(buffer);
        self.error.decode(buffer);
        self.offset.decode(buffer);
    }
}

impl FromByte for FetchResponse {
    type R = FetchResponse;

    fn decode<T: Read>(&mut self, buffer: &mut T) {
        self.header.decode(buffer);
        self.topic.decode(buffer);
        self.partition.decode(buffer);
        self.error.decode(buffer);
        self.offset.decode(buffer);
        self.messageset_size.decode(buffer);
        self.messageset.decode(buffer);
    }
}

// For Helper Structs

impl FromByte for BrokerMetadata {
    type R = BrokerMetadata;

    fn decode<T: Read>(&mut self, buffer: &mut T) {
        self.nodeid.decode(buffer);
        self.host.decode(buffer);
        self.port.decode(buffer);
    }
}

impl FromByte for TopicMetadata {
    type R = TopicMetadata;

    fn decode<T: Read>(&mut self, buffer: &mut T) {
        self.error.decode(buffer);
        self.topic.decode(buffer);
        self.partitions.decode(buffer);
    }
}

impl FromByte for PartitionMetadata {
    type R = PartitionMetadata;

    fn decode<T: Read>(&mut self, buffer: &mut T) {
        self.error.decode(buffer);
        self.id.decode(buffer);
        self.leader.decode(buffer);
        self.replicas.decode(buffer);
        self.isr.decode(buffer);
    }
}

impl ToByte for TopicPartitionRequest {
    fn encode<T: Write>(&self, buffer: &mut T) {
        self.topic.encode(buffer);
        self.partitions.encode(buffer);
    }
}

impl ToByte for PartitionRequest {
    fn encode<T: Write>(&self, buffer: &mut T) {
        self.partition.encode(buffer);
        self.time.encode(buffer);
        self.max_offsets.encode(buffer);
    }
}

impl ToByte for MessageSet {
    fn encode<T:Write>(&self, buffer: &mut T) {
        self.offset.encode(buffer);
        self.messagesize.encode(buffer);
        self.message.encode(buffer);
    }
}

impl FromByte for MessageSet {
    type R = MessageSet;

    fn decode<T: Read>(&mut self, buffer: &mut T) {
        self.offset.decode(buffer);
        self.messagesize.decode(buffer);
        self.message.decode(buffer);
    }
}

impl ToByte for Message {
    fn encode<T:Write>(&self, buffer: &mut T) {
        self.crc.encode(buffer);
        self.magic.encode(buffer);
        self.attributes.encode(buffer);
        self.key.encode(buffer);
        self.value.encode(buffer);
    }
}

impl FromByte for Message {
    type R = MessageSet;

    fn decode<T: Read>(&mut self, buffer: &mut T) {
        self.crc.decode(buffer);
        self.magic.decode(buffer);
        self.attributes.decode(buffer);
        self.key.decode(buffer);
        self.value.decode(buffer);
    }
}
