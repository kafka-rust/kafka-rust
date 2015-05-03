
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


#[derive(Default)]
#[derive(Debug)]
pub struct HeaderRequest {
    pub key: i16,
    pub version: i16,
    pub correlation: i32,
    pub clientid: String
}

#[derive(Default)]
#[derive(Debug)]
pub struct HeaderResponse {
    pub correlation: i32
}

#[derive(Default)]
#[derive(Debug)]
pub struct MetadataRequest {
    pub header: HeaderRequest,
    pub topics: Vec<String>
}

#[derive(Default)]
#[derive(Debug)]
pub struct MetadataResponse {
    pub header: HeaderResponse,
    pub brokers: Vec<BrokerMetadata>,
    pub topics: Vec<TopicMetadata>
}


#[derive(Default)]
#[derive(Debug)]
pub struct BrokerMetadata {
    pub nodeid: i32,
    pub host: String,
    pub port: i32
}

#[derive(Default)]
#[derive(Debug)]
pub struct TopicMetadata {
    pub error: i16,
    pub topic: String,
    pub partitions: Vec<PartitionMetadata>
}

#[derive(Default)]
#[derive(Debug)]
pub struct PartitionMetadata {
    pub error: i16,
    pub id: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>
}

#[derive(Default)]
#[derive(Debug)]
pub struct TopicPartition {
    pub id: i32,
    pub topic: String
}

impl MetadataRequest {
    pub fn new(correlation: i32, clientid: String, topics: Vec<String>) -> MetadataRequest{
        MetadataRequest{
            header: HeaderRequest{key: METADATA_KEY, correlation: correlation,
                                  clientid: clientid, version: version},
            topics: topics.to_vec()
        }
    }
}

impl <T:Write> ToByte<T> for HeaderRequest {
    fn encode(&self, buffer: &mut T) {
        self.key.encode(buffer);
        self.version.encode(buffer);
        self.correlation.encode(buffer);
        self.clientid.encode(buffer);
    }
}

impl <T:Write> ToByte<T> for MetadataRequest {
    fn encode(&self, buffer: &mut T) {
        self.topics.encode(buffer);
    }
}

impl <T: Read> FromByte<T> for HeaderResponse {
    type R = HeaderResponse;

    fn decode(&mut self, buffer: &mut T) {
        self.correlation.decode(buffer);
    }
}

impl <T: Read> FromByte<T> for MetadataResponse {
    type R = MetadataResponse;

    fn decode(&mut self, buffer: &mut T) {
        self.header.decode(buffer);
        self.brokers.decode(buffer);
        self.topics.decode(buffer);
    }
}

impl <T: Read> FromByte<T> for BrokerMetadata {
    type R = BrokerMetadata;

    fn decode(&mut self, buffer: &mut T) {
        self.nodeid.decode(buffer);
        self.host.decode(buffer);
        self.port.decode(buffer);
    }
}

impl <T: Read> FromByte<T> for TopicMetadata {
    type R = TopicMetadata;

    fn decode(&mut self, buffer: &mut T) {
        self.error.decode(buffer);
        self.topic.decode(buffer);
        self.partitions.decode(buffer);
    }
}

impl <T: Read> FromByte<T> for PartitionMetadata {
    type R = PartitionMetadata;

    fn decode(&mut self, buffer: &mut T) {
        self.error.decode(buffer);
        self.id.decode(buffer);
        self.leader.decode(buffer);
        self.replicas.decode(buffer);
        self.isr.decode(buffer);
    }
}
/*

impl <T: Read> FromByte<T> for HeaderRequest {
    fn decode(&mut self, buffer: &mut T) {
        self.key.decode(buffer);
        self.version.decode(buffer);
        self.correlation.decode(buffer);
        self.clientid.decode(buffer);
    }
}

impl <T: Read> FromByte<T> for MetadataRequest {
    fn decode(&mut self, buffer: &mut T) {
        self.topics.decode(buffer);
    }
}
impl <T:Write> ToByte<T> for MetadataRequest {
    fn encode(&self, buffer: &mut T) {
        let size = self.topics.len() as i32;
        size.encode(buffer);
        for s in self.topics {
            s.encode(buffer);
        }
    }
}

impl <T: Read> FromByte<T> for MetadataRequest {
    fn decode(&mut self, buffer: &mut T) {
        let length = buffer.read_i16::<BigEndian>().unwrap();
        for n in 0..length {
            let topic = String::new();
            topic.decode(buffer);
            self.topics.push(topic);
        }
    }
}

impl <T: Read> FromByte<T> for MetadataResponse {
    fn decode(&mut self, buffer: &mut T) {
        let num_brokers = buffer.read_i16::<BigEndian>().unwrap();
        for n in 0..length {
            let broker = BrokerMetadata{nodeid:0, host:"".to_string(), port:0};
            broker.decode(buffer);
            self.brokers.push(broker);
        }

        let num_topics = buffer.read_i16::<BigEndian>().unwrap();
        for n in 0..length {
            let topic = TopicMetadata{topic:0, partition:"".to_string(), error:0};
            topic.decode(buffer);
            self.topics.push(topic);
        }

    }
}

impl <T: Read> FromByte<T> for BrokerMetadata {
    fn decode(&mut self, buffer: &mut T) {
        self.nodeid.decode(buffer);
        self.host.decode(buffer);
        self.port.decode(buffer);
    }
}

impl <T: Read> FromByte<T> for TopicMetadata {
    fn decode(&mut self, buffer: &mut T) {
        self.error.decode(buffer);
        self.topic.decode(buffer);

        let num_partitions = buffer.read_i16::<BigEndian>().unwrap();
        for n in 0..length {
            let topic = PartitionMetadata{topic:0, partition:"".to_string(), error:0};
            topic.decode(buffer);
            self.topics.push(topic);
        }
        self.partition.decode(buffer);
    }
}

impl <T: Read> FromByte<T> for PartitionMetadata {
    fn decode(&mut self, buffer: &mut T) {
        self.error.decode(buffer);
        self.id.decode(buffer);
        self.leader.decode(buffer);

    }
}
*/


pub fn encode_message_header<T: Write>(request: HeaderRequest, buffer: &mut T) {
    request.encode(buffer);
    /*buffer.write_i16::<BigEndian>(request.key);
    buffer.write_i16::<BigEndian>(request.version);
    buffer.write_i32::<BigEndian>(request.correlation);
    buffer.write_i16::<BigEndian>(request.clientid.len().to_i16().unwrap());
    buffer.write_all(request.clientid.as_bytes());*/
}

/*pub fn decode_message_header<T: Read>(response: &mut T) -> HeaderRequest{
    let mut h: HeaderRequest = Default::default();
    h.decode(response);
    h
}*/
