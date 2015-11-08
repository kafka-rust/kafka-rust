use std::io::{Read, Write};
use std::io::Cursor;
use std::rc::Rc;

use num::traits::FromPrimitive;

use error::{Result, Error};
use utils::{OffsetMessage, TopicMessage, TopicPartitionOffsetError};
use crc32::Crc32;
use codecs::{ToByte, FromByte};
use compression::Compression;
use snappy;
use gzip;

/// Macro to return Result<()> from multiple statements
macro_rules! try_multi {
    (
        $($expr:expr),*
    ) => ({
        $(
            try!($expr);
        )*;
        Ok(())
    })
}


const PRODUCE_KEY: i16 = 0;
const FETCH_KEY: i16 = 1;
const OFFSET_KEY: i16 = 2;
const METADATA_KEY: i16 = 3;
const OFFSET_COMMIT_KEY: i16 = 8;
const OFFSET_FETCH_KEY: i16 = 9;
//const CONSUMER_METADATA_KEY: i16 = 10;

const VERSION: i16 = 0;

const FETCH_MAX_WAIT_TIME: i32 = 100;
const FETCH_MIN_BYTES: i32 = 4096;
const FETCH_BUFFER_SIZE_BYTES: i32 = 4096;
const MAX_FETCH_BUFFER_SIZE_BYTES: i32 = FETCH_BUFFER_SIZE_BYTES * 8;

// Header
#[derive(Default, Debug, Clone)]
pub struct HeaderRequest {
    pub key: i16,
    pub version: i16,
    pub correlation: i32,
    pub clientid: Rc<String>
}

#[derive(Default, Debug, Clone)]
pub struct HeaderResponse {
    pub correlation: i32
}

// Metadata
#[derive(Default, Debug, Clone)]
pub struct MetadataRequest {
    pub header: HeaderRequest,
    pub topics: Vec<String>
}

#[derive(Default, Debug, Clone)]
pub struct MetadataResponse {
    pub header: HeaderResponse,
    pub brokers: Vec<BrokerMetadata>,
    pub topics: Vec<TopicMetadata>
}

// Produce
#[derive(Default, Debug, Clone)]
pub struct ProduceRequest {
    pub header: HeaderRequest,
    pub required_acks: i16,
    pub timeout: i32,
    pub topic_partitions: Vec<TopicPartitionProduceRequest>,
    pub compression: Compression
}

#[derive(Default, Debug, Clone)]
pub struct TopicPartitionProduceRequest {
    pub topic: String,
    pub partitions: Vec<PartitionProduceRequest>,
    pub compression: Compression
}

#[derive(Default, Debug, Clone)]
pub struct PartitionProduceRequest {
    pub partition: i32,
    pub messageset_size: i32,
    pub messageset: MessageSet,
    pub compression: Compression
}

#[derive(Default, Debug, Clone)]
pub struct ProduceResponse {
    pub header: HeaderResponse,
    pub topic_partitions: Vec<TopicPartitionProduceResponse>
}

#[derive(Default, Debug, Clone)]
pub struct TopicPartitionProduceResponse {
    pub topic: String,
    pub partitions: Vec<PartitionProduceResponse>,
}

#[derive(Default, Debug, Clone)]
pub struct PartitionProduceResponse {
    pub partition: i32,
    pub error: i16,
    pub offset: i64
}

// Offset
#[derive(Default, Debug, Clone)]
pub struct OffsetRequest {
    pub header: HeaderRequest,
    pub replica: i32,
    pub topic_partitions: Vec<TopicPartitionOffsetRequest>
}

#[derive(Default, Debug, Clone)]
pub struct TopicPartitionOffsetRequest {
    pub topic: String,
    pub partitions: Vec<PartitionOffsetRequest>
}

#[derive(Default, Debug, Clone)]
pub struct PartitionOffsetRequest {
    pub partition: i32,
    pub time: i64,
    pub max_offsets: i32
}


#[derive(Default, Debug, Clone)]
pub struct OffsetResponse {
    pub header: HeaderResponse,
    pub topic_partitions: Vec<TopicPartitionOffsetResponse>,
}

#[derive(Default, Debug, Clone)]
pub struct TopicPartitionOffsetResponse {
    pub topic: String,
    pub partitions: Vec<PartitionOffsetResponse>,
}

#[derive(Default, Debug, Clone)]
pub struct PartitionOffsetResponse {
    pub partition: i32,
    pub error: i16,
    pub offset: Vec<i64>
}

// Fetch
#[derive(Default, Debug, Clone)]
pub struct FetchRequest {
    pub header: HeaderRequest,
    pub replica: i32,
    pub max_wait_time: i32,
    pub min_bytes: i32,
    pub topic_partitions: Vec<TopicPartitionFetchRequest>
}

#[derive(Default, Debug, Clone)]
pub struct TopicPartitionFetchRequest {
    pub topic: String,
    pub partitions: Vec<PartitionFetchRequest>
}

#[derive(Default, Debug, Clone)]
pub struct PartitionFetchRequest {
    pub partition: i32,
    pub offset: i64,
    pub max_bytes: i32
}


#[derive(Default, Debug, Clone)]
pub struct FetchResponse {
    pub header: HeaderResponse,
    pub topic_partitions: Vec<TopicPartitionFetchResponse>,
}

#[derive(Default, Debug, Clone)]
pub struct TopicPartitionFetchResponse {
    pub topic: String,
    pub partitions: Vec<PartitionFetchResponse>,
}

#[derive(Default, Debug, Clone)]
pub struct PartitionFetchResponse {
    pub partition: i32,
    pub error: i16,
    pub offset: i64,
    pub messageset: MessageSet
}

// Consumer Metadata
#[derive(Default, Debug, Clone)]
pub struct ConsumerMetadataRequest {
    pub header: HeaderRequest,
    pub group: String
}

#[derive(Default, Debug, Clone)]
pub struct ConsumerMetadataResponse {
    pub header: HeaderResponse,
    pub error: i16,
    pub id: i32,
    pub host: String,
    pub port: i32
}

// Offset Commit
#[derive(Default, Debug, Clone)]
pub struct OffsetCommitRequest {
    pub header: HeaderRequest,
    pub group: String,
    pub topic_partitions: Vec<TopicPartitionOffsetCommitRequest>
}

#[derive(Default, Debug, Clone)]
pub struct TopicPartitionOffsetCommitRequest {
    pub topic: String,
    pub partitions: Vec<PartitionOffsetCommitRequest>
}

#[derive(Default, Debug, Clone)]
pub struct PartitionOffsetCommitRequest {
    pub partition: i32,
    pub offset: i64,
    pub metadata: String
}

#[derive(Default, Debug, Clone)]
pub struct OffsetCommitResponse {
    pub header: HeaderResponse,
    pub topic_partitions: Vec<TopicPartitionOffsetCommitResponse>
}

#[derive(Default, Debug, Clone)]
pub struct TopicPartitionOffsetCommitResponse {
    pub topic: String,
    pub partitions: Vec<PartitionOffsetCommitResponse>
}

#[derive(Default, Debug, Clone)]
pub struct PartitionOffsetCommitResponse {
    pub partition: i32,
    pub error: i16
}

// Offset Fetch
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


#[derive(Default, Debug, Clone)]
pub struct OffsetFetchResponse {
    pub header: HeaderResponse,
    pub topic_partitions: Vec<TopicPartitionOffsetFetchResponse>
}

#[derive(Default, Debug, Clone)]
pub struct TopicPartitionOffsetFetchResponse {
    pub topic: String,
    pub partitions: Vec<PartitionOffsetFetchResponse>
}

#[derive(Default, Debug, Clone)]
pub struct PartitionOffsetFetchResponse {
    pub partition: i32,
    pub offset: i64,
    pub metadata: String,
    pub error: i16
}


// Helper Structs

#[derive(Default, Debug, Clone)]
pub struct BrokerMetadata {
    pub nodeid: i32,
    pub host: String,
    pub port: i32
}

#[derive(Default, Debug, Clone)]
pub struct TopicMetadata {
    pub error: i16,
    pub topic: String,
    pub partitions: Vec<PartitionMetadata>
}

#[derive(Default, Debug, Clone)]
pub struct PartitionMetadata {
    pub error: i16,
    pub id: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>
}

#[derive(Default, Debug, Clone)]
pub struct MessageSet {
    pub message: Vec<MessageSetInner>
}

#[derive(Default, Debug, Clone)]
pub struct MessageSetOuter {
    pub offset: i64,
    pub messagesize: i32,
    pub message: Vec<u8>
}

#[derive(Default, Debug, Clone)]
pub struct MessageSetInner {
    pub offset: i64,
    pub messagesize: i32,
    pub message: Message
}

#[derive(Default, Debug, Clone)]
pub struct Message {
    pub crc: i32,
    pub magic: i8,
    pub attributes: i8,
    pub key: Vec<u8>,
    pub value: Vec<u8>
}


// Constructors for Requests
impl MetadataRequest {
    pub fn new(correlation: i32, clientid: Rc<String>, topics: Vec<String>) -> MetadataRequest{
        MetadataRequest{
            header: HeaderRequest{key: METADATA_KEY, correlation: correlation,
                                  clientid: clientid, version: VERSION},
            topics: topics
        }
    }
}

impl OffsetRequest {
    pub fn new(correlation: i32, clientid: Rc<String>) -> OffsetRequest{
        OffsetRequest{
            header: HeaderRequest{key: OFFSET_KEY, correlation: correlation,
                                  clientid: clientid, version: VERSION},
            replica: -1,
            topic_partitions: vec!()
        }
    }

    pub fn add(&mut self, topic: &str, partition: i32, time: i64) {
        for tp in &mut self.topic_partitions {
            if tp.topic == topic {
                tp.add(partition, time);
                return;
            }
        }
        let mut tp = TopicPartitionOffsetRequest::new(topic.to_owned());
        tp.add(partition, time);
        self.topic_partitions.push(tp);
    }
}

impl TopicPartitionOffsetRequest {
    pub fn new(topic: String) -> TopicPartitionOffsetRequest{
        TopicPartitionOffsetRequest {
            topic: topic,
            partitions: vec!()
        }
    }

    pub fn add(&mut self, partition: i32, time: i64) {
        self.partitions.push(PartitionOffsetRequest::new(partition, time));
    }
}

impl PartitionOffsetRequest {
    pub fn new(partition: i32, time: i64) -> PartitionOffsetRequest {

        PartitionOffsetRequest{
            partition: partition,
            time: time,
            max_offsets: 1
        }
    }
}

impl OffsetResponse {
    pub fn get_offsets(&self) -> Vec<TopicPartitionOffsetError> {
        self.topic_partitions
            .iter()
            .flat_map(|ref tp| tp.get_offsets(tp.topic.clone()))
            .collect()
    }
}

impl TopicPartitionOffsetResponse {
    pub fn get_offsets(&self, topic: String) -> Vec<TopicPartitionOffsetError> {
        self.partitions
            .iter()
            .map(|ref p| p.get_offsets(topic.clone()))
            .collect()
    }
}

impl PartitionOffsetResponse {
    pub fn get_offsets(&self, topic: String) -> TopicPartitionOffsetError {
        TopicPartitionOffsetError{
            topic: topic,
            partition: self.partition,
            offset: match self.offset.first() {
                Some(offs) => *offs,
                None => -1,
            },
            error: Error::from_i16(self.error)
        }
    }
}


impl ProduceRequest {
    pub fn new(required_acks: i16, timeout: i32,
               correlation: i32, clientid: Rc<String>, compression: Compression) -> ProduceRequest{
        ProduceRequest{
            header: HeaderRequest{key: PRODUCE_KEY, correlation: correlation,
                                  clientid: clientid, version: VERSION},
            required_acks: required_acks,
            timeout: timeout,
            topic_partitions: vec!(),
            compression: compression
        }
    }

    pub fn add(&mut self, topic: String, partition: i32, message: Vec<u8>) {
        for tp in &mut self.topic_partitions {
            if tp.topic == topic {
                tp.add(partition, message);
                return;
            }
        }
        let mut tp = TopicPartitionProduceRequest::new(topic.clone(), self.compression);
        tp.add(partition, message);
        self.topic_partitions.push(tp);
    }
}

impl TopicPartitionProduceRequest {
    pub fn new(topic: String, compression: Compression) -> TopicPartitionProduceRequest {
        TopicPartitionProduceRequest {
            topic: topic,
            partitions: vec!(),
            compression: compression
        }
    }

    pub fn add(&mut self, partition: i32, message: Vec<u8>) {
        for pp in &mut self.partitions {
            if pp.partition == partition {
                pp.add(message);
                return;
            }
        }
        self.partitions.push(PartitionProduceRequest:: new(partition, message, self.compression))
    }
}

impl PartitionProduceRequest {
    pub fn new(partition: i32, message: Vec<u8>, compression: Compression) -> PartitionProduceRequest {
        PartitionProduceRequest{
            partition: partition,
            messageset_size: 0,
            messageset: MessageSet::new(message),
            compression: compression
        }
    }

    pub fn add(&mut self, message: Vec<u8>) {
        self.messageset.add(message)
    }
}

impl ProduceResponse {
    pub fn get_response(&self) -> Vec<TopicPartitionOffsetError>{
        self.topic_partitions
            .iter()
            .flat_map(|ref tp| tp.get_response(tp.topic.clone()))
            .collect()
    }
}

impl TopicPartitionProduceResponse {
    pub fn get_response(& self, topic: String) -> Vec<TopicPartitionOffsetError>{
        self.partitions
            .iter()
            .map(|ref p| p.get_response(topic.clone()))
            .collect()
    }
}

impl PartitionProduceResponse {
    pub fn get_response(& self, topic: String) -> TopicPartitionOffsetError{
        TopicPartitionOffsetError{
            topic: topic,
            partition: self.partition,
            offset:self.offset,
            error: Error::from_i16(self.error)
        }
    }
}

impl FetchRequest {

    pub fn new(correlation: i32, clientid: Rc<String>) -> FetchRequest{
        FetchRequest{
            header: HeaderRequest{key: FETCH_KEY, correlation: correlation,
                                  clientid: clientid, version: VERSION},
            replica: -1,
            max_wait_time: FETCH_MAX_WAIT_TIME,
            min_bytes: FETCH_MIN_BYTES,
            topic_partitions: vec!()
        }
    }

    pub fn add(&mut self, topic: String, partition: i32, offset: i64) {
        for tp in &mut self.topic_partitions {
            if tp.topic == topic {
                tp.add(partition, offset);
                return;
            }
        }
        let mut tp = TopicPartitionFetchRequest::new(topic);
        tp.add(partition, offset);
        self.topic_partitions.push(tp);
    }
}

impl TopicPartitionFetchRequest {
    pub fn new(topic: String) -> TopicPartitionFetchRequest{
        TopicPartitionFetchRequest {
            topic: topic,
            partitions: vec!()
        }
    }

    pub fn add(&mut self, partition: i32, offset: i64) {
        self.partitions.push(PartitionFetchRequest:: new(partition, offset))
    }
}

impl PartitionFetchRequest {
    pub fn new(partition: i32, offset: i64) -> PartitionFetchRequest {
        PartitionFetchRequest{
            partition: partition,
            offset: offset,
            max_bytes: MAX_FETCH_BUFFER_SIZE_BYTES
        }
    }
}

impl FetchResponse {
    pub fn into_messages(self) -> Vec<TopicMessage> {
        self.topic_partitions
            .into_iter()
            .flat_map(|tp| tp.into_messages())
            .collect()
    }
}

impl TopicPartitionFetchResponse {
    pub fn into_messages(self) -> Vec<TopicMessage> {
        let topic = self.topic;
        self.partitions
            .into_iter()
            .flat_map(|p| p.into_messages(topic.clone()))
            .collect()
    }
}

impl PartitionFetchResponse {
    pub fn into_messages(self, topic: String) -> Vec<TopicMessage> {
        if self.error != 0 {
            return vec!(TopicMessage{topic: topic, partition: self.partition.clone(),
                                     offset: self.offset, message: vec!(),
                                     error: Error::from_i16(self.error)});
        }
        let partition = self.partition;
        let error = self.error;
        self.messageset.into_messages()
                       .into_iter()
                       .map(|om| TopicMessage{topic: topic.clone(), partition: partition.clone(),
                                              offset: om.offset, message: om.message,
                                              error: Error::from_i16(error)})
                       .collect()
    }
}

impl OffsetCommitRequest {
    pub fn new(group: String, correlation: i32, clientid: Rc<String>) -> OffsetCommitRequest {
        OffsetCommitRequest{
            header: HeaderRequest{key: OFFSET_COMMIT_KEY, correlation: correlation,
                                  clientid: clientid, version: VERSION},
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

impl OffsetFetchRequest {
    pub fn new(group: String, correlation: i32, clientid: Rc<String>) -> OffsetFetchRequest {
        OffsetFetchRequest{
            header: HeaderRequest{key: OFFSET_FETCH_KEY, correlation: correlation,
                                  clientid: clientid, version: VERSION},
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

impl OffsetFetchResponse {
    pub fn get_offsets(&self) -> Vec<TopicPartitionOffsetError>{
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

impl MessageSet {
    pub fn new(message: Vec<u8>) -> MessageSet {
        MessageSet{message: vec!(MessageSetInner::new(message))}
    }

    pub fn add(&mut self, message: Vec<u8>) {
        self.message.push(MessageSetInner::new(message))
    }

    fn into_messages(self) -> Vec<OffsetMessage> {
        self.message
            .into_iter()
            .flat_map(|m| m.into_messages())
            .collect()
    }
}

impl MessageSetOuter {
    fn new(message: Vec<u8>) -> MessageSetOuter {
        MessageSetOuter{offset:0, messagesize:0, message: message}
    }
}

impl MessageSetInner {
    fn new(message: Vec<u8>) -> MessageSetInner {
        MessageSetInner{offset:0, messagesize:0, message: Message::new(message)}
    }
    fn into_messages(self) -> Vec<OffsetMessage>{
        self.message.into_messages(self.offset)
    }
}

impl Message {
    pub fn new(message: Vec<u8>) -> Message {
        Message{crc: 0, value: message, ..Default::default()}
    }

    fn into_messages(self, offset: i64) -> Vec<OffsetMessage>{
        match self.attributes & 3 {
            codec if codec == Compression::NONE as i8 => vec!(OffsetMessage{offset:offset, message: self.value}),
            codec if codec == Compression::GZIP as i8 => message_decode_gzip(self.value),
            codec if codec == Compression::SNAPPY as i8 => message_decode_snappy(self.value),
            _ => vec!()
        }
    }
}

fn message_decode_snappy(value: Vec<u8>) -> Vec<OffsetMessage>{
    // SNAPPY
    let mut buffer = Cursor::new(value);
    let _ = snappy::SnappyHeader::decode_new(&mut buffer);
    //if (!snappy::check_header(&header)) return;

    let mut v = vec!();
    loop {
        match message_decode_loop_snappy(&mut buffer) {
            Ok(x) => v.extend(x),
            Err(_) => break
        }
    }
    v
}

fn message_decode_loop_snappy<T:Read>(buffer: &mut T) -> Result<Vec<OffsetMessage>> {
    let sms = try!(snappy::SnappyMessage::decode_new(buffer));
    let msg = try!(snappy::uncompress(sms.message));
    let mset = try!(MessageSet::decode_new(&mut Cursor::new(msg)));
    Ok(mset.into_messages())
}

fn message_decode_gzip(value: Vec<u8>) -> Vec<OffsetMessage>{
    // Gzip
    let mut buffer = Cursor::new(value);
    match message_decode_loop_gzip(&mut buffer) {
        Ok(x) => x,
        Err(_) => vec!()
    }
}

fn message_decode_loop_gzip<T:Read>(buffer: &mut T) -> Result<Vec<OffsetMessage>> {
    let msg = try!(gzip::uncompress(buffer));
    let mset = try!(MessageSet::decode_new(&mut Cursor::new(msg)));
    Ok(mset.into_messages())
}

// Encoder and Decoder implementations
impl ToByte for HeaderRequest {
    fn encode<T:Write>(&self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.key.encode(buffer),
            self.version.encode(buffer),
            self.correlation.encode(buffer),
            self.clientid.encode(buffer)
        )
    }
}

impl ToByte for MetadataRequest {
    fn encode<T:Write>(&self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.header.encode(buffer),
            self.topics.encode(buffer)
        )
    }
}

impl ToByte for OffsetRequest {
    fn encode<T:Write>(&self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.header.encode(buffer),
            self.replica.encode(buffer),
            self.topic_partitions.encode(buffer)
        )
    }
}

impl ToByte for ProduceRequest {
    fn encode<T:Write>(&self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.header.encode(buffer),
            self.required_acks.encode(buffer),
            self.timeout.encode(buffer),
            self.topic_partitions.encode(buffer)
        )
    }
}

impl ToByte for TopicPartitionProduceRequest {
    fn encode<T:Write>(&self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.topic.encode(buffer),
            self.partitions.encode(buffer)
        )
    }
}

impl ToByte for PartitionProduceRequest {
    fn encode<T:Write>(&self, buffer: &mut T) -> Result<()> {
        let mut buf_msgset = vec!();
        try!(self.partition.encode(buffer));
        try!(self.messageset.encode(&mut buf_msgset));
        match self.compression {
            Compression::NONE => buf_msgset.encode(buffer),
            Compression::GZIP | Compression::SNAPPY => {
                let mut msg = Message::new(buf_msgset);
                msg.attributes |= self.compression as i8;
                let mut buf_msg = vec!();
                try!(msg.encode(&mut buf_msg));
                let outer_msgset = MessageSetOuter::new(buf_msg);
                let mut buf_outer_msgset = vec!();
                try!(outer_msgset.encode(&mut buf_outer_msgset));
                buf_outer_msgset.encode(buffer)
            }
        }
    }
}

impl ToByte for FetchRequest {
    fn encode<T:Write>(&self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.header.encode(buffer),
            self.replica.encode(buffer),
            self.max_wait_time.encode(buffer),
            self.min_bytes.encode(buffer),
            self.topic_partitions.encode(buffer)
        )
    }
}

impl ToByte for TopicPartitionFetchRequest {
    fn encode<T:Write>(&self, buffer: &mut T) -> Result<()> {
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

impl ToByte for ConsumerMetadataRequest {
    fn encode<T:Write>(&self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.header.encode(buffer),
            self.group.encode(buffer)
        )
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

// Responses
impl FromByte for HeaderResponse {
    type R = HeaderResponse;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        self.correlation.decode(buffer)
    }
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

impl FromByte for OffsetResponse {
    type R = OffsetResponse;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.header.decode(buffer),
            self.topic_partitions.decode(buffer)
        )
    }
}

impl FromByte for TopicPartitionOffsetResponse {
    type R = TopicPartitionOffsetResponse;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.topic.decode(buffer),
            self.partitions.decode(buffer)
        )
    }
}

impl FromByte for PartitionOffsetResponse {
    type R = PartitionOffsetResponse;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.partition.decode(buffer),
            self.error.decode(buffer),
            self.offset.decode(buffer)
        )
    }
}

impl FromByte for ProduceResponse {
    type R = ProduceResponse;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.header.decode(buffer),
            self.topic_partitions.decode(buffer)
        )
    }
}

impl FromByte for TopicPartitionProduceResponse {
    type R = TopicPartitionProduceResponse;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.topic.decode(buffer),
            self.partitions.decode(buffer)
        )
    }
}

impl FromByte for PartitionProduceResponse {
    type R = PartitionProduceResponse;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.partition.decode(buffer),
            self.error.decode(buffer),
            self.offset.decode(buffer)
        )
    }
}


impl FromByte for FetchResponse {
    type R = FetchResponse;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.header.decode(buffer),
            self.topic_partitions.decode(buffer)
        )
    }
}

impl FromByte for TopicPartitionFetchResponse {
    type R = TopicPartitionFetchResponse;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.topic.decode(buffer),
            self.partitions.decode(buffer)
        )
    }
}

impl FromByte for PartitionFetchResponse {
    type R = PartitionFetchResponse;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.partition.decode(buffer),
            self.error.decode(buffer),
            self.offset.decode(buffer),
            self.messageset.decode(buffer)
        )
    }
}

impl FromByte for ConsumerMetadataResponse {
    type R = ConsumerMetadataResponse;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.header.decode(buffer),
            self.error.decode(buffer),
            self.id.decode(buffer),
            self.host.decode(buffer),
            self.port.decode(buffer)
        )
    }
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

// For Helper Structs

impl FromByte for BrokerMetadata {
    type R = BrokerMetadata;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.nodeid.decode(buffer),
            self.host.decode(buffer),
            self.port.decode(buffer)
        )
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

impl ToByte for TopicPartitionOffsetRequest {
    fn encode<T: Write>(&self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.topic.encode(buffer),
            self.partitions.encode(buffer)
        )
    }
}

impl ToByte for PartitionOffsetRequest {
    fn encode<T: Write>(&self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.partition.encode(buffer),
            self.time.encode(buffer),
            self.max_offsets.encode(buffer)
        )
    }
}

impl ToByte for MessageSet {
    fn encode<T:Write>(&self, buffer: &mut T) -> Result<()> {
        self.message.encode_nolen(buffer)
    }
}

impl FromByte for MessageSet {
    type R = MessageSet;

    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        let mssize = try!(i32::decode_new(buffer));
        if mssize <= 0 { return Ok(()); }

        let mssize = mssize as u64;
        let mut buf = buffer.take(mssize);

        while buf.limit() > 0 {
            match MessageSetInner::decode_new(&mut buf) {
                Ok(val) => self.message.push(val),
                // handle partial trailing messages (see #17)
                Err(Error::UnexpectedEOF) => (),
                Err(err) => return Err(err)
            }
        }
        Ok(())
    }

    fn decode_new<T: Read>(buffer: &mut T) -> Result<Self::R> {
        let mut temp: Self::R = Default::default();
        loop {
            match MessageSetInner::decode_new(buffer) {
                Ok(mi) => temp.message.push(mi),
                Err(_) => break
            }
        }
        if temp.message.len() == 0 {
            return Err(Error::UnexpectedEOF)
        }
        Ok(temp)
    }
}

impl ToByte for MessageSetOuter {
    fn encode<T:Write>(&self, buffer: &mut T) -> Result<()> {
        let mut buf = vec!();
        try_multi!(
            self.offset.encode(buffer),
            self.message.encode(&mut buf),
            buf.encode_nolen(buffer)
        )
    }
}

impl ToByte for MessageSetInner {
    fn encode<T:Write>(&self, buffer: &mut T) -> Result<()> {
        let mut buf = vec!();
        try_multi!(
            self.offset.encode(buffer),
            self.message.encode(&mut buf),
            buf.encode(buffer)
        )
    }
}

impl FromByte for MessageSetInner {
    type R = MessageSetInner;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.offset.decode(buffer),
            self.messagesize.decode(buffer),
            self.message.decode(buffer)
        )
    }
}

impl ToByte for Message {
    fn encode<T:Write>(&self, buffer: &mut T) -> Result<()> {
        let mut buf = vec!();
        try!(self.magic.encode(&mut buf));
        try!(self.attributes.encode(&mut buf));
        if self.key.len() == 0 {
            let a: i32 = -1;
            try!(a.encode(&mut buf));
        } else {
            try!(self.key.encode(&mut buf));
        }
        match self.attributes & 3 {
            codec if codec == Compression::NONE as i8 => try!(self.value.encode(&mut buf)),
            codec if codec == Compression::GZIP as i8 => {
                let compressed = try!(gzip::compress(&self.value));
                try!(compressed.encode(&mut buf))
            }
            codec if codec == Compression::SNAPPY as i8 => {
                let compressed = try!(snappy::compress(&self.value));
                try!(compressed.encode(&mut buf))
            },
            _ => panic!("Unsupported compression format")
        };
        let (_, x) = buf.split_at(0);
        let crc = Crc32::tocrc(x) as i32;

        try!(crc.encode(buffer));
        buf.encode_nolen(buffer)
    }
}

impl FromByte for Message {
    type R = Message;

    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.crc.decode(buffer),
            self.magic.decode(buffer),
            self.attributes.decode(buffer),
            self.key.decode(buffer),
            self.value.decode(buffer)
        )
    }
}

#[cfg(test)]
mod tests {

    use std::io::Cursor;

    use super::FetchResponse;
    use codecs::FromByte;

    #[test]
    fn decode_new_fetch_response() {

        // - one topic
        // - 2 x message of 10 bytes (0..10)
        // - 2 x message of 5 bytes (0..5)
        // - 3 x message of 10 bytes (0..10)
        // - 1 x message of 5 bytes (0..5) static
        static FETCH_RESPONSE_RAW_DATA: &'static [u8] = &[
            0, 0, 0, 3, 0, 0, 0, 1, 0, 13, 116, 101, 115, 116, 95,
            116, 111, 112, 105, 99, 95, 49, 112, 0, 0, 0, 1, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 1, 17, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 24, 211, 120, 76, 139, 0, 0, 255,
            255, 255, 255, 0, 0, 0, 10, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
            0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 24, 211, 120, 76, 139, 0,
            0, 255, 255, 255, 255, 0, 0, 0, 10, 0, 1, 2, 3, 4, 5, 6,
            7, 8, 9, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 19, 224, 237,
            15, 248, 0, 0, 255, 255, 255, 255, 0, 0, 0, 5, 0, 1, 2, 3,
            4, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 19, 224, 237, 15, 248,
            0, 0, 255, 255, 255, 255, 0, 0, 0, 5, 0, 1, 2, 3, 4, 0, 0,
            0, 0, 0, 0, 0, 4, 0, 0, 0, 24, 211, 120, 76, 139, 0, 0,
            255, 255, 255, 255, 0, 0, 0, 10, 0, 1, 2, 3, 4, 5, 6, 7,
            8, 9, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 24, 211, 120, 76,
            139, 0, 0, 255, 255, 255, 255, 0, 0, 0, 10, 0, 1, 2, 3, 4,
            5, 6, 7, 8, 9, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 24, 211,
            120, 76, 139, 0, 0, 255, 255, 255, 255, 0, 0, 0, 10, 0, 1,
            2, 3, 4, 5, 6, 7, 8, 9, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0,
            19, 224, 237, 15, 248, 0, 0, 255, 255, 255, 255, 0, 0, 0,
            5, 0, 1, 2, 3, 4];

        let r = FetchResponse::decode_new(&mut Cursor::new(FETCH_RESPONSE_RAW_DATA));
        let msgs = r.unwrap().into_messages();

        macro_rules! assert_msg {
            ($msg:expr, $topic:expr, $partition:expr, $msgdata:expr) => {
                assert_eq!($topic, &$msg.topic[..]);
                assert_eq!($partition, $msg.partition);
                assert_eq!($msgdata, &$msg.message[..]);
            }
        }

        assert_eq!(8, msgs.len());
        let zero_to_ten: Vec<u8> = (0..10).collect();
        assert_msg!(msgs[0], "test_topic_1p", 0, &zero_to_ten[..]);
        assert_msg!(msgs[1], "test_topic_1p", 0, &zero_to_ten[..]);

        assert_msg!(msgs[2], "test_topic_1p", 0, &zero_to_ten[0..5]);
        assert_msg!(msgs[3], "test_topic_1p", 0, &zero_to_ten[0..5]);

        assert_msg!(msgs[4], "test_topic_1p", 0, &zero_to_ten[..]);
        assert_msg!(msgs[5], "test_topic_1p", 0, &zero_to_ten[..]);
        assert_msg!(msgs[6], "test_topic_1p", 0, &zero_to_ten[..]);

        assert_msg!(msgs[7], "test_topic_1p", 0, &zero_to_ten[0..5]);
    }
}
