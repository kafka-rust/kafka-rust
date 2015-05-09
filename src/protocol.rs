use std::io::{Read, Write};
use std::io::Cursor;

use error::{Result, Error};
use utils::{OffsetMessage, TopicPartitions, TopicPartitionOffset};
use crc32::Crc32;
use codecs::{ToByte, FromByte};
use snappy;



const PRODUCE_KEY: i16 = 0;
const FETCH_KEY: i16 = 1;
const OFFSET_KEY: i16 = 2;
const METADATA_KEY: i16 = 3;

const VERSION: i16 = 0;

const FETCH_MAX_WAIT_TIME: i32 = 100;
const FETCH_MIN_BYTES: i32 = 4096;
const FETCH_BUFFER_SIZE_BYTES: i32 = 4096;
const MAX_FETCH_BUFFER_SIZE_BYTES: i32 = FETCH_BUFFER_SIZE_BYTES * 8;

#[derive(Default, Debug, Clone)]
pub struct HeaderRequest {
    pub key: i16,
    pub version: i16,
    pub correlation: i32,
    pub clientid: String
}

#[derive(Default, Debug, Clone)]
pub struct HeaderResponse {
    pub correlation: i32
}

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

#[derive(Default, Debug, Clone)]
pub struct ProduceRequest {
    pub header: HeaderRequest,
    pub required_acks: i16,
    pub timeout: i32,
    pub topic_partitions: Vec<TopicPartitionProduceRequest>,
}

#[derive(Default, Debug, Clone)]
pub struct TopicPartitionProduceRequest {
    pub topic: String,
    pub partitions: Vec<PartitionProduceRequest>,
}

#[derive(Default, Debug, Clone)]
pub struct PartitionProduceRequest {
    pub partition: i32,
    pub messageset_size: i32,
    pub messageset: MessageSet
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
    pub error: i32,
    pub offset: i64
}

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
    pub fn new(correlation: i32, clientid: &String, topics: &Vec<String>) -> MetadataRequest{
        MetadataRequest{
            header: HeaderRequest{key: METADATA_KEY, correlation: correlation,
                                  clientid: clientid.clone(), version: VERSION},
            topics: topics.to_vec()
        }
    }
}

impl OffsetRequest {
    pub fn new(topic_partitions: &Vec<TopicPartitions>, time: i64,
               correlation: i32, clientid: &String) -> OffsetRequest{
        OffsetRequest{
            header: HeaderRequest{key: OFFSET_KEY, correlation: correlation,
                                  clientid: clientid.clone(), version: VERSION},
            replica: -1,
            topic_partitions: topic_partitions.iter()
                                .map(|ref tp|
                                    TopicPartitionOffsetRequest::new(&tp.topic, tp.partitions.to_vec(), &time))
                                .collect()
        }
    }
    pub fn new_latest(topic_partitions: &Vec<TopicPartitions>,
                      correlation: i32, clientid: &String) -> OffsetRequest{
        OffsetRequest::new(topic_partitions, -1, correlation, clientid)
    }
}

impl TopicPartitionOffsetRequest {
    pub fn new(topic: &String, partitions: Vec<i32>, time: &i64) -> TopicPartitionOffsetRequest{
        TopicPartitionOffsetRequest {
            topic: topic.clone(),
            partitions: partitions.iter()
                                  .map(|&partition| PartitionOffsetRequest:: new(partition, time))
                                  .collect()
        }
    }
}

impl PartitionOffsetRequest {
    pub fn new(partition: i32, time: &i64) -> PartitionOffsetRequest {

        PartitionOffsetRequest{
            partition: partition,
            time: *time,
            max_offsets: 1
        }
    }
}

impl OffsetResponse {
    pub fn get_offsets(& self) -> Vec<TopicPartitionOffset>{
        self.topic_partitions
            .iter()
            .flat_map(|ref tp| tp.get_offsets(&tp.topic))
            .collect()
    }
}

impl TopicPartitionOffsetResponse {
    pub fn get_offsets(& self, topic: &String) -> Vec<TopicPartitionOffset>{
        self.partitions
            .iter()
            .map(|ref p| p.get_offsets(topic))
            .collect()
    }
}

impl PartitionOffsetResponse {
    pub fn get_offsets(& self, topic: &String) -> TopicPartitionOffset{
        TopicPartitionOffset{
            topic: topic.clone(),
            partition: self.partition,
            offset:self.offset[0],
            error: self.error
        }
    }
}


impl ProduceRequest {
    pub fn new_single(topic: &String, partition: i32, required_acks: i16,
                      timeout: i32, message: &Vec<u8>,
                      correlation: i32, clientid: &String) -> ProduceRequest{
        ProduceRequest{
            header: HeaderRequest{key: PRODUCE_KEY, correlation: correlation,
                                  clientid: clientid.clone(), version: VERSION},
            required_acks: required_acks,
            timeout: timeout,
            topic_partitions: vec!(TopicPartitionProduceRequest::new_single(topic, partition, message))
        }
    }
}

impl TopicPartitionProduceRequest {
    pub fn new_single(topic: &String, partition: i32, message: &Vec<u8>) -> TopicPartitionProduceRequest{
        TopicPartitionProduceRequest {
            topic: topic.clone(),
            partitions: vec!(PartitionProduceRequest:: new(&partition, message))
        }
    }
}

impl PartitionProduceRequest {
    pub fn new(partition: &i32, message: &Vec<u8>) -> PartitionProduceRequest {
        PartitionProduceRequest{
            partition: *partition,
            messageset_size: 0,
            messageset: MessageSet::new(message)
        }
    }
}

impl FetchRequest {

    pub fn new_single(topic: &String, partition: i32, offset: i64,
               correlation: i32, clientid: &String) -> FetchRequest{
        FetchRequest{
            header: HeaderRequest{key: FETCH_KEY, correlation: correlation,
                                  clientid: clientid.clone(), version: VERSION},
            replica: -1,
            max_wait_time: FETCH_MAX_WAIT_TIME,
            min_bytes: FETCH_MIN_BYTES,
            topic_partitions: vec!(TopicPartitionFetchRequest::new_single(topic, partition, offset))
        }
    }
}

impl TopicPartitionFetchRequest {
    pub fn new_single(topic: &String, partition: i32, offset: i64) -> TopicPartitionFetchRequest{
        TopicPartitionFetchRequest {
            topic: topic.clone(),
            partitions: vec!(PartitionFetchRequest:: new(&partition, &offset))
        }
    }
}

impl PartitionFetchRequest {
    pub fn new(partition: &i32, offset: &i64) -> PartitionFetchRequest {
        PartitionFetchRequest{
            partition: *partition,
            offset: *offset,
            max_bytes: MAX_FETCH_BUFFER_SIZE_BYTES
        }
    }
}

impl FetchResponse {
    pub fn get_messages(& self) -> Vec<OffsetMessage>{
        self.topic_partitions
            .iter()
            .flat_map(|ref tp| tp.get_messages())
            .collect()
    }
}

impl TopicPartitionFetchResponse {
    pub fn get_messages(& self) -> Vec<OffsetMessage>{
        self.partitions
            .iter()
            .flat_map(|ref p| p.get_messages())
            .collect()
    }
}

impl PartitionFetchResponse {
    pub fn get_messages(& self) -> Vec<OffsetMessage>{
        self.messageset.get_messages()
    }
}


impl MessageSet {
    pub fn new(message: &Vec<u8>) -> MessageSet {
        MessageSet{message: vec!(MessageSetInner::new(message))}
    }
    fn get_messages(& self) -> Vec<OffsetMessage>{
        self.message
            .iter()
            .flat_map(|ref m| m.get_messages())
            .collect()
    }
}

impl MessageSetInner {
    fn new(message: &Vec<u8>) -> MessageSetInner {
        MessageSetInner{offset:0, messagesize:0, message: Message::new(message)}
    }
    fn get_messages(& self) -> Vec<OffsetMessage>{
        self.message.get_messages(self.offset)
    }
}

impl Message {
    pub fn new(message: &Vec<u8>) -> Message {
        Message{crc: 0, value: message.clone(), ..Default::default()}
    }

    fn get_messages(& self, offset: i64) -> Vec<OffsetMessage>{
        match self.attributes {
            0 => vec!(OffsetMessage{offset:offset, message: self.value.clone()}),
            // TODO - Handle Gzip
            1 => vec!(OffsetMessage{offset:offset, message: self.value.clone()}),
            2 => message_decode_snappy(&self.value),
            _ => vec!()
        }
    }
}

fn message_decode_snappy(value: & Vec<u8>) -> Vec<OffsetMessage>{
    // SNAPPY
    let mut buffer = Cursor::new(value.to_vec());
    let _ = snappy::SnappyHeader::decode_new(&mut buffer);
    //if (!snappy::check_header(&header)) return;

    let mut v = vec!();
    loop {
        match message_decode_loop(&mut buffer) {
            Ok(x) => v.push(x),
            Err(_) => break
        }
    }
    v.iter().flat_map(|ref x| x.into_iter().cloned()).collect()
}

fn message_decode_loop<T:Read>(buffer: &mut T) -> Result<Vec<OffsetMessage>> {
    let sms = try!(snappy::SnappyMessage::decode_new(buffer));
    let msg = try!(snappy::uncompress(&sms.message));
    let mset = try!(MessageSet::decode_new(&mut Cursor::new(msg)));
    Ok(mset.get_messages())

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

impl ToByte for ProduceRequest {
    fn encode<T:Write>(&self, buffer: &mut T) {
        self.header.encode(buffer);
        self.required_acks.encode(buffer);
        self.timeout.encode(buffer);
        self.topic_partitions.encode(buffer);
    }
}

impl ToByte for TopicPartitionProduceRequest {
    fn encode<T:Write>(&self, buffer: &mut T) {
        self.topic.encode(buffer);
        self.partitions.encode(buffer);
    }
}

impl ToByte for PartitionProduceRequest {
    fn encode<T:Write>(&self, buffer: &mut T) {
        self.partition.encode(buffer);
        let mut buf = vec!();
        self.messageset.encode(&mut buf);
        buf.encode(buffer);
    }
}

impl ToByte for FetchRequest {
    fn encode<T:Write>(&self, buffer: &mut T) {
        self.header.encode(buffer);
        self.replica.encode(buffer);
        self.max_wait_time.encode(buffer);
        self.min_bytes.encode(buffer);
        self.topic_partitions.encode(buffer);
    }
}

impl ToByte for TopicPartitionFetchRequest {
    fn encode<T:Write>(&self, buffer: &mut T) {
        self.topic.encode(buffer);
        self.partitions.encode(buffer);
    }
}

impl ToByte for PartitionFetchRequest {
    fn encode<T:Write>(&self, buffer: &mut T) {
        self.partition.encode(buffer);
        self.offset.encode(buffer);
        self.max_bytes.encode(buffer);
    }
}


impl FromByte for HeaderResponse {
    type R = HeaderResponse;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        self.correlation.decode(buffer);
        Ok(())
    }
}

impl FromByte for MetadataResponse {
    type R = MetadataResponse;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        self.header.decode(buffer);
        self.brokers.decode(buffer);
        self.topics.decode(buffer);
        Ok(())
    }
}

impl FromByte for OffsetResponse {
    type R = OffsetResponse;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        self.header.decode(buffer);
        self.topic_partitions.decode(buffer);
        Ok(())
    }
}

impl FromByte for TopicPartitionOffsetResponse {
    type R = TopicPartitionOffsetResponse;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        self.topic.decode(buffer);
        self.partitions.decode(buffer);
        Ok(())
    }
}

impl FromByte for PartitionOffsetResponse {
    type R = PartitionOffsetResponse;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        self.partition.decode(buffer);
        self.error.decode(buffer);
        self.offset.decode(buffer);
        Ok(())
    }
}

impl FromByte for ProduceResponse {
    type R = ProduceResponse;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        self.header.decode(buffer);
        self.topic_partitions.decode(buffer);
        Ok(())
    }
}

impl FromByte for TopicPartitionProduceResponse {
    type R = TopicPartitionProduceResponse;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        self.topic.decode(buffer);
        self.partitions.decode(buffer);
        Ok(())
    }
}

impl FromByte for PartitionProduceResponse {
    type R = PartitionProduceResponse;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        self.partition.decode(buffer);
        self.error.decode(buffer);
        self.offset.decode(buffer);
        Ok(())
    }
}


impl FromByte for FetchResponse {
    type R = FetchResponse;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        self.header.decode(buffer);
        self.topic_partitions.decode(buffer);
        Ok(())
    }
}

impl FromByte for TopicPartitionFetchResponse {
    type R = TopicPartitionFetchResponse;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        self.topic.decode(buffer);
        self.partitions.decode(buffer);
        Ok(())
    }
}

impl FromByte for PartitionFetchResponse {
    type R = PartitionFetchResponse;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        self.partition.decode(buffer);
        self.error.decode(buffer);
        self.offset.decode(buffer);
        self.messageset.decode(buffer);
        Ok(())
    }
}
// For Helper Structs

impl FromByte for BrokerMetadata {
    type R = BrokerMetadata;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        self.nodeid.decode(buffer);
        self.host.decode(buffer);
        self.port.decode(buffer);
        Ok(())
    }
}

impl FromByte for TopicMetadata {
    type R = TopicMetadata;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        self.error.decode(buffer);
        self.topic.decode(buffer);
        self.partitions.decode(buffer);
        Ok(())
    }
}

impl FromByte for PartitionMetadata {
    type R = PartitionMetadata;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        self.error.decode(buffer);
        self.id.decode(buffer);
        self.leader.decode(buffer);
        self.replicas.decode(buffer);
        self.isr.decode(buffer);
        Ok(())
    }
}

impl ToByte for TopicPartitionOffsetRequest {
    fn encode<T: Write>(&self, buffer: &mut T) {
        self.topic.encode(buffer);
        self.partitions.encode(buffer);
    }
}

impl ToByte for PartitionOffsetRequest {
    fn encode<T: Write>(&self, buffer: &mut T) {
        self.partition.encode(buffer);
        self.time.encode(buffer);
        self.max_offsets.encode(buffer);
    }
}

impl ToByte for MessageSet {
    fn encode<T:Write>(&self, buffer: &mut T) {
        self.message.encode_nolen(buffer);
    }
}

impl FromByte for MessageSet {
    type R = MessageSet;

    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try!(self.message.decode(buffer));
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

impl ToByte for MessageSetInner {
    fn encode<T:Write>(&self, buffer: &mut T) {
        self.offset.encode(buffer);
        let mut buf = vec!();
        self.message.encode(&mut buf);
        buf.encode(buffer);
    }
}

impl FromByte for MessageSetInner {
    type R = MessageSetInner;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        self.offset.decode(buffer);
        self.messagesize.decode(buffer);
        try!(self.message.decode(buffer));
        Ok(())
    }
}

impl ToByte for Message {
    fn encode<T:Write>(&self, buffer: &mut T) {
        let mut buf = vec!();
        self.magic.encode(&mut buf);
        self.attributes.encode(&mut buf);
        if self.key.len() == 0 {
            let a: i32 = -1;
            a.encode(&mut buf);
        } else {
            self.key.encode(&mut buf);
        }

        self.value.encode(&mut buf);
        let (_, x) = buf.split_at(0);
        let crc = Crc32::tocrc(x) as i32;

        crc.encode(buffer);
        buf.encode_nolen(buffer);
    }
}

impl FromByte for Message {
    type R = Message;

    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {

        try!(self.crc.decode(buffer));
        try!(self.magic.decode(buffer));
        try!(self.attributes.decode(buffer));
        try!(self.key.decode(buffer));
        try!(self.value.decode(buffer));
        Ok(())
    }
}
