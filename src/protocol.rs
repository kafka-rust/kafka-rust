
use super::codecs::*;
use super::snappy;
use num::traits::ToPrimitive;
use byteorder::{ByteOrder, BigEndian, ReadBytesExt, WriteBytesExt, Result, Error};
use std::io::{Read, Write};
use std::fmt;
use std::io::Cursor;

const PRODUCE_KEY: i16 = 0;
const FETCH_KEY: i16 = 1;
const OFFSET_KEY: i16 = 2;
const METADATA_KEY: i16 = 3;
const OFFSET_COMMIT_KEY: i16 = 8;
const OFFSET_FETCH_KEY: i16 = 9;

const VERSION: i16 = 0;

const FETCH_DEFAULT_BLOCK_TIMEOUT: i32 = 1;
const FETCH_MAX_WAIT_TIME: i32 = 100;
const FETCH_MIN_BYTES: i32 = 4096;
const FETCH_BUFFER_SIZE_BYTES: i32 = 4096;
const MAX_FETCH_BUFFER_SIZE_BYTES: i32 = FETCH_BUFFER_SIZE_BYTES * 8;


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
    pub topic_partitions: Vec<TopicPartitionOffsetRequest>
}

#[derive(Default)]
#[derive(Debug)]
#[derive(Clone)]
pub struct TopicPartitionOffsetRequest {
    pub topic: String,
    pub partitions: Vec<PartitionOffsetRequest>
}

#[derive(Default)]
#[derive(Debug)]
#[derive(Clone)]
pub struct PartitionOffsetRequest {
    pub partition: i32,
    pub time: i64,
    pub max_offsets: i32
}


#[derive(Default)]
#[derive(Debug)]
#[derive(Clone)]
pub struct OffsetResponse {
    pub header: HeaderResponse,
    pub topic_partitions: Vec<TopicPartitionOffsetResponse>,
}

#[derive(Default)]
#[derive(Debug)]
#[derive(Clone)]
pub struct TopicPartitionOffsetResponse {
    pub topic: String,
    pub partitions: Vec<PartitionOffsetResponse>,
}

#[derive(Default)]
#[derive(Debug)]
#[derive(Clone)]
pub struct PartitionOffsetResponse {
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
    pub topic_partitions: Vec<TopicPartitionFetchRequest>
}

#[derive(Default)]
#[derive(Debug)]
#[derive(Clone)]
pub struct TopicPartitionFetchRequest {
    pub topic: String,
    pub partitions: Vec<PartitionFetchRequest>
}

#[derive(Default)]
#[derive(Debug)]
#[derive(Clone)]
pub struct PartitionFetchRequest {
    pub partition: i32,
    pub offset: i64,
    pub max_bytes: i32
}


#[derive(Default)]
#[derive(Debug)]
#[derive(Clone)]
pub struct FetchResponse {
    pub header: HeaderResponse,
    pub topic_partitions: Vec<TopicPartitionFetchResponse>,
}

#[derive(Default)]
#[derive(Debug)]
#[derive(Clone)]
pub struct TopicPartitionFetchResponse {
    pub topic: String,
    pub partitions: Vec<PartitionFetchResponse>,
}

#[derive(Default)]
#[derive(Debug)]
#[derive(Clone)]
pub struct PartitionFetchResponse {
    pub partition: i32,
    pub error: i16,
    pub offset: i64,
    pub messageset: MessageSet
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
    pub message: Vec<MessageSetInner>
}

#[derive(Default)]
#[derive(Debug)]
#[derive(Clone)]
pub struct MessageSetInner {
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
                                    TopicPartitionOffsetRequest::new(topic, partitions.to_vec(), &time))
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

impl FetchRequest {
    pub fn new(topic_partitions: &Vec<(String, Vec<(i32, i64)>)>,
               correlation: i32, clientid: &String) -> FetchRequest{
        FetchRequest{
            header: HeaderRequest{key: FETCH_KEY, correlation: correlation,
                                  clientid: clientid.clone(), version: VERSION},
            replica: -1,
            max_wait_time: FETCH_MAX_WAIT_TIME,
            min_bytes: FETCH_MIN_BYTES,
            topic_partitions: topic_partitions.iter()
                                .map(|&(ref topic, ref partitions)|
                                TopicPartitionFetchRequest::new(topic, partitions.to_vec()))
                                .collect()
        }
    }

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
    pub fn new(topic: &String, partitions: Vec<(i32, i64)>) -> TopicPartitionFetchRequest{
        TopicPartitionFetchRequest {
            topic: topic.clone(),
            partitions: partitions.iter()
                                  .map(|&(ref partition, ref offset)| PartitionFetchRequest:: new(partition, offset))
                                  .collect()
        }
    }

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
    pub fn get_messages(& self) -> Vec<(i64, String)>{
        let mut x: Vec<(i64, String)> = vec!();
        for tp in self.topic_partitions.iter() { // MessageSetInner
            for p in tp.partitions.iter(){
                for mi in p.clone().messageset.message {
                    for (o, m) in mi.message.get_messages(mi.offset) {
                        x.push((o, String::from_utf8(m).unwrap()));
                    }
                }
            }
        }
        x
    }
}

impl Message {
    fn get_messages(& self, offset: i64) -> Vec<(i64, Vec<u8>)>{
        let mut res: Vec<(i64, Vec<u8>)> = vec!();
        match self.attributes {
            0 => vec!((offset, self.value.to_vec())),
            1 => vec!((offset, self.value.to_vec())),
            2 => {
                // SNAPPY
                let mut buffer = Cursor::new(self.value.to_vec());
                let header = snappy::SnappyHeader::decode_new(&mut buffer);
                println!("Header = {:?}", header);
                let mut count = 0;
                loop {
                    count = count+1;
                    println!("Count = {}", count);
                    match snappy::SnappyMessage::decode_new(&mut buffer) {
                        Ok(x) => {
                            match snappy::uncompress(&x.message) {
                                Some(m) => {
                                    println!("{}. Uncompressed message. Decoding to Messageset", count);
                                    match MessageSet::decode_new(&mut Cursor::new(m)) {
                                        Ok(ms) => {
                                            //println!("{}. Got MS {:?}", count, ms);
                                            ms.get_messages(&mut res);
                                        },
                                        Err(e) => {
                                            println!("{}. Uncompressed message. Error decoding to Messageset", count);
                                            break;
                                        }
                                    }
                                },
                                None => {
                                    println! ("{}. Error with snappy::uncompress", count);
                                    break
                                }
                            }
                        },
                        Err(e) => {
                            println!("SnappyMessage decode error {:?}", e);
                            break;
                        }
                    }
                }
                //println!("Message = {:?}", res);

                res

            },
            _ => vec!()
        }
    }
}

impl MessageSet {
    fn get_messages(& self, res: &mut Vec<(i64, Vec<u8>)>){
        //let mut x: Vec<(i64, Vec<u8>)> = vec!();
        for mi in self.message.iter() { // MessageSetInner
            for (o, m) in mi.message.get_messages(mi.offset) {
                res.push((o, m));
            }
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

    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        self.correlation.decode(buffer);
        Ok(())
    }
}

impl FromByte for MetadataResponse {
    type R = MetadataResponse;

    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        self.header.decode(buffer);
        self.brokers.decode(buffer);
        self.topics.decode(buffer);
        Ok(())
    }
}

impl FromByte for OffsetResponse {
    type R = OffsetResponse;

    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        self.header.decode(buffer);
        self.topic_partitions.decode(buffer);
        Ok(())
    }
}

impl FromByte for TopicPartitionOffsetResponse {
    type R = TopicPartitionOffsetResponse;

    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        self.topic.decode(buffer);
        self.partitions.decode(buffer);
        Ok(())
    }
}

impl FromByte for PartitionOffsetResponse {
    type R = PartitionOffsetResponse;

    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        self.partition.decode(buffer);
        self.error.decode(buffer);
        self.offset.decode(buffer);
        Ok(())
    }
}

impl FromByte for FetchResponse {
    type R = FetchResponse;

    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        self.header.decode(buffer);
        self.topic_partitions.decode(buffer);
        Ok(())
    }
}

impl FromByte for TopicPartitionFetchResponse {
    type R = TopicPartitionFetchResponse;

    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        self.topic.decode(buffer);
        self.partitions.decode(buffer);
        Ok(())
    }
}

impl FromByte for PartitionFetchResponse {
    type R = PartitionFetchResponse;

    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        self.partition.decode(buffer);
        self.error.decode(buffer);
        self.offset.decode(buffer);
        println!("1");
        self.messageset.decode(buffer);
        Ok(())
    }
}
// For Helper Structs

impl FromByte for BrokerMetadata {
    type R = BrokerMetadata;

    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        self.nodeid.decode(buffer);
        self.host.decode(buffer);
        self.port.decode(buffer);
        Ok(())
    }
}

impl FromByte for TopicMetadata {
    type R = TopicMetadata;

    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        self.error.decode(buffer);
        self.topic.decode(buffer);
        self.partitions.decode(buffer);
        Ok(())
    }
}

impl FromByte for PartitionMetadata {
    type R = PartitionMetadata;

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
        self.message.encode(buffer);
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
                Err(e) => break
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
        self.messagesize.encode(buffer);
        self.message.encode(buffer);
    }
}

impl FromByte for MessageSetInner {
    type R = MessageSetInner;

    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        println!("MessageSetInner");
        self.offset.decode(buffer);
        println!("offset = {}", self.offset);

        self.messagesize.decode(buffer);
        println!("messagesize = {}", self.messagesize);
        try!(self.message.decode(buffer));
        Ok(())
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
    type R = Message;

    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        println!("\tMessage");
        try!(self.crc.decode(buffer));
        println!("\tcrc = {}", self.crc);
        try!(self.magic.decode(buffer));
        println!("\tmagic = {}", self.magic);
        try!(self.attributes.decode(buffer));
        println!("\tattributes = {}", self.attributes);
        try!(self.key.decode(buffer));
        println!("\tkey = {}", 1);
        try!(self.value.decode(buffer));
        Ok(())
    }
}
