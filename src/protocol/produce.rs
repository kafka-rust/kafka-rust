use std::io::{Read, Write};

use num::traits::FromPrimitive;

use codecs::{ToByte, FromByte};
use compression::Compression;
use error::{Error, Result};
use utils::TopicPartitionOffsetError;
use super::{HeaderRequest_, HeaderResponse, MessageSet, Message};
use super::{API_KEY_PRODUCE, API_VERSION};


#[derive(Debug)]
pub struct ProduceRequest<'a, 'b> {
    pub header: HeaderRequest_<'a>,
    pub required_acks: i16,
    pub timeout: i32,
    pub topic_partitions: Vec<TopicPartitionProduceRequest<'b>>,
    pub compression: Compression
}

#[derive(Debug)]
pub struct TopicPartitionProduceRequest<'a> {
    pub topic: &'a str,
    pub partitions: Vec<PartitionProduceRequest>,
    pub compression: Compression
}

#[derive(Debug)]
pub struct PartitionProduceRequest {
    pub partition: i32,
    pub messageset_size: i32,
    pub messageset: MessageSet,
    pub compression: Compression
}

impl<'a, 'b> ProduceRequest<'a, 'b> {
    pub fn new(required_acks: i16, timeout: i32,
               correlation_id: i32, client_id: &'a str,
               compression: Compression)
               -> ProduceRequest<'a, 'b>
    {
        ProduceRequest {
            header: HeaderRequest_::new(
                API_KEY_PRODUCE, API_VERSION, correlation_id, client_id),
            required_acks: required_acks,
            timeout: timeout,
            topic_partitions: vec!(),
            compression: compression
        }
    }

    pub fn add(&mut self, topic: &'b str, partition: i32, message: Vec<u8>) {
        for tp in &mut self.topic_partitions {
            if tp.topic == topic {
                tp.add(partition, message);
                return;
            }
        }
        let mut tp = TopicPartitionProduceRequest::new(topic, self.compression);
        tp.add(partition, message);
        self.topic_partitions.push(tp);
    }
}

impl<'a> TopicPartitionProduceRequest<'a> {
    pub fn new(topic: &'a str, compression: Compression) -> TopicPartitionProduceRequest<'a> {
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
        self.partitions.push(PartitionProduceRequest::new(partition, message, self.compression))
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

impl<'a, 'b> ToByte for ProduceRequest<'a, 'b> {
    fn encode<W: Write>(&self, buffer: &mut W) -> Result<()> {
        try_multi!(self.header.encode(buffer),
                   self.required_acks.encode(buffer),
                   self.timeout.encode(buffer),
                   self.topic_partitions.encode(buffer))
    }
}

impl<'a> ToByte for TopicPartitionProduceRequest<'a> {
    fn encode<W: Write>(&self, buffer: &mut W) -> Result<()> {
        try_multi!(self.topic.encode(buffer),
                   self.partitions.encode(buffer))
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

/// ~ a helper struct for serializing a compressed messages set of a
/// ProduceRequest
pub struct MessageSetOuter {
    pub offset: i64,
    pub messagesize: i32,
    pub message: Vec<u8>
}

impl MessageSetOuter {
    fn new(message: Vec<u8>) -> MessageSetOuter {
        MessageSetOuter{offset:0, messagesize:0, message: message}
    }
}

impl ToByte for MessageSetOuter {
    fn encode<T:Write>(&self, buffer: &mut T) -> Result<()> {
        let mut buf = vec!();
        try_multi!(self.offset.encode(buffer),
                   self.message.encode(&mut buf),
                   buf.encode_nolen(buffer))
    }
}

// --------------------------------------------------------------------

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

impl ProduceResponse {
    pub fn get_response(&self) -> Vec<TopicPartitionOffsetError> {
        self.topic_partitions
            .iter()
            .flat_map(|ref tp| tp.get_response(tp.topic.clone()))
            .collect()
    }
}

impl TopicPartitionProduceResponse {
    pub fn get_response(&self, topic: String) -> Vec<TopicPartitionOffsetError> {
        self.partitions
            .iter()
            .map(|ref p| p.get_response(topic.clone()))
            .collect()
    }
}

impl PartitionProduceResponse {
    pub fn get_response(&self, topic: String) -> TopicPartitionOffsetError {
        TopicPartitionOffsetError{
            topic: topic,
            partition: self.partition,
            offset:self.offset,
            error: Error::from_i16(self.error)
        }
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

