use std::io::{Read, Write};

use codecs::{ToByte, FromByte};
use compression::Compression;
use error::{Error, Result};
use gzip;
use num::traits::FromPrimitive;
use snappy;
use utils::TopicPartitionOffsetError;

use super::{HeaderRequest, HeaderResponse};
use super::{API_KEY_PRODUCE, API_VERSION};
use super::tocrc;

/// The magic byte (a.k.a version) we use for sent messages.
const MESSAGE_MAGIC_BYTE: i8 = 0;

#[derive(Debug)]
pub struct ProduceRequest<'a, 'b> {
    pub header: HeaderRequest<'a>,
    pub required_acks: i16,
    pub timeout: i32,
    pub topic_partitions: Vec<TopicPartitionProduceRequest<'b>>,
    pub compression: Compression,
}

#[derive(Debug)]
pub struct TopicPartitionProduceRequest<'a> {
    pub topic: &'a str,
    pub partitions: Vec<PartitionProduceRequest<'a>>,
    pub compression: Compression,
}

#[derive(Debug)]
pub struct PartitionProduceRequest<'a> {
    pub partition: i32,
    pub messages: Vec<MessageProduceRequest<'a>>,
}

#[derive(Debug)]
pub struct MessageProduceRequest<'a> {
    key: Option<&'a [u8]>,
    value: Option<&'a [u8]>,
}

impl<'a, 'b> ProduceRequest<'a, 'b> {
    pub fn new(required_acks: i16, timeout: i32,
               correlation_id: i32, client_id: &'a str,
               compression: Compression)
               -> ProduceRequest<'a, 'b>
    {
        ProduceRequest {
            header: HeaderRequest::new(
                API_KEY_PRODUCE, API_VERSION, correlation_id, client_id),
            required_acks: required_acks,
            timeout: timeout,
            topic_partitions: vec!(),
            compression: compression
        }
    }

    pub fn add(&mut self, topic: &'b str, partition: i32, message: &'b [u8]) {
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

    pub fn add(&mut self, partition: i32, message: &'a[u8]) {
        for pp in &mut self.partitions {
            if pp.partition == partition {
                pp.add(message);
                return;
            }
        }
        self.partitions.push(PartitionProduceRequest::new(partition, message))
    }
}

impl<'a> PartitionProduceRequest<'a> {
    pub fn new(partition: i32, message: &[u8]) -> PartitionProduceRequest {
        PartitionProduceRequest {
            partition: partition,
            messages: vec![MessageProduceRequest::from_value(message)],
        }
    }

    pub fn add(&mut self, message: &'a [u8]) {
        self.messages.push(MessageProduceRequest::from_value(message))
    }
}

impl<'a> MessageProduceRequest<'a> {
    fn from_value(value: &[u8]) -> MessageProduceRequest {
        MessageProduceRequest {
            key: None,
            value: Some(value),
        }
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
    // render: TopicName [Partition MessageSetSize MessageSet]
    fn encode<W: Write>(&self, buffer: &mut W) -> Result<()> {
        try!(self.topic.encode(buffer));
        try!((self.partitions.len() as i32).encode(buffer));
        for e in &self.partitions {
            try!(e._encode(buffer, self.compression))
        }
        Ok(())
    }
}

impl<'a> PartitionProduceRequest<'a> {
    // render: Partition MessageSetSize MessageSet
    //
    // MessetSet => [Offset MessageSize Message]
    // MessageSets are not preceded by an int32 like other array elements in the protocol.
    fn _encode<W: Write>(&self, out: &mut W, compression: Compression)
                         -> Result<()> {
        try!(self.partition.encode(out));

        // ~ render the whole MessageSet first to a temporary buffer
        let mut buf = Vec::new();
        for msg in &self.messages {
            try!(msg._encode_to_buf(&mut buf, MESSAGE_MAGIC_BYTE, 0));
        }
        if let Compression::NONE = compression {
            buf.encode(out)
        } else {
            // ~ Compress the so far produced MessageSet (into a newly allocated buffer)
            let cdata = match compression {
                Compression::GZIP => try!(gzip::compress(&buf)),
                Compression::SNAPPY => try!(snappy::compress(&buf)),
                c => panic!("Unknown compression type: {:?}", c),
            };
            // ~ now wrap the compressed data into a new MessagesSet
            // with exactly one Message
            buf.clear();
            let cmsg = MessageProduceRequest::from_value(&cdata);
            try!(cmsg._encode_to_buf(&mut buf, MESSAGE_MAGIC_BYTE, compression as i8));
            buf.encode(out)
        }
    }
}

impl<'a> MessageProduceRequest<'a> {
    // render a single message as: Offset MessageSize Message
    //
    // Offset => int64 (always encoded as zero here)
    // MessageSize => int32
    // Message => Crc MagicByte Attributes Key Value
    // Crc => int32
    // MagicByte => int8
    // Attributes => int8
    // Key => bytes
    // Value => bytes
    //
    // note: the rendered data corresponds to a single MessageSet in the kafka protocol
    fn _encode_to_buf(&self, buffer: &mut Vec<u8>, magic: i8, attributes: i8) -> Result<()> {

        try!((0i64).encode(buffer)); // offset in the response request can be anything

        let size_pos = buffer.len();
        let mut size: i32 = 0;
        try!(size.encode(buffer)); // reserve space for the size to be computed later

        let crc_pos = buffer.len(); // remember the position where to update the crc later
        let mut crc: i32 = 0;
        try!(crc.encode(buffer)); // reserve space for the crc to be computed later
        try!(magic.encode(buffer));
        try!(attributes.encode(buffer));
        try!(self.key.encode(buffer));
        try!(self.value.encode(buffer));

        // compute the crc and store it back in the reserved space
        crc = tocrc(&buffer[(crc_pos + 4)..]) as i32;
        try!(crc.encode(&mut &mut buffer[crc_pos .. crc_pos + 4]));

        // compute the size and store it back in the reserved space
        size = (buffer.len() - crc_pos) as i32;
        try!(size.encode(&mut &mut buffer[size_pos .. size_pos + 4]));

        Ok(())
    }
}

impl<'a> ToByte for Option<&'a [u8]> {
    fn encode<W: Write>(&self, buffer: &mut W) -> Result<()> {
        match *self {
            Some(xs) => xs.encode(buffer),
            None => (-1i32).encode(buffer),
        }
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

