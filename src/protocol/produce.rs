use std::io::{Read, Write};

use crate::codecs::{FromByte, ToByte};
#[cfg(feature = "gzip")]
use crate::compression::gzip;
#[cfg(feature = "snappy")]
use crate::compression::snappy;
use crate::compression::Compression;

use crate::error::{KafkaCode, Result};

use super::to_crc;
use super::{HeaderRequest, HeaderResponse};
use super::{API_KEY_PRODUCE, API_VERSION};
use crate::producer::{ProduceConfirm, ProducePartitionConfirm};

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
    pub fn new(
        required_acks: i16,
        timeout: i32,
        correlation_id: i32,
        client_id: &'a str,
        compression: Compression,
    ) -> ProduceRequest<'a, 'b> {
        ProduceRequest {
            header: HeaderRequest::new(API_KEY_PRODUCE, API_VERSION, correlation_id, client_id),
            required_acks,
            timeout,
            topic_partitions: vec![],
            compression,
        }
    }

    pub fn add(
        &mut self,
        topic: &'b str,
        partition: i32,
        key: Option<&'b [u8]>,
        value: Option<&'b [u8]>,
    ) {
        for tp in &mut self.topic_partitions {
            if tp.topic == topic {
                tp.add(partition, key, value);
                return;
            }
        }
        let mut tp = TopicPartitionProduceRequest::new(topic, self.compression);
        tp.add(partition, key, value);
        self.topic_partitions.push(tp);
    }
}

impl<'a> TopicPartitionProduceRequest<'a> {
    pub fn new(topic: &'a str, compression: Compression) -> TopicPartitionProduceRequest<'a> {
        TopicPartitionProduceRequest {
            topic,
            partitions: vec![],
            compression,
        }
    }

    pub fn add(&mut self, partition: i32, key: Option<&'a [u8]>, value: Option<&'a [u8]>) {
        if let Some(pp) = self
            .partitions
            .iter_mut()
            .find(|pp| pp.partition == partition)
        {
            pp.add(key, value);
            return;
        }

        self.partitions
            .push(PartitionProduceRequest::new(partition, key, value));
    }
}

impl<'a> PartitionProduceRequest<'a> {
    pub fn new<'b>(
        partition: i32,
        key: Option<&'b [u8]>,
        value: Option<&'b [u8]>,
    ) -> PartitionProduceRequest<'b> {
        let mut r = PartitionProduceRequest {
            partition,
            messages: Vec::new(),
        };
        r.add(key, value);
        r
    }

    pub fn add(&mut self, key: Option<&'a [u8]>, value: Option<&'a [u8]>) {
        self.messages.push(MessageProduceRequest::new(key, value));
    }
}

impl<'a, 'b> ToByte for ProduceRequest<'a, 'b> {
    fn encode<W: Write>(&self, buffer: &mut W) -> Result<()> {
        try_multi!(
            self.header.encode(buffer),
            self.required_acks.encode(buffer),
            self.timeout.encode(buffer),
            self.topic_partitions.encode(buffer)
        )
    }
}

impl<'a> ToByte for TopicPartitionProduceRequest<'a> {
    // render: TopicName [Partition MessageSetSize MessageSet]
    fn encode<W: Write>(&self, buffer: &mut W) -> Result<()> {
        self.topic.encode(buffer)?;
        (self.partitions.len() as i32).encode(buffer)?;
        for e in &self.partitions {
            e._encode(buffer, self.compression)?;
        }
        Ok(())
    }
}

impl<'a> PartitionProduceRequest<'a> {
    // render: Partition MessageSetSize MessageSet
    //
    // MessetSet => [Offset MessageSize Message]
    // MessageSets are not preceded by an int32 like other array elements in the protocol.
    fn _encode<W: Write>(&self, out: &mut W, compression: Compression) -> Result<()> {
        self.partition.encode(out)?;

        // ~ render the whole MessageSet first to a temporary buffer
        let mut buf = Vec::new();
        for msg in &self.messages {
            msg._encode_to_buf(&mut buf, MESSAGE_MAGIC_BYTE, 0)?;
        }
        match compression {
            Compression::NONE => {
                // ~ nothing to do
            }
            #[cfg(feature = "gzip")]
            Compression::GZIP => {
                let cdata = gzip::compress(&buf)?;
                render_compressed(&mut buf, &cdata, compression)?;
            }
            #[cfg(feature = "snappy")]
            Compression::SNAPPY => {
                let cdata = snappy::compress(&buf)?;
                render_compressed(&mut buf, &cdata, compression)?;
            }
        }
        buf.encode(out)
    }
}

// ~ A helper method to render `cdata` into `out` as a compressed message.
// ~ `out` is first cleared and then populated with the rendered message.
#[cfg(any(feature = "snappy", feature = "gzip"))]
fn render_compressed(out: &mut Vec<u8>, cdata: &[u8], compression: Compression) -> Result<()> {
    out.clear();
    let cmsg = MessageProduceRequest::new(None, Some(cdata));
    cmsg._encode_to_buf(out, MESSAGE_MAGIC_BYTE, compression as i8)
}

impl<'a> MessageProduceRequest<'a> {
    fn new<'b>(key: Option<&'b [u8]>, value: Option<&'b [u8]>) -> MessageProduceRequest<'b> {
        MessageProduceRequest { key, value }
    }

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
        (0i64).encode(buffer)?; // offset in the response request can be anything

        let size_pos = buffer.len();
        let mut size: i32 = 0;
        size.encode(buffer)?; // reserve space for the size to be computed later

        let crc_pos = buffer.len(); // remember the position where to update the crc later
        let mut crc: i32 = 0;
        crc.encode(buffer)?; // reserve space for the crc to be computed later
        magic.encode(buffer)?;
        attributes.encode(buffer)?;
        self.key.encode(buffer)?;
        self.value.encode(buffer)?;

        // compute the crc and store it back in the reserved space
        crc = to_crc(&buffer[(crc_pos + 4)..]) as i32;
        crc.encode(&mut &mut buffer[crc_pos..crc_pos + 4])?;

        // compute the size and store it back in the reserved space
        size = (buffer.len() - crc_pos) as i32;
        size.encode(&mut &mut buffer[size_pos..size_pos + 4])?;

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
    pub topic_partitions: Vec<TopicPartitionProduceResponse>,
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
    pub offset: i64,
}

impl ProduceResponse {
    pub fn get_response(self) -> Vec<ProduceConfirm> {
        self.topic_partitions
            .into_iter()
            .map(TopicPartitionProduceResponse::get_response)
            .collect()
    }
}

impl TopicPartitionProduceResponse {
    pub fn get_response(self) -> ProduceConfirm {
        let Self { topic, partitions } = self;
        let partition_confirms = partitions
            .iter()
            .map(PartitionProduceResponse::get_response)
            .collect();

        ProduceConfirm {
            topic,
            partition_confirms,
        }
    }
}

impl PartitionProduceResponse {
    pub fn get_response(&self) -> ProducePartitionConfirm {
        ProducePartitionConfirm {
            partition: self.partition,
            offset: match KafkaCode::from_protocol(self.error) {
                None => Ok(self.offset),
                Some(code) => Err(code),
            },
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
        try_multi!(self.topic.decode(buffer), self.partitions.decode(buffer))
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
