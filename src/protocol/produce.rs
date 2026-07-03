use std::io::{Read, Write};

use crate::codecs::{FromByte, ToByte};
use crate::compression::Compression;
#[cfg(feature = "gzip")]
use crate::compression::gzip;
#[cfg(feature = "snappy")]
use crate::compression::snappy;

use crate::error::{KafkaCode, Result};

use super::to_crc;
use super::{API_KEY_PRODUCE, API_VERSION};
use super::{HeaderRequest, HeaderResponse};
use crate::producer::{ProduceConfirm, ProducePartitionConfirm};

/// The magic byte (a.k.a version) we use for sent messages.
const MESSAGE_MAGIC_BYTE: i8 = 0;

#[cfg(feature = "producer_timestamp")]
/// The magic byte (a.k.a version) used to specify message type with timestamp.
// In versions prior to Kafka 0.10, the only supported message format version (which is indicated in the magic value) was 0.
// Message format version 1 was introduced with timestamp support in version 0.10.
// See https://kafka.apache.org/documentation/#messageset
const MESSAGE_MAGIC_BYTE_WITH_TIMESTAMP: i8 = 1;

#[derive(Debug)]
pub struct ProduceRequest<'a, 'b> {
    pub header: HeaderRequest<'a>,
    pub required_acks: i16,
    pub timeout: i32,
    pub topic_partitions: Vec<TopicPartitionProduceRequest<'b>>,
    pub compression: Compression,
    pub timestamp: Option<ProducerTimestamp>,
}

#[derive(Debug)]
pub struct TopicPartitionProduceRequest<'a> {
    pub topic: &'a str,
    pub partitions: Vec<PartitionProduceRequest<'a>>,
    pub compression: Compression,
    #[allow(unused)]
    pub timestamp: Option<ProducerTimestamp>,
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
    #[cfg_attr(not(feature = "producer_timestamp"), allow(dead_code))]
    timestamp: Option<i64>,
}

#[allow(unused)]
#[derive(Debug, Copy, Clone)]
#[repr(u8)]
pub enum ProducerTimestamp {
    CreateTime = 0,
    LogAppendTime = 8, // attributes bit 3 should be set to 1 in case of the LogAppend param. See https://kafka.apache.org/39/documentation/#messageset
}

impl<'a, 'b> ProduceRequest<'a, 'b> {
    pub fn new(
        required_acks: i16,
        timeout: i32,
        correlation_id: i32,
        client_id: &'a str,
        compression: Compression,
        #[cfg(feature = "producer_timestamp")] timestamp: Option<ProducerTimestamp>,
    ) -> ProduceRequest<'a, 'b> {
        ProduceRequest {
            header: HeaderRequest::new(API_KEY_PRODUCE, API_VERSION, correlation_id, client_id),
            required_acks,
            timeout,
            topic_partitions: vec![],
            compression,
            #[cfg(feature = "producer_timestamp")]
            timestamp,
            #[cfg(not(feature = "producer_timestamp"))]
            timestamp: None,
        }
    }

    pub fn add(
        &mut self,
        topic: &'b str,
        partition: i32,
        key: Option<&'b [u8]>,
        value: Option<&'b [u8]>,
        timestamp: Option<i64>,
    ) {
        for tp in &mut self.topic_partitions {
            if tp.topic == topic {
                tp.add(partition, key, value, timestamp);
                return;
            }
        }
        let mut tp = TopicPartitionProduceRequest::new(topic, self.compression, self.timestamp);
        tp.add(partition, key, value, timestamp);
        self.topic_partitions.push(tp);
    }
}

impl<'a> TopicPartitionProduceRequest<'a> {
    pub fn new(
        topic: &'a str,
        compression: Compression,
        timestamp: Option<ProducerTimestamp>,
    ) -> TopicPartitionProduceRequest<'a> {
        TopicPartitionProduceRequest {
            topic,
            partitions: vec![],
            compression,
            timestamp,
        }
    }

    pub fn add(
        &mut self,
        partition: i32,
        key: Option<&'a [u8]>,
        value: Option<&'a [u8]>,
        timestamp: Option<i64>,
    ) {
        if let Some(pp) = self
            .partitions
            .iter_mut()
            .find(|pp| pp.partition == partition)
        {
            pp.add(key, value, timestamp);
            return;
        }

        self.partitions
            .push(PartitionProduceRequest::new(partition, key, value, timestamp));
    }
}

impl<'a> PartitionProduceRequest<'a> {
    pub fn new<'b>(
        partition: i32,
        key: Option<&'b [u8]>,
        value: Option<&'b [u8]>,
        timestamp: Option<i64>,
    ) -> PartitionProduceRequest<'b> {
        let mut r = PartitionProduceRequest {
            partition,
            messages: Vec::new(),
        };
        r.add(key, value, timestamp);
        r
    }

    pub fn add(&mut self, key: Option<&'a [u8]>, value: Option<&'a [u8]>, timestamp: Option<i64>) {
        self.messages
            .push(MessageProduceRequest::new(key, value, timestamp));
    }
}

impl ToByte for ProduceRequest<'_, '_> {
    fn encode<W: Write>(&self, buffer: &mut W) -> Result<()> {
        try_multi!(
            self.header.encode(buffer),
            self.required_acks.encode(buffer),
            self.timeout.encode(buffer),
            self.topic_partitions.encode(buffer)
        )
    }
}

impl ToByte for TopicPartitionProduceRequest<'_> {
    // render: TopicName [Partition MessageSetSize MessageSet]
    fn encode<W: Write>(&self, buffer: &mut W) -> Result<()> {
        self.topic.encode(buffer)?;
        (self.partitions.len() as i32).encode(buffer)?;
        for e in &self.partitions {
            #[cfg(not(feature = "producer_timestamp"))]
            e._encode(buffer, self.compression)?;

            #[cfg(feature = "producer_timestamp")]
            {
                let has_explicit_message_timestamps =
                    e.messages.iter().any(|message| message.timestamp.is_some());
                match (self.timestamp, has_explicit_message_timestamps) {
                    (Some(timestamp), _) => {
                        e._encode_with_timestamp(buffer, self.compression, Some(timestamp))?;
                    }
                    (None, true) => {
                        e._encode_with_timestamp(
                            buffer,
                            self.compression,
                            Some(ProducerTimestamp::CreateTime),
                        )?;
                    }
                    (None, false) => e._encode(buffer, self.compression)?,
                }
            }
        }
        Ok(())
    }
}

impl PartitionProduceRequest<'_> {
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

    #[cfg(feature = "producer_timestamp")]
    fn _encode_with_timestamp<W: Write>(
        &self,
        out: &mut W,
        compression: Compression,
        timestamp_mode: Option<ProducerTimestamp>,
    ) -> Result<()> {
        self.partition.encode(out)?;

        let timestamp_attributes = timestamp_mode.map_or(0, |timestamp| timestamp as i8);
        // ~ render the whole MessageSet first to a temporary buffer
        let mut buf = Vec::new();
        let mut wrapper_timestamp: Option<i64> = None;
        for msg in &self.messages {
            let now = chrono::Utc::now().timestamp_millis();
            let timestamp = msg.timestamp.unwrap_or(now);
            wrapper_timestamp = Some(wrapper_timestamp.map_or(timestamp, |current| current.max(timestamp)));
            let attributes = if msg.timestamp.is_some() {
                ProducerTimestamp::CreateTime as i8
            } else {
                timestamp_attributes
            };
            msg._encode_to_buf_with_timestamp(
                &mut buf,
                MESSAGE_MAGIC_BYTE_WITH_TIMESTAMP,
                attributes,
                timestamp,
            )?;
        }
        match compression {
            Compression::NONE => {
                // ~ nothing to do
            }
            #[cfg(feature = "gzip")]
            Compression::GZIP => {
                let cdata = gzip::compress(&buf)?;
                render_compressed_with_timestamp(
                    &mut buf,
                    &cdata,
                    compression as i8 | timestamp_attributes,
                    wrapper_timestamp.unwrap_or_else(|| chrono::Utc::now().timestamp_millis()),
                )?;
            }
            #[cfg(feature = "snappy")]
            Compression::SNAPPY => {
                let cdata = snappy::compress(&buf)?;
                render_compressed_with_timestamp(
                    &mut buf,
                    &cdata,
                    compression as i8 | timestamp_attributes,
                    wrapper_timestamp.unwrap_or_else(|| chrono::Utc::now().timestamp_millis()),
                )?;
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
    let cmsg = MessageProduceRequest::new(None, Some(cdata), None);

    cmsg._encode_to_buf(out, MESSAGE_MAGIC_BYTE, compression as i8)
}

// available when feature producer_timestamp and snappy || gzip are available
// ~ A helper method to render `cdata` into `out` as a compressed message.
// ~ `out` is first cleared and then populated with the rendered message.
#[cfg(all(
    feature = "producer_timestamp",
    any(feature = "snappy", feature = "gzip")
))]
fn render_compressed_with_timestamp(
    out: &mut Vec<u8>,
    cdata: &[u8],
    attributes: i8,
    timestamp: i64,
) -> Result<()> {
    out.clear();
    let cmsg = MessageProduceRequest::new(None, Some(cdata), None);

    cmsg._encode_to_buf_with_timestamp(
        out,
        MESSAGE_MAGIC_BYTE_WITH_TIMESTAMP,
        attributes,
        timestamp,
    )
}

impl MessageProduceRequest<'_> {
    fn new<'b>(
        key: Option<&'b [u8]>,
        value: Option<&'b [u8]>,
        timestamp: Option<i64>,
    ) -> MessageProduceRequest<'b> {
        MessageProduceRequest {
            key,
            value,
            timestamp,
        }
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

    #[cfg(feature = "producer_timestamp")]
    // render a single message as: Offset MessageSize Message
    //
    // Offset => int64 (always encoded as zero here)
    // MessageSize => int32
    // Message => Crc MagicByte Attributes Key Value
    // Crc => int32
    // MagicByte => int8
    // Attributes => int8
    // Timestamp => int64
    // Key => bytes
    // Value => bytes
    //
    // note: the rendered data corresponds to a single MessageSet in the kafka protocol
    fn _encode_to_buf_with_timestamp(
        &self,
        buffer: &mut Vec<u8>,
        magic: i8,
        attributes: i8,
        timestamp: i64,
    ) -> Result<()> {
        (0i64).encode(buffer)?; // offset in the response request can be anything

        let size_pos = buffer.len();
        let mut size: i32 = 0;
        size.encode(buffer)?; // reserve space for the size to be computed later

        let crc_pos = buffer.len(); // remember the position where to update the crc later
        let mut crc: i32 = 0;
        crc.encode(buffer)?; // reserve space for the crc to be computed later
        magic.encode(buffer)?;
        attributes.encode(buffer)?;
        timestamp.encode(buffer)?;
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

#[cfg(all(test, feature = "producer_timestamp"))]
mod tests {
    use super::{PartitionProduceRequest, ProducerTimestamp};
    use crate::compression::Compression;
    use byteorder::{BigEndian, ReadBytesExt};
    use std::io::{Cursor, Read};

    fn decode_first_message_attributes_and_timestamp(encoded_partition: &[u8]) -> (i8, i64) {
        let mut cursor = Cursor::new(encoded_partition);

        let _partition = cursor.read_i32::<BigEndian>().unwrap();
        let message_set_size = cursor.read_i32::<BigEndian>().unwrap();

        let mut message_set = vec![0u8; message_set_size as usize];
        cursor.read_exact(&mut message_set).unwrap();

        let mut message_cursor = Cursor::new(message_set);
        let _offset = message_cursor.read_i64::<BigEndian>().unwrap();
        let _message_size = message_cursor.read_i32::<BigEndian>().unwrap();
        let _crc = message_cursor.read_i32::<BigEndian>().unwrap();
        let _magic = message_cursor.read_i8().unwrap();
        let attributes = message_cursor.read_i8().unwrap();
        let timestamp = message_cursor.read_i64::<BigEndian>().unwrap();

        (attributes, timestamp)
    }

    #[test]
    fn explicit_message_timestamp_forces_create_time_attributes() {
        let mut request = PartitionProduceRequest::new(0, Some(b"k"), Some(b"v"), Some(12345));
        request.add(Some(b"k2"), Some(b"v2"), Some(23456));

        let mut encoded = Vec::new();
        request
            ._encode_with_timestamp(
                &mut encoded,
                Compression::NONE,
                Some(ProducerTimestamp::LogAppendTime),
            )
            .unwrap();

        let (attributes, timestamp) = decode_first_message_attributes_and_timestamp(&encoded);
        assert_eq!(ProducerTimestamp::CreateTime as i8, attributes);
        assert_eq!(12345, timestamp);
    }

    #[test]
    fn global_log_append_time_is_used_without_explicit_timestamp() {
        let request = PartitionProduceRequest::new(0, Some(b"k"), Some(b"v"), None);

        let mut encoded = Vec::new();
        request
            ._encode_with_timestamp(
                &mut encoded,
                Compression::NONE,
                Some(ProducerTimestamp::LogAppendTime),
            )
            .unwrap();

        let (attributes, timestamp) = decode_first_message_attributes_and_timestamp(&encoded);
        assert_eq!(ProducerTimestamp::LogAppendTime as i8, attributes);
        assert!(timestamp > 0);
    }

    #[cfg(feature = "gzip")]
    #[test]
    fn compressed_wrapper_uses_max_timestamp_in_batch() {
        let mut request = PartitionProduceRequest::new(0, Some(b"k1"), Some(b"v1"), Some(12345));
        request.add(Some(b"k2"), Some(b"v2"), Some(23456));

        let mut encoded = Vec::new();
        request
            ._encode_with_timestamp(
                &mut encoded,
                Compression::GZIP,
                Some(ProducerTimestamp::LogAppendTime),
            )
            .unwrap();

        let (attributes, timestamp) = decode_first_message_attributes_and_timestamp(&encoded);
        assert_eq!(Compression::GZIP as i8 | ProducerTimestamp::LogAppendTime as i8, attributes);
        assert_eq!(23456, timestamp);
    }

    #[cfg(feature = "snappy")]
    #[test]
    fn snappy_compressed_wrapper_uses_max_timestamp_in_batch() {
        let mut request = PartitionProduceRequest::new(0, Some(b"k1"), Some(b"v1"), Some(12345));
        request.add(Some(b"k2"), Some(b"v2"), Some(23456));

        let mut encoded = Vec::new();
        request
            ._encode_with_timestamp(
                &mut encoded,
                Compression::SNAPPY,
                Some(ProducerTimestamp::LogAppendTime),
            )
            .unwrap();

        let (attributes, timestamp) = decode_first_message_attributes_and_timestamp(&encoded);
        assert_eq!(
            Compression::SNAPPY as i8 | ProducerTimestamp::LogAppendTime as i8,
            attributes
        );
        assert_eq!(23456, timestamp);
    }
}

impl ToByte for Option<&[u8]> {
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
