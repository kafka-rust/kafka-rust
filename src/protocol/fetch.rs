use std::io::{Cursor, Read, Write};

use codecs::{ToByte, FromByte};
use compression::snappy::SnappyReader;
use compression::{Compression, gzip};
use error::{Error, Result};
use utils::{TopicMessage, OffsetMessage};

use super::{HeaderRequest, HeaderResponse};
use super::{API_KEY_FETCH, API_VERSION};

#[derive(Debug)]
pub struct FetchRequest<'a, 'b> {
    pub header: HeaderRequest<'a>,
    pub replica: i32,
    pub max_wait_time: i32,
    pub min_bytes: i32,
    pub topic_partitions: Vec<TopicPartitionFetchRequest<'b>>
}

#[derive(Debug)]
pub struct TopicPartitionFetchRequest<'a> {
    pub topic: &'a str,
    pub partitions: Vec<PartitionFetchRequest>
}

#[derive(Debug)]
pub struct PartitionFetchRequest {
    pub partition: i32,
    pub offset: i64,
    pub max_bytes: i32
}

impl<'a, 'b> FetchRequest<'a, 'b> {

    pub fn new(correlation_id: i32, client_id: &'a str, max_wait_time: i32, min_bytes: i32)
               -> FetchRequest<'a, 'b> {
        FetchRequest {
            header: HeaderRequest::new(
                API_KEY_FETCH, API_VERSION, correlation_id, client_id),
            replica: -1,
            max_wait_time: max_wait_time,
            min_bytes: min_bytes,
            topic_partitions: vec!()
        }
    }

    pub fn add(&mut self, topic: &'b str, partition: i32, offset: i64, max_bytes: i32) {
        for tp in &mut self.topic_partitions {
            if tp.topic == topic {
                tp.add(partition, offset, max_bytes);
                return;
            }
        }
        let mut tp = TopicPartitionFetchRequest::new(topic);
        tp.add(partition, offset, max_bytes);
        self.topic_partitions.push(tp);
    }
}

impl<'a> TopicPartitionFetchRequest<'a> {
    pub fn new(topic: &'a str) -> TopicPartitionFetchRequest<'a> {
        TopicPartitionFetchRequest {
            topic: topic,
            partitions: vec!()
        }
    }

    pub fn add(&mut self, partition: i32, offset: i64, max_bytes: i32) {
        self.partitions.push(PartitionFetchRequest:: new(partition, offset, max_bytes))
    }
}

impl PartitionFetchRequest {
    pub fn new(partition: i32, offset: i64, max_bytes: i32) -> PartitionFetchRequest {
        PartitionFetchRequest {
            partition: partition,
            offset: offset,
            max_bytes: max_bytes,
        }
    }
}

impl<'a, 'b> ToByte for FetchRequest<'a, 'b> {
    fn encode<W: Write>(&self, buffer: &mut W) -> Result<()> {
        try_multi!(
            self.header.encode(buffer),
            self.replica.encode(buffer),
            self.max_wait_time.encode(buffer),
            self.min_bytes.encode(buffer),
            self.topic_partitions.encode(buffer)
        )
    }
}

impl<'a> ToByte for TopicPartitionFetchRequest<'a> {
    fn encode<W: Write>(&self, buffer: &mut W) -> Result<()> {
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

// --------------------------------------------------------------------

#[derive(Default, Debug)]
pub struct FetchResponse {
    pub header: HeaderResponse,
    pub topic_partitions: Vec<TopicPartitionFetchResponse>,
}

#[derive(Default, Debug)]
pub struct TopicPartitionFetchResponse {
    pub topic: String,
    pub partitions: Vec<PartitionFetchResponse>,
}

#[derive(Default, Debug)]
pub struct PartitionFetchResponse {
    pub partition: i32,
    pub error: i16,
    pub highwatermark_offset: i64,
    pub message_set: MessageSet
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
        if let Some(e) = Error::from_protocol_error(self.error) {
            return vec!(TopicMessage {
                topic: topic,
                partition: self.partition,
                message: Err(e),
            });
        }
        let partition = self.partition;
        self.message_set
            .into_messages()
            .into_iter()
            .map(|om| TopicMessage {
                topic: topic.clone(),
                partition: partition,
                message: Ok(om)
            })
            .collect()
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
            self.highwatermark_offset.decode(buffer),
            self.message_set.decode(buffer)
        )
    }
}

#[derive(Default, Debug)]
pub struct MessageSet {
    pub message: Vec<MessageSetInner>
}

#[derive(Default, Debug)]
pub struct MessageSetInner {
    pub offset: i64,
    pub messagesize: i32,
    pub message: Message
}

#[derive(Default, Debug)]
pub struct Message {
    pub crc: i32,
    pub magic: i8,
    pub attributes: i8,
    pub key: Vec<u8>,
    pub value: Vec<u8>
}

impl MessageSet {
    fn into_messages(self) -> Vec<OffsetMessage> {
        self.message
            .into_iter()
            .flat_map(|m| m.into_messages())
            .collect()
    }
}

impl MessageSetInner {
    fn into_messages(self) -> Vec<OffsetMessage>{
        self.message.into_messages(self.offset)
    }
}

impl Message {
    fn into_messages(self, offset: i64) -> Vec<OffsetMessage> {
        match self.attributes & 3 {
            codec if codec == Compression::NONE as i8 => vec!(OffsetMessage{
                offset:offset,
                value: self.value
            }),
            codec if codec == Compression::GZIP as i8 => message_decode_gzip(self.value),
            codec if codec == Compression::SNAPPY as i8 => message_decode_snappy(self.value),
            _ => vec!()
        }
    }
}

fn message_decode_snappy(value: Vec<u8>) -> Vec<OffsetMessage> {
    // XXX avoid unwrap, have this return an error!!!
    let mut r = SnappyReader::new(&value).unwrap();
    let mset = MessageSet::decode_new(&mut r).unwrap();
    mset.into_messages()
}

fn message_decode_gzip(value: Vec<u8>) -> Vec<OffsetMessage>{
    // Gzip
    let mut buffer = Cursor::new(value);
    // XXX is the loop really necessary?
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
        while let Ok(mi) = MessageSetInner::decode_new(buffer) {
            temp.message.push(mi);
        }
        if temp.message.is_empty() {
            return Err(Error::UnexpectedEOF)
        }
        Ok(temp)
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

// --------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::str;

    use super::FetchResponse;
    use codecs::FromByte;

    static FETCH1_TXT: &'static str =
        include_str!("../../test-data/fetch1.txt");
    static FETCH1_FETCH_RESPONSE_NOCOMPRESSION_K0821: &'static [u8] =
        include_bytes!("../../test-data/fetch1.mytopic.1p.nocompression.kafka.0821");
    static FETCH1_FETCH_RESPONSE_SNAPPY_K0821: &'static [u8] =
        include_bytes!("../../test-data/fetch1.mytopic.1p.snappy.kafka.0821");
    static FETCH1_FETCH_RESPONSE_SNAPPY_K0822: &'static [u8] =
        include_bytes!("../../test-data/fetch1.mytopic.1p.snappy.kafka.0822");
    static FETCH1_FETCH_RESPONSE_GZIP_K0821: &'static [u8] =
        include_bytes!("../../test-data/fetch1.mytopic.1p.gzip.kafka.0821");

    fn test_decode_new_fetch_response(msg_per_line: &str, mut fetch_response_bytes: &[u8]) {
        let resp = FetchResponse::decode_new(&mut fetch_response_bytes);
        let resp = resp.unwrap();

        // ~ response for exactly one topic expected
        assert_eq!(1, resp.topic_partitions.len());

        let msgs = resp.into_messages();
        let original: Vec<_> = msg_per_line.lines().collect();
        assert_eq!(original.len(), msgs.len());
        for (msg, orig) in msgs.into_iter().zip(original.iter()) {
            assert_eq!(str::from_utf8(&msg.message.unwrap().value).unwrap(), *orig);
        }
    }

    #[test]
    fn test_decode_new_fetch_response_nocompression_k0821() {
        test_decode_new_fetch_response(FETCH1_TXT, FETCH1_FETCH_RESPONSE_NOCOMPRESSION_K0821);
    }

    #[test]
    fn test_decode_new_fetch_response_snappy_k0821() {
        test_decode_new_fetch_response(FETCH1_TXT, FETCH1_FETCH_RESPONSE_SNAPPY_K0821);
    }

    #[test]
    fn test_decode_new_fetch_response_snappy_k0822() {
        test_decode_new_fetch_response(FETCH1_TXT, FETCH1_FETCH_RESPONSE_SNAPPY_K0822);
    }

    #[test]
    fn test_decode_new_fetch_response_gzip_k0821() {
        test_decode_new_fetch_response(FETCH1_TXT, FETCH1_FETCH_RESPONSE_GZIP_K0821);
    }

    #[cfg(feature = "nightly")]
    mod benches {
        use std::io::Cursor;
        use test::Bencher;

        use codecs::FromByte;
        use super::super::FetchResponse;

        fn bench_decode_new_fetch_response(b: &mut Bencher, data: &[u8]) {
            b.bytes = data.len() as u64;
            b.iter(|| FetchResponse::decode_new(&mut Cursor::new(data))
                   .unwrap()
                   .into_messages());
        }

        #[bench]
        fn bench_decode_new_fetch_response_nocompression_k0821(b: &mut Bencher) {
            bench_decode_new_fetch_response(b, super::FETCH1_FETCH_RESPONSE_NOCOMPRESSION_K0821)
        }

        #[bench]
        fn bench_decode_new_fetch_response_snappy_k0821(b: &mut Bencher) {
            bench_decode_new_fetch_response(b, super::FETCH1_FETCH_RESPONSE_SNAPPY_K0821)
        }

        #[bench]
        fn bench_decode_new_fetch_response_snappy_k0822(b: &mut Bencher) {
            bench_decode_new_fetch_response(b, super::FETCH1_FETCH_RESPONSE_SNAPPY_K0822)
        }

        #[bench]
        fn bench_decode_new_fetch_response_gzip_k0821(b: &mut Bencher) {
            bench_decode_new_fetch_response(b, super::FETCH1_FETCH_RESPONSE_GZIP_K0821)
        }
    }
}
