use std::io::{Cursor, Read, Write};

use codecs::{ToByte, FromByte};
use compression::Compression;
use error::{Error, Result};
use gzip;
use num::traits::FromPrimitive;
use snappy;
use utils::{TopicMessage};

use super::{HeaderRequest, HeaderResponse};
use super::{API_KEY_FETCH, API_VERSION, FETCH_MAX_WAIT_TIME, FETCH_MIN_BYTES, MAX_FETCH_BUFFER_SIZE_BYTES};

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

    pub fn new(correlation_id: i32, client_id: &'a str) -> FetchRequest<'a, 'b> {
        FetchRequest {
            header: HeaderRequest::new(
                API_KEY_FETCH, API_VERSION, correlation_id, client_id),
            replica: -1,
            max_wait_time: FETCH_MAX_WAIT_TIME,
            min_bytes: FETCH_MIN_BYTES,
            topic_partitions: vec!()
        }
    }

    pub fn add(&mut self, topic: &'b str, partition: i32, offset: i64) {
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

impl<'a> TopicPartitionFetchRequest<'a> {
    pub fn new(topic: &'a str) -> TopicPartitionFetchRequest<'a> {
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
    pub offset: i64,
    pub messageset: MessageSet
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

#[derive(Debug)]
pub struct OffsetMessage {
    pub offset: i64,
    pub message: Vec<u8>
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
    while let Ok(x) = message_decode_loop_snappy(&mut buffer) {
        v.extend(x);
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
