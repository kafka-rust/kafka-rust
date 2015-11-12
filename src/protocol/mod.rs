use std::io::{Read, Write};
use std::io::Cursor;
use std::rc::Rc;

use error::{Result, Error};
use crc32::Crc32;
use codecs::{ToByte, FromByte};
use compression::Compression;
use snappy;
use gzip;

/// Macro to return Result<()> from multiple statements
macro_rules! try_multi {
    ($($expr:expr),*) => ({
        $(try!($expr);)*;
        Ok(())
    })
}

// ~ these modules will see the above defined macro
mod produce;
mod offset;
mod metadata;
mod fetch;
mod consumer;

// ~ re-exports for request/response types defined in submodules
pub use self::produce::{ProduceRequest, ProduceResponse};
pub use self::offset::{OffsetRequest, OffsetResponse};
pub use self::metadata::{MetadataRequest, MetadataResponse};
pub use self::fetch::{FetchRequest, FetchResponse};
pub use self::consumer::{ConsumerMetadataRequest, ConsumerMetadataResponse,
                         OffsetFetchRequest, OffsetFetchResponse,
                         OffsetCommitRequest, OffsetCommitResponse};

// --------------------------------------------------------------------

const API_KEY_PRODUCE: i16   = 0;
const API_KEY_FETCH: i16     = 1;
const OFFSET_KEY: i16        = 2;
const API_KEY_METADATA: i16  = 3;
// 4-7 reserved for non-public kafka api services
const OFFSET_COMMIT_KEY: i16 = 8;
const OFFSET_FETCH_KEY: i16  = 9;
//const CONSUMER_METADATA_KEY: i16 = 10;

// the version of Kafka API we are requesting
const API_VERSION: i16 = 0;

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

// this is a replacement for the old HeaderRequest
#[derive(Debug)]
pub struct HeaderRequest_<'a> {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: &'a str,
}


// Helper Structs

#[derive(Debug)]
pub struct OffsetMessage {
    pub offset: i64,
    pub message: Vec<u8>
}

#[derive(Default, Debug, Clone)]
pub struct Message {
    pub crc: i32,
    pub magic: i8,
    pub attributes: i8,
    pub key: Vec<u8>,
    pub value: Vec<u8>
}

impl<'a> HeaderRequest_<'a> {
    fn new(api_key: i16, api_version: i16, correlation_id: i32, client_id: &'a str)
           -> HeaderRequest_
    {
        HeaderRequest_ {
            api_key: api_key,
            api_version: api_version,
            correlation_id: correlation_id,
            client_id: client_id,
        }
    }
}

impl<'a> ToByte for HeaderRequest_<'a> {
    fn encode<T:Write>(&self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.api_key.encode(buffer),
            self.api_version.encode(buffer),
            self.correlation_id.encode(buffer),
            self.client_id.encode(buffer))
    }
}

// Constructors for Requests



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


// Responses
impl FromByte for HeaderResponse {
    type R = HeaderResponse;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        self.correlation.decode(buffer)
    }
}


// For Helper Structs

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
        while let Ok(mi) = MessageSetInner::decode_new(buffer) {
            temp.message.push(mi);
        }
        if temp.message.is_empty() {
            return Err(Error::UnexpectedEOF)
        }
        Ok(temp)
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
        if self.key.is_empty() {
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
