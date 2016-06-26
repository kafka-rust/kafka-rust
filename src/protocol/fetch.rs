//! A representation of fetched messages from Kafka.

use std::borrow::Cow;
use std::io::Write;
use std::mem;
use std::cell::Cell;
use std::collections::HashMap;
use std::hash::BuildHasherDefault;

use fnv::FnvHasher;

use codecs::ToByte;
use error::{Error, KafkaCode, Result};
use compression::Compression;
#[cfg(feature = "gzip")]
use compression::gzip;
#[cfg(feature = "snappy")]
use compression::snappy::SnappyReader;

use super::{HeaderRequest, API_KEY_FETCH, API_VERSION};
use super::zreader::ZReader;
use super::to_crc;

pub type PartitionHasher = BuildHasherDefault<FnvHasher>;

#[derive(Debug)]
pub struct FetchRequest<'a, 'b> {
    pub header: HeaderRequest<'a>,
    pub replica: i32,
    pub max_wait_time: i32,
    pub min_bytes: i32,
    // topic -> partitions
    pub topic_partitions: HashMap<&'b str, TopicPartitionFetchRequest>,
}

#[derive(Debug)]
pub struct TopicPartitionFetchRequest {
    // partition-id -> partition-data
    pub partitions: HashMap<i32, PartitionFetchRequest, PartitionHasher>
}

#[derive(Debug)]
pub struct PartitionFetchRequest {
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
            topic_partitions: HashMap::new(),
        }
    }

    pub fn add(&mut self, topic: &'b str, partition: i32, offset: i64, max_bytes: i32) {
        self.topic_partitions.entry(topic)
            .or_insert_with(|| TopicPartitionFetchRequest::new())
            .add(partition, offset, max_bytes)
    }

    pub fn get<'d>(&'a self, topic: &'d str) -> Option<&'a TopicPartitionFetchRequest> {
        self.topic_partitions.get(topic)
    }
}

impl TopicPartitionFetchRequest {
    pub fn new() -> TopicPartitionFetchRequest {
        TopicPartitionFetchRequest { partitions: HashMap::default() }
    }

    pub fn add(&mut self, partition: i32, offset: i64, max_bytes: i32) {
        self.partitions.insert(partition, PartitionFetchRequest::new(offset, max_bytes));
    }

    pub fn get(&self, partition: i32) -> Option<&PartitionFetchRequest> {
        self.partitions.get(&partition)
    }
}

impl PartitionFetchRequest {
    pub fn new(offset: i64, max_bytes: i32) -> PartitionFetchRequest {
        PartitionFetchRequest {
            offset: offset,
            max_bytes: max_bytes,
        }
    }
}

impl<'a, 'b> ToByte for FetchRequest<'a, 'b> {
    fn encode<W: Write>(&self, buffer: &mut W) -> Result<()> {
        try!(self.header.encode(buffer));
        try!(self.replica.encode(buffer));
        try!(self.max_wait_time.encode(buffer));
        try!(self.min_bytes.encode(buffer));
        // encode the hashmap as a vector
        try!((self.topic_partitions.len() as i32).encode(buffer));
        for (name, tp) in self.topic_partitions.iter() {
            try!(tp.encode(name, buffer));
        }
        Ok(())
    }
}

impl TopicPartitionFetchRequest {
    fn encode<W: Write>(&self, topic: &str, buffer: &mut W) -> Result<()> {
        try!(topic.encode(buffer));
        // encode the hashmap as a vector
        try!((self.partitions.len() as i32).encode(buffer));
        for (&pid, p) in self.partitions.iter() {
            try!(p.encode(pid, buffer));
        }
        Ok(())
    }
}

impl PartitionFetchRequest {
    fn encode<T:Write>(&self, partition: i32, buffer: &mut T) -> Result<()> {
        try_multi!(
            partition.encode(buffer),
            self.offset.encode(buffer),
            self.max_bytes.encode(buffer)
        )
    }
}

// ~ response related -------------------------------------------------

pub struct ResponseParser<'a, 'b, 'c>
    where 'a: 'c, 'b: 'c
{
    pub validate_crc: bool,
    pub requests: Option<&'c FetchRequest<'a, 'b>>,
}

impl<'a, 'b, 'c> super::ResponseParser for ResponseParser<'a, 'b, 'c> {
    type T = Response;
    fn parse(&self, response: Vec<u8>) -> Result<Self::T> {
        Response::from_vec(response, self.requests, self.validate_crc)
    }
}

// ~ helper macro to aid parsing arrays of values (as defined by the
// Kafka protocol.)
macro_rules! array_of {
    ($zreader:ident, $parse_elem:expr) => {{
        let n_elems = try!($zreader.read_array_len());
        let mut array = Vec::with_capacity(n_elems);
        for _ in 0..n_elems {
            array.push(try!($parse_elem));
        }
        array
    }}
}

/// The result of a "fetch messages" request from a particular Kafka
/// broker. Such a response can contain messages for multiple topic
/// partitions.
pub struct Response {
    // used to "own" the data all other references of this struct
    // point to.
    #[allow(dead_code)]
    raw_data: Vec<u8>,

    correlation_id: i32,

    // ~ Static is used here to get around the fact that we don't want
    // Response have to a lifetime parameter as well.  The field is
    // exposed only through an accessor which binds the exposed
    // lifetime to the lifetime of the Response instance.
    topics: Vec<Topic<'static>>,
}

impl Response {
    /// Parses a Response from binary data as defined by the
    /// Kafka Protocol.
    fn from_vec(response: Vec<u8>, reqs: Option<&FetchRequest>, validate_crc: bool)
                -> Result<Response>
    {
        let slice = unsafe { mem::transmute(&response[..]) };
        let mut r = ZReader::new(slice);
        let correlation_id = try!(r.read_i32());
        let topics = array_of!(r, Topic::read(&mut r, reqs, validate_crc));
        Ok(Response {
            raw_data: response,
            correlation_id: correlation_id,
            topics: topics
        })
    }

    /// Retrieves the id corresponding to the fetch messages request
    /// (provided for debugging purposes only).
    #[inline]
    pub fn correlation_id(&self) -> i32 {
        self.correlation_id
    }

    /// Provides an iterator over all the topics and the fetched data
    /// relative to these topics.
    #[inline]
    pub fn topics<'a>(&'a self) -> &[Topic<'a>] {
        &self.topics
    }
}

/// The result of a "fetch messages" request from a particular Kafka
/// broker for a single topic only.  Beside the name of the topic,
/// this structure provides an iterator over the topic partitions from
/// which messages were requested.
pub struct Topic<'a> {
    topic: &'a str,
    partitions: Vec<Partition<'a>>,
}

impl<'a> Topic<'a> {
    fn read(r: &mut ZReader<'a>, reqs: Option<&FetchRequest>, validate_crc: bool)
            -> Result<Topic<'a>>
    {
        let name = try!(r.read_str());
        let preqs = reqs.and_then(|reqs| reqs.get(name));
        let partitions = array_of!(r, Partition::read(r, preqs, validate_crc));
        Ok(Topic {
            topic: name,
            partitions: partitions,
        })
    }

    /// Retrieves the identifier/name of the represented topic.
    #[inline]
    pub fn topic(&self) -> &'a str {
        self.topic
    }

    /// Provides an iterator over all the partitions of this topic for
    /// which messages were requested.
    #[inline]
    pub fn partitions(&self) -> &[Partition<'a>] {
        &self.partitions
    }
}

/// The result of a "fetch messages" request from a particular Kafka
/// broker for a single topic partition only.  Beside the partition
/// identifier, this structure provides an iterator over the actually
/// requested message data.
///
/// Note: There might have been a (recoverable) error for a particular
/// partition (but not for another).
pub struct Partition<'a> {
    /// The identifier of the represented partition.
    partition: i32,

    /// Either an error or the partition data.
    data: Result<Data<'a>>,
}

impl<'a> Partition<'a> {
    fn read(r: &mut ZReader<'a>, preqs: Option<&TopicPartitionFetchRequest>, validate_crc: bool)
            -> Result<Partition<'a>>
    {
        let partition = try!(r.read_i32());
        let proffs = preqs.and_then(|preqs| preqs.get(partition))
            .map(|preq| preq.offset)
            .unwrap_or(0);
        let error = Error::from_protocol(try!(r.read_i16()));
        // we need to parse the rest even if there was an error to
        // consume the input stream (zreader)
        let highwatermark = try!(r.read_i64());
        let msgset = try!(MessageSet::from_slice(try!(r.read_bytes()), proffs, validate_crc));
        Ok(Partition {
            partition: partition,
            data: match error {
                Some(error) => Err(error),
                None => Ok(Data {
                    highwatermark_offset: highwatermark,
                    message_set: msgset,
                    first_message_idx: Cell::new(0),
                }),
            },
        })
    }

    /// Retrieves the identifier of the represented partition.
    #[inline]
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// Retrieves the data payload for this partition.
    pub fn data(&'a self) -> &'a Result<Data<'a>> {
        &self.data
    }
}

/// The successfully fetched data payload for a particular partition.
pub struct Data<'a> {
    highwatermark_offset: i64,
    message_set: MessageSet<'a>,

    // ~ points to the first message in message_set#messages to
    // deliver through messages()
    first_message_idx: Cell<usize>,
}

impl<'a> Data<'a> {
    /// Retrieves the so-called "high water mark offset" indicating
    /// the "latest" offset for this partition at the remote broker.
    /// This can be used by clients to find out how much behind the
    /// latest available message they are.
    #[inline]
    pub fn highwatermark_offset(&self) -> i64 {
        self.highwatermark_offset
    }

    /// Retrieves the fetched message data for this partition.
    #[inline]
    pub fn messages(&self) -> &[Message<'a>] {
        &self.message_set.messages[self.first_message_idx.get()..]
    }
}

struct MessageSet<'a> {
    #[allow(dead_code)]
    raw_data: Cow<'a, [u8]>, // ~ this field is used to potentially "own" the underlying vector
    messages: Vec<Message<'a>>,
}

/// A fetched message from a remote Kafka broker for a particular
/// topic partition.
#[derive(Debug)]
pub struct Message<'a> {
    /// The offset at which this message resides in the remote kafka
    /// broker topic partition.
    pub offset: i64,

    /// The "key" data of this message.  Empty if there is no such
    /// data for this message.
    pub key: &'a [u8],

    /// The value data of this message.  Empty if there is no such
    /// data for this message.
    pub value: &'a [u8],
}

impl<'a> MessageSet<'a> {
    #[allow(dead_code)]
    fn from_vec(data: Vec<u8>, req_offset: i64, validate_crc: bool) -> Result<MessageSet<'a>> {
        // since we're going to keep the original
        // uncompressed vector around without
        // further modifying it and providing
        // publicly no mutability possibilities
        // this is safe
        let ms = try!(MessageSet::from_slice(unsafe {
            mem::transmute(&data[..])
        }, req_offset, validate_crc));
        return Ok(MessageSet {
            raw_data: Cow::Owned(data),
            messages: ms.messages,
        });
    }

    fn from_slice<'b>(raw_data: &'b [u8], req_offset: i64, validate_crc: bool)
                      -> Result<MessageSet<'b>>
    {
        let mut r = ZReader::new(raw_data);
        let mut msgs = Vec::new();
        while !r.is_empty() {
            match MessageSet::next_message(&mut r, validate_crc) {
                // this is the last messages which might be
                // incomplete; a valid case to be handled by
                // consumers
                Err(Error::UnexpectedEOF) => {
                    break;
                }
                Err(e) => {
                    return Err(e);
                }
                Ok((offset, pmsg)) => {
                    // handle compression (denoted by the last 2 bits
                    // of the attr field)
                    match pmsg.attr & 0x03 {
                        c if c == Compression::NONE as i8 => {
                            // skip messages with a lower offset
                            // than the request one
                            if offset >= req_offset {
                                msgs.push(Message {
                                    offset: offset,
                                    key: pmsg.key,
                                    value: pmsg.value
                                });
                            }
                        }
                        // XXX handle recursive compression in future
                        #[cfg(feature = "gzip")]
                        c if c == Compression::GZIP as i8 => {
                            let v = try!(gzip::uncompress(pmsg.value));
                            return Ok(try!(MessageSet::from_vec(v, req_offset, validate_crc)));
                        }
                        #[cfg(feature = "snappy")]
                        c if c == Compression::SNAPPY as i8 => {
                            use std::io::Read;
                            let mut v = Vec::new();
                            try!(try!(SnappyReader::new(pmsg.value)).read_to_end(&mut v));
                            return Ok(try!(MessageSet::from_vec(v, req_offset, validate_crc)));
                        }
                        _ => return Err(Error::UnsupportedCompression),
                    }
                }
            };
        }
        Ok(MessageSet {
            raw_data: Cow::Borrowed(raw_data),
            messages: msgs,
        })
    }

    fn next_message<'b>(r: &mut ZReader<'b>, validate_crc: bool) -> Result<(i64, ProtocolMessage<'b>)> {
        let offset = try!(r.read_i64());
        let msg_data = try!(r.read_bytes());
        Ok((offset, try!(ProtocolMessage::from_slice(msg_data, validate_crc))))
    }
}

/// Represents a messages exactly as defined in the protocol.
struct ProtocolMessage<'a> {
    attr: i8,
    key: &'a [u8],
    value: &'a [u8],
}

impl<'a> ProtocolMessage<'a> {
    /// Parses a raw message from the given byte slice.  Does _not_
    /// handle any compression.
    fn from_slice<'b>(raw_data: &'b [u8], validate_crc: bool) -> Result<ProtocolMessage<'b>> {
        let mut r = ZReader::new(raw_data);

        // ~ optionally validate the crc checksum
        let msg_crc = try!(r.read_i32());
        if validate_crc && to_crc(r.rest()) as i32 != msg_crc {
            return Err(Error::Kafka(KafkaCode::CorruptMessage));
        }
        // ~ we support parsing only messages with the "zero"
        // magic_byte; this covers kafka 0.8 and 0.9.
        let msg_magic = try!(r.read_i8());
        if msg_magic != 0 {
            return Err(Error::UnsupportedProtocol);
        }
        let msg_attr = try!(r.read_i8());
        let msg_key = try!(r.read_bytes());
        let msg_val = try!(r.read_bytes());

        debug_assert!(r.is_empty());

        Ok(ProtocolMessage {
            attr: msg_attr,
            key: msg_key,
            value: msg_val,
        })
    }
}

// tests --------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::str;

    use super::{FetchRequest, Response, Message};
    use error::{Error, KafkaCode};

    static FETCH1_TXT: &'static str =
        include_str!("../../test-data/fetch1.txt");
    static FETCH1_FETCH_RESPONSE_NOCOMPRESSION_K0821: &'static [u8] =
        include_bytes!("../../test-data/fetch1.mytopic.1p.nocompression.kafka.0821");
    static FETCH1_FETCH_RESPONSE_SNAPPY_K0821: &'static [u8] =
        include_bytes!("../../test-data/fetch1.mytopic.1p.snappy.kafka.0821");
    #[cfg(feature = "snappy")]
    static FETCH1_FETCH_RESPONSE_SNAPPY_K0822: &'static [u8] =
        include_bytes!("../../test-data/fetch1.mytopic.1p.snappy.kafka.0822");
    #[cfg(feature = "gzip")]
    static FETCH1_FETCH_RESPONSE_GZIP_K0821: &'static [u8] =
        include_bytes!("../../test-data/fetch1.mytopic.1p.gzip.kafka.0821");

    static FETCH2_TXT: &'static str =
        include_str!("../../test-data/fetch2.txt");
    static FETCH2_FETCH_RESPONSE_NOCOMPRESSION_K0900: &'static [u8] =
        include_bytes!("../../test-data/fetch2.mytopic.nocompression.kafka.0900");
    static FETCH2_FETCH_RESPONSE_NOCOMPRESSION_INVALID_CRC_K0900: &'static [u8] =
        include_bytes!("../../test-data/fetch2.mytopic.nocompression.invalid_crc.kafka.0900");

    fn into_messages<'a>(r: &'a Response) -> Vec<&'a Message<'a>> {
        let mut all_msgs = Vec::new();
        for t in r.topics() {
            for p in t.partitions() {
                match p.data() {
                    &Err(_) => {
                        println!("Skipping error partition: {}:{}", t.topic, p.partition);
                    }
                    &Ok(ref data) => {
                        all_msgs.extend(data.messages());
                    }
                }
            }
        }
        all_msgs
    }

    fn test_decode_new_fetch_response(msg_per_line: &str, response: Vec<u8>, requests: Option<&FetchRequest>, validate_crc: bool) {
        let resp = Response::from_vec(response, requests, validate_crc);
        let resp = resp.unwrap();

        let original: Vec<_> = msg_per_line.lines().collect();

        // ~ response for exactly one topic expected
        assert_eq!(1, resp.topics.len());
        // ~ topic name
        assert_eq!("my-topic", resp.topics[0].topic);
        // ~ exactly one partition
        assert_eq!(1, resp.topics[0].partitions.len());
        // ~ the first partition
        assert_eq!(0, resp.topics[0].partitions[0].partition);
        // ~ no error
        assert!(resp.topics[0].partitions[0].data.is_ok());

        let msgs = into_messages(&resp);
        assert_eq!(original.len(), msgs.len());
        for (msg, orig) in msgs.into_iter().zip(original.iter()) {
            assert_eq!(str::from_utf8(msg.value).unwrap(), *orig);
        }
    }

    fn skip_lines(mut lines: &str, mut n: usize) -> &str {
        while n > 0 {
            n -= 1;
            lines = &lines[lines.find('\n').map(|i| i+1).unwrap_or(0) ..];
        }
        lines
    }

    #[test]
    fn test_from_slice_nocompression_k0821() {
        let mut req = FetchRequest::new(0, "test", -1, -1);
        req.add("my-topic", 0, 0, -1);
        req.add("foo-quux", 0, 100, -1);
        test_decode_new_fetch_response(
            FETCH1_TXT, FETCH1_FETCH_RESPONSE_NOCOMPRESSION_K0821.to_owned(), Some(&req), false);

        // ~ pretend we asked for messages as of offset five (while
        // the server delivered the zero-offset message as well)
        req = FetchRequest::new(0, "test", -1, -1);
        req.add("my-topic", 0, 5, -1);
        test_decode_new_fetch_response(
            skip_lines(FETCH1_TXT, 5), FETCH1_FETCH_RESPONSE_NOCOMPRESSION_K0821.to_owned(), Some(&req), false);
    }

    // verify we don't crash but cleanly fail and report we don't
    // support the compression
    #[cfg(not(feature = "snappy"))]
    #[test]
    fn test_unsupported_compression_snappy() {
        let mut req = FetchRequest::new(0, "test", -1, -1);
        req.add("my-topic", 0, 0, -1);
        let r = Response::from_vec(FETCH1_FETCH_RESPONSE_SNAPPY_K0821.to_owned(), Some(&req), false);
        assert!(match r { Err(Error::UnsupportedCompression) => true, _ => false });
    }

    #[cfg(feature = "snappy")]
    #[test]
    fn test_from_slice_snappy_k0821() {
        let mut req = FetchRequest::new(0, "test", -1, -1);
        req.add("my-topic", 0, 0, -1);
        test_decode_new_fetch_response(
            FETCH1_TXT, FETCH1_FETCH_RESPONSE_SNAPPY_K0821.to_owned(), Some(&req), false);

        // ~ pretend we asked for messages as of offset three (while
        // the server delivered the zero-offset message as well)
        req = FetchRequest::new(0, "test", -1, -1);
        req.add("my-topic", 0, 3, -1);
        test_decode_new_fetch_response(
            skip_lines(FETCH1_TXT, 3), FETCH1_FETCH_RESPONSE_SNAPPY_K0821.to_owned(), Some(&req), false);
    }

    #[cfg(feature = "snappy")]
    #[test]
    fn test_from_slice_snappy_k0822() {
        let mut req = FetchRequest::new(0, "test", -1, -1);
        req.add("my-topic", 0, 0, -1);
        test_decode_new_fetch_response(
            FETCH1_TXT, FETCH1_FETCH_RESPONSE_SNAPPY_K0822.to_owned(), Some(&req), false);
    }

    #[cfg(feature = "gzip")]
    #[test]
    fn test_from_slice_gzip_k0821() {
        let mut req = FetchRequest::new(0, "test", -1, -1);
        req.add("my-topic", 0, 0, -1);
        test_decode_new_fetch_response(
            FETCH1_TXT, FETCH1_FETCH_RESPONSE_GZIP_K0821.to_owned(), Some(&req), false);

        // ~ pretend we asked for messages as of offset one (while the
        // server delivered the zero-offset message as well)
        req.add("my-topic", 0, 1, -1);
        test_decode_new_fetch_response(
            skip_lines(FETCH1_TXT, 1), FETCH1_FETCH_RESPONSE_GZIP_K0821.to_owned(), Some(&req), false);

        // ~ pretend we asked for messages as of offset ten (while the
        // server delivered the zero-offset message as well)
        req.add("my-topic", 0, 10, -1);
        test_decode_new_fetch_response(
            skip_lines(FETCH1_TXT, 10), FETCH1_FETCH_RESPONSE_GZIP_K0821.to_owned(), Some(&req), false);
    }

    #[test]
    fn test_crc_validation() {
        test_decode_new_fetch_response(FETCH2_TXT, FETCH2_FETCH_RESPONSE_NOCOMPRESSION_K0900.to_owned(), None, true);

        // now test the same message but with an invalid crc checksum
        // (modified by hand)
        // 1) without checking the crc ... since only the crc field is
        // artificially falsified ... we expect the rest of the
        // message to be parsed correctly
        test_decode_new_fetch_response(FETCH2_TXT, FETCH2_FETCH_RESPONSE_NOCOMPRESSION_INVALID_CRC_K0900.to_owned(), None, false);
        // 2) with checking the crc ... parsing should fail immediately
        match Response::from_vec(FETCH2_FETCH_RESPONSE_NOCOMPRESSION_INVALID_CRC_K0900.to_owned(), None, true) {
            Ok(_) => panic!("Expected error, but got successful response!"),
            Err(Error::Kafka(KafkaCode::CorruptMessage)) => {}
            Err(e) => panic!("Expected KafkaCode::CorruptMessage error, but got: {:?}", e),
        }
    }

    #[cfg(feature = "nightly")]
    mod benches {
        use test::{black_box, Bencher};

        use super::super::{Response, FetchRequest};
        use super::into_messages;

        fn bench_decode_new_fetch_response(b: &mut Bencher, data: Vec<u8>, validate_crc: bool) {
            let mut reqs = FetchRequest::new(0, "foo", -1, -1);
            reqs.add("my-topic", 0, 0, -1);
            b.bytes = data.len() as u64;
            b.iter(|| {
                let data = data.clone();
                let r = black_box(Response::from_vec(data, Some(&reqs), validate_crc).unwrap());
                let v = black_box(into_messages(&r));
                v.len()
            });
        }

        #[bench]
        fn bench_decode_new_fetch_response_nocompression_k0821(b: &mut Bencher) {
            bench_decode_new_fetch_response(b, super::FETCH1_FETCH_RESPONSE_NOCOMPRESSION_K0821.to_owned(), false)
        }

        #[cfg(feature = "snappy")]
        #[bench]
        fn bench_decode_new_fetch_response_snappy_k0821(b: &mut Bencher) {
            bench_decode_new_fetch_response(b, super::FETCH1_FETCH_RESPONSE_SNAPPY_K0821.to_owned(), false)
        }

        #[cfg(feature = "snappy")]
        #[bench]
        fn bench_decode_new_fetch_response_snappy_k0822(b: &mut Bencher) {
            bench_decode_new_fetch_response(b, super::FETCH1_FETCH_RESPONSE_SNAPPY_K0822.to_owned(), false)
        }

        #[cfg(feature = "gzip")]
        #[bench]
        fn bench_decode_new_fetch_response_gzip_k0821(b: &mut Bencher) {
            bench_decode_new_fetch_response(b, super::FETCH1_FETCH_RESPONSE_GZIP_K0821.to_owned(), false)
        }

        #[bench]
        fn bench_decode_new_fetch_response_nocompression_k0821_validate_crc(b: &mut Bencher) {
            bench_decode_new_fetch_response(b, super::FETCH1_FETCH_RESPONSE_NOCOMPRESSION_K0821.to_owned(), true)
        }

        #[cfg(feature = "snappy")]
        #[bench]
        fn bench_decode_new_fetch_response_snappy_k0821_validate_crc(b: &mut Bencher) {
            bench_decode_new_fetch_response(b, super::FETCH1_FETCH_RESPONSE_SNAPPY_K0821.to_owned(), true)
        }

        #[cfg(feature = "snappy")]
        #[bench]
        fn bench_decode_new_fetch_response_snappy_k0822_validate_crc(b: &mut Bencher) {
            bench_decode_new_fetch_response(b, super::FETCH1_FETCH_RESPONSE_SNAPPY_K0822.to_owned(), true)
        }

        #[cfg(feature = "gzip")]
        #[bench]
        fn bench_decode_new_fetch_response_gzip_k0821_validate_crc(b: &mut Bencher) {
            bench_decode_new_fetch_response(b, super::FETCH1_FETCH_RESPONSE_GZIP_K0821.to_owned(), true)
        }
    }
}
