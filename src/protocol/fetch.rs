//! A representation of fetched messages from Kafka.

use std::borrow::Cow;
use std::io::{Read, Write};
use std::mem;
use std::cell::Cell;

use codecs::ToByte;
use error::{Error, KafkaCode, Result};
use compression::{gzip, Compression};
use compression::snappy::SnappyReader;

use super::{HeaderRequest, FromResponse, API_KEY_FETCH, API_VERSION};
use super::zreader::ZReader;
use super::tocrc;

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
    pub offset: i64,
    pub partition: i32,
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

// ~ response related -------------------------------------------------

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

impl FromResponse for Response {
    fn from_response(response: Vec<u8>) -> Result<Self> {
        // XXX
        Response::from_vec(response, false)
    }
}

impl Response {
    /// Parses a Response from binary data as defined by the
    /// Kafka Protocol.
    fn from_vec(response: Vec<u8>, validate_crc: bool) -> Result<Response> {
        let slice = unsafe { mem::transmute(&response[..]) };
        let mut r = ZReader::new(slice);
        let correlation_id = try!(r.read_i32());
        let topics = array_of!(r, Topic::read(&mut r, validate_crc));
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
    fn read(r: &mut ZReader<'a>, validate_crc: bool) -> Result<Topic<'a>> {
        let name = try!(r.read_str());
        let partitions = array_of!(r, Partition::read(r, validate_crc));
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
    fn read(r: &mut ZReader<'a>, validate_crc: bool) -> Result<Partition<'a>> {
        let partition = try!(r.read_i32());
        let error = Error::from_protocol_error(try!(r.read_i16()));
        // we need to parse the rest even if there was an error to
        // consume the input stream (zreader)
        let highwatermark = try!(r.read_i64());
        let msgset = try!(MessageSet::from_slice(try!(r.read_bytes()), validate_crc));
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

    /// *Mutates* this partition data in such a way that the next call
    /// to `messages()` will deliver a slice of messages with the
    /// property `msg.offset >= offset`.  A convenient method to skip
    /// past a certain message offset in the retrieved data.
    ///
    /// Note: this method *does* mutate the receiver even though it is
    /// accepted merely through a shared reference for convenience
    /// reasons.  Calling this method is safe as long as you don't
    /// rely on the length of a previously retrieved `messages()`
    /// slice while working with a newly retrieved `messages()` slice.
    /// Therefore, this method is marked as unsafe.
    #[inline]
    pub unsafe fn forget_before_offset(&self, offset: i64) {
        let msgs = &self.message_set.messages;
        if let Some(m) = msgs.first() {
            if offset <= m.offset {
                if self.first_message_idx.get() != 0 {
                    self.first_message_idx.set(0);
                }
                return;
            }
        }
        match msgs.binary_search_by(|m| m.offset.cmp(&offset)) {
            Ok(i) => self.first_message_idx.set(i),
            Err(i) => self.first_message_idx.set(i),
        };
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
    fn from_vec(data: Vec<u8>, validate_crc: bool) -> Result<MessageSet<'a>> {
        // since we're going to keep the original
        // uncompressed vector around without
        // further modifying it and providing
        // publicly no mutability possibilities
        // this is safe
        let ms = try!(MessageSet::from_slice(unsafe {
            mem::transmute(&data[..])
        }, validate_crc));
        return Ok(MessageSet {
            raw_data: Cow::Owned(data),
            messages: ms.messages,
        });
    }

    fn from_slice<'b>(raw_data: &'b [u8], validate_crc: bool) -> Result<MessageSet<'b>> {
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
                            msgs.push(Message {
                                offset: offset,
                                key: pmsg.key,
                                value: pmsg.value
                            });
                        }
                        // XXX handle recursive compression in future
                        c if c == Compression::GZIP as i8 => {
                            let v = try!(gzip::uncompress(pmsg.value));
                            return Ok(try!(MessageSet::from_vec(v, validate_crc)));
                        }
                        c if c == Compression::SNAPPY as i8 => {
                            let mut v = Vec::new();
                            try!(try!(SnappyReader::new(pmsg.value)).read_to_end(&mut v));
                            return Ok(try!(MessageSet::from_vec(v, validate_crc)));
                        }
                        _ => panic!("Unknown compression type!"),
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
        if validate_crc && tocrc(r.rest()) as i32 != msg_crc {
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

    use super::{Response, Message};

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

    fn test_decode_new_fetch_response(msg_per_line: &str, response: Vec<u8>, validate_crc: bool) {
        let resp = Response::from_vec(response, validate_crc);
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

    #[test]
    fn test_forget_before_offset() {
        let r = Response::from_vec(FETCH1_FETCH_RESPONSE_NOCOMPRESSION_K0821.to_owned(), false)
            .unwrap();
        let t = &r.topics()[0];
        let p = &t.partitions()[0];
        let data = p.data().as_ref().unwrap();

        fn assert_offsets(msgs: &[Message], len: usize, first_offset: i64, last_offset: i64) {
            assert_eq!(len, msgs.len());
            assert_eq!(first_offset, msgs[0].offset);
            assert_eq!(last_offset, msgs[msgs.len()-1].offset);
        }

        unsafe {
            // verify our assumptions about the input data
            assert_offsets(data.messages(), 42, 0, 41);

            // 1) forget about very early, not present offsets
            data.forget_before_offset(-1);
            assert_offsets(data.messages(), 42, 0, 41);
            data.forget_before_offset(0);
            assert_offsets(data.messages(), 42, 0, 41);

            // 2) forget about present offsets
            data.forget_before_offset(1);
            assert_offsets(data.messages(), 41, 1, 41);
            data.forget_before_offset(30);
            assert_offsets(data.messages(), 12, 30, 41);
            data.forget_before_offset(41);
            assert_offsets(data.messages(), 1, 41, 41);

            // 3) forget about very late, not present offsets
            data.forget_before_offset(42);
            assert!(data.messages().is_empty());
            data.forget_before_offset(100);
            assert!(data.messages().is_empty());

            // 4) verify "re-winding" works
            data.forget_before_offset(-1);
            assert_offsets(data.messages(), 42, 0, 41);
        }
    }

    #[test]
    fn test_from_slice_nocompression_k0821() {
        test_decode_new_fetch_response(FETCH1_TXT, FETCH1_FETCH_RESPONSE_NOCOMPRESSION_K0821.to_owned(), false);
    }

    #[test]
    fn test_from_slice_snappy_k0821() {
        test_decode_new_fetch_response(FETCH1_TXT, FETCH1_FETCH_RESPONSE_SNAPPY_K0821.to_owned(), false);
    }

    #[test]
    fn test_from_slice_snappy_k0822() {
        test_decode_new_fetch_response(FETCH1_TXT, FETCH1_FETCH_RESPONSE_SNAPPY_K0822.to_owned(), false);
    }

    #[test]
    fn test_from_slice_gzip_k0821() {
        test_decode_new_fetch_response(FETCH1_TXT, FETCH1_FETCH_RESPONSE_GZIP_K0821.to_owned(), false);
    }

    // XXX test that validating_crc does and doesn't fail on request

    #[cfg(feature = "nightly")]
    mod benches {
        use test::{black_box, Bencher};

        use super::super::Response;
        use super::into_messages;

        fn bench_decode_new_fetch_response(b: &mut Bencher, data: Vec<u8>, validate_crc: bool) {
            b.bytes = data.len() as u64;
            b.iter(|| {
                let data = data.clone();
                let r = black_box(Response::from_vec(data, validate_crc).unwrap());
                let v = black_box(into_messages(&r));
                v.len()
            });
        }

        #[bench]
        fn bench_decode_new_fetch_response_nocompression_k0821(b: &mut Bencher) {
            bench_decode_new_fetch_response(b, super::FETCH1_FETCH_RESPONSE_NOCOMPRESSION_K0821.to_owned(), false)
        }

        #[bench]
        fn bench_decode_new_fetch_response_snappy_k0821(b: &mut Bencher) {
            bench_decode_new_fetch_response(b, super::FETCH1_FETCH_RESPONSE_SNAPPY_K0821.to_owned(), false)
        }

        #[bench]
        fn bench_decode_new_fetch_response_snappy_k0822(b: &mut Bencher) {
            bench_decode_new_fetch_response(b, super::FETCH1_FETCH_RESPONSE_SNAPPY_K0822.to_owned(), false)
        }

        #[bench]
        fn bench_decode_new_fetch_response_gzip_k0821(b: &mut Bencher) {
            bench_decode_new_fetch_response(b, super::FETCH1_FETCH_RESPONSE_GZIP_K0821.to_owned(), false)
        }

        #[bench]
        fn bench_decode_new_fetch_response_nocompression_k0821_validate_crc(b: &mut Bencher) {
            bench_decode_new_fetch_response(b, super::FETCH1_FETCH_RESPONSE_NOCOMPRESSION_K0821.to_owned(), true)
        }

        #[bench]
        fn bench_decode_new_fetch_response_snappy_k0821_validate_crc(b: &mut Bencher) {
            bench_decode_new_fetch_response(b, super::FETCH1_FETCH_RESPONSE_SNAPPY_K0821.to_owned(), true)
        }

        #[bench]
        fn bench_decode_new_fetch_response_snappy_k0822_validate_crc(b: &mut Bencher) {
            bench_decode_new_fetch_response(b, super::FETCH1_FETCH_RESPONSE_SNAPPY_K0822.to_owned(), true)
        }

        #[bench]
        fn bench_decode_new_fetch_response_gzip_k0821_validate_crc(b: &mut Bencher) {
            bench_decode_new_fetch_response(b, super::FETCH1_FETCH_RESPONSE_GZIP_K0821.to_owned(), true)
        }
    }
}
