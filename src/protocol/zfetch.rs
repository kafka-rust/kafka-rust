//! An efficient response parser for the fetch_messages use-case.

use std::borrow::Cow;
use std::slice::Iter;
use std::mem;

use error::{Error, Result};
use compression::{gzip, Compression};

use super::zreader::ZReader;


pub struct FetchResponse<'a> {
    raw_data: &'a [u8],
    correlation_id: i32,
    topics: Vec<TopicFetchResponse<'a>>,
}

impl<'a> FetchResponse<'a> {
    #[inline]
    pub fn topics<'b>(&'b self) -> Topics<'b, 'a> {
        Topics { iter: self.topics.iter() }
    }
}

pub struct TopicFetchResponse<'a> {
    pub topic: &'a str,
    partitions: Vec<PartitionFetchResponse<'a>>,
}

impl<'a> TopicFetchResponse<'a> {
    #[inline]
    pub fn partitions<'b>(&'b self) -> Partitions<'b, 'a> {
        Partitions { iter: self.partitions.iter() }
    }
}

pub struct PartitionFetchResponse<'a> {
    pub partition: i32,
    error: Option<Error>,
    highwatermark_offset: i64,
    message_set: MessageSet<'a>,
}

impl<'a> PartitionFetchResponse<'a> {
    #[inline]
    pub fn highwatermark_offset(&self) -> Result<i64> {
        match self.error {
            None => Ok(self.highwatermark_offset),
            Some(ref e) => Err(e.clone()),
        }
    }

    #[inline]
    pub fn messages<'b>(&'b self) -> Result<Messages<'b, 'a>> {
        match self.error {
            None => Ok(Messages { iter: self.message_set.messages.iter() }),
            Some(ref e) => Err(e.clone()),
        }
    }
}

pub struct MessageSet<'a> {
    raw_data: Cow<'a, [u8]>,
    messages: Vec<Message<'a>>,
}

pub struct Message<'a> {
    pub offset: i64,
    pub key: &'a [u8],
    pub value: &'a [u8],
}

/// An iterator over the topics of a `FetchResponse`.
pub struct Topics<'a, 'b: 'a> {
    iter: Iter<'a, TopicFetchResponse<'b>>,
}

impl<'a, 'b> Iterator for Topics<'a, 'b> {
    type Item = &'a TopicFetchResponse<'a>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

/// An iterator over the partitions of a `TopicFetchResponse`.
pub struct Partitions<'a, 'b: 'a> {
    iter: Iter<'a, PartitionFetchResponse<'b>>,
}

impl<'a, 'b> Iterator for Partitions<'a, 'b> {
    type Item = &'a PartitionFetchResponse<'b>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

/// An iterator over the messages of a `PartitionFetchResponse`.
pub struct Messages<'a, 'b: 'a> {
    iter: Iter<'a, Message<'b>>,
}

impl<'a, 'b> Iterator for Messages<'a, 'b> {
    type Item = &'a Message<'b>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

// ~ kafka protocol parsing -------------------------------------------

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

impl<'a> FetchResponse<'a> {
    pub fn from_slice<'b>(response: &'b [u8]) -> Result<FetchResponse<'b>> {
        let mut r = ZReader::new(response);
        let correlation_id = try!(r.read_i32());
        let topics = array_of!(r, TopicFetchResponse::read(&mut r));
        Ok(FetchResponse {raw_data: response, correlation_id: correlation_id, topics: topics})
    }
}

impl<'a> TopicFetchResponse<'a> {
    fn read(r: &mut ZReader<'a>) -> Result<TopicFetchResponse<'a>> {
        let name = try!(r.read_str());
        let partitions = array_of!(r, PartitionFetchResponse::read(r));
        Ok(TopicFetchResponse {
            topic: name,
            partitions: partitions,
        })
    }
}

impl<'a> PartitionFetchResponse<'a> {
    fn read(r: &mut ZReader<'a>) -> Result<PartitionFetchResponse<'a>> {
        let partition = try!(r.read_i32());
        let error = Error::from_protocol_error(try!(r.read_i16()));
        let highwatermark = try!(r.read_i64());
        let msgset = try!(MessageSet::from_slice(try!(r.read_bytes())));
        Ok(PartitionFetchResponse {
            partition: partition,
            error: error,
            highwatermark_offset: highwatermark,
            message_set: msgset,
        })
    }
}

impl<'a> MessageSet<'a> {
    fn from_vec(data: Vec<u8>) -> Result<MessageSet<'a>> {
        // since we're going to keep the original
        // uncompressed vector around without
        // further modifying it and providing
        // publicly no mutability possibilities
        // this is safe
        let ms = try!(MessageSet::from_slice(unsafe {
            mem::transmute(&data[..])
        }));
        return Ok(MessageSet {
            raw_data: Cow::Owned(data),
            messages: ms.messages,
        });
    }

    fn from_slice<'b>(raw_data: &'b [u8]) -> Result<MessageSet<'b>> {
        let mut r = ZReader::new(raw_data);
        let mut msgs = Vec::new();
        while !r.is_empty() {
            let offset = try!(r.read_i64());
            let msg_data = try!(r.read_bytes());
            if msg_data.is_empty() {
                continue;
            }
            // XXX handle multiple compressed messages in future (see
            // KAFKA-1718)
            match ProtocolMessage::from_slice(msg_data) {
                Ok(p) => {
                    // handle compression (denoted by the last 2 bits
                    // of the attr field)
                    match p.attr & 0x03 {
                        c if c == Compression::NONE as i8 => {
                            msgs.push(Message { offset: offset, key: p.key, value: p.value });
                        }
                        c if c == Compression::GZIP as i8 => {
                            return Ok(try!(MessageSet::from_vec(try!(gzip::uncompress(p.value)))));
                        }
                        _ => panic!("Unknown compression type!"),
                    }
                }
                // this is the last messages which might be
                // incomplete; a valid case to be handled by
                // consumers
                Err(Error::UnexpectedEOF) => {
                    debug_assert!(r.is_empty());
                    break;
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
        Ok(MessageSet {
            raw_data: Cow::Borrowed(raw_data),
            messages: msgs,
        })
    }
}

/// Represents a messages exactly as defined in the protocol.
struct ProtocolMessage<'a> {
    crc: i32,
    magic: i8,
    attr: i8,
    key: &'a [u8],
    value: &'a [u8],
}

impl<'a> ProtocolMessage<'a> {
    /// Parses a raw message from the given byte slice.  Does _not_
    /// handle any compression.
    fn from_slice<'b>(raw_data: &'b [u8]) -> Result<ProtocolMessage<'b>> {
        let mut r = ZReader::new(raw_data);

        let msg_crc = try!(r.read_i32());
        // XXX later validate the checksum
        let msg_magic = try!(r.read_i8());
        // XXX validate that `msg_magic == 0`
        let msg_attr = try!(r.read_i8());
        let msg_key = try!(r.read_bytes());
        let msg_val = try!(r.read_bytes());

        debug_assert!(r.is_empty());

        Ok(ProtocolMessage {
            crc: msg_crc,
            magic: msg_magic,
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

    use super::{FetchResponse, Message};

    static FETCH1_TXT: &'static str =
        include_str!("../../test-data/fetch1.txt");
    static FETCH1_FETCH_RESPONSE_NOCOMPRESSION_K0821: &'static [u8] =
        include_bytes!("../../test-data/fetch1.mytopic.1p.nocompression.kafka.0821");
    static FETCH1_FETCH_RESPONSE_GZIP_K0821: &'static [u8] =
        include_bytes!("../../test-data/fetch1.mytopic.1p.gzip.kafka.0821");

    fn into_messages<'a: 'b, 'b>(data: &'a FetchResponse<'b>) -> Vec<&'a Message<'b>> {
        let mut all_msgs = Vec::new();
        for t in data.topics() {
            for p in t.partitions() {
                match p.messages() {
                    Err(_) => {
                        println!("Skipping error partition: {}:{}", t.topic, p.partition);
                    }
                    Ok(msgs) => {
                        all_msgs.extend(msgs);
                    }
                }
            }
        }
        all_msgs
    }

    fn test_decode_new_fetch_response(msg_per_line: &str, fetch_response_bytes: &[u8]) {
        let resp = FetchResponse::from_slice(fetch_response_bytes);
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
        assert!(resp.topics[0].partitions[0].error.is_none());

        let msgs = into_messages(&resp);
        assert_eq!(original.len(), msgs.len());
        for (msg, orig) in msgs.into_iter().zip(original.iter()) {
            assert_eq!(str::from_utf8(msg.value).unwrap(), *orig);
        }
    }

    #[test]
    fn test_decode_new_fetch_response_nocompression_k0821() {
        test_decode_new_fetch_response(FETCH1_TXT, FETCH1_FETCH_RESPONSE_NOCOMPRESSION_K0821);
    }

    #[test]
    fn test_decode_new_fetch_response_gzip_k0821() {
        test_decode_new_fetch_response(FETCH1_TXT, FETCH1_FETCH_RESPONSE_GZIP_K0821);
    }

    #[cfg(feature = "nightly")]
    mod benches {
        use test::{black_box, Bencher};

        use super::super::FetchResponse;
        use super::into_messages;

        fn bench_decode_new_fetch_response(b: &mut Bencher, data: &[u8]) {
            b.bytes = data.len() as u64;
            b.iter(|| {
                let r = black_box(FetchResponse::from_slice(data).unwrap());
                let v = black_box(into_messages(&r));
                v.len()
            });
        }

        #[bench]
        fn bench_decode_new_fetch_response_nocompression_k0821(b: &mut Bencher) {
            bench_decode_new_fetch_response(b, super::FETCH1_FETCH_RESPONSE_NOCOMPRESSION_K0821)
        }

        #[bench]
        fn bench_decode_new_fetch_response_gzip_k0821(b: &mut Bencher) {
            bench_decode_new_fetch_response(b, super::FETCH1_FETCH_RESPONSE_GZIP_K0821)
        }
    }
}
