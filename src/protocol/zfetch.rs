//! An efficient response parser for the "fetch messages" use-case.

use std::borrow::Cow;
use std::io::Read;
use std::mem;
use std::slice::Iter;

use error::{Error, Result};
use compression::{gzip, Compression};
use compression::snappy::SnappyReader;

use super::FromResponse;
use super::zreader::ZReader;

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
pub struct FetchResponse<'a> {
    #[allow(dead_code)]
    raw_data: Cow<'a, [u8]>, // ~ this field is used to potentially "own" the underlying vector
    /// The id corresponding to the fetch messages request (provided
    /// for debugging purposes only)
    pub correlation_id: i32,
    topics: Vec<TopicFetchResponse<'a>>,
}

impl<'a> FromResponse for FetchResponse<'a> {
    fn from_response(response: Vec<u8>) -> Result<Self> {
        FetchResponse::from_slice(Cow::Owned(response))
    }
}

impl<'a> FetchResponse<'a> {
    /// Parses a FetchResponse from binary data as defined by the
    /// Kafka Protocol.
    fn from_slice(response: Cow<'a, [u8]>) -> Result<FetchResponse<'a>> {
        let slice = unsafe { mem::transmute(&response[..]) };
        let mut r = ZReader::new(slice);
        let correlation_id = try!(r.read_i32());
        let topics = array_of!(r, TopicFetchResponse::read(&mut r));
        Ok(FetchResponse {
            raw_data: response,
            correlation_id: correlation_id,
            topics: topics
        })
    }

    /// Provides an iterator over all the topics and the fetched data
    /// relative to these topics.
    #[inline]
    pub fn topics<'b>(&'b self) -> Topics<'b, 'a> {
        Topics { iter: self.topics.iter() }
    }
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


/// The result of a "fetch messages" request from a particular Kafka
/// broker for a single topic only.  Beside the name of the topic,
/// this structure provides an iterator over the topic partitions from
/// which messages were requested.
pub struct TopicFetchResponse<'a> {
    /// The identifier/name of the represented topic.
    pub topic: &'a str,

    partitions: Vec<PartitionFetchResponse<'a>>,
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

    /// Provides an iterator over all the partitions of this topic for
    /// which messages were requested.
    #[inline]
    pub fn partitions<'b>(&'b self) -> Partitions<'b, 'a> {
        Partitions { iter: self.partitions.iter() }
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


/// The result of a "fetch messages" request from a particular Kafka
/// broker for a single topic partition only.  Beside the partition
/// identifier, this structure provides an iterator over the actually
/// requested message data.
///
/// Note: There might have been an error for a particular partition
/// (but not for another), in which case data accessors of this
/// structure will result in that error.
pub struct PartitionFetchResponse<'a> {
    /// The identifier of the represented partition.
    pub partition: i32,

    error: Option<Error>,
    highwatermark_offset: i64,
    message_set: MessageSet<'a>,
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

    /// Retrieves the so-called "high water mark offset" indicating
    /// the "latest" offset for this partition at the remote broker.
    /// This can be used by clients to find out how much behind the
    /// latest message available in this particular partition they are
    /// behind.
    #[inline]
    pub fn highwatermark_offset(&self) -> Result<i64> {
        match self.error {
            None => Ok(self.highwatermark_offset),
            Some(ref e) => Err(e.clone()),
        }
    }

    /// Retrieves an iterator over the messages fetched for this
    /// particular topic partition.
    #[inline]
    pub fn messages<'b>(&'b self) -> Result<Messages<'b, 'a>> {
        match self.error {
            None => Ok(Messages { iter: self.message_set.messages.iter() }),
            Some(ref e) => Err(e.clone()),
        }
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


struct MessageSet<'a> {
    #[allow(dead_code)]
    raw_data: Cow<'a, [u8]>, // ~ this field is used to potentially "own" the underlying vector
    messages: Vec<Message<'a>>,
}

/// A fetched messages from a remote Kafka broker for a particular
/// topic partition.
pub struct Message<'a> {
    /// The offset at which this message resides in the remote kafka
    /// broker topic partition.
    pub offset: i64,
    /// The key data of this message.  Empty if there is no such data
    /// for this message.
    pub key: &'a [u8],
    /// The value data of this message.  Empty if there is no such
    /// data for this message.
    pub value: &'a [u8],
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
            match MessageSet::next_message(&mut r) {
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
                            return Ok(try!(MessageSet::from_vec(v)));
                        }
                        c if c == Compression::SNAPPY as i8 => {
                            let mut v = Vec::new();
                            try!(try!(SnappyReader::new(pmsg.value)).read_to_end(&mut v));
                            return Ok(try!(MessageSet::from_vec(v)));
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

    fn next_message<'b>(r: &mut ZReader<'b>) -> Result<(i64, ProtocolMessage<'b>)> {
        let offset = try!(r.read_i64());
        let msg_data = try!(r.read_bytes());
        Ok((offset, try!(ProtocolMessage::from_slice(msg_data))))
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
    use std::borrow::Cow;

    use super::{FetchResponse, Message};

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
        let resp = FetchResponse::from_slice(Cow::Borrowed(fetch_response_bytes));
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
    fn test_from_slice_nocompression_k0821() {
        test_decode_new_fetch_response(FETCH1_TXT, FETCH1_FETCH_RESPONSE_NOCOMPRESSION_K0821);
    }

    #[test]
    fn test_from_slice_snappy_k0821() {
        test_decode_new_fetch_response(FETCH1_TXT, FETCH1_FETCH_RESPONSE_SNAPPY_K0821);
    }

    #[test]
    fn test_from_slice_snappy_k0822() {
        test_decode_new_fetch_response(FETCH1_TXT, FETCH1_FETCH_RESPONSE_SNAPPY_K0822);
    }

    #[test]
    fn test_from_slice_gzip_k0821() {
        test_decode_new_fetch_response(FETCH1_TXT, FETCH1_FETCH_RESPONSE_GZIP_K0821);
    }

    #[cfg(feature = "nightly")]
    mod benches {
        use std::borrow::Cow;
        use test::{black_box, Bencher};

        use super::super::FetchResponse;
        use super::into_messages;

        fn bench_decode_new_fetch_response(b: &mut Bencher, data: &[u8]) {
            b.bytes = data.len() as u64;
            b.iter(|| {
                let r = black_box(FetchResponse::from_slice(Cow::Borrowed(data)).unwrap());
                let v = black_box(into_messages(&r));
                v.len()
            });
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
