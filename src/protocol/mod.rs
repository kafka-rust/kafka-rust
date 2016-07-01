use std::io::{Read, Write};
use std::mem;

use codecs::{ToByte, FromByte};
use crc::crc32;
use error::{Error, KafkaCode, Result};

/// Macro to return Result<()> from multiple statements
macro_rules! try_multi {
    ($($expr:expr),*) => ({
        $(try!($expr);)*;
        Ok(())
    })
}

pub mod produce;
pub mod offset;
pub mod metadata;
pub mod consumer;

mod zreader;
pub mod fetch;

// ~ convenient re-exports for request/response types defined in the
// submodules
pub use self::fetch::FetchRequest;
pub use self::produce::{ProduceRequest, ProduceResponse};
pub use self::offset::{OffsetRequest, OffsetResponse};
pub use self::metadata::{MetadataRequest, MetadataResponse};
pub use self::consumer::{GroupCoordinatorRequest, GroupCoordinatorResponse,
                         OffsetFetchVersion, OffsetFetchRequest, OffsetFetchResponse,
                         OffsetCommitVersion, OffsetCommitRequest, OffsetCommitResponse};

// --------------------------------------------------------------------

const API_KEY_PRODUCE: i16   = 0;
const API_KEY_FETCH: i16     = 1;
const API_KEY_OFFSET: i16    = 2;
const API_KEY_METADATA: i16  = 3;
// 4-7 reserved for non-public kafka api services
const API_KEY_OFFSET_COMMIT: i16     = 8;
const API_KEY_OFFSET_FETCH: i16      = 9;
const API_KEY_GROUP_COORDINATOR: i16 = 10;

// the default version of Kafka API we are requesting
const API_VERSION: i16 = 0;

// --------------------------------------------------------------------

/// Provides a way to parse the full raw response data into a
/// particular response structure.
pub trait ResponseParser {
    type T;
    fn parse(&self, response: Vec<u8>) -> Result<Self::T>;
}

// --------------------------------------------------------------------

impl KafkaCode {
    fn from_protocol(n: i16) -> Option<KafkaCode> {
        if n == 0 {
            return None;
        }
        if n >= KafkaCode::OffsetOutOfRange as i16 && n <= KafkaCode::UnsupportedVersion as i16 {
            return Some(unsafe { mem::transmute(n as i8) });
        }
        Some(KafkaCode::Unknown)
    }
}

#[test]
fn test_kafka_code_from_protocol() {
    use std::i16;

    macro_rules! assert_kafka_code {
        ($kcode:path, $n:expr) => {
            assert!(if let Some($kcode) = KafkaCode::from_protocol($n) { true } else { false })
        }
    };

    assert!(if let None = KafkaCode::from_protocol(0) { true } else { false });
    assert_kafka_code!(KafkaCode::OffsetOutOfRange, KafkaCode::OffsetOutOfRange as i16);
    assert_kafka_code!(KafkaCode::IllegalGenerationCode, KafkaCode::IllegalGenerationCode as i16);
    assert_kafka_code!(KafkaCode::UnsupportedVersion, KafkaCode::UnsupportedVersion as i16);
    assert_kafka_code!(KafkaCode::Unknown, KafkaCode::Unknown as i16);
    // ~ test some un mapped non-zero codes; should all map to "unknown"
    assert_kafka_code!(KafkaCode::Unknown, i16::MAX);
    assert_kafka_code!(KafkaCode::Unknown, i16::MIN);
    assert_kafka_code!(KafkaCode::Unknown, -100);
    assert_kafka_code!(KafkaCode::Unknown, 100);
}

// a (sub-) module private method for error
impl Error {
    fn from_protocol(n: i16) -> Option<Error> {
        KafkaCode::from_protocol(n).map(Error::Kafka)
    }
}

// --------------------------------------------------------------------

#[derive(Debug)]
pub struct HeaderRequest<'a> {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: &'a str,
}

impl<'a> HeaderRequest<'a> {
    fn new(api_key: i16, api_version: i16, correlation_id: i32, client_id: &'a str)
           -> HeaderRequest
    {
        HeaderRequest {
            api_key: api_key,
            api_version: api_version,
            correlation_id: correlation_id,
            client_id: client_id,
        }
    }
}

impl<'a> ToByte for HeaderRequest<'a> {
    fn encode<W: Write>(&self, buffer: &mut W) -> Result<()> {
        try_multi!(
            self.api_key.encode(buffer),
            self.api_version.encode(buffer),
            self.correlation_id.encode(buffer),
            self.client_id.encode(buffer))
    }
}

// --------------------------------------------------------------------

#[derive(Default, Debug, Clone)]
pub struct HeaderResponse {
    pub correlation: i32
}

impl FromByte for HeaderResponse {
    type R = HeaderResponse;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        self.correlation.decode(buffer)
    }
}

// --------------------------------------------------------------------

pub fn to_crc(data: &[u8]) -> u32 {
    crc32::checksum_ieee(data)
}
