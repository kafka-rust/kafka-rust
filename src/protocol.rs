use std::borrow::Cow;
use std::mem;
use std::time::Duration;

use crate::error::{Error, KafkaCode, Result};

mod api_key;
pub mod consumer;
mod decoder;
mod error;
pub mod metadata;
pub mod offset;
pub mod produce;

pub mod fetch;

use self::api_key::ApiKey;
// ~ convenient re-exports for request/response types defined in the
// submodules
pub use self::consumer::{
    GroupCoordinatorRequest, GroupCoordinatorResponse, OffsetCommitRequest, OffsetCommitResponse,
    OffsetCommitVersion, OffsetFetchRequest, OffsetFetchResponse, OffsetFetchVersion,
};
use self::decoder::Decoder;
pub use self::fetch::FetchRequest;
pub use self::metadata::{MetadataRequest, MetadataResponse};
pub use self::offset::{OffsetRequest, OffsetResponse};
pub use self::produce::{ProduceRequest, ProduceResponse};

// the default version of Kafka API we are requesting
const API_VERSION: i16 = 0;

// --------------------------------------------------------------------

pub trait Decode<'de> {
    fn decode(decoder: &mut Decoder<'de>) -> Result<Self, decoder::Error>;
}

/// Provides a way to parse the full raw response data into a
/// particular response structure.
pub trait ResponseParser {
    type T;
    fn parse(&self, response: Vec<u8>) -> Result<Self::T>;
}

// --------------------------------------------------------------------

#[derive(Debug)]
pub struct HeaderRequest<'a> {
    pub api_key: ApiKey,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: &'a str,
}

impl<'de> Decode<'de> for HeaderRequest<'de> {
    fn decode(decoder: &mut Decoder<'de>) -> Result<Self> {
        Ok(Self {
            api_key: decoder.decode_api_key()?,
            api_version: decoder.decode_int16()?,
            correlation_id: decoder.decode_int32()?,
            client_id: decoder.decode_string()?,
        })
    }
}

// --------------------------------------------------------------------

#[derive(Default, Debug, Clone)]
pub struct HeaderResponse {
    pub correlation: i32,
}

// --------------------------------------------------------------------

pub fn to_crc(data: &[u8]) -> u32 {
    crc32c::crc32c(data)
}

// --------------------------------------------------------------------

/// Safely converts a Duration into the number of milliseconds as a
/// i32 as often required in the kafka protocol.
pub fn to_millis_i32(d: Duration) -> Result<i32> {
    use std::i32;
    let m = d
        .as_secs()
        .saturating_mul(1_000)
        .saturating_add(d.subsec_millis() as u64);
    if m > i32::MAX as u64 {
        Err(Error::InvalidDuration)
    } else {
        Ok(m as i32)
    }
}

#[test]
fn test_to_millis_i32() {
    use std::{i32, u32, u64};

    fn assert_invalid(d: Duration) {
        match to_millis_i32(d) {
            Err(Error::InvalidDuration) => {}
            other => panic!("Expected Err(InvalidDuration) but got {:?}", other),
        }
    }
    fn assert_valid(d: Duration, expected_millis: i32) {
        let r = to_millis_i32(d);
        match r {
            Ok(m) => assert_eq!(expected_millis, m),
            Err(e) => panic!("Expected Ok({}) but got Err({:?})", expected_millis, e),
        }
    }
    assert_valid(Duration::from_millis(1_234), 1_234);
    assert_valid(Duration::new(540, 123_456_789), 540_123);
    assert_invalid(Duration::from_millis(u64::MAX));
    assert_invalid(Duration::from_millis(u32::MAX as u64));
    assert_invalid(Duration::from_millis(i32::MAX as u64 + 1));
    assert_valid(Duration::from_millis(i32::MAX as u64 - 1), i32::MAX - 1);
}
