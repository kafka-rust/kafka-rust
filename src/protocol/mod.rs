use std::io::{Read, Write};
use std::rc::Rc;

use codecs::{ToByte, FromByte};
use crc::crc32;
use error::Result;

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
// pub use self::consumer::{ConsumerMetadataRequest, ConsumerMetadataResponse};
pub use self::consumer::{OffsetFetchRequest, OffsetFetchResponse,
                         OffsetCommitRequest, OffsetCommitResponse};

// --------------------------------------------------------------------

const API_KEY_PRODUCE: i16   = 0;
const API_KEY_FETCH: i16     = 1;
const API_KEY_OFFSET: i16    = 2;
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

impl FromByte for HeaderResponse {
    type R = HeaderResponse;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        self.correlation.decode(buffer)
    }
}

// this is a replacement for the old HeaderRequest
#[derive(Debug)]
pub struct HeaderRequest_<'a> {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: &'a str,
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

pub fn tocrc(data: &[u8]) -> u32 {
    crc32::checksum_ieee(data)
}
