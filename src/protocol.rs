use std::io;
use std::time::Duration;

mod api_key;
pub mod consumer;
mod decoder;
mod encoder;
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
use self::encoder::Encoder;
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
pub trait Encode<W: io::Write> {
    fn encode(&self, encoder: &mut Encoder<W>) -> Result<(), encoder::Error>;
}

// --------------------------------------------------------------------

#[derive(Debug)]
pub struct HeaderRequest<'a> {
    pub api_key: ApiKey,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: &'a str,
}

impl<W: io::Write> Encode<W> for HeaderRequest<'_> {
    fn encode(&self, encoder: &mut Encoder<W>) -> Result<(), encoder::Error> {
        todo!()
    }
}

impl<'de> Decode<'de> for HeaderRequest<'de> {
    fn decode(decoder: &mut Decoder<'de>) -> Result<Self, decoder::Error> {
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
