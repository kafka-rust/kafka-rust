use std::io::{Read, Write};

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

// ~ these modules will see the above defined macro
mod produce;
mod offset;
mod metadata;
mod consumer;

mod zreader;
mod fetch_req;
pub mod fetch;

// ~ re-exports for request/response types defined in submodules
pub use self::fetch_req::FetchRequest;
pub use self::produce::{ProduceRequest, ProduceResponse};
pub use self::offset::{OffsetRequest, OffsetResponse};
pub use self::metadata::{MetadataRequest, MetadataResponse};
// pub use self::consumer::{ConsumerMetadataRequest, ConsumerMetadataResponse};
pub use self::consumer::{OffsetFetchRequest, OffsetFetchResponse,
                         OffsetCommitRequest, OffsetCommitResponse};

// --------------------------------------------------------------------

const API_KEY_PRODUCE: i16   = 0;
const API_KEY_FETCH: i16     = 1;
const API_KEY_OFFSET: i16    = 2;
const API_KEY_METADATA: i16  = 3;
// 4-7 reserved for non-public kafka api services
const API_KEY_OFFSET_COMMIT: i16 = 8;
const API_KEY_OFFSET_FETCH: i16  = 9;
//const API_KEY_CONSUMER_METADATA: i16 = 10;

// the version of Kafka API we are requesting
const API_VERSION: i16 = 0;

// --------------------------------------------------------------------

/// Provides a way to parse the full raw response data into a
/// particular response structure.
pub trait FromResponse : Sized {
    fn from_response(response: Vec<u8>) -> Result<Self>;
}

// --------------------------------------------------------------------

// a (sub-) module private method for error
impl Error {
    fn from_protocol_error(n: i16) -> Option<Error> {
        match n {
            1 => Some(Error::Kafka(KafkaCode::OffsetOutOfRange)),
            2 => Some(Error::Kafka(KafkaCode::InvalidMessage)),
            3 => Some(Error::Kafka(KafkaCode::UnknownTopicOrPartition)),
            4 => Some(Error::Kafka(KafkaCode::InvalidMessageSize)),
            5 => Some(Error::Kafka(KafkaCode::LeaderNotAvailable)),
            6 => Some(Error::Kafka(KafkaCode::NotLeaderForPartition)),
            7 => Some(Error::Kafka(KafkaCode::RequestTimedOut)),
            8 => Some(Error::Kafka(KafkaCode::BrokerNotAvailable)),
            9 => Some(Error::Kafka(KafkaCode::ReplicaNotAvailable)),
            10 => Some(Error::Kafka(KafkaCode::MessageSizeTooLarge)),
            11 => Some(Error::Kafka(KafkaCode::StaleControllerEpochCode)),
            12 => Some(Error::Kafka(KafkaCode::OffsetMetadataTooLargeCode)),
            14 => Some(Error::Kafka(KafkaCode::OffsetsLoadInProgressCode)),
            15 => Some(Error::Kafka(KafkaCode::ConsumerCoordinatorNotAvailableCode)),
            16 => Some(Error::Kafka(KafkaCode::NotCoordinatorForConsumerCode)),
            17 => Some(Error::Kafka(KafkaCode::InvalidTopicCode)),
            18 => Some(Error::Kafka(KafkaCode::RecordListTooLargeCode)),
            19 => Some(Error::Kafka(KafkaCode::NotEnoughReplicasCode)),
            20 => Some(Error::Kafka(KafkaCode::NotEnoughReplicasAfterAppendCode)),
            21 => Some(Error::Kafka(KafkaCode::InvalidRequiredAcksCode)),
            22 => Some(Error::Kafka(KafkaCode::IllegalGenerationCode)),
            23 => Some(Error::Kafka(KafkaCode::InconsistentGroupProtocolCode)),
            24 => Some(Error::Kafka(KafkaCode::InvalidGroupIdCode)),
            25 => Some(Error::Kafka(KafkaCode::UnknownMemberIdCode)),
            26 => Some(Error::Kafka(KafkaCode::InvalidSessionTimeoutCode)),
            27 => Some(Error::Kafka(KafkaCode::RebalanceInProgressCode)),
            28 => Some(Error::Kafka(KafkaCode::InvalidCommitOffsetSizeCode)),
            29 => Some(Error::Kafka(KafkaCode::TopicAuthorizationFailedCode)),
            30 => Some(Error::Kafka(KafkaCode::GroupAuthorizationFailedCode)),
            31 => Some(Error::Kafka(KafkaCode::ClusterAuthorizationFailedCode)),
            -1 => Some(Error::Kafka(KafkaCode::Unknown)),
            _ => None
        }
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
    fn new(api_key: i16, api_version: i16, correlation_id: i32, client_id: &'a str) -> HeaderRequest {
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

pub fn tocrc(data: &[u8]) -> u32 {
    crc32::checksum_ieee(data)
}
