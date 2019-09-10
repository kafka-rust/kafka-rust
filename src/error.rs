//! Error struct and methods

use std::io;

#[cfg(feature = "security")]
use openssl::error::ErrorStack;
#[cfg(feature = "security")]
use openssl::ssl::{self, Error as SslError};

/// The various errors this library can produce.
error_chain! {
    foreign_links {
        Io(io::Error) #[doc="Input/Output error while communicating with Kafka"];

        Ssl(SslError) #[cfg(feature = "security")] #[doc="An error as reported by OpenSsl"];

        InvalidSnappy(::snap::Error) #[cfg(feature = "snappy")] #[doc="Failure to encode/decode a snappy compressed response from Kafka"];
    }

    errors {
        /// An error as reported by a remote Kafka server
        Kafka(error_code: KafkaCode) {
            description("Kafka Error")
            display("Kafka Error ({:?})", error_code)
        }

        /// An error when transmitting a request for a particular topic and partition.
        /// Contains the topic and partition of the request that failed,
        /// and the error code as reported by the Kafka server, respectively.
        TopicPartitionError(topic_name: String, partition_id: i32, error_code: KafkaCode) {
            description("Error in request for topic and partition")
            display("Topic Partition Error ({:?}, {:?}, {:?})", topic_name, partition_id, error_code)
        }

        /// Failure to correctly parse the server response due to the
        /// server speaking a newer protocol version (than the one this
        /// library supports)
        UnsupportedProtocol {
            description("Unsupported protocol version")
        }

        /// Failure to correctly parse the server response by this library
        /// due to an unsupported compression format of the data
        UnsupportedCompression {
            description("Unsupported compression format")
        }

        /// Failure to decode a response due to an insufficient number of bytes available
        UnexpectedEOF {
            description("Unexpected EOF")
        }

        /// Failure to decode or encode a response or request respectively
        CodecError {
            description("Encoding/Decoding Error")
        }

        /// Failure to decode a string into a valid utf8 byte sequence
        StringDecodeError {
            description("String decoding error")
        }

        /// Unable to reach any host
        NoHostReachable {
            description("No host reachable")
        }

        /// Unable to set up `Consumer` due to missing topic assignments
        NoTopicsAssigned {
            description("No topic assigned")
        }

        /// An invalid user-provided duration
        InvalidDuration {
            description("Invalid duration")
        }
    }
}

/// Various errors reported by a remote Kafka server.
/// See also [Kafka Errors](http://kafka.apache.org/protocol.html)
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum KafkaCode {
    /// An unexpected server error
    Unknown = -1,
    /// The requested offset is outside the range of offsets
    /// maintained by the server for the given topic/partition
    OffsetOutOfRange = 1,
    /// This indicates that a message contents does not match its CRC
    CorruptMessage = 2,
    /// This request is for a topic or partition that does not exist
    /// on this broker.
    UnknownTopicOrPartition = 3,
    /// The message has a negative size
    InvalidMessageSize = 4,
    /// This error is thrown if we are in the middle of a leadership
    /// election and there is currently no leader for this partition
    /// and hence it is unavailable for writes.
    LeaderNotAvailable = 5,
    /// This error is thrown if the client attempts to send messages
    /// to a replica that is not the leader for some partition. It
    /// indicates that the clients metadata is out of date.
    NotLeaderForPartition = 6,
    /// This error is thrown if the request exceeds the user-specified
    /// time limit in the request.
    RequestTimedOut = 7,
    /// This is not a client facing error and is used mostly by tools
    /// when a broker is not alive.
    BrokerNotAvailable = 8,
    /// If replica is expected on a broker, but is not (this can be
    /// safely ignored).
    ReplicaNotAvailable = 9,
    /// The server has a configurable maximum message size to avoid
    /// unbounded memory allocation. This error is thrown if the
    /// client attempt to produce a message larger than this maximum.
    MessageSizeTooLarge = 10,
    /// Internal error code for broker-to-broker communication.
    StaleControllerEpoch = 11,
    /// If you specify a string larger than configured maximum for
    /// offset metadata
    OffsetMetadataTooLarge = 12,
    /// The server disconnected before a response was received.
    NetworkException = 13,
    /// The broker returns this error code for an offset fetch request
    /// if it is still loading offsets (after a leader change for that
    /// offsets topic partition), or in response to group membership
    /// requests (such as heartbeats) when group metadata is being
    /// loaded by the coordinator.
    GroupLoadInProgress = 14,
    /// The broker returns this error code for group coordinator
    /// requests, offset commits, and most group management requests
    /// if the offsets topic has not yet been created, or if the group
    /// coordinator is not active.
    GroupCoordinatorNotAvailable = 15,
    /// The broker returns this error code if it receives an offset
    /// fetch or commit request for a group that it is not a
    /// coordinator for.
    NotCoordinatorForGroup = 16,
    /// For a request which attempts to access an invalid topic
    /// (e.g. one which has an illegal name), or if an attempt is made
    /// to write to an internal topic (such as the consumer offsets
    /// topic).
    InvalidTopic = 17,
    /// If a message batch in a produce request exceeds the maximum
    /// configured segment size.
    RecordListTooLarge = 18,
    /// Returned from a produce request when the number of in-sync
    /// replicas is lower than the configured minimum and requiredAcks is
    /// -1.
    NotEnoughReplicas = 19,
    /// Returned from a produce request when the message was written
    /// to the log, but with fewer in-sync replicas than required.
    NotEnoughReplicasAfterAppend = 20,
    /// Returned from a produce request if the requested requiredAcks is
    /// invalid (anything other than -1, 1, or 0).
    InvalidRequiredAcks = 21,
    /// Returned from group membership requests (such as heartbeats) when
    /// the generation id provided in the request is not the current
    /// generation.
    IllegalGeneration = 22,
    /// Returned in join group when the member provides a protocol type or
    /// set of protocols which is not compatible with the current group.
    InconsistentGroupProtocol = 23,
    /// Returned in join group when the groupId is empty or null.
    InvalidGroupId = 24,
    /// Returned from group requests (offset commits/fetches, heartbeats,
    /// etc) when the memberId is not in the current generation.
    UnknownMemberId = 25,
    /// Return in join group when the requested session timeout is outside
    /// of the allowed range on the broker
    InvalidSessionTimeout = 26,
    /// Returned in heartbeat requests when the coordinator has begun
    /// rebalancing the group. This indicates to the client that it
    /// should rejoin the group.
    RebalanceInProgress = 27,
    /// This error indicates that an offset commit was rejected because of
    /// oversize metadata.
    InvalidCommitOffsetSize = 28,
    /// Returned by the broker when the client is not authorized to access
    /// the requested topic.
    TopicAuthorizationFailed = 29,
    /// Returned by the broker when the client is not authorized to access
    /// a particular groupId.
    GroupAuthorizationFailed = 30,
    /// Returned by the broker when the client is not authorized to use an
    /// inter-broker or administrative API.
    ClusterAuthorizationFailed = 31,
    /// The timestamp of the message is out of acceptable range.
    InvalidTimestamp = 32,
    /// The broker does not support the requested SASL mechanism.
    UnsupportedSaslMechanism = 33,
    /// Request is not valid given the current SASL state.
    IllegalSaslState = 34,
    /// The version of API is not supported.
    UnsupportedVersion = 35,
}

#[cfg(feature = "security")]
impl<S> From<ssl::HandshakeError<S>> for Error {
    fn from(err: ssl::HandshakeError<S>) -> Error {
        match err {
            ssl::HandshakeError::SetupFailure(e) => from_sslerror_ref(&From::from(e)).into(),
            ssl::HandshakeError::Failure(s) | ssl::HandshakeError::WouldBlock(s) => {
                from_sslerror_ref(s.error()).into()
            }
        }
    }
}

impl Clone for Error {
    fn clone(&self) -> Error {
        match self {
            &Error(ErrorKind::Io(ref err), _) => ErrorKind::Io(clone_ioe(err)).into(),
            &Error(ErrorKind::Kafka(x), _) => ErrorKind::Kafka(x).into(),
            &Error(ErrorKind::TopicPartitionError(ref topic, partition, error_code), _) => {
                ErrorKind::TopicPartitionError(topic.clone(), partition, error_code).into()
            }
            #[cfg(feature = "security")]
            &Error(ErrorKind::Ssl(ref x), _) => from_sslerror_ref(x).into(),
            &Error(ErrorKind::UnsupportedProtocol, _) => ErrorKind::UnsupportedProtocol.into(),
            &Error(ErrorKind::UnsupportedCompression, _) => {
                ErrorKind::UnsupportedCompression.into()
            }
            #[cfg(feature = "snappy")]
            &Error(ErrorKind::InvalidSnappy(ref err), _) => from_snap_error_ref(err).into(),
            &Error(ErrorKind::UnexpectedEOF, _) => ErrorKind::UnexpectedEOF.into(),
            &Error(ErrorKind::CodecError, _) => ErrorKind::CodecError.into(),
            &Error(ErrorKind::StringDecodeError, _) => ErrorKind::StringDecodeError.into(),
            &Error(ErrorKind::NoHostReachable, _) => ErrorKind::NoHostReachable.into(),
            &Error(ErrorKind::NoTopicsAssigned, _) => ErrorKind::NoTopicsAssigned.into(),
            &Error(ErrorKind::InvalidDuration, _) => ErrorKind::InvalidDuration.into(),
            &Error(ErrorKind::Msg(ref msg), _) => ErrorKind::Msg(msg.clone()).into(),
        }
    }
}

#[cfg(feature = "security")]
fn from_sslerror_ref(err: &ssl::Error) -> ErrorKind {
    if let Some(io_err) = err.io_error() {
        return ErrorKind::Io(clone_ioe(io_err));
    }
    if let Some(ssl_err) = err.ssl_error() {
        return ErrorKind::Ssl(From::from(ssl_err.to_owned()));
    }

    // xxx: return something as default?
    unreachable!()
}

#[cfg(feature = "snappy")]
fn from_snap_error_ref(err: &::snap::Error) -> ErrorKind {
    match err {
        &::snap::Error::TooBig { given, max } => {
            ErrorKind::InvalidSnappy(::snap::Error::TooBig { given, max })
        }
        &::snap::Error::BufferTooSmall { given, min } => {
            ErrorKind::InvalidSnappy(::snap::Error::BufferTooSmall { given, min })
        }
        &::snap::Error::Empty => ErrorKind::InvalidSnappy(::snap::Error::Empty),
        &::snap::Error::Header => ErrorKind::InvalidSnappy(::snap::Error::Header),
        &::snap::Error::HeaderMismatch {
            expected_len,
            got_len,
        } => ErrorKind::InvalidSnappy(::snap::Error::HeaderMismatch {
            expected_len,
            got_len,
        }),
        &::snap::Error::Literal {
            len,
            src_len,
            dst_len,
        } => ErrorKind::InvalidSnappy(::snap::Error::Literal {
            len,
            src_len,
            dst_len,
        }),
        &::snap::Error::CopyRead { len, src_len } => {
            ErrorKind::InvalidSnappy(::snap::Error::CopyRead { len, src_len })
        }
        &::snap::Error::CopyWrite { len, dst_len } => {
            ErrorKind::InvalidSnappy(::snap::Error::CopyWrite { len, dst_len })
        }
        &::snap::Error::Offset { offset, dst_pos } => {
            ErrorKind::InvalidSnappy(::snap::Error::Offset { offset, dst_pos })
        }
        &::snap::Error::StreamHeader { byte } => {
            ErrorKind::InvalidSnappy(::snap::Error::StreamHeader { byte })
        }
        &::snap::Error::StreamHeaderMismatch { ref bytes } => {
            ErrorKind::InvalidSnappy(::snap::Error::StreamHeaderMismatch {
                bytes: bytes.clone(),
            })
        }
        &::snap::Error::UnsupportedChunkType { byte } => {
            ErrorKind::InvalidSnappy(::snap::Error::UnsupportedChunkType { byte })
        }
        &::snap::Error::UnsupportedChunkLength { len, header } => {
            ErrorKind::InvalidSnappy(::snap::Error::UnsupportedChunkLength { len, header })
        }
        &::snap::Error::Checksum { expected, got } => {
            ErrorKind::InvalidSnappy(::snap::Error::Checksum { expected, got })
        }
    }
}

/// Attempt to clone `io::Error`.
fn clone_ioe(e: &io::Error) -> io::Error {
    match e.raw_os_error() {
        Some(code) => io::Error::from_raw_os_error(code),
        None => io::Error::new(e.kind(), format!("Io error: {}", e)),
    }
}
