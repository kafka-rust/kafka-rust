//! Error struct and methods

use std::{io, result, sync::Arc};
use thiserror::Error;

pub type Result<T> = result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] io::Error),

    #[cfg(feature = "security")]
    #[error(transparent)]
    Ssl(#[from] rustls::Error),

    #[cfg(feature = "snappy")]
    #[error(transparent)]
    InvalidSnappy(#[from] ::snap::Error),

    /// An error as reported by a remote Kafka server
    #[error("Kafka Error ({0:?})")]
    Kafka(KafkaCode),

    /// An error when transmitting a request for a particular topic and partition.
    /// Contains the topic and partition of the request that failed,
    /// and the error code as reported by the Kafka server, respectively.
    #[error("Topic Partition Error ({topic_name:?}, {partition_id:?}, {error_code:?})")]
    TopicPartitionError {
        topic_name: String,
        partition_id: i32,
        error_code: KafkaCode,
    },

    /// Failure to correctly parse the server response due to the
    /// server speaking a newer protocol version (than the one this
    /// library supports)
    #[error("Unsupported protocol version")]
    UnsupportedProtocol,

    /// Failure to correctly parse the server response by this library
    /// due to an unsupported compression format of the data
    #[error("Unsupported compression format")]
    UnsupportedCompression,

    /// Failure to decode a response due to an insufficient number of bytes available
    #[error("Unexpected EOF")]
    UnexpectedEOF,

    /// Failure to decode or encode a response or request respectively
    #[error("Encoding/Decoding Error")]
    CodecError,

    /// Failure to decode a string into a valid utf8 byte sequence
    #[error("String decoding error")]
    StringDecodeError,

    /// Unable to reach any host
    #[error("No host reachable")]
    NoHostReachable,

    /// Unable to set up `Consumer` due to missing topic assignments
    #[error("No topic assigned")]
    NoTopicsAssigned,

    /// An invalid user-provided duration
    #[error("Invalid duration")]
    InvalidDuration,

    #[error(transparent)]
    ArcSelf(#[from] Arc<Self>),

    #[error("Operation requires offset storage but no offset storage was set")]
    UnsetOffsetStorage,

    #[error("Operation requires group id but no group was set")]
    UnsetGroupId,
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
    // CAUTION! When adding to this list, KafkaCode::from_protocol must be updated. If there's a better way, please open an issue for it!
}
