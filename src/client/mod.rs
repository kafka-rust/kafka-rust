//! Kafka Client - A mid-level abstraction for a kafka cluster
//! allowing building higher level constructs.
//!
//! The entry point into this module is `KafkaClient` obtained by a
//! call to `KafkaClient::new()`.

use std;
use std::collections::hash_map;
use std::collections::hash_map::HashMap;
use std::io::Cursor;
use std::iter::Iterator;
use std::mem;
use std::thread;
use std::time::{Duration, Instant};

// pub re-export
pub use compression::Compression;
pub use utils::PartitionOffset;

#[cfg(feature = "security")]
pub use self::network::SecurityConfig;

use codecs::{FromByte, ToByte};
use error::{Error, ErrorKind, KafkaCode, Result};
use protocol::{self, ResponseParser};

use client_internals::KafkaClientInternals;

pub mod metadata;
mod network;
mod state;

// ~ re-export (only) certain types from the protocol::fetch module as
// 'client::fetch'.
pub mod fetch {
    //! A representation of fetched messages from Kafka.

    pub use protocol::fetch::{Data, Message, Partition, Response, Topic};
}

const DEFAULT_CONNECTION_RW_TIMEOUT_SECS: u64 = 120;

fn default_conn_rw_timeout() -> Option<Duration> {
    match DEFAULT_CONNECTION_RW_TIMEOUT_SECS {
        0 => None,
        n => Some(Duration::from_secs(n)),
    }
}

/// The default value for `KafkaClient::set_compression(..)`
pub const DEFAULT_COMPRESSION: Compression = Compression::NONE;

/// The default value for `KafkaClient::set_fetch_max_wait_time(..)`
pub const DEFAULT_FETCH_MAX_WAIT_TIME_MILLIS: u64 = 100;

/// The default value for `KafkaClient::set_fetch_min_bytes(..)`
pub const DEFAULT_FETCH_MIN_BYTES: i32 = 4096;

/// The default value for `KafkaClient::set_fetch_max_bytes(..)`
pub const DEFAULT_FETCH_MAX_BYTES_PER_PARTITION: i32 = 32 * 1024;

/// The default value for `KafkaClient::set_fetch_crc_validation(..)`
pub const DEFAULT_FETCH_CRC_VALIDATION: bool = true;

/// The default value for `KafkaClient::set_group_offset_storage(..)`
pub const DEFAULT_GROUP_OFFSET_STORAGE: GroupOffsetStorage = GroupOffsetStorage::Zookeeper;

/// The default value for `KafkaClient::set_retry_backoff_time(..)`
pub const DEFAULT_RETRY_BACKOFF_TIME_MILLIS: u64 = 100;

/// The default value for `KafkaClient::set_retry_max_attempts(..)`
// the default value: re-attempt a repeatable operation for
// approximetaly up to two minutes
pub const DEFAULT_RETRY_MAX_ATTEMPTS: u32 = 120_000 / DEFAULT_RETRY_BACKOFF_TIME_MILLIS as u32;

/// The default value for `KafkaClient::set_connection_idle_timeout(..)`
pub const DEFAULT_CONNECTION_IDLE_TIMEOUT_MILLIS: u64 = 540_000;

/// Client struct keeping track of brokers and topic metadata.
///
/// Implements methods described by the [Kafka Protocol](http://kafka.apache.org/protocol.html).
///
/// You will have to load metadata before making any other request.
#[derive(Debug)]
pub struct KafkaClient {
    // ~ this kafka client configuration
    config: ClientConfig,

    // ~ a pool of re-usable connections to kafka brokers
    conn_pool: network::Connections,

    // ~ the current state of this client
    state: state::ClientState,
}

#[derive(Debug)]
struct ClientConfig {
    client_id: String,
    hosts: Vec<String>,
    // ~ compression to use when sending messages
    compression: Compression,
    // ~ these are the defaults when fetching messages for details
    // refer to the kafka wire protocol
    fetch_max_wait_time: i32,
    fetch_min_bytes: i32,
    fetch_max_bytes_per_partition: i32,
    fetch_crc_validation: bool,
    // ~ the version of the API to use for the corresponding kafka
    // calls; note that this might have an effect on the storage type
    // kafka will then use (zookeeper or __consumer_offsets).  it is
    // important to use version for both of them which target the same
    // storage type.
    offset_fetch_version: protocol::OffsetFetchVersion,
    offset_commit_version: protocol::OffsetCommitVersion,
    // ~ the duration to wait before retrying a failed
    // operation like refreshing group coordinators; this avoids
    // operation retries in a tight loop.
    retry_backoff_time: Duration,
    // ~ the number of repeated retry attempts; prevents endless
    // repetition of a retry attempt
    retry_max_attempts: u32,
}

// --------------------------------------------------------------------

/// Possible values when querying a topic's offset.
/// See `KafkaClient::fetch_offsets`.
#[derive(Debug, Copy, Clone)]
pub enum FetchOffset {
    /// Receive the earliest available offset.
    Earliest,
    /// Receive the latest offset.
    Latest,
    /// Used to ask for all messages before a certain time (ms); unix
    /// timestamp in milliseconds.
    /// See https://cwiki.apache.org/confluence/display/KAFKA/Writing+a+Driver+for+Kafka#WritingaDriverforKafka-Offsets
    ByTime(i64),
}

impl FetchOffset {
    fn to_kafka_value(&self) -> i64 {
        match *self {
            FetchOffset::Earliest => -2,
            FetchOffset::Latest => -1,
            FetchOffset::ByTime(n) => n,
        }
    }
}

// --------------------------------------------------------------------

/// Defines the availale storage types to utilize when fetching or
/// comitting group offsets.  See also `KafkaClient::set_group_offset_storage`.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum GroupOffsetStorage {
    /// Zookeeper based storage (available as of kafka 0.8.1)
    Zookeeper,
    /// Kafka based storage (available as of Kafka 0.8.2). This is the
    /// prefered method for groups to store their offsets at.
    Kafka,
}

impl GroupOffsetStorage {
    fn offset_fetch_version(self) -> protocol::OffsetFetchVersion {
        match self {
            GroupOffsetStorage::Zookeeper => protocol::OffsetFetchVersion::V0,
            GroupOffsetStorage::Kafka => protocol::OffsetFetchVersion::V1,
        }
    }
    fn offset_commit_version(self) -> protocol::OffsetCommitVersion {
        match self {
            GroupOffsetStorage::Zookeeper => protocol::OffsetCommitVersion::V0,
            // ~ if we knew we'll be communicating with a kafka 0.9+
            // broker we could set the commit-version to V2; however,
            // since we still want to support Kafka 0.8.2 versions,
            // we'll go with the less efficient but safe option V1.
            GroupOffsetStorage::Kafka => protocol::OffsetCommitVersion::V1,
        }
    }
}

/// Data point identifying a topic partition to fetch a group's offset
/// for.  See `KafkaClient::fetch_group_offsets`.
#[derive(Debug)]
pub struct FetchGroupOffset<'a> {
    /// The topic to fetch the group offset for
    pub topic: &'a str,
    /// The partition to fetch the group offset for
    pub partition: i32,
}

impl<'a> FetchGroupOffset<'a> {
    #[inline]
    pub fn new(topic: &'a str, partition: i32) -> Self {
        FetchGroupOffset { topic, partition }
    }
}

impl<'a> AsRef<FetchGroupOffset<'a>> for FetchGroupOffset<'a> {
    fn as_ref(&self) -> &Self {
        self
    }
}

// --------------------------------------------------------------------

/// Data point identifying a particular topic partition offset to be
/// commited.
/// See `KafkaClient::commit_offsets`.
#[derive(Debug)]
pub struct CommitOffset<'a> {
    /// The offset to be committed
    pub offset: i64,
    /// The topic to commit the offset for
    pub topic: &'a str,
    /// The partition to commit the offset for
    pub partition: i32,
}

impl<'a> CommitOffset<'a> {
    pub fn new(topic: &'a str, partition: i32, offset: i64) -> Self {
        CommitOffset {
            topic,
            partition,
            offset,
        }
    }
}

impl<'a> AsRef<CommitOffset<'a>> for CommitOffset<'a> {
    fn as_ref(&self) -> &Self {
        self
    }
}

// --------------------------------------------------------------------

/// Possible choices on acknowledgement requirements when
/// producing/sending messages to Kafka. See
/// `KafkaClient::produce_messages`.
#[derive(Debug, Copy, Clone)]
pub enum RequiredAcks {
    /// Indicates to the receiving Kafka broker not to acknowlegde
    /// messages sent to it at all. Sending messages with this
    /// acknowledgement requirement translates into a fire-and-forget
    /// scenario which - of course - is very fast but not reliable.
    None = 0,
    /// Requires the receiving Kafka broker to wait until the sent
    /// messages are written to local disk.  Such messages can be
    /// regarded as acknowledged by one broker in the cluster.
    One = 1,
    /// Requires the sent messages to be acknowledged by all in-sync
    /// replicas of the targeted topic partitions.
    All = -1,
}

// --------------------------------------------------------------------

/// Message data to be sent/produced to a particular topic partition.
/// See `KafkaClient::produce_messages` and `Producer::send`.
#[derive(Debug)]
pub struct ProduceMessage<'a, 'b> {
    /// The "key" data of this message.
    pub key: Option<&'b [u8]>,

    /// The "value" data of this message.
    pub value: Option<&'b [u8]>,

    /// The topic to produce this message to.
    pub topic: &'a str,

    /// The partition (of the corresponding topic) to produce this
    /// message to.
    pub partition: i32,
}

impl<'a, 'b> AsRef<ProduceMessage<'a, 'b>> for ProduceMessage<'a, 'b> {
    fn as_ref(&self) -> &Self {
        self
    }
}

impl<'a, 'b> ProduceMessage<'a, 'b> {
    /// A convenient constructor method to create a new produce
    /// message with all attributes specified.
    pub fn new(
        topic: &'a str,
        partition: i32,
        key: Option<&'b [u8]>,
        value: Option<&'b [u8]>,
    ) -> Self {
        ProduceMessage {
            key,
            value,
            topic,
            partition,
        }
    }
}

// --------------------------------------------------------------------

/// Partition related request data for fetching messages.
/// See `KafkaClient::fetch_messages`.
#[derive(Debug)]
pub struct FetchPartition<'a> {
    /// The topic to fetch messages from.
    pub topic: &'a str,

    /// The offset as of which to fetch messages.
    pub offset: i64,

    /// The partition to fetch messasges from.
    pub partition: i32,

    /// Specifies the max. amount of data to fetch (for this
    /// partition.)  This implicitely defines the biggest message the
    /// client can accept.  If this value is too small, no messages
    /// can be delivered.  Setting this size should be in sync with
    /// the producers to the partition.
    ///
    /// Zero or negative values are treated as "unspecified".
    pub max_bytes: i32,
}

impl<'a> FetchPartition<'a> {
    /// Creates a new "fetch messages" request structure with an
    /// unspecified `max_bytes`.
    pub fn new(topic: &'a str, partition: i32, offset: i64) -> Self {
        FetchPartition {
            topic,
            partition,
            offset,
            max_bytes: -1,
        }
    }

    /// Sets the `max_bytes` value for the "fetch messages" request.
    pub fn with_max_bytes(mut self, max_bytes: i32) -> Self {
        self.max_bytes = max_bytes;
        self
    }
}

impl<'a> AsRef<FetchPartition<'a>> for FetchPartition<'a> {
    fn as_ref(&self) -> &Self {
        self
    }
}

/// A confirmation of messages sent back by the Kafka broker
/// to confirm delivery of producer messages.
#[derive(Debug)]
pub struct ProduceConfirm {
    /// The topic the messages were sent to.
    pub topic: String,

    /// The list of individual confirmations for each offset and partition.
    pub partition_confirms: Vec<ProducePartitionConfirm>,
}

/// A confirmation of messages sent back by the Kafka broker
/// to confirm delivery of producer messages for a particular topic.
#[derive(Debug)]
pub struct ProducePartitionConfirm {
    /// The offset assigned to the first message in the message set appended
    /// to this partition, or an error if one occurred.
    pub offset: std::result::Result<i64, KafkaCode>,

    /// The partition to which the message(s) were appended.
    pub partition: i32,
}

// --------------------------------------------------------------------

impl KafkaClient {
    /// Creates a new instance of KafkaClient. Before being able to
    /// successfully use the new client, you'll have to load metadata.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// let mut client = kafka::client::KafkaClient::new(vec!("localhost:9092".to_owned()));
    /// client.load_metadata_all().unwrap();
    /// ```
    pub fn new(hosts: Vec<String>) -> KafkaClient {
        KafkaClient {
            config: ClientConfig {
                client_id: String::new(),
                hosts,
                compression: DEFAULT_COMPRESSION,
                fetch_max_wait_time: protocol::to_millis_i32(Duration::from_millis(
                    DEFAULT_FETCH_MAX_WAIT_TIME_MILLIS,
                ))
                .expect("invalid default-fetch-max-time-millis"),
                fetch_min_bytes: DEFAULT_FETCH_MIN_BYTES,
                fetch_max_bytes_per_partition: DEFAULT_FETCH_MAX_BYTES_PER_PARTITION,
                fetch_crc_validation: DEFAULT_FETCH_CRC_VALIDATION,
                offset_fetch_version: DEFAULT_GROUP_OFFSET_STORAGE.offset_fetch_version(),
                offset_commit_version: DEFAULT_GROUP_OFFSET_STORAGE.offset_commit_version(),
                retry_backoff_time: Duration::from_millis(DEFAULT_RETRY_BACKOFF_TIME_MILLIS),
                retry_max_attempts: DEFAULT_RETRY_MAX_ATTEMPTS,
            },
            conn_pool: network::Connections::new(
                default_conn_rw_timeout(),
                Duration::from_millis(DEFAULT_CONNECTION_IDLE_TIMEOUT_MILLIS),
            ),
            state: state::ClientState::new(),
        }
    }

    /// Creates a new secure instance of KafkaClient. Before being able to
    /// successfully use the new client, you'll have to load metadata.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// extern crate openssl;
    /// extern crate kafka;
    ///
    /// use openssl::ssl::{SslConnector, SslMethod, SslFiletype, SslVerifyMode};
    /// use kafka::client::{KafkaClient, SecurityConfig};
    ///
    /// fn main() {
    ///     let (key, cert) = ("client.key".to_string(), "client.crt".to_string());
    ///
    ///     // OpenSSL offers a variety of complex configurations. Here is an example:
    ///     let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
    ///     builder.set_cipher_list("DEFAULT").unwrap();
    ///     builder
    ///         .set_certificate_file(cert, SslFiletype::PEM)
    ///         .unwrap();
    ///     builder
    ///         .set_private_key_file(key, SslFiletype::PEM)
    ///         .unwrap();
    ///     builder.check_private_key().unwrap();
    ///     builder.set_default_verify_paths().unwrap();
    ///     builder.set_verify(SslVerifyMode::PEER);
    ///     let connector = builder.build();
    ///
    ///     let mut client = KafkaClient::new_secure(vec!("localhost:9092".to_owned()),
    ///                                              SecurityConfig::new(connector));
    ///     client.load_metadata_all().unwrap();
    /// }
    /// ```
    /// See also `SecurityConfig#with_hostname_verification` to disable hostname verification.
    ///
    /// See also `KafkaClient::load_metadatata_all` and
    /// `KafkaClient::load_metadata` methods, the creates
    /// [openssl](https://crates.io/crates/openssl)
    /// and [openssl_verify](https://crates.io/crates/openssl-verify),
    /// as well as
    /// [Kafka's documentation](https://kafka.apache.org/documentation.html#security_ssl).
    #[cfg(feature = "security")]
    pub fn new_secure(hosts: Vec<String>, security: SecurityConfig) -> KafkaClient {
        KafkaClient {
            config: ClientConfig {
                client_id: String::new(),
                hosts,
                compression: DEFAULT_COMPRESSION,
                fetch_max_wait_time: protocol::to_millis_i32(Duration::from_millis(
                    DEFAULT_FETCH_MAX_WAIT_TIME_MILLIS,
                ))
                .expect("invalid default-fetch-max-time-millis"),
                fetch_min_bytes: DEFAULT_FETCH_MIN_BYTES,
                fetch_max_bytes_per_partition: DEFAULT_FETCH_MAX_BYTES_PER_PARTITION,
                fetch_crc_validation: DEFAULT_FETCH_CRC_VALIDATION,
                offset_fetch_version: DEFAULT_GROUP_OFFSET_STORAGE.offset_fetch_version(),
                offset_commit_version: DEFAULT_GROUP_OFFSET_STORAGE.offset_commit_version(),
                retry_backoff_time: Duration::from_millis(DEFAULT_RETRY_BACKOFF_TIME_MILLIS),
                retry_max_attempts: DEFAULT_RETRY_MAX_ATTEMPTS,
            },
            conn_pool: network::Connections::new_with_security(
                default_conn_rw_timeout(),
                Duration::from_millis(DEFAULT_CONNECTION_IDLE_TIMEOUT_MILLIS),
                Some(security),
            ),
            state: state::ClientState::new(),
        }
    }

    /// Exposes the hosts used for discovery of the target kafka
    /// cluster.  This set of hosts corresponds to the values supplied
    /// to `KafkaClient::new`.
    #[inline]
    pub fn hosts(&self) -> &[String] {
        &self.config.hosts
    }

    /// Sets the client_id to be sent along every request to the
    /// remote Kafka brokers.  By default, this value is the empty
    /// string.
    ///
    /// Kafka brokers write out this client id to their
    /// request/response trace log - if configured appropriately.
    pub fn set_client_id(&mut self, client_id: String) {
        self.config.client_id = client_id;
    }

    /// Retrieves the current `KafkaClient::set_client_id` setting.
    pub fn client_id(&self) -> &str {
        &self.config.client_id
    }

    /// Sets the compression algorithm to use when sending out messages.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use kafka::client::{Compression, KafkaClient};
    ///
    /// let mut client = KafkaClient::new(vec!("localhost:9092".to_owned()));
    /// client.load_metadata_all().unwrap();
    /// client.set_compression(Compression::NONE);
    /// ```
    #[inline]
    pub fn set_compression(&mut self, compression: Compression) {
        self.config.compression = compression;
    }

    /// Retrieves the current `KafkaClient::set_compression` setting.
    #[inline]
    pub fn compression(&self) -> Compression {
        self.config.compression
    }

    /// Sets the maximum time in milliseconds to wait for insufficient
    /// data to become available when fetching messages.
    ///
    /// See also `KafkaClient::set_fetch_min_bytes(..)` and
    /// `KafkaClient::set_fetch_max_bytes_per_partition(..)`.
    #[inline]
    pub fn set_fetch_max_wait_time(&mut self, max_wait_time: Duration) -> Result<()> {
        self.config.fetch_max_wait_time = try!(protocol::to_millis_i32(max_wait_time));
        Ok(())
    }

    /// Retrieves the current `KafkaClient::set_fetch_max_wait_time`
    /// setting.
    #[inline]
    pub fn fetch_max_wait_time(&self) -> Duration {
        Duration::from_millis(self.config.fetch_max_wait_time as u64)
    }

    /// Sets the minimum number of bytes of available data to wait for
    /// as long as specified by `KafkaClient::set_fetch_max_wait_time`
    /// when fetching messages.
    ///
    /// By setting higher values in combination with the timeout the
    /// consumer can tune for throughput and trade a little additional
    /// latency for reading only large chunks of data (e.g. setting
    /// MaxWaitTime to 100 ms and setting MinBytes to 64k would allow
    /// the server to wait up to 100ms to try to accumulate 64k of
    /// data before responding).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use std::time::Duration;
    /// use kafka::client::{KafkaClient, FetchPartition};
    ///
    /// let mut client = KafkaClient::new(vec!["localhost:9092".to_owned()]);
    /// client.load_metadata_all().unwrap();
    /// client.set_fetch_max_wait_time(Duration::from_millis(100));
    /// client.set_fetch_min_bytes(64 * 1024);
    /// let r = client.fetch_messages(&[FetchPartition::new("my-topic", 0, 0)]);
    /// ```
    ///
    /// See also `KafkaClient::set_fetch_max_wait_time(..)` and
    /// `KafkaClient::set_fetch_max_bytes_per_partition(..)`.
    #[inline]
    pub fn set_fetch_min_bytes(&mut self, min_bytes: i32) {
        self.config.fetch_min_bytes = min_bytes;
    }

    /// Retrieves the current `KafkaClient::set_fetch_min_bytes`
    /// setting.
    #[inline]
    pub fn fetch_min_bytes(&self) -> i32 {
        self.config.fetch_min_bytes
    }

    /// Sets the default maximum number of bytes to obtain from _a
    /// single kafka partition_ when fetching messages.
    ///
    /// This basically determines the maximum message size this client
    /// will be able to fetch.  If a topic partition contains a
    /// message larger than this specified number of bytes, the server
    /// will not deliver it.
    ///
    /// Note that this setting is related to a single partition.  The
    /// overall potential data size in a fetch messages response will
    /// thus be determined by the number of partitions in the fetch
    /// messages request times this "max bytes per partitions."
    ///
    /// This client will use this setting by default for all queried
    /// partitions, however, `fetch_messages` does allow you to
    /// override this setting for a particular partition being
    /// queried.
    ///
    /// See also `KafkaClient::set_fetch_max_wait_time`,
    /// `KafkaClient::set_fetch_min_bytes`, and `KafkaClient::fetch_messages`.
    #[inline]
    pub fn set_fetch_max_bytes_per_partition(&mut self, max_bytes: i32) {
        self.config.fetch_max_bytes_per_partition = max_bytes;
    }

    /// Retrieves the current
    /// `KafkaClient::set_fetch_max_bytes_per_partition` setting.
    #[inline]
    pub fn fetch_max_bytes_per_partition(&self) -> i32 {
        self.config.fetch_max_bytes_per_partition
    }

    /// Specifies whether the to perform CRC validation on fetched
    /// messages.
    ///
    /// This ensures detection of on-the-wire or on-disk corruption to
    /// fetched messages.  This check adds some overhead, so it may be
    /// disabled in cases seeking extreme performance.
    #[inline]
    pub fn set_fetch_crc_validation(&mut self, validate_crc: bool) {
        self.config.fetch_crc_validation = validate_crc;
    }

    /// Retrieves the current `KafkaClient::set_fetch_crc_validation`
    /// setting.
    #[inline]
    pub fn fetch_crc_validation(&self) -> bool {
        self.config.fetch_crc_validation
    }

    /// Specifies the group offset storage to address when fetching or
    /// committing group offsets.
    ///
    /// In addition to Zookeeper, Kafka 0.8.2 brokers or later offer a
    /// more performant (and scalable) way to manage group offset
    /// directly by itself. Note that the remote storages are separate
    /// and independent on each other. Hence, you typically want
    /// consistently hard-code your choice in your program.
    ///
    /// Unless you have a 0.8.1 broker or want to participate in a
    /// group which is already based on Zookeeper, you generally want
    /// to choose `GroupOffsetStorage::Kafka` here.
    ///
    /// See also `KafkaClient::fetch_group_offsets` and
    /// `KafkaClient::commit_offsets`.
    #[inline]
    pub fn set_group_offset_storage(&mut self, storage: GroupOffsetStorage) {
        self.config.offset_fetch_version = storage.offset_fetch_version();
        self.config.offset_commit_version = storage.offset_commit_version();
    }

    /// Retrieves the current `KafkaClient::set_group_offset_storage`
    /// settings.
    pub fn group_offset_storage(&self) -> GroupOffsetStorage {
        // ~ only protocol V0 is zookeeper
        let zkv = GroupOffsetStorage::Zookeeper.offset_fetch_version();
        if zkv == self.config.offset_fetch_version {
            GroupOffsetStorage::Zookeeper
        } else {
            GroupOffsetStorage::Kafka
        }
    }

    /// Specifies the time to wait before retrying a failed,
    /// repeatable operation against Kafka.  This avoids retrying such
    /// operations in a tight loop.
    #[inline]
    pub fn set_retry_backoff_time(&mut self, time: Duration) {
        self.config.retry_backoff_time = time;
    }

    /// Retrieves the current `KafkaClient::set_retry_backoff_time`
    /// setting.
    pub fn retry_backoff_time(&self) -> Duration {
        self.config.retry_backoff_time
    }

    /// Specifies the upper limit of retry attempts for failed,
    /// repeatable operations against kafka.  This avoids retrying
    /// them forever.
    #[inline]
    pub fn set_retry_max_attempts(&mut self, attempts: u32) {
        self.config.retry_max_attempts = attempts;
    }

    /// Retrieves the current `KafkaClient::set_retry_max_attempts`
    /// setting.
    #[inline]
    pub fn retry_max_attempts(&self) -> u32 {
        self.config.retry_max_attempts
    }

    /// Specifies the timeout after which idle connections will
    /// transparently be closed/re-established by `KafkaClient`.
    ///
    /// To be effective this value must be smaller than the [remote
    /// broker's `connections.max.idle.ms`
    /// setting](https://kafka.apache.org/documentation.html#brokerconfigs).
    #[inline]
    pub fn set_connection_idle_timeout(&mut self, timeout: Duration) {
        self.conn_pool.set_idle_timeout(timeout);
    }

    /// Retrieves the current
    /// `KafkaClient::set_connection_idle_timeout` setting.
    #[inline]
    pub fn connection_idle_timeout(&self) -> Duration {
        self.conn_pool.idle_timeout()
    }

    /// Provides a view onto the currently loaded metadata of known .
    ///
    /// # Examples
    /// ```no_run
    /// use kafka::client::KafkaClient;
    /// use kafka::client::metadata::Broker;
    ///
    /// let mut client = KafkaClient::new(vec!["localhost:9092".to_owned()]);
    /// client.load_metadata_all().unwrap();
    /// for topic in client.topics() {
    ///   for partition in topic.partitions() {
    ///     println!("{} #{} => {}", topic.name(), partition.id(),
    ///              partition.leader()
    ///                       .map(Broker::host)
    ///                       .unwrap_or("no-leader!"));
    ///   }
    /// }
    /// ```
    #[inline]
    pub fn topics(&self) -> metadata::Topics {
        metadata::Topics::new(self)
    }

    /// Resets and loads metadata for all topics from the underlying
    /// brokers.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// let mut client = kafka::client::KafkaClient::new(vec!("localhost:9092".to_owned()));
    /// client.load_metadata_all().unwrap();
    /// for topic in client.topics().names() {
    ///   println!("topic: {}", topic);
    /// }
    /// ```
    ///
    /// Returns the metadata for all loaded topics underlying this
    /// client.
    #[inline]
    pub fn load_metadata_all(&mut self) -> Result<()> {
        self.reset_metadata();
        self.load_metadata::<&str>(&[])
    }

    /// Reloads metadata for a list of supplied topics.
    ///
    /// Note: if any of the specified topics does not exist yet on the
    /// underlying brokers and these have the [configuration for "auto
    /// create topics"
    /// enabled](https://kafka.apache.org/documentation.html#configuration),
    /// the remote kafka instance will create the yet missing topics
    /// on the fly as a result of explicitely loading their metadata.
    /// This is in contrast to other methods of this `KafkaClient`
    /// which will silently filter out requests to
    /// not-yet-loaded/not-yet-known topics and, thus, not cause
    /// topics to be automatically created.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// let mut client = kafka::client::KafkaClient::new(vec!("localhost:9092".to_owned()));
    /// let _ = client.load_metadata(&["my-topic"]).unwrap();
    /// ```
    ///
    /// Returns the metadata for _all_ loaded topics underlying this
    /// client (this might be more topics than specified right to this
    /// method call.)
    #[inline]
    pub fn load_metadata<T: AsRef<str>>(&mut self, topics: &[T]) -> Result<()> {
        let resp = try!(self.fetch_metadata(topics));
        self.state.update_metadata(resp)
    }

    /// Clears metadata stored in the client.  You must load metadata
    /// after this call if you want to use the client.
    #[inline]
    pub fn reset_metadata(&mut self) {
        self.state.clear_metadata();
    }

    /// Fetches metadata about the specified topics from all of the
    /// underlying brokers (`self.hosts`).
    fn fetch_metadata<T: AsRef<str>>(
        &mut self,
        topics: &[T],
    ) -> Result<protocol::MetadataResponse> {
        let correlation = self.state.next_correlation_id();
        let now = Instant::now();

        for host in &self.config.hosts {
            debug!("fetch_metadata: requesting metadata from {}", host);
            match self.conn_pool.get_conn(host, now) {
                Ok(conn) => {
                    let req =
                        protocol::MetadataRequest::new(correlation, &self.config.client_id, topics);
                    match __send_request(conn, req) {
                        Ok(_) => return __get_response::<protocol::MetadataResponse>(conn),
                        Err(e) => debug!(
                            "fetch_metadata: failed to request metadata from {}: {}",
                            host, e
                        ),
                    }
                }
                Err(e) => {
                    debug!("fetch_metadata: failed to connect to {}: {}", host, e);
                }
            }
        }
        bail!(ErrorKind::NoHostReachable)
    }

    /// Fetch offsets for a list of topics
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use kafka::client::KafkaClient;
    ///
    /// let mut client = KafkaClient::new(vec!["localhost:9092".to_owned()]);
    /// client.load_metadata_all().unwrap();
    /// let topics: Vec<String> = client.topics().names().map(ToOwned::to_owned).collect();
    /// let offsets = client.fetch_offsets(&topics, kafka::client::FetchOffset::Latest).unwrap();
    /// ```
    ///
    /// Returns a mapping of topic name to `PartitionOffset`s for each
    /// currently available partition of the corresponding topic.
    pub fn fetch_offsets<T: AsRef<str>>(
        &mut self,
        topics: &[T],
        offset: FetchOffset,
    ) -> Result<HashMap<String, Vec<PartitionOffset>>> {
        let time = offset.to_kafka_value();
        let n_topics = topics.len();

        let state = &mut self.state;
        let correlation = state.next_correlation_id();

        // Map topic and partition to the corresponding broker
        let config = &self.config;
        let mut reqs: HashMap<&str, protocol::OffsetRequest> = HashMap::with_capacity(n_topics);
        for topic in topics {
            let topic = topic.as_ref();
            if let Some(ps) = state.partitions_for(topic) {
                for (id, host) in ps
                    .iter()
                    .filter_map(|(id, p)| p.broker(&state).map(|b| (id, b.host())))
                {
                    let entry = reqs.entry(host).or_insert_with(|| {
                        protocol::OffsetRequest::new(correlation, &config.client_id)
                    });
                    entry.add(topic, id, time);
                }
            }
        }

        // Call each broker with the request formed earlier
        let now = Instant::now();
        let mut res: HashMap<String, Vec<PartitionOffset>> = HashMap::with_capacity(n_topics);
        for (host, req) in reqs {
            let resp = try!(__send_receive::<_, protocol::OffsetResponse>(
                &mut self.conn_pool,
                &host,
                now,
                req,
            ));
            for tp in resp.topic_partitions {
                let mut entry = res.entry(tp.topic);
                let mut new_resp_offsets = None;
                let mut err = None;
                // Use an explicit scope here to allow insertion into a vacant entry
                // below
                {
                    // Return a &mut to the response we will be collecting into to
                    // return from this function. If there are some responses we have
                    // already prepared, keep collecting into that; otherwise, make a
                    // new collection to return.
                    let resp_offsets = match entry {
                        hash_map::Entry::Occupied(ref mut e) => e.get_mut(),
                        hash_map::Entry::Vacant(_) => {
                            new_resp_offsets = Some(Vec::new());
                            new_resp_offsets.as_mut().unwrap()
                        }
                    };
                    for p in tp.partitions {
                        let partition_offset = match p.into_offset() {
                            Ok(po) => po,
                            Err(code) => {
                                err = Some((p.partition, code));
                                break;
                            }
                        };
                        resp_offsets.push(partition_offset);
                    }
                }
                if let Some((partition, code)) = err {
                    let topic = KafkaClient::get_key_from_entry(entry);
                    bail!(ErrorKind::TopicPartitionError(topic, partition, code));
                }
                if let hash_map::Entry::Vacant(e) = entry {
                    // unwrap is ok because if it is Vacant, it would have
                    // been made into a Some above
                    e.insert(new_resp_offsets.unwrap());
                }
            }
        }

        Ok(res)
    }

    /// Takes ownership back from the given HashMap Entry.
    fn get_key_from_entry<'a, K: 'a, V: 'a>(entry: hash_map::Entry<'a, K, V>) -> K {
        match entry {
            hash_map::Entry::Occupied(e) => e.remove_entry().0,
            hash_map::Entry::Vacant(e) => e.into_key(),
        }
    }

    /// Fetch offset for a single topic.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use kafka::client::{KafkaClient, FetchOffset};
    ///
    /// let mut client = KafkaClient::new(vec!["localhost:9092".to_owned()]);
    /// client.load_metadata_all().unwrap();
    /// let offsets = client.fetch_topic_offsets("my-topic", FetchOffset::Latest).unwrap();
    /// ```
    ///
    /// Returns a vector of the offset data for each available partition.
    /// See also `KafkaClient::fetch_offsets`.
    pub fn fetch_topic_offsets<T: AsRef<str>>(
        &mut self,
        topic: T,
        offset: FetchOffset,
    ) -> Result<Vec<PartitionOffset>> {
        let topic = topic.as_ref();

        let mut m = try!(self.fetch_offsets(&[topic], offset));
        let offs = m.remove(topic).unwrap_or_else(|| vec![]);
        if offs.is_empty() {
            bail!(ErrorKind::Kafka(KafkaCode::UnknownTopicOrPartition))
        } else {
            Ok(offs)
        }
    }

    /// Fetch messages from Kafka (multiple topic, partitions).
    ///
    /// It takes a vector specifying the topic partitions and their
    /// offsets as of which to fetch messages.  Additionally, the
    /// default "max fetch size per partition" can be explicitely
    /// overriden if it is "defined" - this is, if `max_bytes` is
    /// greater than zero.
    ///
    /// The result is exposed in a raw, complicated manner but allows
    /// for very efficient consumption possibilities. All of the data
    /// available through the returned fetch responses is bound to
    /// their lifetime as that data is merely a "view" into parts of
    /// the response structs.  If you need to keep individual messages
    /// for a longer time than the whole fetch responses, you'll need
    /// to make a copy of the message data.
    ///
    /// * This method transparently uncompresses messages (while Kafka
    /// might sent them in compressed format.)
    ///
    /// * This method ensures to skip messages with a lower offset
    /// than requested (while Kafka might for efficiency reasons sent
    /// messages with a lower offset.)
    ///
    /// Note: before using this method consider using
    /// `kafka::consumer::Consumer` instead which provides an easier
    /// to use API for the regular use-case of fetching messesage from
    /// Kafka.
    ///
    /// # Example
    ///
    /// This example demonstrates iterating all fetched messages from
    /// two topic partitions.  From one partition we allow Kafka to
    /// deliver to us the default number bytes as defined by
    /// `KafkaClient::set_fetch_max_bytes_per_partition`, from the
    /// other partition we allow Kafka to deliver up to 1MiB of
    /// messages.
    ///
    /// ```no_run
    /// use kafka::client::{KafkaClient, FetchPartition};
    ///
    /// let mut client = KafkaClient::new(vec!("localhost:9092".to_owned()));
    /// client.load_metadata_all().unwrap();
    /// let reqs = &[FetchPartition::new("my-topic", 0, 0),
    ///              FetchPartition::new("my-topic-2", 0, 0).with_max_bytes(1024*1024)];
    /// let resps = client.fetch_messages(reqs).unwrap();
    /// for resp in resps {
    ///   for t in resp.topics() {
    ///     for p in t.partitions() {
    ///       match p.data() {
    ///         &Err(ref e) => {
    ///           println!("partition error: {}:{}: {}", t.topic(), p.partition(), e)
    ///         }
    ///         &Ok(ref data) => {
    ///           println!("topic: {} / partition: {} / latest available message offset: {}",
    ///                    t.topic(), p.partition(), data.highwatermark_offset());
    ///           for msg in data.messages() {
    ///             println!("topic: {} / partition: {} / message.offset: {} / message.len: {}",
    ///                      t.topic(), p.partition(), msg.offset, msg.value.len());
    ///           }
    ///         }
    ///       }
    ///     }
    ///   }
    /// }
    /// ```
    /// See also `kafka::consumer`.
    /// See also `KafkaClient::set_fetch_max_bytes_per_partition`.
    pub fn fetch_messages<'a, I, J>(&mut self, input: I) -> Result<Vec<fetch::Response>>
    where
        J: AsRef<FetchPartition<'a>>,
        I: IntoIterator<Item = J>,
    {
        let state = &mut self.state;
        let config = &self.config;

        let correlation = state.next_correlation_id();

        // Map topic and partition to the corresponding broker
        let mut reqs: HashMap<&str, protocol::FetchRequest> = HashMap::new();
        for inp in input {
            let inp = inp.as_ref();
            if let Some(broker) = state.find_broker(inp.topic, inp.partition) {
                reqs.entry(broker)
                    .or_insert_with(|| {
                        protocol::FetchRequest::new(
                            correlation,
                            &config.client_id,
                            config.fetch_max_wait_time,
                            config.fetch_min_bytes,
                        )
                    })
                    .add(
                        inp.topic,
                        inp.partition,
                        inp.offset,
                        if inp.max_bytes > 0 {
                            inp.max_bytes
                        } else {
                            config.fetch_max_bytes_per_partition
                        },
                    );
            }
        }

        __fetch_messages(&mut self.conn_pool, config, reqs)
    }

    /// Fetch messages from a single kafka partition.
    ///
    /// See `KafkaClient::fetch_messages`.
    pub fn fetch_messages_for_partition<'a>(
        &mut self,
        req: &FetchPartition<'a>,
    ) -> Result<Vec<fetch::Response>> {
        self.fetch_messages(&[req])
    }

    /// Send a message to Kafka
    ///
    /// `required_acks` - indicates how many acknowledgements the
    /// servers should receive before responding to the request
    ///
    /// `ack_timeout` - provides a maximum time in milliseconds the
    /// server can await the receipt of the number of acknowledgements
    /// in `required_acks`
    ///
    /// `input` - the set of `ProduceMessage`s to send
    ///
    /// Note: Unlike the higher-level `Producer` API, this method will
    /// *not* automatically determine the partition to deliver the
    /// message to.  It will strictly try to send the message to the
    /// specified partition.
    ///
    /// Note: Trying to send messages to non-existing topics or
    /// non-existing partitions will result in an error.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use std::time::Duration;
    /// use kafka::client::{KafkaClient, ProduceMessage, RequiredAcks};
    ///
    /// let mut client = KafkaClient::new(vec!("localhost:9092".to_owned()));
    /// client.load_metadata_all().unwrap();
    /// let req = vec![ProduceMessage::new("my-topic", 0, None, Some("a".as_bytes())),
    ///                ProduceMessage::new("my-topic-2", 0, None, Some("b".as_bytes()))];
    /// let resp = client.produce_messages(RequiredAcks::One, Duration::from_millis(100), req);
    /// println!("{:?}", resp);
    /// ```
    ///
    /// The return value will contain a vector of topic, partition,
    /// offset and error if any OR error:Error.

    // XXX rework signaling an error; note that we need to either return the
    // messages which kafka failed to accept or otherwise tell the client about them

    pub fn produce_messages<'a, 'b, I, J>(
        &mut self,
        acks: RequiredAcks,
        ack_timeout: Duration,
        messages: I,
    ) -> Result<Vec<ProduceConfirm>>
    where
        J: AsRef<ProduceMessage<'a, 'b>>,
        I: IntoIterator<Item = J>,
    {
        self.internal_produce_messages(
            acks as i16,
            try!(protocol::to_millis_i32(ack_timeout)),
            messages,
        )
    }

    /// Commit offset for a topic partitions on behalf of a consumer group.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use kafka::client::{KafkaClient, CommitOffset};
    ///
    /// let mut client = KafkaClient::new(vec!["localhost:9092".to_owned()]);
    /// client.load_metadata_all().unwrap();
    /// client.commit_offsets("my-group",
    ///     &[CommitOffset::new("my-topic", 0, 100),
    ///       CommitOffset::new("my-topic", 1, 99)])
    ///    .unwrap();
    /// ```
    ///
    /// In this example, we commit the offset 100 for the topic
    /// partition "my-topic:0" and 99 for the topic partition
    /// "my-topic:1".  Once successfully committed, these can then be
    /// retrieved using `fetch_group_offsets` even from another
    /// process or at much later point in time to resume comusing the
    /// topic partitions as of these offsets.
    pub fn commit_offsets<'a, J, I>(&mut self, group: &str, offsets: I) -> Result<()>
    where
        J: AsRef<CommitOffset<'a>>,
        I: IntoIterator<Item = J>,
    {
        let mut req = protocol::OffsetCommitRequest::new(
            group,
            self.config.offset_commit_version,
            self.state.next_correlation_id(),
            &self.config.client_id,
        );
        for o in offsets {
            let o = o.as_ref();
            if self.state.contains_topic_partition(o.topic, o.partition) {
                req.add(o.topic, o.partition, o.offset, "");
            } else {
                bail!(ErrorKind::Kafka(KafkaCode::UnknownTopicOrPartition));
            }
        }
        if req.topic_partitions.is_empty() {
            debug!("commit_offsets: no offsets provided");
            Ok(())
        } else {
            __commit_offsets(req, &mut self.state, &mut self.conn_pool, &self.config)
        }
    }

    /// Commit offset of a particular topic partition on behalf of a
    /// consumer group.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use kafka::client::KafkaClient;
    ///
    /// let mut client = KafkaClient::new(vec!["localhost:9092".to_owned()]);
    /// client.load_metadata_all().unwrap();
    /// client.commit_offset("my-group", "my-topic", 0, 100).unwrap();
    /// ```
    ///
    /// See also `KafkaClient::commit_offsets`.
    pub fn commit_offset(
        &mut self,
        group: &str,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<()> {
        self.commit_offsets(group, &[CommitOffset::new(topic, partition, offset)])
    }

    /// Fetch offset for a specified list of topic partitions of a consumer group
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use kafka::client::{KafkaClient, FetchGroupOffset};
    ///
    /// let mut client = KafkaClient::new(vec!["localhost:9092".to_owned()]);
    /// client.load_metadata_all().unwrap();
    ///
    /// let offsets =
    ///      client.fetch_group_offsets("my-group",
    ///             &[FetchGroupOffset::new("my-topic", 0),
    ///               FetchGroupOffset::new("my-topic", 1)])
    ///             .unwrap();
    /// ```
    ///
    /// See also `KafkaClient::fetch_group_topic_offsets`.
    pub fn fetch_group_offsets<'a, J, I>(
        &mut self,
        group: &str,
        partitions: I,
    ) -> Result<HashMap<String, Vec<PartitionOffset>>>
    where
        J: AsRef<FetchGroupOffset<'a>>,
        I: IntoIterator<Item = J>,
    {
        let mut req = protocol::OffsetFetchRequest::new(
            group,
            self.config.offset_fetch_version,
            self.state.next_correlation_id(),
            &self.config.client_id,
        );
        for p in partitions {
            let p = p.as_ref();
            if self.state.contains_topic_partition(p.topic, p.partition) {
                req.add(p.topic, p.partition);
            } else {
                bail!(ErrorKind::Kafka(KafkaCode::UnknownTopicOrPartition));
            }
        }
        __fetch_group_offsets(req, &mut self.state, &mut self.conn_pool, &self.config)
    }

    /// Fetch offset for all partitions of a particular topic of a consumer group
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use kafka::client::KafkaClient;
    ///
    /// let mut client = KafkaClient::new(vec!["localhost:9092".to_owned()]);
    /// client.load_metadata_all().unwrap();
    /// let offsets = client.fetch_group_topic_offsets("my-group", "my-topic").unwrap();
    /// ```
    pub fn fetch_group_topic_offsets(
        &mut self,
        group: &str,
        topic: &str,
    ) -> Result<Vec<PartitionOffset>> {
        let mut req = protocol::OffsetFetchRequest::new(
            group,
            self.config.offset_fetch_version,
            self.state.next_correlation_id(),
            &self.config.client_id,
        );

        match self.state.partitions_for(topic) {
            None => bail!(ErrorKind::Kafka(KafkaCode::UnknownTopicOrPartition)),
            Some(tp) => {
                for (id, _) in tp {
                    req.add(topic, id);
                }
            }
        }

        Ok(try!(__fetch_group_offsets(
            req,
            &mut self.state,
            &mut self.conn_pool,
            &self.config
        ))
        .remove(topic)
        .unwrap_or_else(Vec::new))
    }
}

impl KafkaClientInternals for KafkaClient {
    fn internal_produce_messages<'a, 'b, I, J>(
        &mut self,
        required_acks: i16,
        ack_timeout: i32,
        messages: I,
    ) -> Result<Vec<ProduceConfirm>>
    where
        J: AsRef<ProduceMessage<'a, 'b>>,
        I: IntoIterator<Item = J>,
    {
        let state = &mut self.state;
        let correlation = state.next_correlation_id();

        // ~ map topic and partition to the corresponding brokers
        let config = &self.config;
        let mut reqs: HashMap<&str, protocol::ProduceRequest> = HashMap::new();
        for msg in messages {
            let msg = msg.as_ref();
            match state.find_broker(msg.topic, msg.partition) {
                None => bail!(ErrorKind::Kafka(KafkaCode::UnknownTopicOrPartition)),
                Some(broker) => reqs
                    .entry(broker)
                    .or_insert_with(|| {
                        protocol::ProduceRequest::new(
                            required_acks,
                            ack_timeout,
                            correlation,
                            &config.client_id,
                            config.compression,
                        )
                    })
                    .add(msg.topic, msg.partition, msg.key, msg.value),
            }
        }
        __produce_messages(&mut self.conn_pool, reqs, required_acks == 0)
    }
}

fn __get_group_coordinator<'a>(
    group: &str,
    state: &'a mut state::ClientState,
    conn_pool: &mut network::Connections,
    config: &ClientConfig,
    now: Instant,
) -> Result<&'a str> {
    if let Some(host) = state.group_coordinator(group) {
        // ~ decouple the lifetimes to make borrowck happy;
        // this is actually safe since we're immediatelly
        // returning this, so the follow up code is not
        // affected here
        return Ok(unsafe { mem::transmute(host) });
    }
    let correlation_id = state.next_correlation_id();
    let req = protocol::GroupCoordinatorRequest::new(group, correlation_id, &config.client_id);
    let mut attempt = 1;
    loop {
        // ~ idealy we'd make this work even if `load_metadata` has not
        // been called yet; if there are no connections available we can
        // try connecting to the user specified bootstrap server similar
        // to the way `load_metadata` works.
        let conn = conn_pool.get_conn_any(now).expect("available connection");
        debug!(
            "get_group_coordinator: asking for coordinator of '{}' on: {:?}",
            group, conn
        );
        let r = try!(__send_receive_conn::<_, protocol::GroupCoordinatorResponse>(conn, &req));
        let retry_code;
        match r.to_result() {
            Ok(r) => {
                return Ok(state.set_group_coordinator(group, &r));
            }
            Err(Error(ErrorKind::Kafka(e @ KafkaCode::GroupCoordinatorNotAvailable), _)) => {
                retry_code = e;
            }
            Err(e) => {
                return Err(e);
            }
        }
        if attempt < config.retry_max_attempts {
            debug!(
                "get_group_coordinator: will retry request (c: {}) due to: {:?}",
                req.header.correlation_id, retry_code
            );
            attempt += 1;
            __retry_sleep(config);
        } else {
            bail!(ErrorKind::Kafka(retry_code));
        }
    }
}

fn __commit_offsets(
    req: protocol::OffsetCommitRequest,
    state: &mut state::ClientState,
    conn_pool: &mut network::Connections,
    config: &ClientConfig,
) -> Result<()> {
    let mut attempt = 1;
    loop {
        let now = Instant::now();

        let tps = {
            let host = try!(__get_group_coordinator(
                req.group, state, conn_pool, config, now
            ));
            debug!(
                "__commit_offsets: sending offset commit request '{:?}' to: {}",
                req, host
            );
            try!(__send_receive::<_, protocol::OffsetCommitResponse>(
                conn_pool, host, now, &req
            ))
            .topic_partitions
        };

        let mut retry_code = None;

        'rproc: for tp in tps {
            for p in tp.partitions {
                match p.to_error() {
                    None => {}
                    Some(e @ KafkaCode::GroupLoadInProgress) => {
                        retry_code = Some(e);
                        break 'rproc;
                    }
                    Some(e @ KafkaCode::NotCoordinatorForGroup) => {
                        debug!(
                            "commit_offsets: resetting group coordinator for '{}'",
                            req.group
                        );
                        state.remove_group_coordinator(&req.group);
                        retry_code = Some(e);
                        break 'rproc;
                    }
                    Some(code) => {
                        // ~ immediately abort with the error
                        bail!(ErrorKind::Kafka(code));
                    }
                }
            }
        }
        match retry_code {
            Some(e) => {
                if attempt < config.retry_max_attempts {
                    debug!(
                        "commit_offsets: will retry request (c: {}) due to: {:?}",
                        req.header.correlation_id, e
                    );
                    attempt += 1;
                    __retry_sleep(config);
                }
            }
            None => {
                return Ok(());
            }
        }
    }
}

fn __fetch_group_offsets(
    req: protocol::OffsetFetchRequest,
    state: &mut state::ClientState,
    conn_pool: &mut network::Connections,
    config: &ClientConfig,
) -> Result<HashMap<String, Vec<PartitionOffset>>> {
    let mut attempt = 1;
    loop {
        let now = Instant::now();

        let r = {
            let host = try!(__get_group_coordinator(
                req.group, state, conn_pool, config, now
            ));
            debug!(
                "fetch_group_offsets: sending request {:?} to: {}",
                req, host
            );
            try!(__send_receive::<_, protocol::OffsetFetchResponse>(
                conn_pool, host, now, &req
            ))
        };

        debug!("fetch_group_offsets: received response: {:#?}", r);

        let mut retry_code = None;
        let mut topic_map = HashMap::with_capacity(r.topic_partitions.len());

        'rproc: for tp in r.topic_partitions {
            let mut partition_offsets = Vec::with_capacity(tp.partitions.len());

            for p in tp.partitions {
                match p.get_offsets() {
                    Ok(o) => {
                        partition_offsets.push(o);
                    }
                    Err(Error(ErrorKind::Kafka(e @ KafkaCode::GroupLoadInProgress), _)) => {
                        retry_code = Some(e);
                        break 'rproc;
                    }
                    Err(Error(ErrorKind::Kafka(e @ KafkaCode::NotCoordinatorForGroup), _)) => {
                        debug!(
                            "fetch_group_offsets: resetting group coordinator for '{}'",
                            req.group
                        );
                        state.remove_group_coordinator(&req.group);
                        retry_code = Some(e);
                        break 'rproc;
                    }
                    Err(e) => {
                        // ~ immeditaly abort with the error
                        return Err(e);
                    }
                }
            }

            topic_map.insert(tp.topic, partition_offsets);
        }

        // ~ have we processed the result successfully or shall we
        // retry once more?
        match retry_code {
            Some(e) => {
                if attempt < config.retry_max_attempts {
                    debug!(
                        "fetch_group_offsets: will retry request (c: {}) due to: {:?}",
                        req.header.correlation_id, e
                    );
                    attempt += 1;
                    __retry_sleep(config)
                } else {
                    bail!(ErrorKind::Kafka(e));
                }
            }
            None => {
                return Ok(topic_map);
            }
        }
    }
}

/// ~ carries out the given fetch requests and returns the response
fn __fetch_messages(
    conn_pool: &mut network::Connections,
    config: &ClientConfig,
    reqs: HashMap<&str, protocol::FetchRequest>,
) -> Result<Vec<fetch::Response>> {
    let now = Instant::now();
    let mut res = Vec::with_capacity(reqs.len());
    for (host, req) in reqs {
        let p = protocol::fetch::ResponseParser {
            validate_crc: config.fetch_crc_validation,
            requests: Some(&req),
        };
        res.push(try!(__z_send_receive(conn_pool, host, now, &req, &p)));
    }
    Ok(res)
}

/// ~ carries out the given produce requests and returns the response
fn __produce_messages(
    conn_pool: &mut network::Connections,
    reqs: HashMap<&str, protocol::ProduceRequest>,
    no_acks: bool,
) -> Result<Vec<ProduceConfirm>> {
    let now = Instant::now();
    if no_acks {
        for (host, req) in reqs {
            try!(__send_noack::<_, protocol::ProduceResponse>(
                conn_pool, host, now, req
            ));
        }
        Ok(vec![])
    } else {
        let mut res: Vec<ProduceConfirm> = vec![];
        for (host, req) in reqs {
            let resp = try!(__send_receive::<_, protocol::ProduceResponse>(
                conn_pool, &host, now, req
            ));
            for tpo in resp.get_response() {
                res.push(tpo);
            }
        }
        Ok(res)
    }
}

fn __send_receive<T, V>(
    conn_pool: &mut network::Connections,
    host: &str,
    now: Instant,
    req: T,
) -> Result<V::R>
where
    T: ToByte,
    V: FromByte,
{
    __send_receive_conn::<T, V>(try!(conn_pool.get_conn(host, now)), req)
}

fn __send_receive_conn<T, V>(conn: &mut network::KafkaConnection, req: T) -> Result<V::R>
where
    T: ToByte,
    V: FromByte,
{
    try!(__send_request(conn, req));
    __get_response::<V>(conn)
}

fn __send_noack<T, V>(
    conn_pool: &mut network::Connections,
    host: &str,
    now: Instant,
    req: T,
) -> Result<usize>
where
    T: ToByte,
    V: FromByte,
{
    let mut conn = try!(conn_pool.get_conn(host, now));
    __send_request(&mut conn, req)
}

fn __send_request<T: ToByte>(conn: &mut network::KafkaConnection, request: T) -> Result<usize> {
    // ~ buffer to receive data to be sent
    let mut buffer = Vec::with_capacity(4);
    // ~ reserve bytes for the actual request size (we'll fill in that later)
    buffer.extend_from_slice(&[0, 0, 0, 0]);
    // ~ encode the request data
    try!(request.encode(&mut buffer));
    // ~ put the size of the request data into the reseved area
    let size = buffer.len() as i32 - 4;
    try!(size.encode(&mut &mut buffer[..]));

    trace!("__send_request: Sending bytes: {:?}", &buffer);

    // ~ send the prepared buffer
    conn.send(&buffer)
}

fn __get_response<T: FromByte>(conn: &mut network::KafkaConnection) -> Result<T::R> {
    let size = try!(__get_response_size(conn));
    let resp = try!(conn.read_exact_alloc(size as u64));

    trace!("__get_response: received bytes: {:?}", &resp);

    // {
    //     use std::fs::OpenOptions;
    //     use std::io::Write;
    //     let mut f = OpenOptions::new()
    //         .write(true)
    //         .truncate(true)
    //         .create(true)
    //         .open("/tmp/dump.dat")
    //         .unwrap();
    //     f.write_all(&resp[..]).unwrap();
    // }

    T::decode_new(&mut Cursor::new(resp))
}

fn __z_send_receive<R, P>(
    conn_pool: &mut network::Connections,
    host: &str,
    now: Instant,
    req: R,
    parser: &P,
) -> Result<P::T>
where
    R: ToByte,
    P: ResponseParser,
{
    let mut conn = try!(conn_pool.get_conn(host, now));
    try!(__send_request(&mut conn, req));
    __z_get_response(&mut conn, parser)
}

fn __z_get_response<P>(conn: &mut network::KafkaConnection, parser: &P) -> Result<P::T>
where
    P: ResponseParser,
{
    let size = try!(__get_response_size(conn));
    let resp = try!(conn.read_exact_alloc(size as u64));

    // {
    //     use std::fs::OpenOptions;
    //     use std::io::Write;
    //     let mut f = OpenOptions::new()
    //         .write(true)
    //         .truncate(true)
    //         .create(true)
    //         .open("/tmp/dump.dat")
    //         .unwrap();
    //     f.write_all(&resp[..]).unwrap();
    // }

    parser.parse(resp)
}

fn __get_response_size(conn: &mut network::KafkaConnection) -> Result<i32> {
    let mut buf = [0u8; 4];
    try!(conn.read_exact(&mut buf));
    i32::decode_new(&mut Cursor::new(&buf))
}

/// Suspends the calling thread for the configured "retry" time. This
/// method should be called _only_ as part of a retry attempt.
fn __retry_sleep(cfg: &ClientConfig) {
    thread::sleep(cfg.retry_backoff_time)
}
