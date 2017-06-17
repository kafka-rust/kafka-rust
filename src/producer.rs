//! Kafka Producer - A higher-level API for sending messages to Kafka
//! topics.
//!
//! This module hosts a multi-topic capable producer for a Kafka
//! cluster providing a convenient API for sending messages
//! synchronously.
//!
//! In Kafka, each message is a key/value pair where one or the other
//! is optional.  A `Record` represents all the data necessary to
//! produce such a message to Kafka using the `Producer`.  It
//! specifies the target topic and the target partition the message is
//! supposed to be delivered to as well as the key and the value.
//!
//! # Example
//! ```no_run
//! use std::fmt::Write;
//! use std::time::Duration;
//! use kafka::producer::{Producer, Record, RequiredAcks};
//!
//! let mut producer =
//!     Producer::from_hosts(vec!("localhost:9092".to_owned()))
//!         .with_ack_timeout(Duration::from_secs(1))
//!         .with_required_acks(RequiredAcks::One)
//!         .create()
//!         .unwrap();
//!
//! let mut buf = String::with_capacity(2);
//! for i in 0..10 {
//!   let _ = write!(&mut buf, "{}", i); // some computation of the message data to be sent
//!   producer.send(&Record::from_value("my-topic", buf.as_bytes())).unwrap();
//!   buf.clear();
//! }
//! ```
//!
//! In this example, when the `producer.send(..)` returns
//! successfully, we are guaranteed the message is delivered to Kafka
//! and persisted by at least one Kafka broker.  However, when sending
//! multiple messages just like in this example, it is more efficient
//! to send them in batches using `Producer::send_all`.
//!
//! Since some of the `Record`s attributes are optional, convenience
//! methods exist to ease their creation.  In this example, the call
//! to `Record::from_value` creates a key-less, value-only record with
//! an unspecified partition.  The `Record` struct, however, is
//! intended to provide full control over its lifecycle to client
//! code, and, hence, is fully open.  Its current constructor methods
//! are provided for convience only.
//!
//! Beside the target topic, key, and the value of a `Record`, client
//! code is allowed to specify the topic partition the message is
//! supposed to be delivered to.  If the partition of a `Record` is
//! not specified - more precisely speaking if it's negative -
//! `Producer` will rely on its underlying `Partitioner` to find a
//! suitable one.  A `Partitioner` implementation can be supplied by
//! client code at the `Producer`'s construction time and defaults to
//! `DefaultPartitioner`.  See that for more information for its
//! strategy to find a partition.

// XXX 1) rethink return values for the send_all() method
// XXX 2) Handle recoverable errors behind the scenes through retry attempts

use std::collections::HashMap;
use std::fmt;
use std::hash::{Hasher, BuildHasher, BuildHasherDefault};
use std::time::Duration;
use client::{self, KafkaClient};
use error::{ErrorKind, Result};
use ref_slice::ref_slice;
use twox_hash::XxHash32;

#[cfg(feature = "security")]
use client::SecurityConfig;

#[cfg(not(feature = "security"))]
type SecurityConfig = ();
use client_internals::KafkaClientInternals;
use protocol;

// public re-exports
pub use client::{Compression, RequiredAcks, ProduceConfirm, ProducePartitionConfirm};

/// The default value for `Builder::with_ack_timeout`.
pub const DEFAULT_ACK_TIMEOUT_MILLIS: u64 = 30 * 1000;

/// The default value for `Builder::with_required_acks`.
pub const DEFAULT_REQUIRED_ACKS: RequiredAcks = RequiredAcks::One;

// --------------------------------------------------------------------

/// A trait used by `Producer` to obtain the bytes `Record::key` and
/// `Record::value` represent.  This leaves the choice of the types
/// for `key` and `value` with the client.
pub trait AsBytes {
    fn as_bytes(&self) -> &[u8];
}

impl AsBytes for () {
    fn as_bytes(&self) -> &[u8] {
        &[]
    }
}

// There seems to be some compiler issue with this:
// impl<T: AsRef<[u8]>> AsBytes for T {
//     fn as_bytes(&self) -> &[u8] { self.as_ref() }
// }

// for now we provide the impl for some standard library types
impl AsBytes for String {
    fn as_bytes(&self) -> &[u8] {
        self.as_ref()
    }
}
impl AsBytes for Vec<u8> {
    fn as_bytes(&self) -> &[u8] {
        self.as_ref()
    }
}

impl<'a> AsBytes for &'a [u8] {
    fn as_bytes(&self) -> &[u8] {
        self
    }
}
impl<'a> AsBytes for &'a str {
    fn as_bytes(&self) -> &[u8] {
        str::as_bytes(self)
    }
}

// --------------------------------------------------------------------

/// A structure representing a message to be sent to Kafka through the
/// `Producer` API.  Such a message is basically a key/value pair
/// specifying the target topic and optionally the topic's partition.
pub struct Record<'a, K, V> {
    /// Key data of this (message) record.
    pub key: K,

    /// Value data of this (message) record.
    pub value: V,

    /// Name of the topic this message is supposed to be delivered to.
    pub topic: &'a str,

    /// The partition id of the topic to deliver this message to.
    /// This partition may be `< 0` in which case it is considered
    /// "unspecified".  A `Producer` will then typically try to derive
    /// a partition on its own.
    pub partition: i32,
}

impl<'a, K, V> Record<'a, K, V> {
    /// Convenience function to create a new key/value record with an
    /// "unspecified" partition - this is, a partition set to a negative
    /// value.
    #[inline]
    pub fn from_key_value(topic: &'a str, key: K, value: V) -> Record<'a, K, V> {
        Record {
            key: key,
            value: value,
            topic: topic,
            partition: -1,
        }
    }

    /// Convenience method to set the partition.
    #[inline]
    pub fn with_partition(mut self, partition: i32) -> Self {
        self.partition = partition;
        self
    }
}

impl<'a, V> Record<'a, (), V> {
    /// Convenience function to create a new value only record with an
    /// "unspecified" partition - this is, a partition set to a negative
    /// value.
    #[inline]
    pub fn from_value(topic: &'a str, value: V) -> Record<'a, (), V> {
        Record {
            key: (),
            value: value,
            topic: topic,
            partition: -1,
        }
    }
}

impl<'a, K: fmt::Debug, V: fmt::Debug> fmt::Debug for Record<'a, K, V> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Record {{ topic: {}, partition: {}, key: {:?}, value: {:?} }}",
            self.topic,
            self.partition,
            self.key,
            self.value
        )
    }
}

// --------------------------------------------------------------------

/// The Kafka Producer
///
/// See module level documentation.
pub struct Producer<P = DefaultPartitioner> {
    client: KafkaClient,
    state: State<P>,
    config: Config,
}

struct State<P> {
    /// A list of available partition IDs for each topic.
    partitions: HashMap<String, Partitions>,
    /// The partitioner to decide how to distribute messages
    partitioner: P,
}

struct Config {
    /// The maximum time to wait for acknowledgements. See
    /// `KafkaClient::produce_messages`.
    ack_timeout: i32,
    /// The number of acks to request. See
    /// `KafkaClient::produce_messages`.
    required_acks: i16,
}

impl Producer {
    /// Starts building a new producer using the given Kafka client.
    pub fn from_client(client: KafkaClient) -> Builder<DefaultPartitioner> {
        Builder::new(Some(client), Vec::new())
    }

    /// Starts building a producer bootstraping internally a new kafka
    /// client from the given kafka hosts.
    pub fn from_hosts(hosts: Vec<String>) -> Builder<DefaultPartitioner> {
        Builder::new(None, hosts)
    }

    /// Borrows the underlying kafka client.
    pub fn client(&self) -> &KafkaClient {
        &self.client
    }

    /// Destroys this producer returning the underlying kafka client.
    pub fn into_client(self) -> KafkaClient {
        self.client
    }
}


impl<P: Partitioner> Producer<P> {
    /// Synchronously send the specified message to Kafka.
    pub fn send<'a, K, V>(&mut self, rec: &Record<'a, K, V>) -> Result<()>
    where
        K: AsBytes,
        V: AsBytes,
    {
        let mut rs = try!(self.send_all(ref_slice(rec)));

        if self.config.required_acks == 0 {
            // ~ with no required_acks we get no response and
            // consider the send-data request blindly as successful
            Ok(())
        } else {
            assert_eq!(1, rs.len());
            let mut produce_confirm = rs.pop().unwrap();

            assert_eq!(1, produce_confirm.partition_confirms.len());
            produce_confirm
                .partition_confirms
                .pop()
                .unwrap()
                .offset
                .map(|_| ())
                .map_err(|err| ErrorKind::Kafka(err).into())
        }
    }

    /// Synchronously send all of the specified messages to Kafka. To validate
    /// that all of the specified records have been successfully delivered,
    /// inspection of the offsets on the returned confirms is necessary.
    pub fn send_all<'a, K, V>(&mut self, recs: &[Record<'a, K, V>]) -> Result<Vec<ProduceConfirm>>
    where
        K: AsBytes,
        V: AsBytes,
    {
        let partitioner = &mut self.state.partitioner;
        let partitions = &self.state.partitions;
        let client = &mut self.client;
        let config = &self.config;

        client.internal_produce_messages(
            config.required_acks,
            config.ack_timeout,
            recs.into_iter().map(|r| {
                let mut m = client::ProduceMessage {
                    key: to_option(r.key.as_bytes()),
                    value: to_option(r.value.as_bytes()),
                    topic: r.topic,
                    partition: r.partition,
                };
                partitioner.partition(Topics::new(partitions), &mut m);
                m
            }),
        )
    }
}

fn to_option(data: &[u8]) -> Option<&[u8]> {
    if data.is_empty() { None } else { Some(data) }
}

// --------------------------------------------------------------------

impl<P> State<P> {
    fn new(client: &mut KafkaClient, partitioner: P) -> Result<State<P>> {
        let ts = client.topics();
        let mut ids = HashMap::with_capacity(ts.len());
        for t in ts {
            let ps = t.partitions();
            ids.insert(
                t.name().to_owned(),
                Partitions {
                    available_ids: ps.available_ids(),
                    num_all_partitions: ps.len() as u32,
                },
            );
        }
        Ok(State {
            partitions: ids,
            partitioner: partitioner,
        })
    }
}

// --------------------------------------------------------------------

/// A Kafka Producer builder easing the process of setting up various
/// configuration settings.
pub struct Builder<P = DefaultPartitioner> {
    client: Option<KafkaClient>,
    hosts: Vec<String>,
    compression: Compression,
    ack_timeout: Duration,
    conn_idle_timeout: Duration,
    required_acks: RequiredAcks,
    partitioner: P,
    security_config: Option<SecurityConfig>,
    client_id: Option<String>,
}

impl Builder {
    fn new(client: Option<KafkaClient>, hosts: Vec<String>) -> Builder<DefaultPartitioner> {
        let mut b = Builder {
            client: client,
            hosts: hosts,
            compression: client::DEFAULT_COMPRESSION,
            ack_timeout: Duration::from_millis(DEFAULT_ACK_TIMEOUT_MILLIS),
            conn_idle_timeout: Duration::from_millis(
                client::DEFAULT_CONNECTION_IDLE_TIMEOUT_MILLIS,
            ),
            required_acks: DEFAULT_REQUIRED_ACKS,
            partitioner: DefaultPartitioner::default(),
            security_config: None,
            client_id: None,
        };
        if let Some(ref c) = b.client {
            b.compression = c.compression();
            b.conn_idle_timeout = c.connection_idle_timeout();
        }
        b
    }

    /// Specifies the security config to use.
    /// See `KafkaClient::new_secure` for more info.
    #[cfg(feature = "security")]
    pub fn with_security(mut self, security: SecurityConfig) -> Self {
        self.security_config = Some(security);
        self
    }

    /// Sets the compression algorithm to use when sending out data.
    ///
    /// See `KafkaClient::set_compression`.
    pub fn with_compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }

    /// Sets the maximum time the kafka brokers can await the receipt
    /// of required acknowledgements (which is specified through
    /// `Builder::with_required_acks`.)  Note that Kafka explicitely
    /// documents this not to be a hard limit.
    pub fn with_ack_timeout(mut self, timeout: Duration) -> Self {
        self.ack_timeout = timeout;
        self
    }

    /// Specifies the timeout for idle connections.
    /// See `KafkaClient::set_connection_idle_timeout`.
    pub fn with_connection_idle_timeout(mut self, timeout: Duration) -> Self {
        self.conn_idle_timeout = timeout;
        self
    }

    /// Sets how many acknowledgements the kafka brokers should
    /// receive before responding to sent messages.
    ///
    /// See `RequiredAcks`.
    pub fn with_required_acks(mut self, acks: RequiredAcks) -> Self {
        self.required_acks = acks;
        self
    }

    /// Specifies a client_id to be sent along every request to Kafka
    /// brokers. See `KafkaClient::set_client_id`.
    pub fn with_client_id(mut self, client_id: String) -> Self {
        self.client_id = Some(client_id);
        self
    }
}

impl<P> Builder<P> {
    /// Sets the partitioner to dispatch when sending messages without
    /// an explicit partition assignment.
    pub fn with_partitioner<Q: Partitioner>(self, partitioner: Q) -> Builder<Q> {
        Builder {
            client: self.client,
            hosts: self.hosts,
            compression: self.compression,
            ack_timeout: self.ack_timeout,
            conn_idle_timeout: self.conn_idle_timeout,
            required_acks: self.required_acks,
            partitioner: partitioner,
            security_config: None,
            client_id: None,
        }
    }

    #[cfg(not(feature = "security"))]
    fn new_kafka_client(hosts: Vec<String>, _: Option<SecurityConfig>) -> KafkaClient {
        KafkaClient::new(hosts)
    }

    #[cfg(feature = "security")]
    fn new_kafka_client(hosts: Vec<String>, security: Option<SecurityConfig>) -> KafkaClient {
        if let Some(security) = security {
            KafkaClient::new_secure(hosts, security)
        } else {
            KafkaClient::new(hosts)
        }
    }

    /// Finally creates/builds a new producer based on the so far
    /// supplied settings.
    pub fn create(self) -> Result<Producer<P>> {
        // ~ create the client if necessary
        let (mut client, need_metadata) = match self.client {
            Some(client) => (client, false),
            None => (Self::new_kafka_client(self.hosts, self.security_config), true),
        };
        // ~ apply configuration settings
        client.set_compression(self.compression);
        client.set_connection_idle_timeout(self.conn_idle_timeout);
        if let Some(client_id) = self.client_id {
            client.set_client_id(client_id);
        }
        let producer_config = Config {
            ack_timeout: try!(protocol::to_millis_i32(self.ack_timeout)),
            required_acks: self.required_acks as i16,
        };
        // ~ load metadata if necessary
        if need_metadata {
            try!(client.load_metadata_all());
        }
        // ~ create producer state
        let state = try!(State::new(&mut client, self.partitioner));
        Ok(Producer {
            client: client,
            state: state,
            config: producer_config,
        })
    }
}

// --------------------------------------------------------------------

/// A description of available topics and their available partitions.
///
/// Indented for use by `Partitioner`s.
pub struct Topics<'a> {
    partitions: &'a HashMap<String, Partitions>,
}

/// Producer relevant partition information of a particular topic.
///
/// Indented for use by `Partition`s.
pub struct Partitions {
    available_ids: Vec<i32>,
    num_all_partitions: u32,
}

impl Partitions {
    /// Retrieves the list of the identifiers of currently "available"
    /// partitions for the given topic.  This list excludes partitions
    /// which do not have a leader broker assigned.
    #[inline]
    pub fn available_ids(&self) -> &[i32] {
        &self.available_ids
    }

    /// Retrieves the number of "available" partitions. This is a
    /// merely a convenience method. See `Partitions::available_ids`.
    #[inline]
    pub fn num_available(&self) -> u32 {
        self.available_ids.len() as u32
    }

    /// The total number of partitions of the underlygin topic.  This
    /// number includes also partitions without a current leader
    /// assignment.
    #[inline]
    pub fn num_all(&self) -> u32 {
        self.num_all_partitions
    }
}

impl<'a> Topics<'a> {
    fn new(partitions: &'a HashMap<String, Partitions>) -> Topics<'a> {
        Topics { partitions: partitions }
    }

    /// Retrieves informationa about a topic's partitions.
    #[inline]
    pub fn partitions(&'a self, topic: &str) -> Option<&'a Partitions> {
        self.partitions.get(topic)
    }
}

/// A partitioner is given a chance to choose/redefine a partition for
/// a message to be sent to Kafka.  See also
/// `Record#with_partition`.
///
/// Implementations can be stateful.
pub trait Partitioner {
    /// Supposed to inspect the given message and if desired re-assign
    /// the message's target partition.
    ///
    /// `topics` a description of the currently known topics and their
    /// currently available partitions.
    ///
    /// `msg` the message whose partition assignment potentially to
    /// change.
    fn partition(&mut self, topics: Topics, msg: &mut client::ProduceMessage);
}

/// The default hasher implementation used of `DefaultPartitioner`.
pub type DefaultHasher = XxHash32;

/// As its name implies `DefaultPartitioner` is the default
/// partitioner for `Producer`.
///
/// For every message it proceedes as follows:
///
/// - If the messages contains a non-negative partition value it
/// leaves the message untouched.  This will cause `Producer` to try
/// to send the message to exactly that partition to.
///
/// - Otherwise, if the message has an "unspecified" `partition` -
/// this is, it has a negative partition value - and a specified key,
/// `DefaultPartitioner` will compute a hash from the key using the
/// underlying hasher and take `hash % num_all_partitions` to derive
/// the partition to send the message to.  This will consistently
/// cause messages with the same key to be sent to the same partition.
///
/// - Otherwise - a message with an "unspecified" `partition` and no
/// key - `DefaultPartitioner` will "randomly" pick one from the
/// "available" partitions trying to distribute the messages across
/// the multiple partitions.  In particular, it tries to distribute
/// such messsages across the "available" partitions in a round robin
/// fashion.  "Available" it this context means partitions with a
/// known leader.
///
/// This behavior may not suffice every workload.  If your application
/// is dependent on a particular distribution scheme different from
/// the one outlined above, you want to provide your own partioner to
/// the `Producer` at its initialization time.
///
/// See `Builder::with_partitioner`.
#[derive(Default)]
pub struct DefaultPartitioner<H = BuildHasherDefault<DefaultHasher>> {
    // ~ a hasher builder; used to consistently hash keys
    hash_builder: H,
    // ~ a counter incremented with each partitioned message to
    // achieve a different partition assignment for each message
    cntr: u32,
}

impl DefaultPartitioner {
    /// Creates a new partitioner which will use the given hash
    /// builder to hash message keys.
    pub fn with_hasher<B: BuildHasher>(hash_builder: B) -> DefaultPartitioner<B> {
        DefaultPartitioner {
            hash_builder: hash_builder.into(),
            cntr: 0,
        }
    }

    pub fn with_default_hasher<B>() -> DefaultPartitioner<BuildHasherDefault<B>>
    where
        B: Hasher + Default,
    {
        DefaultPartitioner {
            hash_builder: BuildHasherDefault::<B>::default(),
            cntr: 0,
        }
    }
}

impl<H: BuildHasher> Partitioner for DefaultPartitioner<H> {
    #[allow(unused_variables)]
    fn partition(&mut self, topics: Topics, rec: &mut client::ProduceMessage) {
        if rec.partition >= 0 {
            // ~ partition explicitely defined, trust the user
            return;
        }
        let partitions = match topics.partitions(rec.topic) {
            None => return, // ~ unknown topic, this is not the place to deal with it.
            Some(partitions) => partitions,
        };
        match rec.key {
            Some(key) => {
                let num_partitions = partitions.num_all();
                if num_partitions == 0 {
                    // ~ no partitions at all ... a rather strange
                    // topic. again, this is not the right place to
                    // deal with it.
                    return;
                }
                let mut h = self.hash_builder.build_hasher();
                h.write(key);
                // ~ unconditionally dispatch to partitions no matter
                // whether they are currently available or not.  this
                // guarantees consistency which is the point of
                // partitioning by key.  other behaviour - if desired
                // - can be implemented in custom, user provided
                // partitioners.
                let hash = h.finish() as u32;
                // if `num_partitions == u32::MAX` this can lead to a
                // negative partition ... such a partition count is very
                // unlikely though
                rec.partition = (hash % num_partitions) as i32;
            }
            None => {
                // ~ no key available, determine a partition from the
                // available ones.
                let avail = partitions.available_ids();
                if avail.len() > 0 {
                    rec.partition = avail[self.cntr as usize % avail.len()];
                    // ~ update internal state so that the next time we choose
                    // a different partition
                    self.cntr = self.cntr.wrapping_add(1);
                }
            }
        }
    }
}

// --------------------------------------------------------------------

#[cfg(test)]
mod default_partitioner_tests {
    use std::hash::{Hasher, BuildHasherDefault};
    use std::collections::HashMap;

    use client;
    use super::{DefaultPartitioner, DefaultHasher, Partitioner, Partitions, Topics};

    fn topics_map(topics: Vec<(&str, Partitions)>) -> HashMap<String, Partitions> {
        let mut h = HashMap::new();
        for topic in topics {
            h.insert(topic.0.into(), topic.1);
        }
        h
    }

    fn assert_partitioning<P: Partitioner>(
        topics: &HashMap<String, Partitions>,
        p: &mut P,
        topic: &str,
        key: &str,
    ) -> i32 {
        let mut msg = client::ProduceMessage {
            key: Some(key.as_bytes()),
            value: None,
            topic: topic,
            partition: -1,
        };
        p.partition(Topics::new(topics), &mut msg);
        let num_partitions = topics.get(topic).unwrap().num_all_partitions as i32;
        assert!(msg.partition >= 0 && msg.partition < num_partitions);
        msg.partition
    }

    /// Validate consistent partitioning on a message's key
    #[test]
    fn test_key_partitioning() {
        let h = topics_map(vec![
            (
                "foo",
                Partitions {
                    available_ids: vec![0, 1, 4],
                    num_all_partitions: 5,
                }
            ),
            (
                "bar",
                Partitions {
                    available_ids: vec![0, 1],
                    num_all_partitions: 2,
                }
            ),
        ]);

        let mut p: DefaultPartitioner<BuildHasherDefault<DefaultHasher>> = Default::default();

        // ~ validate that partitioning by the same key leads to the same
        // partition
        let h1 = assert_partitioning(&h, &mut p, "foo", "foo-key");
        let h2 = assert_partitioning(&h, &mut p, "foo", "foo-key");
        assert_eq!(h1, h2);

        // ~ validate that partitioning by different keys leads to
        // different partitions (the keys are chosen such that they lead
        // to different partitions)
        let h3 = assert_partitioning(&h, &mut p, "foo", "foo-key");
        let h4 = assert_partitioning(&h, &mut p, "foo", "bar-key");
        assert!(h3 != h4);
    }

    #[derive(Default)]
    struct MyCustomHasher(u64);

    impl Hasher for MyCustomHasher {
        fn finish(&self) -> u64 {
            self.0
        }
        fn write(&mut self, bytes: &[u8]) {
            self.0 = bytes[0] as u64;
        }
    }

    /// Validate it is possible to register a custom hasher with the
    /// default partitioner
    #[test]
    fn default_partitioner_with_custom_hasher_default() {
        // this must compile
        let mut p = DefaultPartitioner::with_default_hasher::<MyCustomHasher>();

        let h = topics_map(vec![
            (
                "confirms",
                Partitions {
                    available_ids: vec![0, 1],
                    num_all_partitions: 2,
                }
            ),
            (
                "contents",
                Partitions {
                    available_ids: vec![0, 1, 9],
                    num_all_partitions: 10,
                }
            ),
        ]);

        // verify also the partitioner derives the correct partition
        // ... this is hash modulo num_all_partitions. here it is a
        // topic with a total of 2 partitions.
        let p1 = assert_partitioning(&h, &mut p, "confirms", "A" /* ascii: 65 */);
        assert_eq!(1, p1);

        // here it is a topic with a total of 10 partitions
        let p2 = assert_partitioning(&h, &mut p, "contents", "B" /* ascii: 66 */);
        assert_eq!(6, p2);
    }
}
