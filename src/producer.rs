//! Kafka Producer
//!
//! A multi-topic capable producer for a Kafka cluster providing a
//! convenient API for sending messages.  So far the producer has only
//! synchronous capabilities.
//!
//! In Kafka, each message is basically a key/value pair.  A `ProduceRecord`
//! is all the data necessary to produce such a message to Kafka.
//!
//! # Example
//! ```no_run
//! use std::fmt::Write;
//! use kafka::producer::{Producer, Record};
//!
//! let mut producer =
//!     Producer::from_hosts(vec!("localhost:9092".to_owned()))
//!         .with_ack_timeout(1000)
//!         .with_required_acks(1)
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
//! In this example, when the method call to `producer.send` returns
//! successfully, we are guaranteed the message is delivered to Kafka
//! and persisted by at least one Kafka broker.  However, when sending
//! multiple messages just like this example, it is more efficient to
//! send them in batches using `Producer::send_all`.

// XXX 0) Always invoke partitioner
// XXX 1) rethink return values for the send_all method
// XXX 2) maintain a background thread to provide an async version of the send* methods
// XXX 3) Handle recoverable errors behind the scenes through retry attempts

use std::collections::HashMap;
use std::fmt;

use client::{self, KafkaClient};
// public re-exports
pub use client::{Compression};
use error::Result;
use utils::TopicPartitionOffsetError;

use ref_slice::ref_slice;

/// The default value for `Builder::with_ack_timeout`.
pub const DEFAULT_ACK_TIMEOUT: i32 = 30 * 1000;

/// The default value for `Builder::with_required_acks`.
pub const DEFAULT_REQUIRED_ACKS: i16 = 1;

// --------------------------------------------------------------------

/// A trait used by `Producer` to obtain the bytes `Record::key` and
/// `Record::value` represent.  This leaves the choice of the types
/// for `key` and `value` with the client.
pub trait AsBytes {
    fn as_bytes(&self) -> &[u8];
}

impl AsBytes for () {
    fn as_bytes(&self) -> &[u8] { &[] }
}

// There seems to be some compiler issue with this:
// impl<T: AsRef<[u8]>> AsBytes for T {
//     fn as_bytes(&self) -> &[u8] { self.as_ref() }
// }

// for now we provide the impl for some standard library types
impl AsBytes for String {
    fn as_bytes(&self) -> &[u8] { self.as_ref() }
}
impl AsBytes for Vec<u8> {
    fn as_bytes(&self) -> &[u8] { self.as_ref() }
}

impl<'a> AsBytes for &'a [u8] {
    fn as_bytes(&self) -> &[u8] { self }
}
impl<'a> AsBytes for &'a str {
    fn as_bytes(&self) -> &[u8] { str::as_bytes(self) }
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
        Record { key: key, value: value, topic: topic, partition: -1 }
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
        Record { key: (), value: value, topic: topic, partition: -1 }
    }
}

impl<'a, K: fmt::Debug, V: fmt::Debug> fmt::Debug for Record<'a, K, V> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Record {{ topic: {}, partition: {}, key: {:?}, value: {:?} }}",
               self.topic, self.partition, self.key, self.value)
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
    partition_ids: HashMap<String, Vec<i32>>,
    /// The partitioner to decide how to distribute messages
    partitioner: P,
}

struct Config {
    /// The maximum time in millis to wait for acknowledgements. See
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

    /// Destroys this producer returning the underlying kafka client.
    pub fn client(self) -> KafkaClient {
        self.client
    }
}


impl<P: Partitioner> Producer<P> {

    /// Synchronously send the specified message to Kafka.
    pub fn send<'a, K, V>(&mut self, rec: &Record<'a, K, V>)
                          -> Result<()>
        where K: AsBytes, V: AsBytes
    {
        let mut rs = try!(self.send_all(ref_slice(rec)));
        assert_eq!(1, rs.len());
        if let Some(e) = rs.pop().unwrap().error {
            Err(e)
        } else {
            Ok(())
        }
    }

    /// Synchronously send all of the specified messages to Kafka.
    pub fn send_all<'a, K, V>(&mut self, recs: &[Record<'a, K, V>])
                              -> Result<Vec<TopicPartitionOffsetError>>
        where K: AsBytes, V: AsBytes
    {
        let partitioner = &mut self.state.partitioner;
        let partition_ids = &self.state.partition_ids;
        let client = &mut self.client;
        let config = &self.config;

        client.produce_messages(
            config.required_acks, config.ack_timeout,
            recs.into_iter().map(|r| {
                let mut m = client::ProduceMessage {
                    key: to_option(r.key.as_bytes()),
                    value: to_option(r.value.as_bytes()),
                    topic: r.topic,
                    partition: r.partition,
                };
                // XXX always invoke the partitioner
                if r.partition < 0 {
                    if let Some(ps) = partition_ids.get(r.topic).map(|ps| &ps[..]) {
                        partitioner.partition(&mut m, ps);
                    }
                }
                m
            }))
    }
}

fn to_option(data: &[u8]) -> Option<&[u8]> {
    if data.is_empty() {
        None
    } else {
        Some(data)
    }
}

// --------------------------------------------------------------------

impl<P> State<P> {
    fn new(client: &mut KafkaClient, partitioner: P) -> Result<State<P>> {
        let ts = client.topics();
        let mut ids = HashMap::with_capacity(ts.len());
        for t in ts {
            ids.insert(t.name().to_owned(), t.partition_ids());
        }
        Ok(State{partition_ids: ids, partitioner: partitioner})
    }
}

// --------------------------------------------------------------------

/// A Kafka Producer builder easing the process of setting up various
/// configuration settings.
pub struct Builder<P = DefaultPartitioner> {
    client: Option<KafkaClient>,
    hosts: Vec<String>,
    compression: Compression,
    ack_timeout: i32,
    required_acks: i16,
    partitioner: P,
}

impl Builder {
    fn new(client: Option<KafkaClient>, hosts: Vec<String>)
           -> Builder<DefaultPartitioner>
    {
        let mut b = Builder {
            client: client,
            hosts: hosts,
            compression: client::DEFAULT_COMPRESSION,
            ack_timeout: DEFAULT_ACK_TIMEOUT,
            required_acks: DEFAULT_REQUIRED_ACKS,
            partitioner: DefaultPartitioner::default(),
        };
        if let Some(ref c) = b.client {
            b.compression = c.compression();
        }
        b
    }

    /// Sets the compression algorithm to use when sending out data.
    ///
    /// See `KafkaClient::set_compression`.
    pub fn with_compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }

    /// Sets the maximum time in milliseconds the kafka brokers can
    /// await the receipt of required acknowledgements (which is
    /// specified through `Builder::with_required_acks`.)  Note that
    /// Kafka explicitely documents this not to be a hard limit.
    pub fn with_ack_timeout(mut self, timeout: i32) -> Self {
        self.ack_timeout = timeout;
        self
    }

    /// Sets how many acknowledgements the kafka brokers should
    /// receive before responding to sent messages.  If it is 0 the
    /// servers will not send any response.  If it is 1, the server
    /// will wait the data is written to the local server log before
    /// sending a replying.  If it is -1 the servers will block until
    /// the messages are committed by all in sync replicas before
    /// replaying.  For any number `> 1` the servers will block
    /// waiting for this number of acknowledgements to occur (but the
    /// servers will never wait for more acknowledgements than there
    /// are in-sync replicas).
    pub fn with_required_acks(mut self, required_acks: i16) -> Self {
        self.required_acks = required_acks;
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
            required_acks: self.required_acks,
            partitioner: partitioner,
        }
    }

    /// Finally creates/builds a new producer based on the so far
    /// supplied settings.
    pub fn create(self) -> Result<Producer<P>> {
        let mut client = match self.client {
            Some(client) => client,
            None => {
                let mut client = KafkaClient::new(self.hosts);
                try!(client.load_metadata_all());
                client
            }
        };
        client.set_compression(self.compression);
        let state = try!(State::new(&mut client, self.partitioner));
        let config = Config {
            ack_timeout: self.ack_timeout,
            required_acks: self.required_acks,
        };
        Ok(Producer{
            client: client,
            state: state,
            config: config,
        })
    }
}

// --------------------------------------------------------------------

/// A partitioner is given a chance to choose/redefine a partition for
/// a message to be sent to Kafka.  See also
/// `Record#with_partition`.
///
/// Implementations can be stateful.
pub trait Partitioner {
    /// Supposed to inspect the given message and if desired re-assign
    /// the message's target partition.  Since the partitioner is
    /// given a mutable reference and may potentially change even more
    /// than just the partition.

    // XXX needs to be given a `Cluster/Topics` structure, such that a
    // partitioner has the chance to avoid topic look ups.

    fn partition(&mut self, msg: &mut client::ProduceMessage, partitions: &[i32]);
}

/// As its name implies `DefaultPartitioner` is the default
/// partitioner for `Producer`.
///
/// Every received message with a non-negative value will not be
/// changed by this partitioner and will be merely passed through.
///
/// However, for every message with a negative `partition` it will try
/// to find a target partition. In a very simple manner, it tries to
/// distribute such messsages across available partitions in a round
/// robin fashion.
///
/// This behavior may not suffice every workload.  If you're
/// application is dependent on a particular distribution scheme, you
/// want to provide your own partioner to the `Producer` at
/// initialization time.
///
/// See `Builder::with_partitioner`.
pub struct DefaultPartitioner {
    // ~ a counter incremented with each partitioned message to
    // achieve a different partition assignment for each message
    cntr: u32
}

impl Partitioner for DefaultPartitioner {
    #[allow(unused_variables)]
    fn partition(&mut self, rec: &mut client::ProduceMessage, partitions: &[i32]) {
        if rec.partition < 0 && !partitions.is_empty() {
            // ~ get a partition
            rec.partition = partitions[self.cntr as usize % partitions.len()];
            // ~ update internal state so that the next time we choose
            // a different partition
            self.cntr = self.cntr.wrapping_add(1);
        }
    }
}

impl Default for DefaultPartitioner {
    fn default() -> Self {
        DefaultPartitioner { cntr: 0 }
    }
}
