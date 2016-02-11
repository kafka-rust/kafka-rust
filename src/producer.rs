//! Kafka Producer
//!
//! A multi-topic capable producer for a Kafka cluster.  So far the
//! producer has only synchronous capabilities.
//!
//! In Kafka, each message is basically a key/value pair. A
//! `ProduceMessage` is all the data necessary to produce such a
//! message.
//!
//! # Example
//! ```no_run
//! use std::fmt::Write;
//! use kafka::producer::{Producer, ProduceMessage};
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
//!   producer.send(&ProduceMessage::from_value("my-topic", buf.as_bytes())).unwrap();
//!   buf.clear();
//! }
//! ```
//!
//! In this example, when the method call to `producer.send` returns
//! successfully, we are guaranteed the message is delivered to Kafka
//! and persisted by at least one Kafka broker.  However, when sending
//! multiple messages just like this example, it is more efficient to
//! send them in batches using `Producer::send_all`.

// XXX 1) rethink return values for the send* methods
// XXX 2) maintain a background thread to provide an async version of the send* methods
// XXX 3) allow client to pass in real objects (instead of raw byte slices) which get serialized using a registered serializer
// XXX 4) Handle recoverable errors behind the scenes through retry attempts

use std::collections::HashMap;

use client::{self, KafkaClient};
// public re-exports
pub use client::{Compression, ProduceMessage};
use error::Result;
use utils::TopicPartitionOffsetError;

/// The default value for `Builder::with_ack_timeout`.
pub const DEFAULT_ACK_TIMEOUT: i32 = 30 * 1000;

/// The default value for `Builder::with_required_acks`.
pub const DEFAULT_REQUIRED_ACKS: i16 = 1;

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
    /// Starts building a producer using the given kafka client.
    pub fn from_client(client: KafkaClient) -> Builder<DefaultPartitioner> {
        Builder::new(Some(client), Vec::new())
    }

    /// Starts building a producer bootstraping internally a new kafka
    /// client from the given kafka hosts.
    pub fn from_hosts(hosts: Vec<String>) -> Builder {
        Builder::new(None, hosts)
    }

    /// Destroys this producer returning the underlying kafka client.
    pub fn client(self) -> KafkaClient {
        self.client
    }
}

impl<P: Partitioner> Producer<P> {

    /// Synchronously send the specified message to Kafka.
    ///
    /// If the given message's `ProduceMessage::partition` is
    /// negative, the underlying partitioner will be called to
    /// determine the partition to deliver this message to.
    pub fn send<'a, 'b>(&mut self, msg: &ProduceMessage<'a, 'b>) -> Result<()> {

        let mut rs = try!(self.send_all(&[msg]));
        assert_eq!(1, rs.len());
        if let Some(e) = rs.pop().unwrap().error {
            Err(e)
        } else {
            Ok(())
        }
    }

    /// Synchronously send all of the specified messages to Kafka.
    pub fn send_all<'a, 'b, I, J>(&mut self, msgs: I) -> Result<Vec<TopicPartitionOffsetError>>
        where J: AsRef<ProduceMessage<'a, 'b>>, I: IntoIterator<Item=J>
    {
        let partitioner = &mut self.state.partitioner;
        let partition_ids = &self.state.partition_ids;
        let client = &mut self.client;
        let config = &self.config;

        client.produce_messages(
            config.required_acks, config.ack_timeout,
            msgs.into_iter().map(|m| {
                let m = m.as_ref();
                client::ProduceMessage {
                    key: m.key,
                    value: m.value,
                    topic: m.topic,
                    partition: if m.partition < 0 {
                        match partition_ids.get(m.topic).map(|ps| &ps[..]) {
                            // ~ invoke the partitioner (only if we
                            // really have any partitions to choose
                            // from)
                            Some(ps) if !ps.is_empty() => partitioner.partition(m, ps),
                            // ~ this is likely to result in an error
                            // code from KafkaClient.
                            _ => m.partition, 
                        }
                    } else {
                        m.partition
                    }
                }
            }))
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
    partitioner: P
}

impl Builder {
    fn new(client: Option<KafkaClient>, hosts: Vec<String>) -> Builder<DefaultPartitioner> {
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
        Ok(Producer{ client: client, state: state, config: config })
    }
}

// --------------------------------------------------------------------

/// A partitioner chooses a partition for a to-be-sent message which
/// has an "unspecified" partition.  See also
/// `ProduceMessage#with_partition`.
///
/// Implementations can be stateful.
pub trait Partitioner {
    /// Given a list of available `partitions`, decides for the given
    /// `ProduceMessage` which partition to send it to.  The returned
    /// value must be chosen from the given `partitions` slice which
    /// specifies the list of currently available partitions for the
    /// message's topic.
    ///
    /// Due to potential retry attempts to send a message a partitioner
    /// might be invoked multiple times for a particular message.
    fn partition(&mut self, msg: &ProduceMessage, partitions: &[i32]) -> i32;
}

/// As its name implies `DefaultPartitioner` is the default
/// partitioner for `Producer`.  In a very simple manner, it tries to
/// distribute every messsage to the "next" partition in a round robin
/// fashion.  However, the implementation is kept very simplistic and
/// may not suffice every workload.  Further, if you're application is
/// dependent on a particular distribution scheme, you want to provide
/// your own partioner to the `Producer` instead.  See also
/// `Builder::with_partitioner`.
pub struct DefaultPartitioner {
    // ~ a counter incremented with each partitioned message to
    // achieve a different partition assignment for each message
    cntr: u32
}

impl Partitioner for DefaultPartitioner {
    #[allow(unused_variables)]
    fn partition(&mut self, rec: &ProduceMessage, partitions: &[i32]) -> i32 {
        let p = partitions[self.cntr as usize % partitions.len()];
        self.cntr = self.cntr.wrapping_add(1);
        p
    }
}

impl Default for DefaultPartitioner {
    fn default() -> Self {
        DefaultPartitioner { cntr: 0 }
    }
}
