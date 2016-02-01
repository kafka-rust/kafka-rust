//! The primary module of this library providing a mid-level
//! abstraction for a kafka server while supposed to allow building
//! higher level constructs.

use std::collections::hash_map::{HashMap, Entry};
use std::io::Cursor;
use std::io::Read;
use std::iter::Iterator;
use std::mem;
use std::cell::Cell;

// pub re-export
pub use compression::Compression;
pub use protocol::fetch;

use codecs::{ToByte, FromByte};
use connection::KafkaConnection;
use error::{Result, Error, KafkaCode};
use protocol::{self, FromResponse};
use utils;

pub mod metadata;

const CLIENTID: &'static str = "kafka-rust";
const DEFAULT_SO_TIMEOUT_SECS: i32 = 120; // socket read, write timeout seconds

/// The default value for `KafkaClient::set_compression(..)`
pub const DEFAULT_COMPRESSION: Compression = Compression::NONE;

/// The default value for `KafkaClient::set_fetch_max_wait_time(..)`
pub const DEFAULT_FETCH_MAX_WAIT_TIME: i32 = 100; // milliseconds

/// The default value for `KafkaClient::set_fetch_min_bytes(..)`
pub const DEFAULT_FETCH_MIN_BYTES: i32 = 4096;

/// The default value for `KafkaClient::set_fetch_max_bytes(..)`
pub const DEFAULT_FETCH_MAX_BYTES_PER_PARTITION: i32 = 32 * 1024;


/// Client struct keeping track of brokers and topic metadata.
///
/// Implements methods described by the [Kafka Protocol](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol).
///
/// You will have to load metadata before making any other request.
#[derive(Debug)]
pub struct KafkaClient {
    // ~ this kafka client configuration
    config: ClientConfig,

    // ~ a pool of re-usable connections to kafka brokers
    conn_pool: ConnectionPool,

    // ~ the current state of this client
    state: ClientState,
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
}

#[derive(Debug)]
struct ClientState {
    // ~ the last correlation used when communicating with kafka
    // (see `#next_correlation_id`)
    correlation: i32,

    // ~ a list of known brokers referred to by the index in this
    // vector.  This index is also referred to as `BrokerIndex` in
    // this module.
    //
    // note: loading of additional topic metadata must preserve
    // already present brokers in this vector at their position.
    brokers: Vec<Broker>,

    // ~ a mapping of topic to information about its partitions
    // ~ `TopicPartitions#partitions` are kept in ascending order by `partition_id`
    topic_partitions: HashMap<String, TopicPartitions>,
}

#[derive(Debug)]
struct Broker {
    /// ~ the kafka identifier of this broker
    node_id: i32,
    /// ~ host:port of this broker
    host: String,
}

/// A representation of partitions for a single topic.
#[derive(Debug)]
struct TopicPartitions {
    curr_partition_idx: Cell<usize>,
    partitions: Vec<TopicPartition>,
}

/// A custom identifier for a broker. KafkaClient maintains an array
/// of brokers where a BrokerId is the index into this array.
#[derive(Debug, Copy, Clone)]
struct BrokerIndex(u32);

/// Metadata for a single topic partition.
#[derive(Debug)]
struct TopicPartition {
    partition_id: i32,
    // ~ an index into ClientState#brokers
    broker_index: BrokerIndex,
}

impl TopicPartitions {
    fn new(partitions: Vec<TopicPartition>) -> TopicPartitions {
        TopicPartitions { curr_partition_idx: Cell::new(0), partitions: partitions }
    }

    fn partition(&self, partition_id: i32) -> Option<&TopicPartition> {
        self.partitions.binary_search_by(|e| {
            e.partition_id.cmp(&partition_id)
        }).ok().and_then(|i| {
            // `get_unchecked` is safe here due to the preceeding binary_search
            unsafe { Some(self.partitions.get_unchecked(i)) }
        })
    }
}

impl TopicPartition {
    fn new(partition_id: i32, broker_index: BrokerIndex) -> TopicPartition {
        TopicPartition { partition_id: partition_id, broker_index: broker_index }
    }
}

impl ClientState {
    fn next_correlation_id(&mut self) -> i32 {
        self.correlation = (self.correlation + 1) % (1i32 << 30);
        self.correlation
    }

    fn broker_by_partition(&self, partition: &TopicPartition) -> &Broker {
        &self.brokers[partition.broker_index.0 as usize]
    }

    fn find_broker<'a>(&'a self, topic: &str, partition: i32) -> Option<&'a str> {
        self.topic_partitions.get(topic)
            .and_then(|tp| tp.partition(partition).map(|p| {
                let host: &str = &self.broker_by_partition(p).host;
                host
            }))
    }

    /// Chooses for the given topic a partition to write the next message to.
    /// Returns the partition_id and the corresponding broker host.
    fn choose_partition<'a, 'b>(&'a self, topic: &'b str) -> Option<(i32, &'a str)> {
        match self.topic_partitions.get(topic) {
            None => None,
            Some(ref topic) if topic.partitions.is_empty() => None,
            Some(ref topic) => {
                let p_idx = (topic.curr_partition_idx.get() + 1) % topic.partitions.len();
                topic.curr_partition_idx.set(p_idx);
                let p = &topic.partitions[p_idx];
                let broker: &str = &self.broker_by_partition(p).host;
                Some((p.partition_id, broker))
            }
        }
    }
}

#[derive(Debug)]
struct ConnectionPool {
    conns: HashMap<String, KafkaConnection>,
    timeout: i32,
}

impl ConnectionPool {
    fn new(timeout: i32) -> ConnectionPool {
        ConnectionPool {
            conns: HashMap::new(),
            timeout: timeout,
        }
    }

    fn get_conn<'a>(&'a mut self, host: &str) -> Result<&'a mut KafkaConnection> {
        if let Some(conn) = self.conns.get_mut(host) {
            // ~ decouple the lifetimes to make borrowck happy; this
            // is actually safe since we're immediatelly returning
            // this, so the follow up code is not affected here (this
            // method is no longer recursive).
            return Ok(unsafe { mem::transmute(conn) });
        }
        self.conns.insert(host.to_owned(),
                          try!(KafkaConnection::new(host, self.timeout)));
        Ok(self.conns.get_mut(host).unwrap())
    }
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
    /// timestamp in milliseconds.  See also
    /// https://cwiki.apache.org/confluence/display/KAFKA/Writing+a+Driver+for+Kafka#WritingaDriverforKafka-Offsets
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
            topic: topic,
            partition: partition,
            offset: offset,
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
        &self
    }
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
    ///
    /// See also `KafkaClient::load_metadatata_all` and `KafkaClient::load_metadata`
    pub fn new(hosts: Vec<String>) -> KafkaClient {
        KafkaClient {
            config: ClientConfig {
                client_id: CLIENTID.to_owned(),
                hosts: hosts,
                compression: DEFAULT_COMPRESSION,
                fetch_max_wait_time: DEFAULT_FETCH_MAX_WAIT_TIME,
                fetch_min_bytes: DEFAULT_FETCH_MIN_BYTES,
                fetch_max_bytes_per_partition: DEFAULT_FETCH_MAX_BYTES_PER_PARTITION,
            },
            conn_pool: ConnectionPool::new(DEFAULT_SO_TIMEOUT_SECS),
            state: ClientState {
                correlation: 0,
                brokers: Vec::new(),
                topic_partitions: HashMap::new(),
            },
        }
    }

    /// Exposes the hosts used for discovery of the target kafka
    /// cluster.  This set of hosts corresponds to the values supplied
    /// to `KafkaClient::new`.
    #[inline]
    pub fn hosts(&self) -> &[String] {
        &self.config.hosts
    }

    /// Sets the compression algorithm to use when sending out messages.
    ///
    /// # Example
    ///
    /// ```no_run
    /// let mut client = kafka::client::KafkaClient::new(vec!("localhost:9092".to_owned()));
    /// client.load_metadata_all().unwrap();
    /// client.set_compression(kafka::client::Compression::SNAPPY);
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
    pub fn set_fetch_max_wait_time(&mut self, max_wait_time: i32) {
        self.config.fetch_max_wait_time = max_wait_time;
    }

    /// Retrieves the current `KafkaClient::set_fetch_max_wait_time`
    /// setting.
    #[inline]
    pub fn fetch_max_wait_time(&self) -> i32 {
        self.config.fetch_max_wait_time
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
    /// use kafka::client::{KafkaClient, FetchPartition};
    ///
    /// let mut client = KafkaClient::new(vec!["localhost:9092".to_owned()]);
    /// client.load_metadata_all().unwrap();
    /// client.set_fetch_max_wait_time(100);
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

    /// Provides a view onto the currently loaded metadata of known topics.
    ///
    /// # Examples
    /// ```no_run
    /// let mut client = kafka::client::KafkaClient::new(vec!("localhost:9092".to_owned()));
    /// client.load_metadata_all().unwrap();
    /// for topic in client.topics() {
    ///   for partition in topic.partitions() {
    ///     println!("{} #{} => {}", topic.name(), partition.id(), partition.leader_host());
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
        self.load_metadata_impl(resp)
    }

    // ~ this is the rest of `load_metadata`. this code is _not_
    // generic (and hence will _not_ get generated for each distinct
    // input type T to the `load_metadata` method).
    fn load_metadata_impl(&mut self, md: protocol::MetadataResponse) -> Result<()> {
        // ~ build up an index of the already loaded brokers -- if any
        let mut brokers: HashMap<i32, BrokerIndex> =
            HashMap::with_capacity(md.brokers.len() + self.state.brokers.len());
        for (i, broker) in (0u32..).zip(self.state.brokers.iter()) {
            brokers.insert(broker.node_id, BrokerIndex(i));
        }
        // ~ now update add new brokers or updated existing ones
        for broker in md.brokers {
            let broker_host = format!("{}:{}", broker.host, broker.port);
            match brokers.entry(broker.nodeid) {
                Entry::Occupied(e) => {
                    // ~ verify our information of the already tracked
                    // broker is up-to-date
                    let broker_index = *e.get();
                    let b = &mut self.state.brokers[broker_index.0 as usize];
                    if b.host != broker_host {
                        b.host = broker_host;
                    }
                }
                Entry::Vacant(e) => {
                    // ~ insert the new broker
                    self.state.brokers.push(Broker {
                        node_id: broker.nodeid,
                        host: broker_host,
                    });
                    // ~ track the new broker's index
                    e.insert(BrokerIndex((self.state.brokers.len() - 1) as u32));
                }
            }
        }

        for t in md.topics {
            let mut known_partitions = Vec::with_capacity(t.partitions.len());
            for partition in t.partitions {
                match brokers.get(&partition.leader) {
                    Some(broker_index) => {
                        known_partitions.push(TopicPartition::new(partition.id, *broker_index));
                    },
                    None => {
                        debug!("unknown leader {} for topic-partition: {}:{}",
                               partition.leader, t.topic, partition.id);
                    }
                }
            }
            known_partitions.sort_by(|a, b| a.partition_id.cmp(&b.partition_id));
            self.state.topic_partitions.insert(
                t.topic, TopicPartitions::new(known_partitions));
        }
        Ok(())
    }

    /// Clears metadata stored in the client.  You must load metadata
    /// after this call if you want to use the client.
    #[inline]
    pub fn reset_metadata(&mut self) {
        self.state.topic_partitions.clear();
        self.state.brokers.clear();
    }

    /// Fetches metadata about the specified topics from all of the
    /// underlying brokers (`self.hosts`).
    fn fetch_metadata<T: AsRef<str>>(&mut self, topics: &[T]) -> Result<protocol::MetadataResponse> {
        let correlation = self.state.next_correlation_id();
        for host in &self.config.hosts {
            if let Ok(conn) = self.conn_pool.get_conn(host) {
                let req = protocol::MetadataRequest::new(correlation, &self.config.client_id, topics);
                if __send_request(conn, req).is_ok() {
                    return __get_response::<protocol::MetadataResponse>(conn);
                }
            }
        }
        Err(Error::NoHostReachable)
    }

    /// Fetch offsets for a list of topics.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// let mut client = kafka::client::KafkaClient::new(vec!("localhost:9092".to_owned()));
    /// client.load_metadata_all().unwrap();
    /// let topics: Vec<String> = client.topics().names().map(ToOwned::to_owned).collect();
    /// let offsets = client.fetch_offsets(&topics, kafka::client::FetchOffset::Latest).unwrap();
    /// ```
    /// Returns a hashmap of (topic, PartitionOffset data).
    /// PartitionOffset will contain parition and offset info Or Error code as returned by Kafka.
    pub fn fetch_offsets<T: AsRef<str>>(&mut self, topics: &[T], offset: FetchOffset)
                                        -> Result<HashMap<String, Vec<utils::PartitionOffset>>>
    {
        let time = offset.to_kafka_value();
        let n_topics = topics.len();

        let state = &mut self.state;
        let correlation = state.next_correlation_id();

        // Map topic and partition to the corresponding broker
        let config = &self.config;
        let mut reqs: HashMap<&str, protocol::OffsetRequest> = HashMap::with_capacity(n_topics);
        for topic in topics {
            let topic = topic.as_ref();
            if let Some(tp) = state.topic_partitions.get(topic) {
                for p in &tp.partitions {
                    let host: &str = &state.broker_by_partition(p).host;
                    let entry = reqs.entry(host)
                        .or_insert_with(|| protocol::OffsetRequest::new(correlation, &config.client_id));
                    entry.add(topic, p.partition_id, time);
                }
            }
        }

        // Call each broker with the request formed earlier
        let mut res: HashMap<String, Vec<utils::PartitionOffset>> = HashMap::with_capacity(n_topics);
        for (host, req) in reqs {
            let resp = try!(__send_receive::<protocol::OffsetRequest, protocol::OffsetResponse>(&mut self.conn_pool, &host, req));
            for tp in resp.topic_partitions {
                let e = res.entry(tp.topic).or_insert(vec!());
                for p in tp.partitions {
                    e.push(p.into_offset());
                }
            }
        }
        Ok(res)
    }

    /// Fetch offset for a single topic.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// let mut client = kafka::client::KafkaClient::new(vec!("localhost:9092".to_owned()));
    /// let res = client.load_metadata_all();
    /// let offsets = client.fetch_topic_offset("my-topic", kafka::client::FetchOffset::Latest);
    /// ```
    /// Returns a vector of offset data for each available partition.
    /// PartitionOffset will contain parition and offset info Or Error code as returned by Kafka.
    // XXX rename to fetch_topic_offsets (plural)
    pub fn fetch_topic_offset<T: AsRef<str>>(&mut self, topic: T, offset: FetchOffset)
                                              -> Result<Vec<utils::PartitionOffset>>
    {
        let topic = topic.as_ref();

        let mut m = try!(self.fetch_offsets(&[topic], offset));
        let offs = m.remove(topic).unwrap_or(vec!());
        if offs.is_empty() {
            Err(Error::Kafka(KafkaCode::UnknownTopicOrPartition))
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
    /// for very efficient consumption possibilities.  In particular,
    /// each of the returned fetch responses directly corresponds to
    /// fetch requests to the underlying kafka brokers.  Except of
    /// transparently uncompressing compressed messages, the result is
    /// not otherwise prepared.
    ///
    /// All of the data available through the returned fetch responses
    /// is bound to their lifetime as that data is merely a "view"
    /// into parts of the response structs.  If you need to keep
    /// individual messages for a longer time than the whole fetch
    /// responses, you'll need to make a copy of the message data.
    ///
    /// Note: before using this method consider using
    /// `kafka::consumer::Consumer` instead which provides a much
    /// easier API for the use-case of fetching messesage from Kafka.
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
    pub fn fetch_messages<'a, I, J>(&mut self, input: I) -> Result<Vec<fetch::FetchResponse>>
        where J: AsRef<FetchPartition<'a>>, I: IntoIterator<Item=J>
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
                            correlation, &config.client_id,
                            config.fetch_max_wait_time, config.fetch_min_bytes)
                    })
                    .add(inp.topic, inp.partition, inp.offset,
                         if inp.max_bytes > 0 {
                             inp.max_bytes
                         } else {
                             config.fetch_max_bytes_per_partition
                         });
            }
        }

        __fetch_messages(&mut self.conn_pool, reqs)
    }

    /// Fetch messages from a single kafka partition.
    ///
    /// See `KafkaClient::fetch_messages`.
    pub fn fetch_messages_for_partition<'a>(&mut self, req: &FetchPartition<'a>)
                                            -> Result<Vec<fetch::FetchResponse>>
    {
        // XXX since we deal with exactly one partition we can generate
        // the fetch request to the corresponding kafka broker more
        // efficiently
        self.fetch_messages(&[req])
    }

    /// Send a message to Kafka
    ///
    /// `required_acks` - indicates how many acknowledgements the servers should receive before
    /// responding to the request. If it is 0 the server will not send any response
    /// (this is the only case where the server will not reply to a request).
    /// If it is 1, the server will wait the data is written to the local log before sending
    /// a response. If it is -1 the server will block until the message is committed by all
    /// in sync replicas before sending a response. For any number > 1 the server will block
    /// waiting for this number of acknowledgements to occur (but the server will never wait
    /// for more acknowledgements than there are in-sync replicas).
    ///
    /// `ack_timeout` - This provides a maximum time in milliseconds the server can await the
    /// receipt of the number of acknowledgements in `required_acks`
    ///
    /// `input` - A vector of `utils::ProduceMessage`
    ///
    /// # Example
    ///
    /// ```no_run
    /// use kafka::utils;
    /// let mut client = kafka::client::KafkaClient::new(vec!("localhost:9092".to_owned()));
    /// let res = client.load_metadata_all();
    /// let req = vec!(utils::ProduceMessage{topic: "my-topic", message: "a".as_bytes()},
    ///                 utils::ProduceMessage{topic: "my-topic-2", message: "b".as_bytes()});
    /// println!("{:?}", client.send_messages(1, 100, req));
    /// ```
    /// The return value will contain a vector of topic, partition, offset and error if any
    /// OR error:Error
    pub fn send_messages<'a, 'b, I, J>(&mut self, required_acks: i16, ack_timeout: i32, messages: I)
                                       -> Result<Vec<utils::TopicPartitionOffsetError>>
        where J: AsRef<utils::ProduceMessage<'a, 'b>>, I: IntoIterator<Item=J>
    {
        let state = &mut self.state;
        let correlation = state.next_correlation_id();

        // ~ map topic and partition to the corresponding brokers
        let config = &self.config;
        let mut reqs: HashMap<&str, protocol::ProduceRequest> = HashMap::new();
        for msg in messages {
            let msg = msg.as_ref();
            if let Some((partition, broker)) = state.choose_partition(msg.topic) {
                reqs.entry(broker)
                    .or_insert_with(
                        || protocol::ProduceRequest::new(required_acks, ack_timeout, correlation,
                                                         &config.client_id, config.compression))
                    .add(msg.topic, partition, msg.message);
            }
        }
        __send_messages(&mut self.conn_pool, reqs, required_acks == 0)
    }

    /// Send a message to Kafka
    ///
    /// `required_acks` - indicates how many acknowledgements the servers should receive before
    /// responding to the request. If it is 0 the server will not send any response
    /// (this is the only case where the server will not reply to a request).
    /// If it is 1, the server will wait the data is written to the local log before sending
    /// a response. If it is -1 the server will block until the message is committed by all
    /// in sync replicas before sending a response. For any number > 1 the server will block
    /// waiting for this number of acknowledgements to occur (but the server will never wait
    /// for more acknowledgements than there are in-sync replicas).
    ///
    /// `ack_timeout` - This provides a maximum time in milliseconds the server can await the
    /// receipt of the number of acknowledgements in `required_acks`
    ///
    /// `message` - A single message as a vector of u8s
    ///
    /// # Example
    ///
    /// ```no_run
    /// let mut client = kafka::client::KafkaClient::new(vec!("localhost:9092".to_owned()));
    /// let res = client.load_metadata_all();
    /// let msgs = client.send_message(1, 100, "my-topic", "msg".as_bytes());
    /// ```
    /// The return value will contain topic, partition, offset and error if any
    /// OR error:Error
    pub fn send_message(&mut self, required_acks: i16, ack_timeout: i32, topic: &str, message: &[u8])
                        -> Result<Vec<utils::TopicPartitionOffsetError>> {
        self.send_messages(required_acks, ack_timeout,
                           &[utils::ProduceMessage { topic: topic, message: message }])
    }

    /// Commit offset to topic, partition of a consumer group
    ///
    /// It takes a group name and list of `utils::TopicPartitionOffset` and returns `()`
    /// or `error::Error`
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use kafka::utils;
    /// let mut client = kafka::client::KafkaClient::new(vec!("localhost:9092".to_owned()));
    /// let res = client.load_metadata_all();
    /// let resp = client.commit_offsets("my-group", vec!(
    ///                 utils::TopicPartitionOffset{topic: "my-topic", partition: 0, offset: 100},
    ///                 utils::TopicPartitionOffset{topic: "my-topic", partition: 1, offset: 100}));
    /// ```
    pub fn commit_offsets<'a, J, I>(&mut self, group: &str, offsets: I) -> Result<()>
        where J: AsRef<utils::TopicPartitionOffset<'a>>, I: IntoIterator<Item=J>
    {
        let state = &mut self.state;
        let correlation = state.next_correlation_id();

        // Map topic and partition to the corresponding broker
        let config = &self.config;
        let mut reqs: HashMap<&str, protocol::OffsetCommitRequest> = HashMap:: new();
        for tp in offsets {
            let tp = tp.as_ref();
            if let Some(broker) = state.find_broker(&tp.topic, tp.partition) {
                reqs.entry(broker)
                    .or_insert(protocol::OffsetCommitRequest::new(group, correlation, &config.client_id))
                    .add(tp.topic, tp.partition, tp.offset, "");
            }
        }
        __commit_offsets(&mut self.conn_pool, reqs)
    }

    /// Commit offset to topic, partition of a consumer group
    ///
    /// It takes a group name, topic, partition and offset and returns `()`
    /// or `error::Error`
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use kafka::utils;
    /// let mut client = kafka::client::KafkaClient::new(vec!("localhost:9092".to_owned()));
    /// let res = client.load_metadata_all();
    /// let resp = client.commit_offset("my-group", "my-topic", 0, 100);
    /// ```
    pub fn commit_offset(&mut self, group: &str, topic: &str, partition: i32, offset: i64) -> Result<()> {
        self.commit_offsets(group, &[utils::TopicPartitionOffset{
            topic: topic,
            partition: partition,
            offset: offset
        }])
    }

    /// Fetch offset for vector of topic, partition of a consumer group
    ///
    /// It takes a group name and list of `utils::TopicPartition` and returns `utils::TopicPartitionOffsetError`
    /// or `error::Error`
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use kafka::utils;
    /// let mut client = kafka::client::KafkaClient::new(vec!("localhost:9092".to_owned()));
    /// let res = client.load_metadata_all();
    /// let resp = client.fetch_group_offsets_multi("my-group", vec!(
    ///                 utils::TopicPartition{topic: "my-topic", partition: 0},
    ///                 utils::TopicPartition{topic: "my-topic", partition: 1}));
    /// ```
    pub fn fetch_group_offsets_multi<'a, J, I>(&mut self, group: &str, partitions: I)
                                               -> Result<Vec<utils::TopicPartitionOffsetError>>
        where J: AsRef<utils::TopicPartition<'a>>, I: IntoIterator<Item=J>
    {
        let correlation = self.state.next_correlation_id();

        // Map topic and partition to the corresponding broker
        let mut reqs: HashMap<&str, protocol::OffsetFetchRequest> = HashMap:: new();
        for tp in partitions {
            let tp = tp.as_ref();
            if let Some(broker) = self.state.find_broker(tp.topic, tp.partition) {
                reqs.entry(broker)
                    .or_insert(protocol::OffsetFetchRequest::new(group, correlation, &self.config.client_id))
                    .add(tp.topic, tp.partition);
            }
        }
        __fetch_group_offsets_multi(&mut self.conn_pool, reqs)
    }

    /// Fetch offset for all partitions of a topic of a consumer group
    ///
    /// It takes a group name and a topic and returns `utils::TopicPartitionOffsetError`
    /// or `error::Error`
    ///
    /// # Examples

    ///
    /// ```no_run
    /// use kafka::utils;
    /// let mut client = kafka::client::KafkaClient::new(vec!("localhost:9092".to_owned()));
    /// let res = client.load_metadata_all();
    /// let resp = client.fetch_group_offsets("my-group", "my-topic");
    /// ```
    pub fn fetch_group_offsets(&mut self, group: &str, topic: &str)
                               -> Result<Vec<utils::TopicPartitionOffsetError>>
    {
        let tps: Vec<_> =
            match self.state.topic_partitions.get(topic) {
                None => return Err(Error::Kafka(KafkaCode::UnknownTopicOrPartition)),
                Some(tp) => tp.partitions
                    .iter()
                    .map(|p| utils::TopicPartition{ topic: topic, partition: p.partition_id })
                    .collect(),
            };
        self.fetch_group_offsets_multi(group, tps)
    }
}

fn __commit_offsets(conn_pool: &mut ConnectionPool,
                    reqs: HashMap<&str, protocol::OffsetCommitRequest>)
                    -> Result<()> {
    // Call each broker with the request formed earlier
    for (host, req) in reqs {
        try!(__send_receive::<protocol::OffsetCommitRequest, protocol::OffsetCommitResponse>(conn_pool, host, req));
    }
    Ok(())
}

fn __fetch_group_offsets_multi(conn_pool: &mut ConnectionPool,
                               reqs: HashMap<&str, protocol::OffsetFetchRequest>)
                               -> Result<Vec<utils::TopicPartitionOffsetError>> {
    // Call each broker with the request formed earlier
    let mut res = vec!();
    for (host, req) in reqs {
        let resp = try!(__send_receive::<protocol::OffsetFetchRequest, protocol::OffsetFetchResponse>(conn_pool, host, req));
        let o = resp.get_offsets();
        for tpo in o {
            res.push(tpo);
        }
    }
    Ok(res)
}

/// ~ carries out the given fetch requests and returns the response
fn __fetch_messages(conn_pool: &mut ConnectionPool, reqs: HashMap<&str, protocol::FetchRequest>)
                    -> Result<Vec<fetch::FetchResponse>>
{
    // Call each broker with the request formed earlier
    let mut res = Vec::with_capacity(reqs.len());
    for (host, req) in reqs {
        let resp = try!(__z_send_receive::<protocol::FetchRequest, fetch::FetchResponse>(conn_pool, host, req));
        res.push(resp);
    }
    Ok(res)
}

/// ~ carries out the given produce requests and returns the reponse
fn __send_messages(conn_pool: &mut ConnectionPool,
                   reqs: HashMap<&str, protocol::ProduceRequest>,
                   no_acks: bool)
                   -> Result<Vec<utils::TopicPartitionOffsetError>>
{
    // Call each broker with the request formed earlier
    if no_acks {
        for (host, req) in reqs {
            try!(__send_noack::<protocol::ProduceRequest, protocol::ProduceResponse>(conn_pool, host, req));
        }
        Ok(vec!())
    } else {
        let mut res: Vec<utils::TopicPartitionOffsetError> = vec!();
        for (host, req) in reqs {
            let resp = try!(__send_receive::<protocol::ProduceRequest, protocol::ProduceResponse>(conn_pool, &host, req));
            for tpo in resp.get_response() {
                res.push(tpo);
            }
        }
        Ok(res)
    }
}

fn __send_receive<T: ToByte, V: FromByte>(conn_pool: &mut ConnectionPool, host: &str, req: T)
                                        -> Result<V::R>
{
    let mut conn = try!(conn_pool.get_conn(host));
    try!(__send_request(&mut conn, req));
    __get_response::<V>(&mut conn)
}

fn __send_noack<T: ToByte, V: FromByte>(conn_pool: &mut ConnectionPool, host: &str, req: T)
                                        -> Result<usize>
{
    let mut conn = try!(conn_pool.get_conn(&host));
    __send_request(&mut conn, req)
}

fn __send_request<T: ToByte>(conn: &mut KafkaConnection, request: T)
                           -> Result<usize>
{
    let mut buffer = vec!();
    try!(request.encode(&mut buffer));

    let mut s = vec!();
    try!((buffer.len() as i32).encode(&mut s));
    for byte in &buffer { s.push(*byte); }

    conn.send(&s)
}

fn __get_response<T: FromByte>(conn: &mut KafkaConnection)
                             -> Result<T::R>
{
    let v = try!(conn.read_exact(4));
    let size = try!(i32::decode_new(&mut Cursor::new(v)));

    let resp = try!(conn.read_exact(size as u64));

    // {
    //     use std::fs::OpenOptions;
    //     use std::io::Write;
    //     let mut f = OpenOptions::new()
    //         .write(true)
    //         .append(true)
    //         .truncate(true)
    //         .create(true)
    //         .open("/tmp/dump.dat")
    //         .unwrap();
    //     f.write_all(&resp[..]).unwrap();
    // }

    T::decode_new(&mut Cursor::new(resp))
}

fn __z_send_receive<T: ToByte, V: FromResponse>(conn_pool: &mut ConnectionPool, host: &str, req: T)
                                                -> Result<V> {
    let mut conn = try!(conn_pool.get_conn(host));
    try!(__send_request(&mut conn, req));
    __z_get_response::<V>(&mut conn)
}

fn __z_get_response<T: FromResponse>(conn: &mut KafkaConnection) -> Result<T> {
    let v = try!(conn.read_exact(4));
    let size = try!(i32::decode_new(&mut Cursor::new(v)));

    let resp = try!(conn.read_exact(size as u64));
    T::from_response(resp)
}
