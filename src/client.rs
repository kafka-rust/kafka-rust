//! The primary module of this library providing a mid-level
//! abstraction for a kafka server while supposed to allow building
//! higher level constructs.

use std::collections::hash_map::{self, HashMap};
use std::io::Cursor;
use std::io::Read;
use std::iter::Iterator;
use std::mem;
use std::rc::Rc;
use std::slice;

// pub re-export
pub use compression::Compression;

use codecs::{ToByte, FromByte};
use connection::KafkaConnection;
use error::{Result, Error, KafkaCode};
use protocol::{self, FromResponse};
// pub re-export
pub use protocol::zfetch;
use utils;


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

    // ~ a mapping of topic to information about its partitions
    // ~ `TopicPartitions#partitions` are kept in ascending order by `partition_id`
    topic_partitions: HashMap<String, TopicPartitions>,
}

impl ClientState {
    fn next_correlation_id(&mut self) -> i32 {
        self.correlation = (self.correlation + 1) % (1i32 << 30);
        self.correlation
    }

    /// Find the leader of the specified partition; the returned
    /// string is the hostname:port of the leader - if any.
    fn find_broker(&self, topic: &str, partition: i32) -> Option<Rc<String>> {
        self.topic_partitions.get(topic)
            .and_then(|tp| {
                // ~ XXX might also just normally iterate and try to
                // find the element.  the number of partitions is
                // typically very constrainted.
                tp.partitions.binary_search_by(|e| {
                    e.partition_id.cmp(&partition)
                }).ok().and_then(|i| {
                    Some(tp.partitions[i].broker_host.clone())
                })
            })
    }

    /// Chooses for the given topic a partition to write the next message to.
    /// Returns the partition_id and the corresponding broker host.
    fn choose_partition(&mut self, topic: &str) -> Option<(i32, Rc<String>)> {
        match self.topic_partitions.get_mut(topic) {
            None => None,
            Some(ref topic) if topic.partitions.is_empty() => None,
            Some(ref mut topic) => {
                topic.curr_partition_idx = (topic.curr_partition_idx + 1) % topic.partitions.len() as i32;
                let p = &topic.partitions[topic.curr_partition_idx as usize];
                Some((p.partition_id, p.broker_host.clone()))
            }
        }
    }
}

#[derive(Debug)]
struct TopicPartition {
    partition_id: i32,
    broker_host: Rc<String>,
}

impl TopicPartition {
    fn new(partition_id: i32, broker_host: Rc<String>) -> TopicPartition {
        TopicPartition { partition_id: partition_id, broker_host: broker_host }
    }
}

#[derive(Debug)]
struct TopicPartitions {
    curr_partition_idx: i32,
    partitions: Vec<TopicPartition>,
}

impl TopicPartitions {
    fn new(partitions: Vec<TopicPartition>) -> TopicPartitions {
        TopicPartitions { curr_partition_idx: 0, partitions: partitions }
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

// view objects on the currently loaded topics metadata ---------------

/// An immutable iterator over a kafka client's known topics.
pub struct Topics<'a> {
    iter: hash_map::Iter<'a, String, TopicPartitions>,
}

impl<'a> Iterator for Topics<'a> {
    type Item=Topic<'a>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|(name, tps) | Topic {
            name: &name[..],
            partitions: &tps.partitions,
        })
    }
}

impl<'a> Topics<'a> {

    /// A conveniece method to turn this topics iterator into an
    /// iterator over the topics' names.
    #[allow(dead_code)]
    #[inline]
    pub fn names(self) -> TopicNames<'a> {
        TopicNames { iter: self.iter }
    }
}

/// An iterator over the names of topics known to the originating
/// kafka client.
pub struct TopicNames<'a> {
    iter: hash_map::Iter<'a, String, TopicPartitions>,
}

impl<'a> Iterator for TopicNames<'a> {
    type Item=&'a str;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|(name, _)| &name[..])
    }
}

#[test]
fn test_topics_names_iter() {
    let mut m = HashMap::new();
    m.insert("foo".to_owned(), TopicPartitions::new(vec![]));
    m.insert("bar".to_owned(), TopicPartitions::new(vec![]));

    let topics = Topics { iter: m.iter() };
    let mut names: Vec<String> = topics.names().map(ToOwned::to_owned).collect();
    names.sort();
    assert_eq!(vec!["bar".to_owned(), "foo".to_owned()], names);
}

/// An immutable view on a topic as known to the originating kafka
/// client.
pub struct Topic<'a> {
    name: &'a str,
    partitions: &'a [TopicPartition],
}

impl<'a> Topic<'a> {

    /// Retrieves the name of this topic.
    #[inline]
    pub fn name(&self) -> &str {
        self.name
    }

    /// Retrieves an iterator over the known partitions of this topic.
    #[inline]
    pub fn partitions(&self) -> Partitions<'a> {
        Partitions {
            iter: self.partitions.iter(),
        }
    }
}

/// An immutable iterator over the kafka client's known partitions for
/// a particular topic.
pub struct Partitions<'a> {
    iter: slice::Iter<'a, TopicPartition>,
}

/// An immutable view on a topic partitions as known to the
/// originating kafka client.
pub struct Partition<'a> {
    partition: &'a TopicPartition,
}

impl<'a> Partition<'a> {

    /// Retrieves the identifier of this topic partition.
    #[inline]
    pub fn id(&self) -> i32 {
        self.partition.partition_id
    }

    /// Retrieves the leader broker this partitions is currently
    /// served by.
    #[inline]
    pub fn leader(&self) -> &str {
        &self.partition.broker_host
    }
}

impl<'a> Iterator for Partitions<'a> {
    type Item=Partition<'a>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|p| Partition {
            partition: p,
        })
    }
}

// --------------------------------------------------------------------

impl KafkaClient {
    /// Create a new instance of KafkaClient. Before being able to
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
        let n_hosts = hosts.len();
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
                topic_partitions: HashMap::with_capacity(n_hosts),
            },
        }
    }

    /// Set the compression algorithm to use when sending out messages.
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

    /// Set the maximum time in milliseconds to wait for insufficient
    /// data to become available when fetching messages.
    ///
    /// See also `KafkaClient::set_fetch_min_bytes(..)` and
    /// `KafkaClient::set_fetch_max_bytes_per_partition(..)`.
    #[inline]
    pub fn set_fetch_max_wait_time(&mut self, max_wait_time: i32) {
        self.config.fetch_max_wait_time = max_wait_time;
    }

    /// Set the minimum number of bytes of available data to wait for
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
    /// let mut client = kafka::client::KafkaClient::new(vec!["localhost:9092".to_owned()]);
    /// client.load_metadata_all().unwrap();
    /// client.set_fetch_max_wait_time(100);
    /// client.set_fetch_min_bytes(64 * 1024);
    /// let r = client.fetch_messages("my-topic", 0, 0);
    /// ```
    ///
    /// See also `KafkaClient::set_fetch_max_wait_time(..)` and
    /// `KafkaClient::set_fetch_max_bytes_per_partition(..)`.
    #[inline]
    pub fn set_fetch_min_bytes(&mut self, min_bytes: i32) {
        self.config.fetch_min_bytes = min_bytes;
    }

    /// Set the maximum number of bytes to fetch from _a single kafka
    /// partition_ when fetching messages.
    ///
    /// This basically determines the maximum message size this client
    /// will be able to fetch.  If a topic partition contains a
    /// message larger than this specified number of bytes, the server
    /// will not deliver it.
    ///
    /// Note that this setting is related to a single partition. The
    /// overall maximum possible data size in a fetch messages
    /// response will thus be determined by the number of partitions
    /// in the fetch messages request.
    ///
    /// See also `KafkaClient::set_fetch_max_wait_time(..)` and
    /// `KafkaClient::set_fetch_min_bytes(..)`.
    #[inline]
    pub fn set_fetch_max_bytes_per_partition(&mut self, max_bytes: i32) {
        self.config.fetch_max_bytes_per_partition = max_bytes;
    }

    /// Determines whether this client current knows about the
    /// specified topic.
    #[inline]
    pub fn contains_topic(&self, topic: &str) -> bool {
        self.state.topic_partitions.contains_key(topic)
    }

    /// Retrieves an iterator over the partitions of the given topic
    /// or an error if the requested topic is unknown (yet.)
    #[inline]
    pub fn topic_partitions(&self, topic: &str) -> Result<Partitions> {
        match self.state.topic_partitions.get(topic) {
            None => Err(Error::Kafka(KafkaCode::UnknownTopicOrPartition)),
            Some(tp) => Ok(Partitions {
                iter: tp.partitions.iter(),
            }),
        }
    }

    /// Iterates the currently known topics.
    ///
    /// # Examples
    /// ```no_run
    /// let mut client = kafka::client::KafkaClient::new(vec!("localhost:9092".to_owned()));
    /// client.load_metadata_all().unwrap();
    /// for topic in client.topics() {
    ///   for partition in topic.partitions() {
    ///     println!("{}:{} => {}", topic.name(), partition.id(), partition.leader());
    ///   }
    /// }
    /// ```
    #[inline]
    pub fn topics(&self) -> Topics {
        Topics { iter: self.state.topic_partitions.iter() }
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
        let mut brokers: HashMap<i32, Rc<String>> = HashMap::new();
        for broker in md.brokers {
            brokers.insert(broker.nodeid, Rc::new(format!("{}:{}", broker.host, broker.port)));
        }
        for topic in md.topics {
            let mut known_partitions_internal = Vec::with_capacity(topic.partitions.len());
            for partition in topic.partitions {
                match brokers.get(&partition.leader) {
                    Some(broker) => {
                        known_partitions_internal.push(TopicPartition::new(partition.id, broker.clone()));
                    },
                    None => {}
                }
            }
            known_partitions_internal.sort_by(|a, b| a.partition_id.cmp(&b.partition_id));
            self.state.topic_partitions.insert(
                topic.topic.clone(), TopicPartitions::new(known_partitions_internal));
        }
        Ok(())
    }

    /// Clears metadata stored in the client.  You must load metadata
    /// after this call if you want to use the client.
    #[inline]
    pub fn reset_metadata(&mut self) {
        self.state.topic_partitions.clear();
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
        let mut reqs: HashMap<Rc<String>, protocol::OffsetRequest> = HashMap::with_capacity(n_topics);
        for topic in topics {
            let topic = topic.as_ref();
            if let Some(tp) = state.topic_partitions.get(topic) {
                for p in &tp.partitions {
                    let entry = reqs.entry(p.broker_host.clone())
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

    /// Fetch messages from Kafka (multiple topic, partition, offset)
    ///
    /// It takes a vector of `utils:TopicPartitionOffset` and returns a vector of `utils::TopicMessage`
    /// or error::Error
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use kafka::client::KafkaClient;
    /// use kafka::utils::TopicPartitionOffset;
    ///
    /// let mut client = KafkaClient::new(vec!("localhost:9092".to_owned()));
    /// client.load_metadata_all().unwrap();
    /// let reqs = &[TopicPartitionOffset{ topic: "my-topic", partition: 0, offset: 0 },
    ///              TopicPartitionOffset{ topic: "my-topic-2", partition: 0, offset: 0 }];
    /// let msgs = client.fetch_messages_multi(reqs).unwrap();
    /// ```
    pub fn fetch_messages_multi<'a, I, J>(&mut self, input: I) -> Result<Vec<utils::TopicMessage>>
        where J: AsRef<utils::TopicPartitionOffset<'a>>, I: IntoIterator<Item=J>
    {
        let reqs = __prepare_fetch_messages_requests(&mut self.state, &self.config, input);
        __fetch_messages_multi(&mut self.conn_pool, reqs)
    }

    /// Fetch messages from Kafka (single topic, partition, offset)
    ///
    /// It takes a single topic, parition and offset and return a vector of messages (`utils::TopicMessage`)
    /// or error::Error
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use kafka::client::KafkaClient;
    ///
    /// let mut client = KafkaClient::new(vec!("localhost:9092".to_owned()));
    /// client.load_metadata_all().unwrap();
    /// let msgs = client.fetch_messages("my-topic", 0, 0);
    /// ```
    pub fn fetch_messages<T: AsRef<str>>(&mut self, topic: T, partition: i32, offset: i64)
                                         -> Result<Vec<utils::TopicMessage>>
    {
        self.fetch_messages_multi(&[utils::TopicPartitionOffset::new(topic.as_ref(), partition, offset)])
    }

    /// Fetch messages from Kafka (multiple topic, partition, offset)
    /// exposing low level details.
    ///
    /// Just as `fetch_messages_multi` it takes a vector specifying
    /// the partitions and their offsets as of which to fetch
    /// messages.  However, unlike `fetch_messages_multi` this method
    /// exposes the result in a raw, more complicated manner but
    /// allows for more efficient consumption possibilities. In
    /// particular, each of the returned fetch responses directly
    /// corresponds to a fetch request to the corresponding,
    /// underlying kafka broker.  Except of transparently
    /// uncompressing compressed messages, the result is not otherwise
    /// prepared.
    ///
    /// All of the data available through the returned fetch responses
    /// is bound to their lifetime as that data is merely a "view"
    /// into parts of the response structs.  If you need to keep
    /// individual messages for a longer time then the fetch whole
    /// responses, you'll need to make a copy of the messages.
    ///
    /// # Example
    ///
    /// This example demonstrates iterating all fetched messages from
    /// two topic partitions.
    ///
    /// ```no_run
    /// use kafka::client::KafkaClient;
    /// use kafka::utils::TopicPartitionOffset;
    ///
    /// let mut client = KafkaClient::new(vec!("localhost:9092".to_owned()));
    /// client.load_metadata_all().unwrap();
    /// let reqs = &[TopicPartitionOffset{ topic: "my-topic", partition: 0, offset: 0 },
    ///              TopicPartitionOffset{ topic: "my-topic-2", partition: 0, offset: 0 }];
    /// let resps = client.zfetch_messages_multi(reqs).unwrap();
    /// for resp in resps {
    ///   for t in resp.topics() {
    ///     for p in t.partitions() {
    ///       match p.messages() {
    ///         Err(e) => println!("partition error: {}:{}: {}", t.topic, p.partition, e),
    ///         Ok(messages) => {
    ///           for msg in messages {
    ///             println!("topic: {} / partition: {} / messages.len: {}",
    ///                      t.topic, p.partition, msg.value.len());
    ///           }
    ///         }
    ///       }
    ///     }
    ///   }
    /// }
    pub fn zfetch_messages_multi<'a, 'b, I, J>(&mut self, input: I)
                                               -> Result<Vec<zfetch::FetchResponse<'b>>>
        where J: AsRef<utils::TopicPartitionOffset<'a>>, I: IntoIterator<Item=J> {

        let reqs = __prepare_fetch_messages_requests(&mut self.state, &self.config, input);
        __zfetch_messages_multi(&mut self.conn_pool, reqs)
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
        let mut reqs: HashMap<Rc<String>, protocol::ProduceRequest> = HashMap::new();
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
        let mut reqs: HashMap<Rc<String>, protocol::OffsetCommitRequest> = HashMap:: new();
        for tp in offsets {
            let tp = tp.as_ref();
            if let Some(broker) = state.find_broker(&tp.topic, tp.partition) {
                reqs.entry(broker.clone()).or_insert(
                            protocol::OffsetCommitRequest::new(group, correlation, &config.client_id))
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
        let mut reqs: HashMap<Rc<String>, protocol::OffsetFetchRequest> = HashMap:: new();
        for tp in partitions {
            let tp = tp.as_ref();
            if let Some(broker) = self.state.find_broker(tp.topic, tp.partition) {
                let entry = reqs.entry(broker.clone()).or_insert(
                    protocol::OffsetFetchRequest::new(group, correlation, &self.config.client_id));
                entry.add(tp.topic, tp.partition);
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
                               -> Result<Vec<utils::TopicPartitionOffsetError>> {
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
                    reqs: HashMap<Rc<String>, protocol::OffsetCommitRequest>)
                    -> Result<()> {
    // Call each broker with the request formed earlier
    for (host, req) in reqs {
        try!(__send_receive::<protocol::OffsetCommitRequest, protocol::OffsetCommitResponse>(conn_pool, &host, req));
    }
    Ok(())
}

fn __fetch_group_offsets_multi(conn_pool: &mut ConnectionPool,
                               reqs: HashMap<Rc<String>, protocol::OffsetFetchRequest>)
                               -> Result<Vec<utils::TopicPartitionOffsetError>> {
    // Call each broker with the request formed earlier
    let mut res = vec!();
    for (host, req) in reqs {
        let resp = try!(__send_receive::<protocol::OffsetFetchRequest, protocol::OffsetFetchResponse>(conn_pool, &host, req));
        let o = resp.get_offsets();
        for tpo in o {
            res.push(tpo);
        }
    }
    Ok(res)
}

fn __prepare_fetch_messages_requests<'a, 'b, I, J>(
    state: &mut ClientState, config: &'b ClientConfig, input: I)
    -> HashMap<Rc<String>, protocol::FetchRequest<'b, 'a>>
    where J: AsRef<utils::TopicPartitionOffset<'a>>, I: IntoIterator<Item=J> {

        let correlation = state.next_correlation_id();

        // Map topic and partition to the corresponding broker
        let mut reqs: HashMap<Rc<String>, protocol::FetchRequest> = HashMap::new();
        for tpo in input {
            let tpo = tpo.as_ref();
            if let Some(broker) = state.find_broker(tpo.topic, tpo.partition) {
                reqs.entry(broker.clone())
                    .or_insert_with(|| {
                        protocol::FetchRequest::new(correlation, &config.client_id,
                                                    config.fetch_max_wait_time, config.fetch_min_bytes)
                    })
                    .add(tpo.topic, tpo.partition, tpo.offset, config.fetch_max_bytes_per_partition);
            }
        }
        reqs
}

/// ~ carries out the given fetch requests and returns the response
fn __fetch_messages_multi(conn_pool: &mut ConnectionPool,
                          reqs: HashMap<Rc<String>, protocol::FetchRequest>)
                          -> Result<Vec<utils::TopicMessage>>
{
    // Call each broker with the request formed earlier
    let mut res: Vec<utils::TopicMessage> = vec!();
    for (host, req) in reqs {
        let resp = try!(__send_receive::<protocol::FetchRequest, protocol::FetchResponse>(conn_pool, &host, req));
        res.extend(resp.into_messages());
    }
    Ok(res)
}

/// ~ carries out the given fetch requests and returns the response
fn __zfetch_messages_multi<'a>(conn_pool: &mut ConnectionPool,
                               reqs: HashMap<Rc<String>, protocol::FetchRequest>)
                               -> Result<Vec<zfetch::FetchResponse<'a>>>
{
    // Call each broker with the request formed earlier
    let mut res = Vec::with_capacity(reqs.len());
    for (host, req) in reqs {
        let resp = try!(__z_send_receive::<protocol::FetchRequest, zfetch::FetchResponse>(conn_pool, &host, req));
        res.push(resp);
    }
    Ok(res)
}

/// ~ carries out the given produce requests and returns the reponse
fn __send_messages(conn_pool: &mut ConnectionPool,
                   reqs: HashMap<Rc<String>, protocol::ProduceRequest>,
                   no_acks: bool)
                   -> Result<Vec<utils::TopicPartitionOffsetError>>
{
    // Call each broker with the request formed earlier
    if no_acks {
        for (host, req) in reqs {
            try!(__send_noack::<protocol::ProduceRequest, protocol::ProduceResponse>(conn_pool, &host, req));
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
