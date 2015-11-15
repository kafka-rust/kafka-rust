//! Kafka Client
//!
//! Primary module of this library.
//!
//! Provides implementation for `KafkaClient` which is used to interact with Kafka

// pub re-export
pub use compression::Compression;

use error::{Result, Error};
use utils;
use protocol;
use connection::KafkaConnection;
use codecs::{ToByte, FromByte};
use std::collections::HashMap;
use std::io::Cursor;
use std::io::Read;
use std::mem;
use std::rc::Rc;


const CLIENTID: &'static str = "kafka-rust";
const DEFAULT_TIMEOUT_SECS: i32 = 120; // seconds


/// Client struct.
///
/// It keeps track of brokers and topic metadata.
///
/// Implements methods described by the [Kafka Protocol](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol).
///
/// # Examples
///
/// ```no_run
/// let mut client = kafka::client::KafkaClient::new(vec!("localhost:9092".to_owned()));
/// let res = client.load_metadata_all();
/// ```
///
/// You will have to load metadata before making any other request.
#[derive(Default, Debug)]
pub struct KafkaClient {
    // ~ this kafka client configuration
    config: ClientConfig,

    // ~ a pool of re-usable connections to kafka brokers
    conn_pool: ConnectionPool,

    // ~ the current state of this client
    state: ClientState,


    /// HashMap where `topic` is the key and list of `partitions` is the value
    pub topic_partitions: HashMap<String, Vec<i32>>,
}

#[derive(Default, Debug)]
struct ClientConfig {
    client_id: Rc<String>,
    hosts: Vec<String>,
    // ~ compression to use when sending messages
    compression: Compression,
}

#[derive(Default, Debug)]
struct ClientState {
    // ~ the last correlation used when communicating with kafka
    // (see `#next_id`)
    correlation: i32,

    // ~ a mapping of topic to information about its partitions
    // ~ `TopicPartitions#partitions` are kept in ascending order by `partition_id`
    topic_partitions_internal: HashMap<String, TopicPartitions>,
}

impl ClientState {
    fn next_correlation_id(&mut self) -> i32 {
        self.correlation = (self.correlation + 1) % (1i32 << 30);
        self.correlation
    }

    /// Find the leader of the specified partition; the returned
    /// string is the hostname:port of the leader - if any.
    fn find_broker(&self, topic: &str, partition: i32) -> Option<Rc<String>> {
        self.topic_partitions_internal.get(topic)
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
        match self.topic_partitions_internal.get_mut(topic) {
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
struct Partition {
    partition_id: i32,
    broker_host: Rc<String>,
}

impl Partition {
    fn new(partition_id: i32, broker_host: Rc<String>) -> Partition {
        Partition { partition_id: partition_id, broker_host: broker_host }
    }
}

#[derive(Debug)]
struct TopicPartitions {
    curr_partition_idx: i32,
    partitions: Vec<Partition>,
}

impl TopicPartitions {
    fn new(partitions: Vec<Partition>) -> TopicPartitions {
        TopicPartitions { curr_partition_idx: 0, partitions: partitions }
    }
}

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

#[derive(Debug, Default)]
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

impl KafkaClient {
    /// Create a new instance of KafkaClient
    ///
    /// # Examples
    ///
    /// ```no_run
    /// let mut client = kafka::client::KafkaClient::new(vec!("localhost:9092".to_owned()));
    /// ```
    pub fn new(hosts: Vec<String>) -> KafkaClient {
        KafkaClient {
            config: ClientConfig {
                client_id: Rc::new(CLIENTID.to_owned()),
                hosts: hosts,
                .. ClientConfig::default()
            },
            conn_pool: ConnectionPool::new(DEFAULT_TIMEOUT_SECS),
            state: ClientState::default(),
            .. KafkaClient::default()
        }
    }

    fn next_id(&mut self) -> i32 {
        self.state.next_correlation_id()
    }

    /// Resets and loads metadata for all topics.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// let mut client = kafka::client::KafkaClient::new(vec!("localhost:9092".to_owned()));
    /// let res = client.load_metadata_all();
    /// ```
    ///
    pub fn load_metadata_all(&mut self) -> Result<()>{
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
    /// let res = client.load_metadata(&["my-topic"]);
    /// ```
    ///
    /// returns `Result<(), error::Error>`
    pub fn load_metadata<T: AsRef<str>>(&mut self, topics: &[T]) -> Result<()> {
        let resp = try!(self.get_metadata(topics));
        self.__load_metadata(resp)
    }

    // ~ this is the rest of `load_metadata`. this code is _not_
    // generic (and hence will _not_ get generated for each distinct
    // input type T to the `load_metadata` method).
    fn __load_metadata(&mut self, md: protocol::MetadataResponse) -> Result<()> {
        let mut brokers: HashMap<i32, Rc<String>> = HashMap::new();
        for broker in md.brokers {
            brokers.insert(broker.nodeid, Rc::new(format!("{}:{}", broker.host, broker.port)));
        }

        for topic in md.topics {
            let mut known_partitions = Vec::with_capacity(topic.partitions.len());
            let mut known_partitions_internal = Vec::with_capacity(topic.partitions.len());

            for partition in topic.partitions {
                match brokers.get(&partition.leader) {
                    Some(broker) => {
                        known_partitions.push(partition.id);
                        known_partitions_internal.push(Partition::new(partition.id, broker.clone()));
                    },
                    None => {}
                }
            }

            self.topic_partitions.insert(topic.topic.clone(), known_partitions);
            known_partitions_internal.sort_by(|a, b| a.partition_id.cmp(&b.partition_id));
            self.state.topic_partitions_internal.insert(
                topic.topic.clone(), TopicPartitions::new(known_partitions_internal));
        }
        Ok(())
    }

    /// Clears metadata stored in the client. You must load metadata after this call if you want
    /// to use the client
    pub fn reset_metadata(&mut self) {
        self.state.topic_partitions_internal.clear();
        self.topic_partitions.clear();
    }

    /// Loads metadata about the specified topics from all of the
    /// underlying brokers (`self.hosts`).
    fn get_metadata<T: AsRef<str>>(&mut self, topics: &[T]) -> Result<protocol::MetadataResponse> {
        let correlation = self.next_id();
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

    /// Set the compression algorithm to use
    ///
    /// `compression` - one of compression::Compression::{NONE, GZIP, SNAPPY}
    ///
    /// # Example
    ///
    /// ```no_run
    /// let mut client = kafka::client::KafkaClient::new(vec!("localhost:9092".to_owned()));
    /// client.set_compression(kafka::client::Compression::SNAPPY);
    /// ```
    pub fn set_compression(&mut self, compression: Compression) {
        self.config.compression = compression;
    }

    /// Fetch offsets for a list of topics.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// let mut client = kafka::client::KafkaClient::new(vec!("localhost:9092".to_owned()));
    /// let res = client.load_metadata_all();
    /// let topics: Vec<String> = client.topic_partitions.keys().cloned().collect();
    /// let offsets = client.fetch_offsets(&topics, kafka::client::FetchOffset::Latest);
    /// ```
    /// Returns a hashmap of (topic, PartitionOffset data).
    /// PartitionOffset will contain parition and offset info Or Error code as returned by Kafka.
    pub fn fetch_offsets<T: AsRef<str>>(&mut self, topics: &[T], offset: FetchOffset)
                                        -> Result<HashMap<String, Vec<utils::PartitionOffset>>>
    {
        let time = offset.to_kafka_value();
        let n_topics = topics.len();

        let correlation = self.next_id();
        let mut reqs: HashMap<Rc<String>, protocol::OffsetRequest> = HashMap::with_capacity(n_topics);

        // Map topic and partition to the corresponding broker
        for topic in topics {
            let topic = topic.as_ref();
            if let Some(tp) = self.state.topic_partitions_internal.get(topic) {
                for p in &tp.partitions {
                    let entry = reqs.entry(p.broker_host.clone())
                        .or_insert_with(|| protocol::OffsetRequest::new(correlation, self.config.client_id.clone()));
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
        let mut m = try!(self.fetch_offsets(&[topic.as_ref()], offset));
        let offs = m.remove(topic.as_ref()).unwrap_or(vec!());
        if offs.is_empty() {
            Err(Error::UnknownTopicOrPartition)
        } else {
            Ok(offs)
        }
    }

    /// Fetch messages from Kafka (Multiple topic, partition, offset)
    ///
    /// It takes a vector of `utils:TopicPartitionOffset` and returns a vector of `utils::TopicMessage`
    /// or error::Error
    ///
    /// You can figure out the appropriate partition and offset using client's
    /// `client.topic_partitions` and `client.fetch_topic_offset`
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use kafka::utils;
    /// let mut client = kafka::client::KafkaClient::new(vec!("localhost:9092".to_owned()));
    /// let res = client.load_metadata_all();
    /// let msgs = client.fetch_messages_multi(&[utils::TopicPartitionOffset{
    ///                                                 topic: "my-topic",
    ///                                                 partition: 0,
    ///                                                 offset: 0
    ///                                                 },
    ///                                             utils::TopicPartitionOffset{
    ///                                                 topic: "my-topic-2",
    ///                                                 partition: 0,
    ///                                                 offset: 0
    ///                                             }]);
    /// ```
    pub fn fetch_messages_multi<'a, I, J>(&mut self, input: I) -> Result<Vec<utils::TopicMessage>>
        where J: AsRef<utils::TopicPartitionOffset<'a>>, I: IntoIterator<Item=J>
    {
        let correlation = self.next_id();

        let config = &self.config;

        // Map topic and partition to the corresponding broker
        let mut reqs: HashMap<Rc<String>, protocol::FetchRequest> = HashMap::new();
        for tpo in input {
            let tpo = tpo.as_ref();
            if let Some(broker) = self.state.find_broker(tpo.topic, tpo.partition) {
                reqs.entry(broker.clone())
                    .or_insert_with(|| protocol::FetchRequest::new(correlation, &config.client_id))
                    .add(tpo.topic, tpo.partition, tpo.offset);
            }
        }
        __fetch_messages_multi(&mut self.conn_pool, reqs)
    }


    /// Fetch messages from Kafka (Single topic, partition, offset)
    ///
    /// It takes a single topic, parition and offset and return a vector of messages (`utils::TopicMessage`)
    /// or error::Error
    ///
    /// You can figure out the appropriate partition and offset using client's
    /// `client.topic_partitions` and `client.fetch_topic_offset`
    ///
    /// # Examples
    ///
    /// ```no_run
    /// let mut client = kafka::client::KafkaClient::new(vec!("localhost:9092".to_owned()));
    /// let res = client.load_metadata_all();
    /// let msgs = client.fetch_messages("my-topic", 0, 0);
    /// ```
    pub fn fetch_messages<T: AsRef<str>>(&mut self, topic: T, partition: i32, offset: i64)
                                         -> Result<Vec<utils::TopicMessage>>
    {
        self.fetch_messages_multi(&[utils::TopicPartitionOffset::new(topic.as_ref(), partition, offset)])
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
    /// let m1 = "a".to_owned().into_bytes();
    /// let m2 = "b".to_owned().into_bytes();
    /// let req = vec!(utils::ProduceMessage{topic: "my-topic", message: m1},
    ///                 utils::ProduceMessage{topic: "my-topic-2", message: m2});
    /// println!("{:?}", client.send_messages(1, 100, req));
    /// ```
    /// The return value will contain a vector of topic, partition, offset and error if any
    /// OR error:Error
    pub fn send_messages(&mut self, required_acks: i16, ack_timeout: i32,
                         // XXX avoid consuming the messages
                         messages: Vec<utils::ProduceMessage>)
                         -> Result<Vec<utils::TopicPartitionOffsetError>>
    {
        let correlation = self.next_id();

        // Map topic and partition to the corresponding brokers
        let state = &mut self.state;
        let config = &self.config;

        let mut reqs: HashMap<Rc<String>, protocol::ProduceRequest> = HashMap::new();
        for msg in &messages {
            if let Some((partition, broker)) = state.choose_partition(&msg.topic) {
                reqs.entry(broker)
                    .or_insert_with(
                        || protocol::ProduceRequest::new(required_acks, ack_timeout, correlation,
                                                         &config.client_id, config.compression))
                    .add(msg.topic, partition, &msg.message);
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
    /// let msgs = client.send_message(1, 100, "my-topic", "msg".to_owned().into_bytes());
    /// ```
    /// The return value will contain topic, partition, offset and error if any
    /// OR error:Error
    pub fn send_message(&mut self, required_acks: i16, ack_timeout: i32,
                      topic: &str, message: Vec<u8>) -> Result<Vec<utils::TopicPartitionOffsetError>> {
        self.send_messages(required_acks, ack_timeout, vec!(utils::ProduceMessage{
            topic: topic,
            message: message
            }))

    }

    /// Commit offset to topic, partition of a consumer group
    ///
    /// It takes a group name and list of `utils::TopicPartitionOffset` and returns `()`
    /// or `error::Error`
    ///
    /// You can figure out the appropriate partition and offset using client's
    /// `client.topic_partitions` and `client.fetch_topic_offset`
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use kafka::utils;
    /// let mut client = kafka::client::KafkaClient::new(vec!("localhost:9092".to_owned()));
    /// let res = client.load_metadata_all();
    /// let resp = client.commit_offsets("my-group".to_owned(), vec!(
    ///                 utils::TopicPartitionOffset{topic: "my-topic", partition: 0, offset: 100},
    ///                 utils::TopicPartitionOffset{topic: "my-topic", partition: 1, offset: 100}));
    /// ```
    pub fn commit_offsets(&mut self, group: String, input: Vec<utils::TopicPartitionOffset>) -> Result<()>{

        let correlation = self.next_id();
        let mut reqs: HashMap<Rc<String>, protocol::OffsetCommitRequest> = HashMap:: new();

        // Map topic and partition to the corresponding broker
        for tp in input {
            self.state.find_broker(&tp.topic, tp.partition).and_then(|broker| {
                let entry = reqs.entry(broker.clone()).or_insert(
                            protocol::OffsetCommitRequest::new(group.clone(), correlation, self.config.client_id.clone()));
                // XXX avoid this cloning of the topic
                entry.add(tp.topic.to_owned(), tp.partition, tp.offset, "".to_owned());
                Some(())
            });
        }

        // Call each broker with the request formed earlier
        for (host, req) in reqs {
            try!(__send_receive::<protocol::OffsetCommitRequest, protocol::OffsetCommitResponse>(&mut self.conn_pool, &host, req));
        }
        Ok(())
    }

    /// Commit offset to topic, partition of a consumer group
    ///
    /// It takes a group name, topic, partition and offset and returns `()`
    /// or `error::Error`
    ///
    /// You can figure out the appropriate partition and offset using client's
    /// `client.topic_partitions` and `client.fetch_topic_offset`
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use kafka::utils;
    /// let mut client = kafka::client::KafkaClient::new(vec!("localhost:9092".to_owned()));
    /// let res = client.load_metadata_all();
    /// let resp = client.commit_offset("my-group".to_owned(), "my-topic".to_owned(), 0, 100);
    /// ```
    pub fn commit_offset(&mut self, group: String, topic: String,
                         partition: i32, offset: i64) -> Result<()>{
        self.commit_offsets(group, vec!(utils::TopicPartitionOffset{
                topic: &topic,
                partition: partition,
                offset: offset}))
    }

    /// Fetch offset for vector of topic, partition of a consumer group
    ///
    /// It takes a group name and list of `utils::TopicPartition` and returns `utils::TopicPartitionOffsetError`
    /// or `error::Error`
    ///
    /// You can figure out the appropriate partition using client's
    /// `client.topic_partitions`
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use kafka::utils;
    /// let mut client = kafka::client::KafkaClient::new(vec!("localhost:9092".to_owned()));
    /// let res = client.load_metadata_all();
    /// let resp = client.fetch_group_topics_offset("my-group".to_owned(), vec!(
    ///                 utils::TopicPartition{topic: "my-topic".to_owned(), partition: 0},
    ///                 utils::TopicPartition{topic: "my-topic".to_owned(), partition: 1}));
    /// ```
    pub fn fetch_group_topics_offset(&mut self, group: String, input: Vec<utils::TopicPartition>)
        -> Result<Vec<utils::TopicPartitionOffsetError>>{

        let correlation = self.next_id();
        let mut reqs: HashMap<Rc<String>, protocol::OffsetFetchRequest> = HashMap:: new();

        // Map topic and partition to the corresponding broker
        for tp in input {
            if let Some(broker) = self.state.find_broker(&tp.topic, tp.partition) {
                let entry = reqs.entry(broker.clone()).or_insert(
                    protocol::OffsetFetchRequest::new(group.clone(), correlation, self.config.client_id.clone()));
                entry.add(tp.topic, tp.partition);
            }
        }

        // Call each broker with the request formed earlier
        let mut res = vec!();
        for (host, req) in reqs {
            let resp = try!(__send_receive::<
                            protocol::OffsetFetchRequest, protocol::OffsetFetchResponse>(&mut self.conn_pool, &host, req));
            let o = resp.get_offsets();
            for tpo in o {
                res.push(tpo);
            }
        }
        Ok(res)
    }

    /// Fetch offset for all partitions of a topic of a consumer group
    ///
    /// It takes a group name and a topic and returns `utils::TopicPartitionOffsetError`
    /// or `error::Error`
    ///
    /// You can figure out the appropriate partition using client's
    /// `client.topic_partitions`
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use kafka::utils;
    /// let mut client = kafka::client::KafkaClient::new(vec!("localhost:9092".to_owned()));
    /// let res = client.load_metadata_all();
    /// let resp = client.fetch_group_topic_offset("my-group".to_owned(),"my-topic".to_owned());
    /// ```
    pub fn fetch_group_topic_offset(&mut self, group: String, topic: String)
        -> Result<Vec<utils::TopicPartitionOffsetError>> {
        let tps = self.topic_partitions.get(&topic)
                        .unwrap()
                        .iter()
                        .map(|p| utils::TopicPartition{topic: topic.clone(), partition: p.clone()})
                        .collect();
        self.fetch_group_topics_offset(group, tps)
    }

    /// Fetch offset for all partitions of all topics of a consumer group
    ///
    /// It takes a group name and returns `utils::TopicPartitionOffsetError`
    /// or `error::Error`
    ///
    /// You can figure out the topics using client's
    /// `client.topic_partitions`
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use kafka::utils;
    /// let mut client = kafka::client::KafkaClient::new(vec!("localhost:9092".to_owned()));
    /// let res = client.load_metadata_all();
    /// let resp = client.fetch_group_offset("my-group".to_owned());
    /// ```
    pub fn fetch_group_offset(&mut self, group: String)
        -> Result<Vec<utils::TopicPartitionOffsetError>> {
        let mut tps = vec!();
        for (topic, partitions) in &self.topic_partitions {
            for p in partitions {
                tps.push(utils::TopicPartition{topic: topic.clone(), partition: p.clone()})
            }
        }
        self.fetch_group_topics_offset(group, tps)
    }
}

/// ~ carries out the given fetch requests and returns the response
fn __fetch_messages_multi(conn_pool: &mut ConnectionPool, reqs: HashMap<Rc<String>, protocol::FetchRequest>)
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

/// ~ carries out the given produce requests and returns the reponse
fn __send_messages(conn_pool: &mut ConnectionPool, reqs: HashMap<Rc<String>, protocol::ProduceRequest>, no_acks: bool)
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
    T::decode_new(&mut Cursor::new(resp))
}
