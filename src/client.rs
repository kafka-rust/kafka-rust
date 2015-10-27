//! Kafka Client
//!
//! Primary module of this library.
//!
//! Provides implementation for `KafkaClient` which is used to interact with Kafka

use error::{Result, Error};
use utils;
use protocol;
use connection::KafkaConnection;
use codecs::{ToByte, FromByte};
use compression::Compression;
use std::collections::HashMap;
use std::io::Cursor;
use std::io::Read;
use std::mem;
use std::rc::Rc;

const CLIENTID: &'static str = "kafka-rust";
const DEFAULT_TIMEOUT: i32 = 120; // seconds


/// Client struct.
///
/// It keeps track of brokers and topic metadata
///
/// Implements methods described by Kafka Protocol (https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol)
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
    clientid: Rc<String>,
    timeout: i32,
    hosts: Vec<String>,
    correlation: i32,
    conns: HashMap<String, KafkaConnection>,
    /// HashMap where `topic` is the key and list of `partitions` is the value
    pub topic_partitions: HashMap<String, Vec<i32>>,

    // ~ an internal representation of `topic_partitions`;
    // ~ `TopicPartitions#partitions` are kept in ascending order by `partition_id`
    topic_partitions_internal: HashMap<String, TopicPartitions>,
    compression: Compression
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

impl KafkaClient {
    /// Create a new instance of KafkaClient
    ///
    /// # Examples
    ///
    /// ```no_run
    /// let mut client = kafka::client::KafkaClient::new(vec!("localhost:9092".to_owned()));
    /// ```
    pub fn new(hosts: Vec<String>) -> KafkaClient {
        KafkaClient { hosts: hosts, clientid: Rc::new(CLIENTID.to_owned()),
                      timeout: DEFAULT_TIMEOUT, ..KafkaClient::default()}
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

    fn next_id(&mut self) -> i32 {
        self.correlation = (self.correlation + 1) % (1i32 << 30);
        self.correlation
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
        self.load_metadata(vec!())
    }

    /// Reloads metadata for a list of supplied topics
    ///
    /// # Examples
    ///
    /// ```no_run
    /// let mut client = kafka::client::KafkaClient::new(vec!("localhost:9092".to_owned()));
    /// let res = client.load_metadata(vec!("my-topic".to_owned()));
    /// ```
    ///
    /// returns `Result<(), error::Error>`
    pub fn load_metadata(&mut self, topics: Vec<String>) -> Result<()> {
        let resp = try!(self.get_metadata(topics));

        let mut brokers: HashMap<i32, Rc<String>> = HashMap::new();
        for broker in resp.brokers {
            brokers.insert(broker.nodeid, Rc::new(format!("{}:{}", broker.host, broker.port)));
        }

        for topic in resp.topics {
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
            self.topic_partitions_internal.insert(
                topic.topic.clone(), TopicPartitions::new(known_partitions_internal));
        }
        Ok(())
    }

    /// Clears metadata stored in the client. You must load metadata after this call if you want
    /// to use the client
    pub fn reset_metadata(&mut self) {
        self.topic_partitions_internal.clear();
        self.topic_partitions.clear();
    }

    fn get_metadata(&mut self, topics: Vec<String>) -> Result<protocol::MetadataResponse> {
        let correlation = self.next_id();
        for host in self.hosts.to_vec() {
            let req = protocol::MetadataRequest::new(correlation, self.clientid.clone(), topics.to_vec());
            match self.get_conn(&host) {
                Ok(conn) => if send_request(conn, req).is_ok() {
                    return get_response::<protocol::MetadataResponse>(conn);
                },
                Err(_) => {}
            }
        }

        Err(Error::NoHostReachable)
    }

    fn find_broker(&self, topic: &String, partition: &i32) -> Option<Rc<String>> {
        let topic: &str = &topic;
        self.topic_partitions_internal.get(topic)
            .and_then(|tp| {
                // ~ XXX might also just normally iterate and try to
                // find the element.  the number of partitions is
                // typically very constrainted.
                tp.partitions.binary_search_by(|e| {
                    e.partition_id.cmp(partition)
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

    /// Set the compression algorithm to use
    ///
    /// `compression` - one of compression::Compression::{NONE, GZIP, SNAPPY}
    ///
    /// # Example
    ///
    /// ```no_run
    /// use kafka::compression::Compression;
    /// let mut client = kafka::client::KafkaClient::new(vec!("localhost:9092".to_owned()));
    /// client.set_compression(Compression::SNAPPY);
    /// ```
    pub fn set_compression(&mut self, compression: Compression) {
        self.compression = compression;
    }

    /// Fetch offsets for a list of topics.
    ///
    /// `time` - Used to ask for all messages before a certain time (ms). There are two special values.
    ///          Specify -1 to receive the latest offset (i.e. the offset of the next coming message)
    ///          and -2 to receive the earliest available offset
    ///
    /// # Examples
    ///
    /// ```no_run
    /// let mut client = kafka::client::KafkaClient::new(vec!("localhost:9092".to_owned()));
    /// let res = client.load_metadata_all();
    /// let topics = client.topic_partitions.keys().cloned().collect();
    /// let offsets = client.fetch_offsets(topics, -1);
    /// ```
    /// Returns a hashmap of (topic, PartitionOffset data).
    /// PartitionOffset will contain parition and offset info Or Error code as returned by Kafka.
    pub fn fetch_offsets(&mut self, topics: Vec<String>, time: i64)
        -> Result<HashMap<String, Vec<utils::PartitionOffset>>> {
        let correlation = self.next_id();
        let mut reqs: HashMap<Rc<String>, protocol::OffsetRequest> = HashMap:: new();

        // Map topic and partition to the corresponding broker
        for topic in topics {
            for p in self.topic_partitions.get(&topic).unwrap_or(&vec!()) {
                // XXX no need to call find_broker here
                self.find_broker(&topic, &p).and_then(|broker| {
                    let entry = reqs.entry(broker.clone()).or_insert(
                                protocol::OffsetRequest::new(correlation, self.clientid.clone()));
                    entry.add(topic.clone(), p.clone(), time);
                    Some(())
                });
            }
        }

        // Call each broker with the request formed earlier
        let mut res: HashMap<String, Vec<utils::PartitionOffset>> = HashMap::new();
        for (host, req) in reqs.iter() {
            let resp = try!(self.send_receive::<protocol::OffsetRequest, protocol::OffsetResponse>(&host, req.clone()));
            for tp in resp.get_offsets() {
                let entry = res.entry(tp.topic).or_insert(vec!());
                entry.push(utils::PartitionOffset{offset:tp.offset, partition: tp.partition});
            }
        }
        Ok(res)
    }

    /// Fetch offset for a topic.
    ///
    /// `time` - Used to ask for all messages before a certain time (ms). There are two special values.
    ///          Specify -1 to receive the latest offset (i.e. the offset of the next coming message)
    ///          and -2 to receive the earliest available offset
    ///
    /// # Examples
    ///
    /// ```no_run
    /// let mut client = kafka::client::KafkaClient::new(vec!("localhost:9092".to_owned()));
    /// let res = client.load_metadata_all();
    /// let offsets = client.fetch_topic_offset("my-topic".to_owned(), -1);
    /// ```
    /// Returns a hashmap of (topic, PartitionOffset data).
    /// PartitionOffset will contain parition and offset info Or Error code as returned by Kafka.
    pub fn fetch_topic_offset(&mut self, topic: String, time: i64)
        -> Result<HashMap<String, Vec<utils::PartitionOffset>>> {
        self.fetch_offsets(vec!(topic), time)
    }

    /// Fetch messages from Kafka (Multiple topic, partition, offset)
    ///
    /// It takes a vector of `utils:TopicPartitionOffset` and returns a vector of `utils::TopicMessage`
    /// or error::Error
    ///
    /// You can figure out the appropriate partition and offset using client's
    /// `client.topic_partitions` and `client.fetch_topic_offset(topic)`
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use kafka::utils;
    /// let mut client = kafka::client::KafkaClient::new(vec!("localhost:9092".to_owned()));
    /// let res = client.load_metadata_all();
    /// let msgs = client.fetch_messages_multi(vec!(utils::TopicPartitionOffset{
    ///                                                 topic: "my-topic".to_owned(),
    ///                                                 partition: 0,
    ///                                                 offset: 0
    ///                                                 },
    ///                                             utils::TopicPartitionOffset{
    ///                                                 topic: "my-topic-2".to_owned(),
    ///                                                 partition: 0,
    ///                                                 offset: 0
    ///                                             }));
    /// ```
    pub fn fetch_messages_multi(&mut self, input: Vec<utils::TopicPartitionOffset>)
                                -> Result<Vec<utils::TopicMessage>>
    {
        let correlation = self.next_id();
        let mut reqs: HashMap<Rc<String>, protocol::FetchRequest> = HashMap:: new();

        // Map topic and partition to the corresponding broker
        for tpo in input {
            if let Some(broker) = self.find_broker(&tpo.topic, &tpo.partition) {
                let entry = reqs.entry(broker.clone()).or_insert_with(
                    || protocol::FetchRequest::new(correlation, self.clientid.clone()));
                entry.add(tpo.topic, tpo.partition, tpo.offset);
            }
        }

        // Call each broker with the request formed earlier
        let mut res: Vec<utils::TopicMessage> = vec!();
        for (host, req) in reqs {
            let resp = try!(self.send_receive::<protocol::FetchRequest, protocol::FetchResponse>(&host, req));
            res.extend(resp.into_messages());
        }
        Ok(res)
    }

    /// Fetch messages from Kafka (Single topic, partition, offset)
    ///
    /// It takes a single topic, parition and offset and return a vector of messages (`utils::TopicMessage`)
    /// or error::Error
    ///
    /// You can figure out the appropriate partition and offset using client's
    /// client.topic_partitions and client.fetch_topic_offset(topic)
    ///
    /// # Examples
    ///
    /// ```no_run
    /// let mut client = kafka::client::KafkaClient::new(vec!("localhost:9092".to_owned()));
    /// let res = client.load_metadata_all();
    /// let msgs = client.fetch_messages("my-topic".to_owned(), 0, 0);
    /// ```
    pub fn fetch_messages(&mut self, topic: String, partition: i32, offset: i64) -> Result<Vec<utils::TopicMessage>>{
        self.fetch_messages_multi(vec!(utils::TopicPartitionOffset{
                                        topic: topic,
                                        partition: partition,
                                        offset: offset
                                        }))
    }

    /// Send a message to Kafka
    ///
    /// You can figure out the appropriate partition and offset using client's
    /// `client.topic_partitions` and `client.fetch_topic_offset(topic)`
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
    /// `timeout` - This provides a maximum time in milliseconds the server can await the
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
    /// let req = vec!(utils::ProduceMessage{topic: "my-topic".to_owned(), message: m1},
    ///                 utils::ProduceMessage{topic: "my-topic-2".to_owned(), message: m2});
    /// println!("{:?}", client.send_messages(1, 100, req));
    /// ```
    /// The return value will contain a vector of topic, partition, offset and error if any
    /// OR error:Error
    pub fn send_messages(&mut self, required_acks: i16, timeout: i32,
                         input: Vec<utils::ProduceMessage>) -> Result<Vec<utils::TopicPartitionOffsetError>> {

        let correlation = self.next_id();
        let mut reqs: HashMap<Rc<String>, protocol::ProduceRequest> = HashMap::new();

        // Map topic and partition to the corresponding broker
        for pm in input {
            if let Some((partition, broker)) = self.choose_partition(&pm.topic) {
                let entry = reqs.entry(broker).or_insert_with(
                    || protocol::ProduceRequest::new(required_acks, timeout, correlation, self.clientid.clone(), self.compression));
                entry.add(pm.topic, partition, pm.message);
            }
        }

        // Call each broker with the request formed earlier
        if required_acks == 0 {
            for (host, req) in reqs {
                try!(self.send_noack::<protocol::ProduceRequest, protocol::ProduceResponse>(&host, req));
            }
            Ok(vec!())
        } else {
            let mut res: Vec<utils::TopicPartitionOffsetError> = vec!();
            for (host, req) in reqs {
                let resp = try!(self.send_receive::<protocol::ProduceRequest, protocol::ProduceResponse>(&host, req));
                for tpo in resp.get_response() {
                    res.push(tpo);
                }
            }
            Ok(res)
        }
    }

    /// Send a message to Kafka
    ///
    /// You can figure out the appropriate partition and offset using client's
    /// `client.topic_partitions` and `client.fetch_topic_offset(topic)`
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
    /// `timeout` - This provides a maximum time in milliseconds the server can await the
    /// receipt of the number of acknowledgements in `required_acks`
    ///
    /// `message` - A single message as a vector of u8s
    ///
    /// # Example
    ///
    /// ```no_run
    /// let mut client = kafka::client::KafkaClient::new(vec!("localhost:9092".to_owned()));
    /// let res = client.load_metadata_all();
    /// let msgs = client.send_message(1, 100, "my-topic".to_owned(), "msg".to_owned().into_bytes());
    /// ```
    /// The return value will contain topic, partition, offset and error if any
    /// OR error:Error
    pub fn send_message(&mut self, required_acks: i16, timeout: i32,
                      topic: String, message: Vec<u8>) -> Result<Vec<utils::TopicPartitionOffsetError>> {
        self.send_messages(required_acks, timeout, vec!(utils::ProduceMessage{
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
    /// `client.topic_partitions` and `client.fetch_topic_offset(topic)`
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use kafka::utils;
    /// let mut client = kafka::client::KafkaClient::new(vec!("localhost:9092".to_owned()));
    /// let res = client.load_metadata_all();
    /// let resp = client.commit_offsets("my-group".to_owned(), vec!(
    ///                 utils::TopicPartitionOffset{topic: "my-topic".to_owned(), partition: 0, offset: 100},
    ///                 utils::TopicPartitionOffset{topic: "my-topic".to_owned(), partition: 1, offset: 100}));
    /// ```
    pub fn commit_offsets(&mut self, group: String, input: Vec<utils::TopicPartitionOffset>) -> Result<()>{

        let correlation = self.next_id();
        let mut reqs: HashMap<Rc<String>, protocol::OffsetCommitRequest> = HashMap:: new();

        // Map topic and partition to the corresponding broker
        for tp in input {
            self.find_broker(&tp.topic, &tp.partition).and_then(|broker| {
                let entry = reqs.entry(broker.clone()).or_insert(
                            protocol::OffsetCommitRequest::new(group.clone(), correlation, self.clientid.clone()));
                entry.add(tp.topic.clone(), tp.partition, tp.offset, "".to_owned());
                Some(())
            });
        }

        // Call each broker with the request formed earlier
        for (host, req) in reqs.iter() {
            try!(self.send_receive::<protocol::OffsetCommitRequest, protocol::OffsetCommitResponse>(&host, req.clone()));
        }
        Ok(())
    }

    /// Commit offset to topic, partition of a consumer group
    ///
    /// It takes a group name, topic, partition and offset and returns `()`
    /// or `error::Error`
    ///
    /// You can figure out the appropriate partition and offset using client's
    /// `client.topic_partitions` and `client.fetch_topic_offset(topic)`
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
                topic: topic,
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
            if let Some(broker) = self.find_broker(&tp.topic, &tp.partition) {
                let entry = reqs.entry(broker.clone()).or_insert(
                    protocol::OffsetFetchRequest::new(group.clone(), correlation, self.clientid.clone()));
                entry.add(tp.topic, tp.partition);
            }
        }

        // Call each broker with the request formed earlier
        let mut res = vec!();
        for (host, req) in reqs {
            let resp = try!(self.send_receive::<
                            protocol::OffsetFetchRequest, protocol::OffsetFetchResponse>(&host, req));
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
        for (topic, partitions) in self.topic_partitions.iter() {
            for p in partitions {
                tps.push(utils::TopicPartition{topic: topic.clone(), partition: p.clone()})
            }
        }
        self.fetch_group_topics_offset(group, tps)
    }

    fn send_noack<T: ToByte, V: FromByte>(&mut self, host: &str, req: T) -> Result<usize> {
        let mut conn = try!(self.get_conn(&host));
        send_request(&mut conn, req)
    }

    fn send_receive<T: ToByte, V: FromByte>(&mut self, host: &str, req: T) -> Result<V::R> {
        let mut conn = try!(self.get_conn(&host));
        try!(send_request(&mut conn, req));
        get_response::<V>(&mut conn)
    }
}

fn send_request<T: ToByte>(conn: &mut KafkaConnection, request: T)
                           -> Result<usize>
{
    let mut buffer = vec!();
    try!(request.encode(&mut buffer));

    let mut s = vec!();
    try!((buffer.len() as i32).encode(&mut s));
    for byte in buffer.iter() { s.push(*byte); }

    conn.send(&s)
}

fn get_response<T: FromByte>(conn: &mut KafkaConnection)
                             -> Result<T::R>
{
    let v = try!(conn.read_exact(4));
    let size = try!(i32::decode_new(&mut Cursor::new(v)));

    let resp = try!(conn.read_exact(size as u64));
    T::decode_new(&mut Cursor::new(resp))
}
