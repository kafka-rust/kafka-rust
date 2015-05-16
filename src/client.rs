/// User facing module of this library.
///
/// Fetching message from multiple (topic, partition) pair or producing messages to multiple
/// topics is not yet supported.
/// It should be added very soon.

use error::{Result, Error};
use utils;
use protocol;
use connection::KafkaConnection;
use codecs::{ToByte, FromByte};
use std::collections::HashMap;
use std::io::Cursor;
use std::io::Read;

const CLIENTID: &'static str = "kafka-rust";
const DEFAULT_TIMEOUT: i32 = 120; // seconds


/// Client struct. It keeps track of brokers and topic metadata
///
/// # Examples
///
/// ```no_run
/// let mut client = kafka::client::KafkaClient::new(vec!("localhost:9092".to_string()));
/// let res = client.load_metadata_all();
/// ```
///
/// You will have to load metadata before making any other request.
#[derive(Default, Debug)]
pub struct KafkaClient {
    clientid: String,
    timeout: i32,
    hosts: Vec<String>,
    correlation: i32,
    conns: HashMap<String, KafkaConnection>,
    pub topic_partitions: HashMap<String, Vec<i32>>,
    topic_brokers: HashMap<String, String>
}

impl KafkaClient {
    /// Create a new instance of KafkaClient
    ///
    /// # Examples
    ///
    /// ```no_run
    /// let mut client = kafka::client::KafkaClient::new(vec!("localhost:9092".to_string()));
    /// ```
    pub fn new(hosts: Vec<String>) -> KafkaClient {
        KafkaClient { hosts: hosts, clientid: CLIENTID.to_string(),
                      timeout: DEFAULT_TIMEOUT, ..KafkaClient::default()}
    }

    fn get_conn(& mut self, host: &str) -> Result<KafkaConnection> {
        match self.conns.get(host) {
            Some (conn) => return conn.clone(),
            None => {}
        }
        // TODO
        // Keeping this out here since get is causing ownership issues
        // Will refactor once I know better
        self.conns.insert(host.to_string(),
                          try!(KafkaConnection::new(host, self.timeout)));
        self.get_conn(host)
    }

    fn next_id(&mut self) -> i32{
        self.correlation = (self.correlation + 1) % (1i32 << 30);
        self.correlation
    }


    /// Resets and loads metadata for all topics.
    pub fn load_metadata_all(&mut self) -> Result<()>{
        self.reset_metadata();
        self.load_metadata(vec!())
    }

    /// Reloads metadata for a list of supplied topics
    ///
    /// returns Result<(), error::Error>
    pub fn load_metadata (&mut self, topics: Vec<String>) -> Result<()>{
        let resp = try!(self.get_metadata(topics));

        let mut brokers: HashMap<i32, String> = HashMap::new();
        for broker in resp.brokers {
            brokers.insert(broker.nodeid, format!("{}:{}", broker.host, broker.port));
        }

        self.topic_brokers.clear();
        for topic in resp.topics {
            self.topic_partitions.insert(topic.topic.clone(), vec!());

            for partition in topic.partitions {
                match brokers.get(&partition.leader) {
                    Some(broker) => {
                        self.topic_partitions.get_mut(&topic.topic).unwrap().push(partition.id);
                        self.topic_brokers.insert(
                            format!("{}-{}", topic.topic, partition.id),
                            broker.clone());
                    },
                    None => {}
                }
            }
        }
        println!("{:?}", self.topic_partitions);
        Ok(())
    }

    /// Clears metadata stored in the client. You must load metadata after this call if you want
    /// to use the client
    pub fn reset_metadata(&mut self) {
        self.topic_partitions.clear();
        self.topic_brokers.clear();
    }

    fn get_metadata(&mut self, topics: Vec<String>) -> Result<protocol::MetadataResponse> {
        let correlation = self.next_id();
        for host in self.hosts.to_vec() {
            let req = protocol::MetadataRequest::new(correlation, self.clientid.clone(), topics.to_vec());
            match self.get_conn(&host) {
                Ok(mut conn) => if self.send_request(&mut conn, req).is_ok() {
                    return self.get_response::<protocol::MetadataResponse>(&mut conn);
                },
                Err(_) => {}
            }
        }

        Err(Error::NoHostReachable)
    }

    fn get_broker(&mut self, topic: &String, partition: &i32) -> Option<String> {
        let key = format!("{}-{}", topic, partition);
        match self.topic_brokers.get(&key) {
            Some(broker) => {
                Some(broker.clone())
            },
            None => None
        }
    }

    /// Fetch offsets for a list of topics
    /// Not implemented as yet.
    pub fn fetch_offsets(&mut self, topics: Vec<String>) -> Result<()> {
        // TODO - Implement method to fetch offsets for more than 1 topic
        let correlation = self.next_id();
        let mut req = protocol::OffsetRequest::new(correlation, self.clientid.clone());
        let mut reqs: HashMap<String, protocol::OffsetRequest> = HashMap:: new();
        for topic in topics {
            let partitions = self.topic_partitions
                                 .get(&topic)
                                 .unwrap_or(&vec!())
                                 .clone();

            // Maybe TODO: Can this be simplified without complexifying?

            for p in partitions {
                let key = format!("{}-{}", topic, p);
                match self.topic_brokers.get(&key) {
                    Some(broker) => {
                        if !reqs.contains_key(broker) {
                            reqs.insert(broker.clone(),
                                           protocol::OffsetRequest::new(correlation, self.clientid.clone()));
                        }
                        reqs.get_mut(broker).unwrap().add(topic.clone(), p, -1);
                    },
                    None => {}
                }
            }
        }

        //let mut res: Vec<utils::TopicPartitionOffset> = vec!();
        for (host, req) in reqs.iter() {
            let resp = try!(self.send_receive::<protocol::OffsetRequest, protocol::OffsetResponse>(&host, req.clone()));
            //res.push(resp.get_offsets());
            println!("{:?}", resp.get_offsets());
        }
        Ok(())
        //Ok(res)

    }
/*
    /// Fetch offset for a topic.
    /// It gets the latest offset only. Support for getting earliest will be added soon
    ///
    /// # Examples
    ///
    /// ```no_run
    /// let mut client = kafka::client::KafkaClient::new(vec!("localhost:9092".to_string()));
    /// let res = client.load_metadata_all();
    /// let offsets = client.fetch_topic_offset("my-topic".to_string());
    /// ```
    /// Returns a vector of (topic, partition offset data).
    /// PartitionOffset will contain parition and offset info Or Error code as returned by Kafka.
    pub fn fetch_topic_offset(&mut self, topic: String) -> Result<Vec<(String, Vec<utils::PartitionOffset>)>> {
        // Doing it like this because HashMap will not return borrow of self otherwise
        let partitions = self.topic_partitions
                             .get(&topic)
                             .unwrap_or(&vec!())
                             .clone();

        // Maybe TODO: Can this be simplified without complexifying?
        let mut brokers: HashMap<String, Vec<i32>> = HashMap:: new();
        for p in partitions {
            let key = format!("{}-{}", topic, p);
            match self.topic_brokers.get(&key) {
                Some(broker) => {
                    if !brokers.contains_key(broker) {brokers.insert(broker.clone(), Vec::new());}
                    brokers.get_mut(broker).unwrap().push(p);
                },
                None => {}
            }
        }

        let mut res: Vec<utils::PartitionOffset> = vec!();
        for (host, partitions) in brokers.iter() {
            let v = vec!(utils::TopicPartitions{
                topic: topic.clone(),
                partitions: partitions.to_vec()
                });
            for tpo in try!(self.fetch_offset(v, host.clone())) {
                res.push(utils::PartitionOffset{partition: tpo.partition, offset: tpo.offset});
            }
        }
        Ok(vec!((topic.clone(), res)))
    }

    fn fetch_offset(&mut self, topic_partitions: Vec<utils::TopicPartitions>, host: String)
                            -> Result<Vec<utils::TopicPartitionOffset>> {
        let correlation = self.next_id();
        let req = protocol::OffsetRequest::new_latest(topic_partitions, correlation, self.clientid.clone());

        let resp = try!(self.send_receive::<protocol::OffsetRequest, protocol::OffsetResponse>(&host, req));

        Ok(resp.get_offsets())

    }

    /// Fetch messages from Kafka
    ///
    /// It takes a single topic, parition and offset and return a vector of messages
    /// or error::Error
    /// You can figure out the appropriate partition and offset using client's
    /// client.topic_partitions and client.fetch_topic_offset(topic)
    ///
    /// # Examples
    ///
    /// ```no_run
    /// let mut client = kafka::client::KafkaClient::new(vec!("localhost:9092".to_string()));
    /// let res = client.load_metadata_all();
    /// let msgs = client.fetch_messages("my-topic".to_string(), 0, 0);
    /// ```
    pub fn fetch_messages(&mut self, topic: String, partition: i32, offset: i64) -> Result<Vec<utils::OffsetMessage>>{

        let host = self.get_broker(&topic, &partition).unwrap();

        let correlation = self.next_id();
        let req = protocol::FetchRequest::new_single(topic, partition, offset, correlation, self.clientid.clone());

        let resp = try!(self.send_receive::<protocol::FetchRequest, protocol::FetchResponse>(&host, req));
        Ok(resp.get_messages()
                .iter()
                .filter(|ref x| x.offset >= offset)
                .cloned()
                .collect()
            )
    }

    /// Send a message to Kafka
    ///
    /// You can figure out the appropriate partition and offset using client's
    /// client.topic_partitions and client.fetch_topic_offset(topic)
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
    /// `message` - A single message as a vector of u8s
    ///
    /// # Example
    ///
    /// ```no_run
    /// let mut client = kafka::client::KafkaClient::new(vec!("localhost:9092".to_string()));
    /// let res = client.load_metadata_all();
    /// let msgs = client.send_message("my-topic".to_string(), 0, 1,
    ///                  100, "b".to_string().into_bytes());
    /// ```
    /// The return value will contain topic, partition, offset and error if any
    /// OR error:Error
    pub fn send_message(&mut self, topic: String, partition: i32, required_acks: i16,
                      timeout: i32, message: Vec<u8>) -> Result<Vec<utils::TopicPartitionOffset>> {

        let host = self.get_broker(&topic, &partition).unwrap();

        let correlation = self.next_id();
        let req = protocol::ProduceRequest::new_single(topic, partition, required_acks,
                                                       timeout, message, correlation,
                                                       self.clientid.clone());

        let resp = try!(self.send_receive
            ::<protocol::ProduceRequest, protocol::ProduceResponse>(&host, req));
        Ok(resp.get_response())

    }

    pub fn commit_offset(&mut self, group: String, topic: String,
                         partition: i32, offset: i64) -> Result<()>{
        let host = self.get_broker(&topic, &partition).unwrap();

        let correlation = self.next_id();


        let req = protocol::OffsetCommitRequest::new(group, topic, partition, offset,
                                                    "".to_string(), correlation, self.clientid.clone());

        try!(self.send_receive
            ::<protocol::OffsetCommitRequest, protocol::OffsetCommitResponse>(&host, req));

        Ok(())
    }

    pub fn fetch_group_topic_offset(&mut self, group: String, topic: String, partition: i32) -> Result<i64> {
        let host = self.get_broker(&topic, &partition).unwrap();

        let correlation = self.next_id();
        let req = protocol::OffsetFetchRequest::new(group, vec!(topic.clone()),
                                                    vec!(partition), correlation, self.clientid.clone());

        let resp = try!(self.send_receive
                    ::<protocol::OffsetFetchRequest, protocol::OffsetFetchResponse>(&host, req));

        Ok(resp.get_offset_partition(topic, partition))

    }
*/
    fn send_receive<T: ToByte, V: FromByte>(&mut self, host: &str, req: T) -> Result<V::R> {
        let mut conn = try!(self.get_conn(&host));
        try!(self.send_request(&mut conn, req));
        self.get_response::<V>(&mut conn)
    }

    fn send_request<T: ToByte>(&self, conn: &mut KafkaConnection, request: T) -> Result<usize>{
        let mut buffer = vec!();
        try!(request.encode(&mut buffer));

        let mut s = vec!();
        try!((buffer.len() as i32).encode(&mut s));
        for byte in buffer.iter() { s.push(*byte); }

        conn.send(&s)
    }

    fn get_response<T: FromByte>(&self, conn:&mut KafkaConnection) -> Result<T::R>{
        let mut v: Vec<u8> = vec!();
        let _ = conn.read(4, &mut v);

        let size = try!(i32::decode_new(&mut Cursor::new(v)));

        let mut resp: Vec<u8> = vec!();
        let _ = try!(conn.read(size as u64, &mut resp));

        T::decode_new(&mut Cursor::new(resp))
    }

}
