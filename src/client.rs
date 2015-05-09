use error::{Result, Error};
use utils;
use protocol;
use super::connection::KafkaConnection;
use super::codecs::*;
use std::collections::HashMap;
use std::io::Cursor;
use std::io::Read;

const CLIENTID: &'static str = "kafka-rust";
const DEFAULT_TIMEOUT: i32 = 120; // seconds


#[derive(Default)]
#[derive(Debug)]
pub struct KafkaClient {
    pub clientid: String,
    pub timeout: i32,
    pub hosts: Vec<String>,
    pub correlation: i32,
    pub conns: HashMap<String, KafkaConnection>,
    pub topic_partitions: HashMap<String, Vec<i32>>,
    pub topic_brokers: HashMap<String, String>
}

impl KafkaClient {
    pub fn new(hosts: &Vec<String>) -> KafkaClient {
        KafkaClient { hosts: hosts.to_vec(), clientid: CLIENTID.to_string(),
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


    pub fn load_metadata_all(&mut self) -> Result<()>{
        self.reset_metadata();
        self.load_metadata(&vec!())
    }

    pub fn load_metadata (&mut self, topics: &Vec<String>) -> Result<()>{
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
        Ok(())
    }

    pub fn reset_metadata(&mut self) {
        self.topic_partitions.clear();
        self.topic_brokers.clear();
    }

    fn get_metadata(&mut self, topics: &Vec<String>) -> Result<protocol::MetadataResponse> {
        let correlation = self.next_id();
        for host in self.hosts.to_vec() {
            let req = protocol::MetadataRequest::new(correlation, &self.clientid, topics);
            match self.get_conn(&host) {
                Ok(mut conn) => if self.send_request(&mut conn, req).is_ok() {
                    return self.get_response::<protocol::MetadataResponse>(&mut conn);
                },
                Err(_) => {}
            }
        }

        Err(Error::NoHostReachable)
    }

    pub fn fetch_offsets(&mut self) {
        // TODO - Implement method to fetch offsets for more than 1 topic

    }

    pub fn fetch_topic_offset(&mut self, topic: &String) -> Result<Vec<(String, Vec<utils::PartitionOffset>)>> {
        // Doing it like this because HashMap will not return borrow of self otherwise
        let partitions = self.topic_partitions
                             .get(topic)
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
            for tpo in try!(self.fetch_offset(&v, host)) {
                res.push(utils::PartitionOffset{partition: tpo.partition, offset: tpo.offset});
            }
        }
        Ok(vec!((topic.clone(), res)))
    }

    fn fetch_offset(&mut self, topic_partitions: &Vec<utils::TopicPartitions>, host: &String)
                            -> Result<Vec<utils::TopicPartitionOffset>> {
        let correlation = self.next_id();
        let req = protocol::OffsetRequest::new_latest(topic_partitions, correlation, &self.clientid);

        let resp = try!(self.send_receive::<protocol::OffsetRequest, protocol::OffsetResponse>(&host, req));

        Ok(resp.get_offsets())

    }

    fn get_broker(&mut self, topic: &String, partition: i32) -> Option<String> {
        let key = format!("{}-{}", topic, partition);
        match self.topic_brokers.get(&key) {
            Some(broker) => {
                Some(broker.clone())
            },
            None => None
        }
    }

    pub fn fetch_messages(&mut self, topic: &String, partition: i32, offset: i64) -> Result<Vec<utils::OffsetMessage>>{

        let host = self.get_broker(topic, partition).unwrap();

        let correlation = self.next_id();
        let req = protocol::FetchRequest::new_single(topic, partition, offset, correlation, &self.clientid);

        let resp = try!(self.send_receive::<protocol::FetchRequest, protocol::FetchResponse>(&host, req));
        Ok(resp.get_messages())
    }

    pub fn send_message(&mut self, topic: &String, partition: i32, required_acks: i16,
                      timeout: i32, message: &Vec<u8>) -> Result<protocol::ProduceResponse> {

        let host = self.get_broker(topic, partition).unwrap();

        let correlation = self.next_id();
        let req = protocol::ProduceRequest::new_single(topic, partition, required_acks,
            timeout, message, correlation, &self.clientid);

        self.send_receive::<protocol::ProduceRequest, protocol::ProduceResponse>(&host, req)

    }

    fn send_receive<T: ToByte, V: FromByte>(&mut self, host: &str, req: T) -> Result<V::R> {
        let mut conn = try!(self.get_conn(&host));
        try!(self.send_request(&mut conn, req));
        self.get_response::<V>(&mut conn)
    }

    fn send_request<T: ToByte>(&self, conn: &mut KafkaConnection, request: T) -> Result<usize>{
        let mut buffer = vec!();
        request.encode(&mut buffer);

        let mut s = vec!();
        (buffer.len() as i32).encode(&mut s);
        for byte in buffer.iter() { s.push(*byte); }

        conn.send(&s)
    }

    fn get_response<T: FromByte>(&self, conn:&mut KafkaConnection) -> Result<T::R>{
        let mut v: Vec<u8> = vec!();
        let _ = conn.read(4, &mut v);
        let size = i32::decode_new(&mut Cursor::new(v)).unwrap();

        let mut resp: Vec<u8> = vec!();
        let _ = try!(conn.read(size as u64, &mut resp));

        T::decode_new(&mut Cursor::new(resp))
    }

}
