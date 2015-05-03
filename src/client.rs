
use super::protocol::*;
use std::collections::HashMap;

const CLIENTID = String::from_str("kafka-rust");
const DEFAULT_TIMEOUT = 120; // seconds

#[derive(Default)]
#[derive(Debug)]
pub struct KafkaClient {
    pub clientid: String,
    pub timeout: i32,
    pub hosts: Vec<String>,
    pub correlation: i32,
    pub conns: HashMap<String, Vec<KafkaConnection>>,
    pub brokers: Vec<BrokerMetadata>,
    pub topic_to_partitions: Vec<TopicMetadata>,
    pub topic_to_brokers: HashMap<String, Vec<String>>
}

impl KafkaClient {
    pub fn new(hosts: &Vec<String>) -> KafkaClient {
        KafkaClient { hosts: hosts.to_vec(), clientid: CLIENTID, timeout: DEFAULT_TIMEOUT, ..KafkaCLient::default()}
    }

    pub fn load_metadata(&mut self, topics: &Vec<String>) {
        let resp = self.get_metadata();
    }

    pub fn reset_metadata(&mut self) {
        self.topic_to_partitions.clear();
        self.topic_to_brokers.clear();
    }

    pub fn reset_topic_metadata(&mut self, topics: &Vec<String>) {
        for topic in topics {

        }
    }

    fn get_metadata(&mut self, topics: &Vec<String>) {
        for host in hosts {
            let correlation = self.next_id();
            let req = MetadataRequest::new(correlation, self.clientid, topics: topics);
            if (self.send_request(host, req)) {
                resp = self.get_response::<MetadataResponse>(conn);
                return resp
            }
        }
        panic!("All Brokers failes to process request!");
    }

    fn next_id(&mut self) {
        self.correlation = (self.correlation_id + 1) % (1u32 << 31);
        self.correlation
    }

    fn send_request<T: ToByte>(&self, host: String, request: T) -> bool {
        let mut buffer = vec!();
        request.encode(&mut buffer);
        let mut s = vec!();
        (buffer.len() as i32).encode(&mut s);
        for byte in buffer.iter() { s.push(*byte); }
        let bytes_to_send = s.len();

        let conn = self.conns.get(host).unwrap();
        match conn.send(s) {
            Ok(num) => return num == bytes_to_send,
            Err(e) => return false
        }
    }

    fn get_response<T>(conn: KafkaConnection) -> T {
        let mut v: Vec<u8> = vec!();
        conn.read(4, &mut x);
        let size = i32::decode(&mut Cursor::new(v));

        let mut resp: Vec<u8> = vec!();
        conn.read(size as u64, &mut resp);
        T::decode_new(&mut Cursor::new(resp))
    }

}
