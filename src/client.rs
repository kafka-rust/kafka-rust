
use super::protocol::*;
use super::connection::*;
use super::codecs::*;
use std::collections::HashMap;
use std::io::Cursor;
use std::io::Read;

const CLIENTID: String = String::from_str("kafka-rust");
const DEFAULT_TIMEOUT: i32 = 120; // seconds

type Readable = Cursor<Vec<u8>>;
type Writable = Cursor<Vec<u8>>;

#[derive(Default)]
#[derive(Debug)]
pub struct KafkaClient {
    pub clientid: String,
    pub timeout: i32,
    pub hosts: Vec<String>,
    pub correlation: i32,
    pub conns: HashMap<String, KafkaConnection>,
    pub brokers: Vec<BrokerMetadata>,
    pub topics: Vec<TopicMetadata>
}

impl KafkaClient {
    pub fn new(hosts: &Vec<String>) -> KafkaClient {
        KafkaClient { hosts: hosts.to_vec(), clientid: CLIENTID,
                      timeout: DEFAULT_TIMEOUT, ..KafkaClient::default()}
    }

    fn get_conn(&mut self, host: String) -> KafkaConnection {
        match self.conns.get(&host) {
            Some(conn) => *conn,
            None => {
                let conn = KafkaConnection::new(host, self.timeout);
                self.conns.insert(host, conn);
                conn
            }
        }
    }

    pub fn load_metadata(&mut self, topics: &Vec<String>) {
        let resp = self.get_metadata(topics);
        self.brokers = resp.brokers.to_vec();
        self.topics = resp.topics.to_vec();
    }

    pub fn reset_metadata(&mut self) {
        self.topics.clear();
        self.brokers.clear();
    }

    fn get_metadata(&mut self, topics: &Vec<String>) -> MetadataResponse {
        for host in self.hosts {
            let correlation = self.next_id();
            let req = MetadataRequest::new(correlation, self.clientid, topics.to_vec());
            let conn = self.get_conn(host);
            if (self.send_request(conn, req)) {
                return self.get_response::<MetadataResponse>(conn);
            }
        }
        panic!("All Brokers failes to process request!");
    }

    fn next_id(&mut self) -> i32{
        self.correlation = (self.correlation + 1) % (1i32 << 30);
        self.correlation
    }

    fn send_request<T: FromByte>(&self, conn: KafkaConnection, request: T) -> bool{
        let mut buffer = vec!();
        request.encode(&mut buffer);

        let mut s = vec!();
        (buffer.len() as i32).encode(&mut s);
        for byte in buffer.iter() { s.push(*byte); }
        let bytes_to_send = s.len();

        match conn.send(&s) {
            Ok(num) => return num == bytes_to_send,
            Err(e) => return false
        }
    }

    fn get_response<T>(&self, conn: KafkaConnection) -> T{
        let mut v: Vec<u8> = vec!();
        conn.read(4, &mut v);
        let size = i32::decode_new(&mut Cursor::new(v));

        let mut resp: Vec<u8> = vec!();
        conn.read(size as u64, &mut resp);
        T::decode_new(&mut Cursor::new(resp))
    }

}
