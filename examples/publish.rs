extern crate kafka;
use kafka::client::KafkaClient;
use kafka::utils;
fn main() {
    let mut client = KafkaClient::new(vec!("localhost:9092".to_owned()));
    client.load_metadata_all();
    client.send_message(1, 100, "my-topic".to_owned(), "msg".to_owned().into_bytes()).unwrap();
}
