extern crate kafka;
use kafka::client::KafkaClient;
use kafka::protocol::*;
use kafka::codecs::*;

fn main() {


    let mut client = KafkaClient::new(&vec!("localhost:9092".to_string()));
    client.load_metadata(&vec!("my-topic".to_string()));
    let p = ProduceRequest::new_single(&"my-topic".to_string(), 0, 1,
                      100, &"b".to_string().into_bytes(),
                      1, &"rust".to_string());
    println!("{:?}", p);
    let mut buf = vec!();
    p.encode(&mut buf);
    println!("{:?}", buf);
    client.send_message(&"my-topic".to_string(), 0, 1,
                      100, &"b".to_string().into_bytes());

    client.fetch_messages(&"my-topic".to_string(), 0, 0);

}
