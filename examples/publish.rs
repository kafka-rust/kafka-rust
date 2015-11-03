extern crate kafka;
use kafka::client::KafkaClient;

fn main() {
    let dest = "localhost:9092";
    println!("Publishing to {}", dest);
    let mut client = KafkaClient::new(vec!(dest.to_owned()));
    client.load_metadata_all().unwrap();
    match client.send_message(1, 100, "my-topic".to_owned(), "msg".to_owned().into_bytes()) {
        Ok(e) => {println!("OK {:?}", e);},
        Err(e) => {println!("Error {:?}", e);}
    }
}
