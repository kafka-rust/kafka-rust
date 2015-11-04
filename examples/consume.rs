extern crate kafka;
use kafka::client::KafkaClient;

/// This program demonstrates consuming messages through
/// `KafkaClient`.
fn main() {
    let broker = "localhost:9092";
    let topic = "my-topic";

    println!("About to consume messages at {} from: {}", broker, topic);


    let mut client = KafkaClient::new(vec!(broker.to_owned()));
    if let Err(e) = client.load_metadata_all() {
        println!("Failed to load meta data from {}: {}", broker, e);
        return;
    }

    // ~ make sure to print out a warning message when the target
    // topic does not yet exist
    if !client.topic_partitions.contains_key(topic) {
        println!("No such topic at {}: {}", broker, topic);
        return;
    }
    
    let con = kafka::consumer::Consumer::new(client, "test-group".to_owned(), topic.to_owned())
             .partition(0);
    for msg in con {
        println!("{:?}", msg);
    }
}
