extern crate kafka;
use kafka::client::KafkaClient;

/// This program demonstrates fetching messages.
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

    let msgs = client.fetch_messages(topic.to_owned(), 0, 0);

    for msg in msgs {
        println!("{:?}", msg);
    }
}
