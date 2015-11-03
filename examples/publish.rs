extern crate kafka;

use kafka::client::KafkaClient;

/// This program demonstrates sending a single message through
/// `KafkaClient`.
fn main() {
    let broker = "localhost:9092";
    let topic = "my-topic";
    let data = "hello, kafka";

    println!("About to publish a message at {} to: {}", broker, topic);

    // ~ create the client and load metadata for all available topics
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
    // ~ now send a small message (if the specified topic would have
    // not been existent) the client would drop the message.
    let r = client.send_message(1, 100, topic.to_owned(), data.as_bytes().to_owned());
    println!("message sent: {:?}", r);
}
