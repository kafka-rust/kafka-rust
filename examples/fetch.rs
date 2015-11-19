extern crate kafka;

use kafka::client::KafkaClient;

/// This program demonstrates the low level api for fetching messages.
/// Please look at examles/consume.rs for an easier to use API.
fn main() {
    let broker = "localhost:9092";
    let topic = "my-topic";
    let partition = 0;
    let offset = 0;

    println!("About to fetch messages at {} from: {} (partition {}, offset {}) ",
        broker, topic, partition, offset);


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

    match client.fetch_messages(topic, partition, offset) {
        Err(e) => {
            println!("Failed to fetch messages: {}", e);
        }
        Ok(msgs) => {
            for msg in msgs {
                println!("{:?}", msg);
            }
            println!("No more messages.");
        }
    }
}
