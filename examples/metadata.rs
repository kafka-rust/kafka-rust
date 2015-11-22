extern crate kafka;

use kafka::client::KafkaClient;

/// Demonstrates accessing metadata using KafkaClient.
fn main() {
    let broker = "localhost:9092";

    let mut client = KafkaClient::new(vec!(broker.to_owned()));
    if let Err(e) = client.load_metadata_all() {
        println!("Failed to load metadata from {}: {}", broker, e);
        return;
    }

    for topic in client.iter_topics() {
        for partition in topic.partitions() {
            println!("{}:{} {}", topic.name(), partition.id(), partition.leader());
        }
    }
    println!("No more topics");
}
