extern crate kafka;

use std::env;
use kafka::client::KafkaClient;

/// Demonstrates accessing metadata using KafkaClient.
fn main() {
    let brokers = {
        let mut xs = env::args().skip(1).collect::<Vec<_>>();
        if xs.is_empty() {
            xs.push("localhost:9092".to_owned());
        }
        xs
    };

    let mut client = KafkaClient::new(brokers);
    if let Err(e) = client.load_metadata_all() {
        println!("Failed to load metadata from {:?}: {}", client.hosts(), e);
        return;
    }

    let mut has_partitions = false;
    for topic in client.topics().iter() {
        let partitions = topic.partitions().as_slice();
        if !partitions.is_empty() {
            has_partitions = true;
            for partition in partitions {
                println!("{}:{} {}", topic.name(), partition.id(), partition.leader());
            }
        }
    }
    if !has_partitions {
        println!("No topic partitions found!");
    }
}
