extern crate kafka;

use std::env;
use kafka::client::KafkaClient;

const DEFAULT_BROKER: &'static str = "localhost:9092";

/// Demonstrates accessing metadata using KafkaClient.
fn main() {
    let mut brokers = env::args().skip(1).collect::<Vec<_>>();
    if brokers.is_empty() {
        brokers.push(DEFAULT_BROKER.to_owned());
    }

    let mut client = KafkaClient::new(brokers);
    if let Err(e) = client.load_metadata_all() {
        println!("Failed to load metadata from {:?}: {}", client.hosts(), e);
        return;
    }

    for t in client.topics() {
        for p in t.partitions() {
            println!("{}\t{}\t{}\t{}", t.name(), p.id(), p.leader_id(), p.leader_host());
        }
    }
}
