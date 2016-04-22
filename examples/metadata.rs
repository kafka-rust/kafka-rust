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

    let mut client = KafkaClient::new(brokers, None);
    if let Err(e) = client.load_metadata_all() {
        println!("Failed to load metadata from {:?}: {}", client.hosts(), e);
        return;
    }

    for t in client.topics() {
        for p in t.partitions() {
            match p.leader() {
                Some(leader) =>
                    println!("{}\t{}\t{}\t{}", t.name(), p.id(), leader.id(), leader.host()),
                None =>
                    println!("{}\t{} => no leader!", t.name(), p.id()),
            }
        }
    }
}
