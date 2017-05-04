pub use super::*;
use kafka::producer::{Producer, RequiredAcks};
use kafka;
use kafka::consumer::Consumer;
use std::time::Duration;

pub fn test_producer_builder() -> kafka::producer::Builder {
    Producer::from_hosts(vec![LOCAL_KAFKA_BOOTSTRAP_HOST.to_owned()])
        .with_ack_timeout(Duration::from_secs(1))
        .with_required_acks(RequiredAcks::One)
}

pub fn test_producer() -> Producer {
    test_producer_builder().create().unwrap()
}


/// Return a Consumer builder with some defaults
pub fn test_consumer_builder() -> kafka::consumer::Builder {
    Consumer::from_hosts(vec![LOCAL_KAFKA_BOOTSTRAP_HOST.to_owned()])
        .with_topic_partitions(TEST_TOPIC_NAME.to_owned(), &TEST_TOPIC_PARTITIONS)
        .with_group(TEST_GROUP_NAME.to_owned())
        .with_fallback_offset(kafka::consumer::FetchOffset::Latest)
        .with_offset_storage(kafka::consumer::GroupOffsetStorage::Kafka)
}

/// Return a ready Kafka consumer with all default settings
pub fn test_consumer() -> Consumer {
    test_consumer_builder().create().unwrap()
}

mod producer;
mod consumer;
mod compression;