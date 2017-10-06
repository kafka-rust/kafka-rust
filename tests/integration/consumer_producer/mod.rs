pub use super::*;
use kafka::producer::{Producer, RequiredAcks};
use kafka;
use kafka::client::FetchOffset;
use kafka::consumer::Consumer;
use std::time::Duration;

pub fn test_producer() -> Producer {
    Producer::from_hosts(vec![LOCAL_KAFKA_BOOTSTRAP_HOST.to_owned()])
        .with_ack_timeout(Duration::from_secs(1))
        .with_required_acks(RequiredAcks::All)
        .create()
        .unwrap()
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
    let mut consumer = test_consumer_builder().create().unwrap();
    let topics = [TEST_TOPIC_NAME, TEST_TOPIC_NAME_2];

    // Fetch the latest offsets and commit those so that this consumer
    // is always at the latest offset before being used.
    {
        let client = consumer.client_mut();
        let latest_offsets = client.fetch_offsets(&topics, FetchOffset::Latest).unwrap();

        for (topic, partition_offsets) in latest_offsets {
            for po in partition_offsets {
                client
                    .commit_offset(TEST_GROUP_NAME, &topic, po.partition, po.offset)
                    .unwrap();
            }
        }
    }

    consumer
}

mod producer;
mod consumer;
