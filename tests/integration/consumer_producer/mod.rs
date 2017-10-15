pub use super::*;

use std::collections::HashSet;
use std::time::Duration;

use kafka::producer::{Producer, RequiredAcks};
use kafka;
use kafka::client::{FetchOffset, FetchGroupOffset, PartitionOffset};
use kafka::consumer::Consumer;

pub fn test_producer() -> Producer {
    Producer::from_hosts(vec![LOCAL_KAFKA_BOOTSTRAP_HOST.to_owned()])
        .with_ack_timeout(Duration::from_secs(1))
        .with_required_acks(RequiredAcks::All)
        .create()
        .unwrap()
}

/// This is to avoid copy/pasting the consumer's config, whether
/// the consumer is built using `from_hosts` or `from_client`.
///
/// TODO: This can go away if we don't use the builder pattern.
macro_rules! test_consumer_config {
    ( $x:expr ) => {
        $x
            .with_topic_partitions(TEST_TOPIC_NAME.to_owned(), &TEST_TOPIC_PARTITIONS)
            .with_group(TEST_GROUP_NAME.to_owned())
            .with_fallback_offset(kafka::consumer::FetchOffset::Latest)
            .with_offset_storage(kafka::consumer::GroupOffsetStorage::Kafka)
    };
}

/// Return a Consumer builder with some defaults
pub fn test_consumer_builder() -> kafka::consumer::Builder {
    test_consumer_config!(Consumer::from_hosts(vec![LOCAL_KAFKA_BOOTSTRAP_HOST.to_owned()]))
}

pub fn test_consumer() -> Consumer {
    test_consumer_with_client(new_ready_kafka_client())
}

/// Return a ready Kafka consumer with all default settings
pub fn test_consumer_with_client(mut client: KafkaClient) -> Consumer {
    let topics = [TEST_TOPIC_NAME, TEST_TOPIC_NAME_2];

    // Fetch the latest offsets and commit those so that this consumer
    // is always at the latest offset before being used.
    let latest_offsets = client.fetch_offsets(&topics, FetchOffset::Latest).unwrap();

    debug!("latest_offsets: {:?}", latest_offsets);

    for (topic, partition_offsets) in latest_offsets {
        for po in partition_offsets {
            if po.offset == -1 {
                continue;
            }

            client
                .commit_offset(TEST_GROUP_NAME, &topic, po.partition, po.offset)
                .unwrap();
        }
    }

    client.load_metadata_all().unwrap();
    let partition_offsets: HashSet<PartitionOffset> = client
        .fetch_group_topic_offsets(TEST_GROUP_NAME, TEST_TOPIC_NAME)
        .unwrap()
        .into_iter()
        .collect();

    debug!("partition_offsets: {:?}", partition_offsets);

    test_consumer_config!(Consumer::from_client(client))
        .create()
        .unwrap()
}

mod producer;
mod consumer;
