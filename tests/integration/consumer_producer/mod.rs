pub use super::*;

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use kafka;
use kafka::producer::{Producer, RequiredAcks};
use kafka::producer::Record;
use kafka::client::{FetchOffset, PartitionOffset};
use kafka::consumer::Consumer;

use rand;
use rand::Rng;

const RANDOM_MESSAGE_SIZE: usize = 32;

pub fn test_producer() -> Producer {
    let client = new_ready_kafka_client();

    Producer::from_client(client)
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
    test_consumer_config!(Consumer::from_client(new_ready_kafka_client()))
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

/// Send `num_messages` randomly-generated messages to the given topic with
/// the given producer.
fn send_random_messages(producer: &mut Producer, topic: &str, num_messages: u32) {
    let mut random_message_buf = [0u8; RANDOM_MESSAGE_SIZE];
    let mut rng = rand::thread_rng();

    for _ in 0..num_messages {
        rng.fill_bytes(&mut random_message_buf);
        producer
            .send(&Record::from_value(topic, &random_message_buf[..]))
            .unwrap();
    }
}

/// Fetch the committed offsets for this group and topic on the given client.
/// If the offset is -1 (i.e., there isn't an offset committed for a partition),
/// use the given optional default value in place of -1. If the default value is
/// None, -1 is kept.
pub(crate) fn get_group_offsets(
    client: &mut KafkaClient,
    group: &str,
    topic: &str,
    default_offset: Option<i64>,
) -> HashMap<i32, i64> {
    client
        .fetch_group_topic_offsets(group, topic)
        .unwrap()
        .iter()
        .map(|po| {
            let offset = if default_offset.is_some() && po.offset == -1 {
                default_offset.unwrap()
            } else {
                po.offset
            };

            (po.partition, offset)
        })
        .collect()
}

/// Given two maps that represent committed offsets at some point in time, where
/// `older` represents some earlier point in time, and `newer` represents some
/// later point in time, compute the number of messages committed between
/// `earlier` and `later`—i.e., for each partition, compute the differences
/// `newer.partition.offset` - `older.partition.offset`, and sum them up.
pub(crate) fn diff_group_offsets(older: &HashMap<i32, i64>, newer: &HashMap<i32, i64>) -> i64 {
    newer
        .iter()
        .map(|(partition, new_offset)| {
            let old_offset = older.get(partition).unwrap();
            new_offset - old_offset
        })
        .sum()
}

mod producer;
mod consumer;
