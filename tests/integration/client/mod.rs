/// Still to test:
///
/// * compression
/// * secure connections

use super::*;
use std::collections::HashSet;
use std::collections::HashMap;
use std::time::Duration;

use env_logger;

use kafka::client::{CommitOffset, PartitionOffset, FetchPartition, ProduceMessage,
                    RequiredAcks, FetchOffset};
use kafka::client::fetch::Response;

fn flatten_fetched_messages(resps: &Vec<Response>) -> Vec<(&str, i32, &[u8])> {
    let mut messages = Vec::new();

    for resp in resps {
        for topic in resp.topics() {
            for partition in topic.partitions() {
                for msg in partition.data().as_ref().unwrap().messages() {
                    messages.push((topic.topic(), partition.partition(), msg.value));
                }
            }
        }
    }

    messages
}

#[test]
fn test_kafka_client_load_metadata() {
    let hosts = vec![LOCAL_KAFKA_BOOTSTRAP_HOST.to_owned()];
    let mut client = new_kafka_client();
    let client_id = "test-id".to_string();
    client.set_client_id(client_id.clone());
    client.load_metadata_all().unwrap();

    let topics = client.topics();

    // sanity checks
    assert_eq!(hosts.as_ref() as &[String], client.hosts());
    assert_eq!(&client_id, client.client_id());

    // names
    let topic_names: HashSet<&str> = topics.names()
        // don't count the consumer offsets internal topic
        .filter(|name| *name != KAFKA_CONSUMER_OFFSETS_TOPIC_NAME)
        .collect();
    let mut correct_topic_names: HashSet<&str> = HashSet::new();
    correct_topic_names.insert(TEST_TOPIC_NAME);
    correct_topic_names.insert(TEST_TOPIC_NAME_2);

    assert_eq!(correct_topic_names, topic_names);

    // partitions
    let mut topic_partitions = topics.partitions(TEST_TOPIC_NAME).unwrap().available_ids();
    let mut correct_topic_partitions = TEST_TOPIC_PARTITIONS.to_vec();
    assert_eq!(correct_topic_partitions, topic_partitions);

    topic_partitions = topics
        .partitions(TEST_TOPIC_NAME_2)
        .unwrap()
        .available_ids();
    correct_topic_partitions = TEST_TOPIC_PARTITIONS.to_vec();
    assert_eq!(correct_topic_partitions, topic_partitions);
}

/// Tests:
///
/// * KafkaClient::produce_messages
/// * KafkaClient::fetch_messages
/// * KafkaClient::fetch_offsets
#[test]
fn test_produce_fetch_messages() {
    let _ = env_logger::init();
    let mut client = new_ready_kafka_client();
    let topics = [TEST_TOPIC_NAME, TEST_TOPIC_NAME_2];
    let init_latest_offsets = client.fetch_offsets(&topics, FetchOffset::Latest).unwrap();

    // first send the messages and verify correct confirmation responses
    // from kafka
    let req = vec![
        ProduceMessage::new(TEST_TOPIC_NAME, 0, None, Some("a".as_bytes())),
        ProduceMessage::new(TEST_TOPIC_NAME, 1, None, Some("b".as_bytes())),
        ProduceMessage::new(TEST_TOPIC_NAME_2, 0, None, Some("c".as_bytes())),
        ProduceMessage::new(TEST_TOPIC_NAME_2, 1, None, Some("d".as_bytes())),
    ];

    let resp = client
        .produce_messages(RequiredAcks::All, Duration::from_millis(1000), req)
        .unwrap();

    assert_eq!(2, resp.len());

    // need to keep track of the offsets so we can fetch them next
    let mut fetches = Vec::new();

    for confirm in &resp {
        assert!(confirm.topic == TEST_TOPIC_NAME || confirm.topic == TEST_TOPIC_NAME_2);
        assert_eq!(2, confirm.partition_confirms.len());

        assert!(confirm.partition_confirms.iter().any(|part_confirm| {
            part_confirm.partition == 0 && part_confirm.offset.is_ok()
        }));

        assert!(confirm.partition_confirms.iter().any(|part_confirm| {
            part_confirm.partition == 1 && part_confirm.offset.is_ok()
        }));

        for part_confirm in confirm.partition_confirms.iter() {
            fetches.push(FetchPartition::new(
                confirm.topic.as_ref(),
                part_confirm.partition,
                part_confirm.offset.unwrap(),
            ));
        }
    }

    // now fetch the messages back and verify that they are the correct
    // messages
    let fetch_resps = client.fetch_messages(fetches).unwrap();
    let messages = flatten_fetched_messages(&fetch_resps);

    let correct_messages = vec![
        (TEST_TOPIC_NAME, 0, "a".as_bytes()),
        (TEST_TOPIC_NAME, 1, "b".as_bytes()),
        (TEST_TOPIC_NAME_2, 0, "c".as_bytes()),
        (TEST_TOPIC_NAME_2, 1, "d".as_bytes()),
    ];

    assert!(correct_messages.into_iter().all(|c_msg| {
        messages.contains(&c_msg)
    }));

    let end_latest_offsets = client.fetch_offsets(&topics, FetchOffset::Latest).unwrap();

    for (topic, begin_partition_offsets) in init_latest_offsets {
        let begin_partition_offsets: HashMap<i32, i64> = begin_partition_offsets
            .iter()
            .map(|po| (po.partition, po.offset))
            .collect();

        let end_partition_offsets: HashMap<i32, i64> = end_latest_offsets
            .get(&topic)
            .unwrap()
            .iter()
            .map(|po| (po.partition, po.offset))
            .collect();

        for (partition, begin_offset) in begin_partition_offsets {
            let end_offset = end_partition_offsets.get(&partition).unwrap();
            assert_eq!(begin_offset + 1, *end_offset);
        }
    }
}

#[test]
fn test_commit_offset() {
    let _ = env_logger::init();
    let mut client = new_ready_kafka_client();

    for &(partition, offset) in
        &[
            (TEST_TOPIC_PARTITIONS[0], 100),
            (TEST_TOPIC_PARTITIONS[1], 200),
            (TEST_TOPIC_PARTITIONS[0], 300),
            (TEST_TOPIC_PARTITIONS[1], 400),
            (TEST_TOPIC_PARTITIONS[0], 500),
            (TEST_TOPIC_PARTITIONS[1], 600),
        ]
    {


        client
            .commit_offset(TEST_GROUP_NAME, TEST_TOPIC_NAME, partition, offset)
            .unwrap();

        let partition_offsets: HashSet<PartitionOffset> = client
            .fetch_group_topic_offsets(TEST_GROUP_NAME, TEST_TOPIC_NAME)
            .unwrap()
            .into_iter()
            .collect();

        let correct_partition_offset = PartitionOffset {
            partition: partition,
            offset: offset,
        };

        assert!(partition_offsets.contains(&correct_partition_offset));
    }
}

#[test]
fn test_commit_offsets() {
    let mut client = new_ready_kafka_client();

    let commits = [
        [
            CommitOffset {
                topic: TEST_TOPIC_NAME,
                partition: TEST_TOPIC_PARTITIONS[0],
                offset: 100,
            },
            CommitOffset {
                topic: TEST_TOPIC_NAME,
                partition: TEST_TOPIC_PARTITIONS[1],
                offset: 200,
            },
        ],
        [
            CommitOffset {
                topic: TEST_TOPIC_NAME,
                partition: TEST_TOPIC_PARTITIONS[0],
                offset: 300,
            },
            CommitOffset {
                topic: TEST_TOPIC_NAME,
                partition: TEST_TOPIC_PARTITIONS[1],
                offset: 400,
            },
        ],
        [
            CommitOffset {
                topic: TEST_TOPIC_NAME,
                partition: TEST_TOPIC_PARTITIONS[0],
                offset: 500,
            },
            CommitOffset {
                topic: TEST_TOPIC_NAME,
                partition: TEST_TOPIC_PARTITIONS[1],
                offset: 600,
            },
        ],
    ];

    for commit_pair in &commits {

        client.commit_offsets(TEST_GROUP_NAME, commit_pair).unwrap();

        let partition_offsets: HashSet<PartitionOffset> = client
            .fetch_group_topic_offsets(TEST_GROUP_NAME, TEST_TOPIC_NAME)
            .unwrap()
            .into_iter()
            .collect();

        println!("partition_offsets: {:?}", partition_offsets);

        let correct_partition_offsets: HashSet<PartitionOffset> = vec![
            PartitionOffset {
                partition: commit_pair[0].partition,
                offset: commit_pair[0].offset,
            },
            PartitionOffset {
                partition: commit_pair[1].partition,
                offset: commit_pair[1].offset,
            },
        ].into_iter()
            .collect();

        assert_eq!(correct_partition_offsets, partition_offsets);
    }
}
