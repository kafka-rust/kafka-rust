//! SASL/PLAIN integration tests.
//!
//! Run via: `./run-sasl-tests` or manually with
//! `KAFKA_SASL_BOOTSTRAP=localhost:9093 cargo test sasl_integration --features integration_tests -- --ignored`

use super::*;
use std::time::Duration;

use kafka::client::fetch::Response;
use kafka::client::{
    Compression, FetchOffset, FetchPartition, GroupOffsetStorage, KafkaClient, ProduceMessage,
    RequiredAcks, SecurityConfig,
};

fn sasl_bootstrap_host() -> Option<String> {
    std::env::var("KAFKA_SASL_BOOTSTRAP").ok()
}

fn new_sasl_client() -> KafkaClient {
    let host = sasl_bootstrap_host()
        .expect("KAFKA_SASL_BOOTSTRAP must be set (e.g. localhost:9093). Run via ./run-sasl-tests");
    let mut client = KafkaClient::new_secure(
        vec![host.clone()],
        SecurityConfig::new().with_sasl_plain("alice".to_owned(), "alice-secret".to_owned()),
    );
    client.set_group_offset_storage(Some(GroupOffsetStorage::Kafka));
    client.set_compression(Compression::NONE);
    client
}

fn flatten_fetched_messages(resps: &[Response]) -> Vec<(&str, i32, &[u8])> {
    let mut messages = Vec::new();
    for resp in resps {
        for topic in resp.topics() {
            for partition in topic.partitions() {
                if let Ok(data) = partition.data() {
                    for msg in data.messages() {
                        messages.push((topic.topic(), partition.partition(), msg.value));
                    }
                }
            }
        }
    }
    messages
}

#[test]
#[ignore] // run via: KAFKA_SASL_BOOTSTRAP=localhost:9093 cargo test sasl_integration -- --ignored
fn test_sasl_integration_load_metadata() {
    let mut client = new_sasl_client();
    client.load_metadata_all().unwrap();

    let topics = client.topics();
    let topic_names: std::collections::HashSet<_> = topics
        .names()
        .filter(|name| *name != KAFKA_CONSUMER_OFFSETS_TOPIC_NAME)
        .collect();

    assert!(
        topic_names.contains(TEST_TOPIC_NAME),
        "expected {} in {:?}",
        TEST_TOPIC_NAME,
        topic_names
    );
    assert!(
        topic_names.contains(TEST_TOPIC_NAME_2),
        "expected {} in {:?}",
        TEST_TOPIC_NAME_2,
        topic_names
    );
}

#[test]
#[ignore] // run via: KAFKA_SASL_BOOTSTRAP=localhost:9093 cargo test sasl_integration -- --ignored
fn test_sasl_integration_produce_fetch() {
    tracing_subscriber::fmt::try_init();
    let mut client = new_sasl_client();
    client.load_metadata_all().unwrap();

    let topics = [TEST_TOPIC_NAME, TEST_TOPIC_NAME_2];
    let _init_offsets = client.fetch_offsets(&topics, FetchOffset::Latest).unwrap();

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

    let mut fetches = Vec::new();
    for confirm in &resp {
        for part_confirm in &confirm.partition_confirms {
            if let Ok(offset) = part_confirm.offset {
                fetches.push(FetchPartition::new(
                    confirm.topic.as_str(),
                    part_confirm.partition,
                    offset,
                ));
            }
        }
    }

    let fetch_resps = client.fetch_messages(fetches).unwrap();
    let messages = flatten_fetched_messages(&fetch_resps);

    let expected = vec![
        (TEST_TOPIC_NAME, 0, "a".as_bytes()),
        (TEST_TOPIC_NAME, 1, "b".as_bytes()),
        (TEST_TOPIC_NAME_2, 0, "c".as_bytes()),
        (TEST_TOPIC_NAME_2, 1, "d".as_bytes()),
    ];

    for exp in &expected {
        assert!(
            messages.contains(exp),
            "expected {:?} in messages {:?}",
            exp,
            messages
        );
    }
}
