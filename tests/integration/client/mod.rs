use super::*;
use kafka::client::KafkaClient;

#[test]
fn test_kafka_client_load_metadata() {
    let mut client = KafkaClient::new(vec![LOCAL_KAFKA_BOOTSTRAP_HOST.to_owned()]);
    client.load_metadata_all().unwrap();

    let topics = client.topics();

    // names
    let topic_names: Vec<&str> = topics.names()
        // don't count the consumer offsets internal topic
        .filter(|name| *name != KAFKA_CONSUMER_OFFSETS_TOPIC_NAME)
        .collect();
    let correct_topic_names = vec![TEST_TOPIC_NAME];
    assert_eq!(correct_topic_names, topic_names);

    // partitions
    let topic_partitions = topics.partitions(TEST_TOPIC_NAME).unwrap().available_ids();
    let correct_topic_partitions = TEST_TOPIC_PARTITIONS.to_vec();
    assert_eq!(correct_topic_partitions, topic_partitions);
}
