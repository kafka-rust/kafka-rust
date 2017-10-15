#[cfg(feature = "integration_tests")]
extern crate kafka;

#[cfg(feature = "integration_tests")]
extern crate rand;

#[cfg(feature = "integration_tests")]
#[macro_use]
extern crate log;

#[cfg(feature = "integration_tests")]
extern crate env_logger;

#[cfg(feature = "integration_tests")]
mod integration {
    use kafka::client::{KafkaClient, GroupOffsetStorage};

    mod client;
    mod consumer_producer;

    pub const LOCAL_KAFKA_BOOTSTRAP_HOST: &str = "localhost:9092";
    pub const TEST_TOPIC_NAME: &str = "kafka-rust-test";
    pub const TEST_TOPIC_NAME_2: &str = "kafka-rust-test2";
    pub const TEST_GROUP_NAME: &str = "kafka-rust-tester";
    pub const TEST_TOPIC_PARTITIONS: [i32; 2] = [0, 1];
    pub const KAFKA_CONSUMER_OFFSETS_TOPIC_NAME: &str = "__consumer_offsets";

    pub(crate) fn new_ready_kafka_client() -> KafkaClient {
        let hosts = vec![LOCAL_KAFKA_BOOTSTRAP_HOST.to_owned()];
        let mut client = KafkaClient::new(hosts);
        client.set_group_offset_storage(GroupOffsetStorage::Kafka);
        client.load_metadata_all().unwrap();
        client
    }
}
