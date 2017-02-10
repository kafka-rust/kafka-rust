#[cfg(feature = "integration_tests")]
extern crate kafka;

#[cfg(feature = "integration_tests")]
mod integration {
    mod client;
    mod consumer_producer;

    pub const LOCAL_KAFKA_BOOTSTRAP_HOST: &'static str = "localhost:9092";
    pub const TEST_TOPIC_NAME: &'static str = "kafka-rust-test";
    pub const TEST_GROUP_NAME: &'static str = "kafka-rust-tester";
    pub const TEST_TOPIC_PARTITIONS: [i32; 2] = [0, 1];
    pub const KAFKA_CONSUMER_OFFSETS_TOPIC_NAME: &'static str = "__consumer_offsets";
}
