#[cfg(feature = "integration_tests")]
extern crate kafka;

#[cfg(feature = "integration_tests")]
extern crate rand;

#[cfg(feature = "integration_tests")]
#[macro_use]
extern crate lazy_static;

#[allow(clippy::needless_borrow)]
#[allow(unused_must_use)]
#[cfg(feature = "integration_tests")]
mod integration {
    use std::collections::HashMap;

    use tracing::debug;

    use kafka::client::{Compression, GroupOffsetStorage, KafkaClient, SecurityConfig};

    mod client;
    mod consumer_producer;
    mod sasl;

    pub const LOCAL_KAFKA_BOOTSTRAP_HOST: &str = "localhost:9092";
    pub const TEST_TOPIC_NAME: &str = "kafka-rust-test";
    pub const TEST_TOPIC_NAME_2: &str = "kafka-rust-test2";
    pub const TEST_TOPIC_NAME_POLL: &str = "kafka-rust-test-poll";
    pub const TEST_GROUP_NAME: &str = "kafka-rust-tester";
    pub const TEST_TOPIC_PARTITIONS: [i32; 2] = [0, 1];
    pub const KAFKA_CONSUMER_OFFSETS_TOPIC_NAME: &str = "__consumer_offsets";

    // env vars
    const KAFKA_CLIENT_SECURE: &str = "KAFKA_CLIENT_SECURE";
    const KAFKA_CLIENT_COMPRESSION: &str = "KAFKA_CLIENT_COMPRESSION";

    lazy_static! {
        static ref COMPRESSIONS: HashMap<&'static str, Compression> = {
            let mut m = HashMap::new();

            m.insert("", Compression::NONE);
            m.insert("none", Compression::NONE);
            m.insert("NONE", Compression::NONE);

            m.insert("snappy", Compression::SNAPPY);
            m.insert("SNAPPY", Compression::SNAPPY);

            m.insert("gzip", Compression::GZIP);
            m.insert("GZIP", Compression::GZIP);

            m
        };
    }

    /// Constructs a Kafka client for the integration tests, and loads
    /// its metadata so it is ready to use.
    pub(crate) fn new_ready_kafka_client() -> KafkaClient {
        let mut client = new_kafka_client();
        client.load_metadata_all();
        client
    }

    /// Constructs a Kafka client for the integration tests.
    pub(crate) fn new_kafka_client() -> KafkaClient {
        let hosts = vec![LOCAL_KAFKA_BOOTSTRAP_HOST.to_owned()];

        let mut client = if let Some(security_config) = new_security_config() {
            KafkaClient::new_secure(hosts, security_config)
        } else {
            KafkaClient::new(hosts)
        };

        client.set_group_offset_storage(Some(GroupOffsetStorage::Kafka));

        let compression = std::env::var(KAFKA_CLIENT_COMPRESSION).unwrap_or(String::from(""));
        let compression = COMPRESSIONS.get(&*compression).unwrap();

        client.set_compression(*compression);
        debug!("Constructing client: {:?}", client);

        client
    }

    /// Returns a new security config if the `KAFKA_CLIENT_SECURE`
    /// environment variable is set to a non-empty string.
    pub(crate) fn new_security_config() -> Option<SecurityConfig> {
        match std::env::var_os(KAFKA_CLIENT_SECURE) {
            Some(ref val) if val.as_os_str() != "" => (),
            _ => return None,
        }

        Some(get_security_config())
    }

    /// If the `KAFKA_CLIENT_SECURE` environment variable is set, return a
    /// `SecurityConfig`.
    pub(crate) fn get_security_config() -> SecurityConfig {
        // Use rustls-based SecurityConfig
        SecurityConfig::new().with_hostname_verification(false)
    }
}
