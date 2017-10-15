use std::collections::HashMap;
use std::time::Duration;

use client::{self, KafkaClient, FetchOffset, GroupOffsetStorage};
use error::{ErrorKind, Result};

use super::{Consumer, DEFAULT_FALLBACK_OFFSET, DEFAULT_RETRY_MAX_BYTES_LIMIT};
use super::config::Config;
use super::state::State;
use super::assignment;

#[cfg(feature = "security")]
use client::SecurityConfig;

#[cfg(not(feature = "security"))]
type SecurityConfig = ();

/// A Kafka Consumer builder easing the process of setting up various
/// configuration settings.
#[derive(Debug)]
pub struct Builder {
    client: Option<KafkaClient>,
    hosts: Vec<String>,
    group: String,
    assignments: HashMap<String, Vec<i32>>,
    fallback_offset: FetchOffset,
    fetch_max_wait_time: Duration,
    fetch_min_bytes: i32,
    fetch_max_bytes_per_partition: i32,
    retry_max_bytes_limit: i32,
    fetch_crc_validation: bool,
    security_config: Option<SecurityConfig>,
    group_offset_storage: GroupOffsetStorage,
    conn_idle_timeout: Duration,
    client_id: Option<String>,
}

// ~ public only to be shared inside the kafka crate; not supposed to
// be published outside the crate itself
pub fn new(client: Option<KafkaClient>, hosts: Vec<String>) -> Builder {
    let mut b = Builder {
        client: client,
        hosts: hosts,
        fetch_max_wait_time: Duration::from_millis(client::DEFAULT_FETCH_MAX_WAIT_TIME_MILLIS),
        fetch_min_bytes: client::DEFAULT_FETCH_MIN_BYTES,
        fetch_max_bytes_per_partition: client::DEFAULT_FETCH_MAX_BYTES_PER_PARTITION,
        fetch_crc_validation: client::DEFAULT_FETCH_CRC_VALIDATION,
        retry_max_bytes_limit: DEFAULT_RETRY_MAX_BYTES_LIMIT,
        group: String::new(),
        assignments: HashMap::new(),
        fallback_offset: DEFAULT_FALLBACK_OFFSET,
        security_config: None,
        group_offset_storage: client::DEFAULT_GROUP_OFFSET_STORAGE,
        conn_idle_timeout: Duration::from_millis(client::DEFAULT_CONNECTION_IDLE_TIMEOUT_MILLIS),
        client_id: None,
    };
    if let Some(ref c) = b.client {
        b.fetch_max_wait_time = c.fetch_max_wait_time();
        b.fetch_min_bytes = c.fetch_min_bytes();
        b.fetch_max_bytes_per_partition = c.fetch_max_bytes_per_partition();
        b.fetch_crc_validation = c.fetch_crc_validation();
        b.group_offset_storage = c.group_offset_storage();
        b.conn_idle_timeout = c.connection_idle_timeout();
    }
    b
}

impl Builder {
    /// Specifies the group on whose behalf to maintain consumed
    /// message offsets.
    ///
    /// The group is allowed to be the empty string, in which case the
    /// resulting consumer will be group-less.
    pub fn with_group(mut self, group: String) -> Builder {
        self.group = group;
        self
    }

    /// Specifies a topic to consume. All of the available partitions
    /// of the identified topic will be consumed unless overriden
    /// later using `with_topic_partitions`.
    ///
    /// This method may be called multiple times to assign the
    /// consumer multiple topics.
    ///
    /// This method or `with_topic_partitions` must be called at least
    /// once, to assign a topic to the consumer.
    pub fn with_topic(mut self, topic: String) -> Builder {
        self.assignments.insert(topic, Vec::new());
        self
    }

    /// Explicitly specifies topic partitions to consume. Only the
    /// specified partitions for the identified topic will be consumed
    /// unless overriden later using `with_topic`.
    ///
    /// This method may be called multiple times to subscribe to
    /// multiple topics.
    ///
    /// This method or `with_topic` must be called at least once, to
    /// assign a topic to the consumer.
    pub fn with_topic_partitions(mut self, topic: String, partitions: &[i32]) -> Builder {
        self.assignments.insert(topic, partitions.to_vec());
        self
    }

    /// Specifies the security config to use.
    /// See `KafkaClient::new_secure` for more info.
    #[cfg(feature = "security")]
    pub fn with_security(mut self, sec: SecurityConfig) -> Builder {
        self.security_config = Some(sec);
        self
    }

    /// Specifies the offset to use when none was committed for the
    /// underlying group yet or the consumer has no group configured.
    ///
    /// Running the underlying group for the first time against a
    /// topic or running the consumer without a group results in the
    /// question where to start reading from the topic, since it might
    /// already contain a lot of messages.  Common strategies are
    /// starting at the earliest available message (thereby consuming
    /// whatever is currently in the topic) or at the latest one
    /// (thereby staring to consume only newly arriving messages.)
    /// The "fallback offset" here corresponds to `time` in
    /// `KafkaClient::fetch_offsets`.
    pub fn with_fallback_offset(mut self, fallback_offset: FetchOffset) -> Builder {
        self.fallback_offset = fallback_offset;
        self
    }

    /// See `KafkaClient::set_fetch_max_wait_time`
    pub fn with_fetch_max_wait_time(mut self, max_wait_time: Duration) -> Builder {
        self.fetch_max_wait_time = max_wait_time;
        self
    }

    /// See `KafkaClient::set_fetch_min_bytes`
    pub fn with_fetch_min_bytes(mut self, min_bytes: i32) -> Builder {
        self.fetch_min_bytes = min_bytes;
        self
    }

    /// See `KafkaClient::set_fetch_max_bytes_per_partition`
    pub fn with_fetch_max_bytes_per_partition(mut self, max_bytes_per_partition: i32) -> Builder {
        self.fetch_max_bytes_per_partition = max_bytes_per_partition;
        self
    }

    /// See `KafkaClient::set_fetch_crc_validation`
    pub fn with_fetch_crc_validation(mut self, validate_crc: bool) -> Builder {
        self.fetch_crc_validation = validate_crc;
        self
    }

    /// See `KafkaClient::set_group_offset_storage`
    pub fn with_offset_storage(mut self, storage: GroupOffsetStorage) -> Builder {
        self.group_offset_storage = storage;
        self
    }

    /// Specifies the upper bound of data bytes to allow fetching from
    /// a kafka partition when retrying a fetch request due to a too
    /// big message in the partition.
    ///
    /// By default, this consumer will fetch up to
    /// `KafkaClient::fetch_max_bytes_per_partition` data from each
    /// partition.  However, when it discovers that there are messages
    /// in an underlying partition which could not be delivered, the
    /// request to that partition might be retried a few times with an
    /// increased `fetch_max_bytes_per_partition`.  The value
    /// specified here defines a limit to this increment.
    ///
    /// A value smaller than the
    /// `KafkaClient::fetch_max_bytes_per_partition`, e.g. zero, will
    /// disable the retry feature of this consumer.  The default value
    /// for this setting is `DEFAULT_RETRY_MAX_BYTES_LIMIT`.
    ///
    /// Note: if the consumed topic partitions are known to host large
    /// messages it is much more efficient to set
    /// `KafkaClient::fetch_max_bytes_per_partition` appropriately
    /// instead of relying on the limit specified here.  This limit is
    /// just an upper bound for already additional retry requests.
    pub fn with_retry_max_bytes_limit(mut self, limit: i32) -> Builder {
        self.retry_max_bytes_limit = limit;
        self
    }

    /// Specifies the timeout for idle connections.
    /// See `KafkaClient::set_connection_idle_timeout`.
    pub fn with_connection_idle_timeout(mut self, timeout: Duration) -> Self {
        self.conn_idle_timeout = timeout;
        self
    }

    /// Specifies a client_id to be sent along every request to Kafka
    /// brokers. See `KafkaClient::set_client_id`.
    pub fn with_client_id(mut self, client_id: String) -> Self {
        self.client_id = Some(client_id);
        self
    }

    #[cfg(not(feature = "security"))]
    fn new_kafka_client(hosts: Vec<String>, _: Option<SecurityConfig>) -> KafkaClient {
        KafkaClient::new(hosts)
    }

    #[cfg(feature = "security")]
    fn new_kafka_client(hosts: Vec<String>, security: Option<SecurityConfig>) -> KafkaClient {
        if let Some(security) = security {
            KafkaClient::new_secure(hosts, security)
        } else {
            KafkaClient::new(hosts)
        }
    }

    /// Finally creates/builds a new consumer based on the so far
    /// supplied settings.
    ///
    /// Fails with the `NoTopicsAssigned` error, if neither
    /// `with_topic` nor `with_topic_partitions` have been called to
    /// assign at least one topic for consumption.
    pub fn create(self) -> Result<Consumer> {
        // ~ fail immediately if there's no topic to be consumed
        if self.assignments.is_empty() {
            bail!(ErrorKind::NoTopicsAssigned);
        }
        // ~ create the client if necessary
        let (mut client, need_metadata) = match self.client {
            Some(client) => (client, false),
            None => (Self::new_kafka_client(self.hosts, self.security_config), true),
        };
        // ~ apply configuration settings
        try!(client.set_fetch_max_wait_time(self.fetch_max_wait_time));
        client.set_fetch_min_bytes(self.fetch_min_bytes);
        client.set_fetch_max_bytes_per_partition(self.fetch_max_bytes_per_partition);
        client.set_group_offset_storage(self.group_offset_storage);
        client.set_connection_idle_timeout(self.conn_idle_timeout);
        if let Some(client_id) = self.client_id {
            client.set_client_id(client_id)
        }
        // ~ load metadata if necessary
        if need_metadata {
            try!(client.load_metadata_all());
        }
        // ~ load consumer state
        let config = Config {
            group: self.group,
            fallback_offset: self.fallback_offset,
            retry_max_bytes_limit: self.retry_max_bytes_limit,
        };
        let state = try!(State::new(&mut client, &config, assignment::from_map(self.assignments)));
        debug!("initialized: Consumer {{ config: {:?}, state: {:?} }}", config, state);
        Ok(Consumer {
            client: client,
            state: state,
            config: config,
        })
    }
}
