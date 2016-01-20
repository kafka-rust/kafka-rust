//! Some utility structures

use error::{Result,Error};

// XXX move to kafka::client module
#[derive(Debug)]
pub struct ProduceMessage<'a, 'b> {
    pub topic: &'a str,
    pub message: &'b [u8],
}

impl<'a, 'b> AsRef<ProduceMessage<'a, 'b>> for ProduceMessage<'a, 'b> {
    fn as_ref(&self) -> &Self {
        &self
    }
}

// XXX move to kafka::client::module
/// Partition related request data for fetching messages.
/// See `KafkaClient::fetch_messages`.
#[derive(Debug)]
pub struct FetchPartition<'a> {
    /// The topic to fetch messages from.
    pub topic: &'a str,

    /// The offset as of which to fetch messages.
    pub offset: i64,

    /// The partition to fetch messasges from.
    pub partition: i32,

    /// Specifies the max. amount of data to fetch (for this
    /// partition.)  This implicitely defines the biggest message the
    /// client can accept.  If this value is too small, no messages
    /// can be delivered.  Setting this size should be in sync with
    /// the producers to the partition.
    ///
    /// Zero or negative values are treated as "unspecified".
    pub max_bytes: i32,
}

impl<'a> FetchPartition<'a> {

    /// Creates a new "fetch messages" request structure with an
    /// unspecified `max_bytes`.
    pub fn new(topic: &'a str, partition: i32, offset: i64) -> Self {
        FetchPartition {
            topic: topic,
            partition: partition,
            offset: offset,
            max_bytes: -1,
        }
    }

    /// Sets the `max_bytes` value for the "fetch messages" request.
    pub fn with_max_bytes(mut self, max_bytes: i32) -> Self {
        self.max_bytes = max_bytes;
        self
    }
}

impl<'a> AsRef<FetchPartition<'a>> for FetchPartition<'a> {
    fn as_ref(&self) -> &Self {
        &self
    }
}

#[derive(Debug)]
pub struct PartitionOffset {
    pub offset: Result<i64>,
    pub partition: i32,
}

#[derive(Debug)]
pub struct TopicPartitionOffset<'a> {
    pub topic: &'a str,
    pub offset: i64,
    pub partition: i32,
}

impl<'a> TopicPartitionOffset<'a> {
    pub fn new(topic: &'a str, partition: i32, offset: i64) -> TopicPartitionOffset<'a> {
        TopicPartitionOffset {
            topic: topic,
            partition: partition,
            offset: offset,
        }
    }
}

impl<'a> AsRef<TopicPartitionOffset<'a>> for TopicPartitionOffset<'a> {
    fn as_ref(&self) -> &Self {
        &self
    }
}

#[derive(Debug)]
pub struct TopicPartitionOffsetError {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub error: Option<Error>
}

#[derive(Debug)]
pub struct TopicPartition<'a> {
    pub topic: &'a str,
    pub partition: i32
}

impl<'a> TopicPartition<'a> {
    #[inline]
    pub fn new(topic: &str, partition: i32) -> TopicPartition {
        TopicPartition { topic: topic, partition: partition }
    }
}

impl<'a> AsRef<TopicPartition<'a>> for TopicPartition<'a> {
    fn as_ref(&self) -> &Self {
        &self
    }
}
