use std::io::{Read, Write};

use crate::codecs::{FromByte, ToByte};
use crate::error::Result;

use super::{API_KEY_CREATE_TOPICS, API_VERSION};
use super::{HeaderRequest, HeaderResponse};

#[derive(Debug)]
pub struct CreateTopicsRequest<'a> {
    pub header: HeaderRequest<'a>,
    pub topic_requests: Vec<TopicCreateTopicsRequest<'a>>,
    pub timeout: i32,
}

#[derive(Default, Debug)]
pub struct TopicCreateTopicsRequest<'a> {
    pub topic: &'a str,
    pub num_partitions: i32,
    pub replication_factor: i16,
    pub replica_assignment: Vec<PartitionReplicaAssignment>,
    pub configs: Vec<TopicConfig>,
}

#[derive(Default, Debug)]
pub struct PartitionReplicaAssignment {
    pub partition: i32,
    pub broker_ids: Vec<i32>,
}

#[derive(Default, Debug)]
pub struct TopicConfig {
    pub config_name: String,
    pub config_value: String,
}

impl<'a> CreateTopicsRequest<'a> {
    pub fn new(correlation_id: i32, client_id: &'a str, timeout: i32) -> CreateTopicsRequest<'a> {
        CreateTopicsRequest {
            header: HeaderRequest::new(
                API_KEY_CREATE_TOPICS,
                API_VERSION,
                correlation_id,
                client_id,
            ),
            topic_requests: vec![],
            timeout,
        }
    }

    pub fn add(&mut self, topic: &'a str, num_partitions: i32, replication_factor: i16) {
        self.topic_requests.push(TopicCreateTopicsRequest::new(
            topic,
            num_partitions,
            replication_factor,
        ));
    }
}

impl<'a> TopicCreateTopicsRequest<'a> {
    #[must_use]
    pub fn new(topic: &'a str, num_partitions: i32, replication_factor: i16) -> Self {
        TopicCreateTopicsRequest {
            topic,
            num_partitions,
            replication_factor,
            replica_assignment: Vec::new(),
            configs: Vec::new(),
        }
    }
}

impl ToByte for CreateTopicsRequest<'_> {
    fn encode<W: Write>(&self, buffer: &mut W) -> Result<()> {
        try_multi!(
            self.header.encode(buffer),
            self.topic_requests.encode(buffer),
            self.timeout.encode(buffer)
        )
    }
}

impl ToByte for TopicCreateTopicsRequest<'_> {
    fn encode<W: Write>(&self, buffer: &mut W) -> Result<()> {
        try_multi!(
            self.topic.encode(buffer),
            self.num_partitions.encode(buffer),
            self.replication_factor.encode(buffer),
            self.replica_assignment.encode(buffer),
            self.configs.encode(buffer)
        )
    }
}

impl ToByte for PartitionReplicaAssignment {
    fn encode<W: Write>(&self, buffer: &mut W) -> Result<()> {
        try_multi!(self.partition.encode(buffer), self.broker_ids.encode(buffer))
    }
}

impl ToByte for TopicConfig {
    fn encode<W: Write>(&self, buffer: &mut W) -> Result<()> {
        try_multi!(
            self.config_name.encode(buffer),
            self.config_value.encode(buffer)
        )
    }
}

// --------------------------------------------------------------------

#[derive(Default, Debug)]
pub struct CreateTopicsResponse {
    pub header: HeaderResponse,
    pub topic_errors: Vec<TopicCreateTopicsResponse>,
}

#[derive(Default, Debug)]
pub struct TopicCreateTopicsResponse {
    pub topic: String,
    pub error_code: i16,
}

impl FromByte for CreateTopicsResponse {
    type R = CreateTopicsResponse;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try_multi!(self.header.decode(buffer), self.topic_errors.decode(buffer))
    }
}

impl FromByte for TopicCreateTopicsResponse {
    type R = TopicCreateTopicsResponse;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try_multi!(self.topic.decode(buffer), self.error_code.decode(buffer))
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use crate::codecs::{FromByte, ToByte};

    use super::{CreateTopicsRequest, CreateTopicsResponse};

    #[test]
    fn test_encode_create_topics_request() {
        let mut req = CreateTopicsRequest::new(42, "client-a", 3_000);
        req.add("topic-a", 3, 1);

        let mut buf = Vec::new();
        req.encode(&mut buf).unwrap();

        assert_eq!(&buf[0..2], &19_i16.to_be_bytes());
        assert_eq!(&buf[2..4], &0_i16.to_be_bytes());
        assert_eq!(&buf[4..8], &42_i32.to_be_bytes());
        assert!(buf.len() > 20);
    }

    #[test]
    fn test_decode_create_topics_response() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&7_i32.to_be_bytes());
        bytes.extend_from_slice(&1_i32.to_be_bytes());
        bytes.extend_from_slice(&7_i16.to_be_bytes());
        bytes.extend_from_slice(b"topic-a");
        bytes.extend_from_slice(&0_i16.to_be_bytes());

        let resp = CreateTopicsResponse::decode_new(&mut Cursor::new(bytes)).unwrap();
        assert_eq!(resp.header.correlation, 7);
        assert_eq!(resp.topic_errors.len(), 1);
        assert_eq!(resp.topic_errors[0].topic, "topic-a");
        assert_eq!(resp.topic_errors[0].error_code, 0);
    }
}
