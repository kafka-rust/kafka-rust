//! Kafka Consumer
//!
//! A simple consumer based on KafkaClient. Accepts an instance of KafkaClient, a group and a
//! topic. Partitions can be specified using builder pattern (Assumes all partitions if not
//! specified).
//!
//! # Example
//!
//! ```no_run
//! let mut client = kafka::client::KafkaClient::new(vec!("localhost:9092".to_owned()));
//! let res = client.load_metadata_all();
//! let con = kafka::consumer::Consumer::new(client, "test-group".to_owned(), "my-topic".to_owned())
//!             .partition(0)
//!             .fallback_offset(kafka::client::FetchOffset::Earliest);
//! for msg in con {
//!     println!("{:?}", msg);
//! }
//! ```
//!
//! Consumer auto-commits the offsets after consuming COMMIT_INTERVAL messages (Currently set at
//! 100)
//!
//! Consumer implements Iterator.

use std::collections::HashMap;
use error::{Error, Result, KafkaCode};
use utils::{TopicMessage, TopicPartitionOffset, TopicPartitionOffsetError, PartitionOffset};
use client::{KafkaClient, FetchOffset};

const COMMIT_INTERVAL: i32 = 100; // Commit after every 100 message

#[derive(Debug)]
pub struct Consumer {
    state: ConsumerState,
    client: KafkaClient,
}

#[derive(Default, Debug)]
struct ConsumerState {
    group: String,
    topic: String,
    partitions: Vec<i32>,
    initialized: bool,
    messages: Vec<TopicMessage>,
    index: usize,
    offsets: HashMap<i32, i64>,
    consumed: i32,

    fallback_offset: Option<FetchOffset>,
}

impl ConsumerState {
    fn commit_offsets(&mut self, client: &mut KafkaClient) -> Result<()> {
        client.commit_offsets(&self.group,
                              self.offsets.iter()
                              .map(|(p, o)| TopicPartitionOffset{
                                  topic: &self.topic,
                                  partition: *p,
                                  offset: *o
                              }))
    }

    fn fetch_offsets(&mut self, client: &mut KafkaClient) -> Result<()> {
        if self.partitions.is_empty() {
            // ~ fails if the underlygin topic is unkonwn to the given
            // client; this actually is what we want
            self.partitions = try!(client.topic_partitions(&self.topic))
                .map(|p| p.id())
                .collect();
        }

        // ~ fetch the so far commited group offsets
        let mut tpos = try!(client.fetch_group_offsets(&self.group, &self.topic));

        // ~ it might well that there were no group offsets committed
        // yet ... fallback to default offsets.
        try!(self.set_fallback_offsets(client, &mut tpos));

        // ~ now initialized from the fetched offsets
        for tpo in &tpos {
            if self.partitions.contains(&tpo.partition) {
                self.offsets.insert(tpo.partition, tpo.offset);
            }
        }
        Ok(())
    }

    /// Try setting the "fallback offsets" for all of `tpos` where
    /// `offset == -1`. Fails if retrieving the fallback offset is not
    /// possible for some reason for the affected elements from
    /// `tpos`.
    fn set_fallback_offsets(&mut self, client: &mut KafkaClient, tpos: &mut [TopicPartitionOffsetError])
                            -> Result<()>
    {
        // ~ it looks like kafka (0.8.2.1) is sending an error code
        // (even though it documents it won't: https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetFetchResponse)
        // so let's check only for `offset == -1` (and don't verify `error == None`)
        let has_missing_offs = tpos.iter().filter(|tpo| tpo.offset == -1).next().is_some();
        if has_missing_offs {
            // ~ if the user specified a "fallback offset" strategy
            // will try to fetch the current offset for that strategy
            // and start consuming starting at those
            if let Some(fallback_offset) = self.fallback_offset {
                // ~ now fetch the offset according to the specified strategy
                let new_offs = try!(client.fetch_topic_offset(&self.topic, fallback_offset));
                // ehm ... not really fast (O(n^2))
                for tpo in tpos.iter_mut() {
                    if tpo.offset == -1 {
                        if let Some(&PartitionOffset {offset: Ok(offset), ..})
                            = new_offs.iter().find(|pt| pt.partition == tpo.partition)
                        {
                            tpo.offset = offset;
                            tpo.error = None;
                        }
                    }
                }
            } else {
                // XXX might want to produce some log message and return a dedicated error code
                return Err(Error::Kafka(KafkaCode::Unknown));
            }
        }
        Ok(())
    }

    fn make_request(&mut self, client: &mut KafkaClient) -> Result<()> {
        if ! self.initialized {
            try!(self.fetch_offsets(client));
        }
        let msg =
            try!(client.fetch_messages_multi(
                self.offsets.iter()
                    .map(|(p, o)| TopicPartitionOffset::new(&self.topic, *p, *o))));
        self.messages = msg;
        self.initialized = true;
        self.index = 0;
        Ok(())
    }

    fn next_message(&mut self, client: &mut KafkaClient) -> Option<TopicMessage> {
        if self.initialized {
            self.index += 1;
            self.consumed += 1;
            if self.consumed % COMMIT_INTERVAL == 0 {
                let _ = self.commit_offsets(client);
            }
            if self.index <= self.messages.len() {
                if self.messages[self.index-1].message.is_ok() {
                    let curr = self.offsets.entry(self.messages[self.index-1].partition).or_insert(0);
                    *curr = *curr+1;
                    return Some(self.messages[self.index-1].clone());
                }
                return None;
            }
            let _ = self.commit_offsets(client);
            if self.messages.is_empty() {
                return None;
            }
        }
        match self.make_request(client) {
            Err(_) => None,
            Ok(_) => self.next_message(client)
        }
    }
}

impl Consumer {

    /// Constructor
    ///
    /// Create a new consumer. Expects a KafkaClient, group, and topic as arguments.
    pub fn new(client: KafkaClient, group: String, topic: String) -> Consumer {
        Consumer {
            state: ConsumerState {
                group: group,
                topic: topic,
                initialized: false,
                index: 0,
                .. ConsumerState::default()
            },
            client: client,
        }
    }

    /// Set the partitions of this consumer.
    ///
    /// If this function is never called, all partitions are assumed.
    /// This function call be called multiple times to add more than 1 partitions.
    pub fn partition(mut self, partition: i32) -> Consumer {
        self.state.partitions.push(partition);
        self
    }

    /// Specify the offset to use when none was committed for the
    /// underlying group yet.
    ///
    /// Running the underlying group for the first time against a
    /// topic results in the question where to start reading from the
    /// topic?  It might already contain a lot of messages.  Common
    /// strategies are starting at the earliest available message
    /// (thereby consuming whatever is currently in the topic) or at
    /// the latest one (thereby staring to consume only newly arriving
    /// messages.)  The parameter here corresponds to `time` in
    /// `KafkaClient::fetch_offsets`.
    ///
    /// Unless this method is called and there is no offset committed
    /// for the underlying group yet, this consumer will _not_ retrieve
    /// any messages from the underlying topic.
    pub fn fallback_offset(mut self, fallback_offset_time: FetchOffset) -> Consumer {
        self.state.fallback_offset = Some(fallback_offset_time);
        self
    }
}

impl Iterator for Consumer {
    type Item = TopicMessage;

    fn next(&mut self) -> Option<TopicMessage> {
        self.state.next_message(&mut self.client)
    }
}
