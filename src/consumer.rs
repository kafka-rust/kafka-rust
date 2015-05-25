//! Kafka Consumer
//!
//! A simple consumer based on KafkaClient. Accepts an instance of KafkaClient, a group and a
//! topic. Partitions can be specfied using builder pattern (Assumes all partitions if not
//! specfied).
//!
//! # Example
//!
//! ```no_run
//! let mut client = kafka::client::KafkaClient::new(vec!("localhost:9092".to_string()));
//! let res = client.load_metadata_all();
//! let con = kafka::consumer::Consumer::new(client, "test-group".to_string(), "my-topic".to_string())
//!             .partition(0);
//! for msg in con {
//!     println!("{:?}", msg);
//! }
//! ```
//!
//! Consumer auto-commits the offsets afer consuming COMMIT_INTERVAL messages (Currently set at
//! 100)
//!
//! Consumer implements Iterator.

use std::collections::HashMap;
use error::Result;
use utils::{TopicMessage, TopicPartitionOffset};
use client::KafkaClient;

const COMMIT_INTERVAL: i32 = 100; // Commit after every 100 message

#[derive(Default, Debug)]
pub struct Consumer {
    client: KafkaClient,
    group: String,
    topic: String,
    partitions: Vec<i32>,
    initialized: bool,
    messages: Vec<TopicMessage>,
    index: usize,
    offsets: HashMap<i32, i64>,
    consumed: i32
}

impl Consumer {
    /// Constructor
    ///
    /// Create a new consumer. Expects a KafkaClient, group, and topic as arguments.
    pub fn new(client: KafkaClient, group: String, topic: String) -> Consumer {
        Consumer{
            client: client,
            group: group,
            topic: topic,
            initialized: false,
            index: 0,
            ..Consumer::default()
        }
    }

    /// Set the partitions of this consumer.
    ///
    /// If this function is never called, all partitions are assumed.
    /// This function call be called multiple times to add more than 1 partitions.
    pub fn partition(mut self, partition: i32) -> Consumer {
        self.partitions.push(partition);
        self
    }

    fn commit_offsets(&mut self) -> Result<()> {
        let tpos = self.offsets.iter()
                        .map(|(p, o)| TopicPartitionOffset{
                                topic: self.topic.clone(),
                                partition: p.clone(),
                                offset: o.clone()
                            })
                        .collect();
        self.client.commit_offsets(self.group.clone(), tpos)
    }

    fn fetch_offsets(&mut self) -> Result<()> {
        let tpos = try!(self.client.fetch_group_topic_offset(self.group.clone(), self.topic.clone()));
        if self.partitions.len() == 0 {
            self.partitions = self.client.topic_partitions.get(&self.topic).unwrap_or(&vec!()).clone();
        }
        for tpo in tpos {
            if self.partitions.contains(&tpo.partition) {
                self.offsets.insert(tpo.partition, tpo.offset);
            }
        }
        Ok(())
    }

    fn make_request(&mut self) -> Result<()>{
        if ! self.initialized {
            try!(self.fetch_offsets());
        }
        let tpos = self.offsets.iter()
                        .map(|(p, o)| TopicPartitionOffset{
                                topic: self.topic.clone(),
                                partition: p.clone(),
                                offset: o.clone()
                            })
                        .collect();
        self.messages = try!(self.client.fetch_messages_multi(tpos));
        self.initialized = true;
        self.index = 0;
        Ok(())
    }
}

impl Iterator for Consumer {
    type Item = TopicMessage;

    fn next(&mut self) -> Option<TopicMessage> {
        if self.initialized {
            self.index += 1;
            self.consumed += 1;
            if self.consumed % COMMIT_INTERVAL == 0 {
                let _ = self.commit_offsets();
            }
            if self.index <= self.messages.len() {
                if self.messages[self.index-1].error.is_none() {
                    let curr = self.offsets.entry(self.messages[self.index-1].partition).or_insert(0);
                    *curr = *curr+1;
                    return Some(self.messages[self.index-1].clone());
                }
                return None;
            }
            let _ = self.commit_offsets();
            if self.messages.len() == 0 {
                return None;
            }
        }
        match self.make_request() {
            Err(_) => None,
            Ok(_) => self.next()
        }
    }
}
