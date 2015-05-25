/// Kafka Consumer
///

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
    pub fn new(client: KafkaClient) -> Consumer {
        Consumer{
            client: client,
            initialized: false,
            index: 0,
            ..Consumer::default()
        }
    }

    /// Set the group of this consumer.
    pub fn group<'a>(&'a mut self, group: String) -> &'a mut Consumer {
        self.group = group;
        self
    }

    /// Set the topic of this consumer.
    pub fn topic<'a>(&'a mut self, topic: String) -> &'a mut Consumer {
        self.topic = topic;
        self
    }

    /// Set the partitions of this consumer.
    pub fn partition<'a>(&'a mut self, partition: i32) -> &'a mut Consumer {
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
