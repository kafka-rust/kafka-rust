/// Kafka Consumer
///

use error::Result;
use utils::TopicMessage;
use client::KafkaClient;


#[derive(Default, Debug)]
pub struct Consumer {
    client: KafkaClient,
    topic: String,
    partition: i32,
    offset: i64,
    initialized: bool,
    messages: Vec<TopicMessage>,
    index: usize
}

impl Consumer {
    pub fn new(client: KafkaClient, topic: String, partition: i32, offset: i64) -> Consumer {
        Consumer{
            client: client,
            topic: topic,
            partition: partition,
            offset: offset,
            initialized: false,
            messages: vec!(),
            index: 0
        }
    }

    fn make_request(&mut self) -> Result<()>{
        self.messages = try!(self.client.fetch_messages(self.topic.clone(), self.partition, self.offset));
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
            if self.index <= self.messages.len() {
                if self.messages[self.index-1].error.is_none() {
                    self.offset = self.messages[self.index-1].offset+1;
                    return Some(self.messages[self.index-1].clone());
                }
                return None;
            }
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
