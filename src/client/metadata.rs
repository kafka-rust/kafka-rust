//! Topics related metadata. See `KafkaClient::topics`.

use std::collections::hash_map::{self, HashMap};

use super::{KafkaClient, TopicPartitions};

/// An immutable view on the load metadata about topics and their partitions.
#[derive(Debug)]
pub struct Topics<'a> {
    topic_partitions: &'a HashMap<String, TopicPartitions>,
}

impl<'a> Topics<'a> {
    /// Constructs a view of the currently loaded topic metadata from
    /// the specified kafka client.
    pub fn new(client: &KafkaClient) -> Topics {
        Topics { topic_partitions: &client.state.topic_partitions }
    }

    /// Provides an iterator over the known topics.
    #[inline]
    pub fn iter(&'a self) -> TopicIter<'a> {
        TopicIter { iter: self.topic_partitions.iter() }
    }

    /// A conveniece method to return an iterator the topics' names.
    #[inline]
    pub fn names(&'a self) -> TopicNames<'a> {
        TopicNames { iter: self.topic_partitions.keys() }
    }

    /// A convenience method to determine whether the specified topic
    /// is known.
    pub fn contains(&'a self, topic: &str) -> bool {
        self.topic_partitions.contains_key(topic)
    }

    /// Retrieves the partitions of a known topic.
    pub fn partitions(&'a self, topic: &str) -> Option<&'a TopicPartitions> {
        self.topic_partitions.get(topic)
    }
}

/// An interator over the underlying topics' metadata.
pub struct TopicIter<'a> {
    iter: hash_map::Iter<'a, String, TopicPartitions>,
}

impl<'a> Iterator for TopicIter<'a> {
    type Item=Topic<'a>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|(name, tps) | Topic {
            name: &name[..],
            partitions: &tps,
        })
    }
}

/// An iterator over the names of topics known to the originating
/// kafka client.
pub struct TopicNames<'a> {
    iter: hash_map::Keys<'a, String, TopicPartitions>,
}

impl<'a> Iterator for TopicNames<'a> {
    type Item=&'a str;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|s| &s[..])
    }
}

#[test]
fn test_topics_names_iter() {
    let mut m = HashMap::new();
    m.insert("foo".to_owned(), TopicPartitions::new(vec![]));
    m.insert("bar".to_owned(), TopicPartitions::new(vec![]));

    let topics = Topics { topic_partitions: &m };
    let mut names: Vec<String> = topics.names().map(ToOwned::to_owned).collect();
    names.sort();
    assert_eq!(vec!["bar".to_owned(), "foo".to_owned()], names);
}

/// An immutable view on a topic.
pub struct Topic<'a> {
    name: &'a str,
    partitions: &'a TopicPartitions,
}

impl<'a> Topic<'a> {

    /// Retrieves the name of this topic.
    #[inline]
    pub fn name(&self) -> &str {
        self.name
    }

    /// Retrieves the list of known partitions for this topic.
    #[inline]
    pub fn partitions(&self) -> &'a TopicPartitions {
        self.partitions
    }
}
