//! Types related to topic metadata for introspection by clients.
//! Example: `KafkaClient::topics()`.

use std::collections::hash_map;
use std::fmt;

use super::KafkaClient;
use super::state::{ClientState, TopicPartitions, TopicPartitionIter, TopicPartition};

// public re-export
pub use super::state::Broker;
pub use super::state::TopicNames;

/// A view on the loaded metadata about topics and their partitions.
pub struct Topics<'a> {
    state: &'a ClientState,
}

impl<'a> Topics<'a> {
    /// Constructs a view of the currently loaded topic metadata from
    /// the specified kafka client.
    #[inline]
    pub fn new(client: &KafkaClient) -> Topics {
        Topics { state: &client.state }
    }

    /// Retrieves the number of the underlying topics.
    #[inline]
    pub fn len(&self) -> usize {
        self.state.num_topics()
    }

    /// Provides an iterator over the underlying topics.
    #[inline]
    pub fn iter(&'a self) -> TopicIter<'a> {
        TopicIter::new(self.state)
    }

    /// A conveniece method to return an iterator over the topics'
    /// names.
    #[inline]
    pub fn names(&'a self) -> TopicNames<'a> {
        self.state.topic_names()
    }

    /// A convenience method to determine whether the specified topic
    /// is known.
    #[inline]
    pub fn contains(&'a self, topic: &str) -> bool {
        self.state.contains_topic(topic)
    }

    /// Retrieves the partitions of a specified topic.
    #[inline]
    pub fn partitions(&'a self, topic: &str) -> Option<Partitions<'a>> {
        self.state.partitions_for(topic).map(|tp| {
            Partitions {
                state: self.state,
                tp: tp,
            }
        })
    }
}

impl<'a> fmt::Debug for Topics<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        try!(write!(f, "Topics {{ topics: ["));
        let mut ts = self.iter();
        if let Some(t) = ts.next() {
            try!(write!(f, "{:?}", t));
        }
        for t in ts {
            try!(write!(f, ", {:?}", t));
        }
        write!(f, "] }}")
    }
}

impl<'a> IntoIterator for &'a Topics<'a> {
    type Item = Topic<'a>;
    type IntoIter = TopicIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a> IntoIterator for Topics<'a> {
    type Item = Topic<'a>;
    type IntoIter = TopicIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        TopicIter::new(self.state)
    }
}

/// An interator over topics.
pub struct TopicIter<'a> {
    state: &'a ClientState,
    iter: hash_map::Iter<'a, String, TopicPartitions>,
}

impl<'a> TopicIter<'a> {
    fn new(state: &'a ClientState) -> TopicIter<'a> {
        TopicIter {
            state: state,
            iter: state.topic_partitions().iter(),
        }
    }
}

impl<'a> Iterator for TopicIter<'a> {
    type Item = Topic<'a>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|(name, tps)| {
            Topic {
                state: self.state,
                name: &name[..],
                tp: tps,
            }
        })
    }
}

/// A view on the loaded metadata for a particular topic.
pub struct Topic<'a> {
    state: &'a ClientState,
    name: &'a str,
    tp: &'a TopicPartitions,
}

impl<'a> Topic<'a> {
    /// Retrieves the name of this topic.
    #[inline]
    pub fn name(&self) -> &str {
        self.name
    }

    /// Retrieves the list of all partitions for this topic.
    #[inline]
    pub fn partitions(&self) -> Partitions<'a> {
        Partitions {
            state: self.state,
            tp: self.tp,
        }
    }
}

impl<'a> fmt::Debug for Topic<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Topic {{ name: {}, partitions: {:?} }}", self.name, self.partitions())
    }
}

/// Metadata relevant to partitions of a particular topic.
pub struct Partitions<'a> {
    state: &'a ClientState,
    tp: &'a TopicPartitions,
}

impl<'a> Partitions<'a> {
    /// Retrieves the number of the topic's partitions.
    #[inline]
    pub fn len(&self) -> usize {
        self.tp.len()
    }

    /// Tests for `.len() > 0`.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.tp.is_empty()
    }

    /// Retrieves an iterator of the partitions of the underlying topic.
    #[inline]
    pub fn iter(&self) -> PartitionIter<'a> {
        PartitionIter::new(self.state, self.tp)
    }

    /// Finds a specified partition identified by its id.
    #[inline]
    pub fn partition(&self, partition_id: i32) -> Option<Partition<'a>> {
        self.tp.partition(partition_id).map(|p| {
            Partition::new(self.state, p, partition_id)
        })
    }

    /// Convenience method to retrieve the identifiers of all
    /// currently "available" partitions.  Such partitions are known
    /// to have a leader broker and can be sent messages to.
    #[inline]
    pub fn available_ids(&self) -> Vec<i32> {
        self.tp
            .iter()
            .filter_map(|(id, p)| p.broker(self.state).map(|_| id))
            .collect()
    }
}

impl<'a> fmt::Debug for Partitions<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        try!(write!(f, "Partitions {{ ["));
        let mut ps = self.iter();
        if let Some(p) = ps.next() {
            try!(write!(f, "{:?}", p));
        }
        for p in ps {
            try!(write!(f, ", {:?}", p));
        }
        write!(f, "] }}")
    }
}

impl<'a> IntoIterator for &'a Partitions<'a> {
    type Item = Partition<'a>;
    type IntoIter = PartitionIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a> IntoIterator for Partitions<'a> {
    type Item = Partition<'a>;
    type IntoIter = PartitionIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        PartitionIter::new(self.state, self.tp)
    }
}

/// An interator over a topic's partitions.
pub struct PartitionIter<'a> {
    state: &'a ClientState,
    iter: TopicPartitionIter<'a>,
}

impl<'a> PartitionIter<'a> {
    fn new(state: &'a ClientState, tp: &'a TopicPartitions) -> Self {
        PartitionIter {
            state: state,
            iter: tp.iter(),
        }
    }
}

impl<'a> Iterator for PartitionIter<'a> {
    type Item = Partition<'a>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(
            |(id, p)| Partition::new(self.state, p, id),
        )
    }
}

/// Metadata about a particular topic partition.
///
/// A partition can be seen as either available or not by
/// `kafka-rust`.  "Available" partitions are partitions with an
/// assigned leader broker and can be send messages to or fetched
/// messages from.  Non-available partitions are ignored by
/// `kafka-rust`.  Whether or not a partition is currently "available"
/// can be determined by testing for `partition.leader().is_some()` or
/// more directly through `partition.is_available()`.
pub struct Partition<'a> {
    state: &'a ClientState,
    partition: &'a TopicPartition,
    id: i32,
}

impl<'a> Partition<'a> {
    fn new(state: &'a ClientState, partition: &'a TopicPartition, id: i32) -> Partition<'a> {
        Partition {
            state: state,
            partition: partition,
            id: id,
        }
    }

    /// Retrieves the identifier of this topic partition.
    #[inline]
    pub fn id(&self) -> i32 {
        self.id
    }

    /// Retrieves the current leader broker of this partition - if
    /// any.  A partition with a leader is said to be "available".
    #[inline]
    pub fn leader(&self) -> Option<&'a Broker> {
        self.partition.broker(self.state)
    }

    /// Determines whether this partition is currently "available".
    /// See `Partition::leader()`.
    pub fn is_available(&self) -> bool {
        self.leader().is_some()
    }
}

impl<'a> fmt::Debug for Partition<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Partition {{ id: {}, leader: {:?} }}", self.id(), self.leader())
    }
}
