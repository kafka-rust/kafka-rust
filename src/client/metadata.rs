//! Types related to topic metadata for introspection by clients.
//! Example: `KafkaClient::topics()`.

use std::collections::hash_map;
use std::slice;
use std::fmt;

use super::{KafkaClient, ClientState, TopicPartitions, TopicPartition};

/// A view on the loaded metadata about topics and their partitions.
pub struct Topics<'a> {
    state: &'a ClientState
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
        self.state.topic_partitions.len()
    }

    /// Provides an iterator over the known topics.
    #[inline]
    pub fn iter(&'a self) -> TopicIter<'a> {
        TopicIter::new(self.state)
    }

    /// A conveniece method to return an iterator the topics' names.
    #[inline]
    pub fn names(&'a self) -> TopicNames<'a> {
        TopicNames { iter: self.state.topic_partitions.keys() }
    }

    /// A convenience method to determine whether the specified topic
    /// is known.
    #[inline]
    pub fn contains(&'a self, topic: &str) -> bool {
        self.state.topic_partitions.contains_key(topic)
    }

    /// Retrieves the partitions of a known topic.
    #[inline]
    pub fn partitions(&'a self, topic: &str) -> Option<Partitions<'a>> {
        self.state.topic_partitions.get(topic).map(|tp| Partitions {
            state: self.state,
            tp: tp,
        })
    }

    /// Retrieves a snapshot/copy of the partition ids available for
    /// the specified topic.  Note that the returned copy may get out
    /// of date if the underlying client's metadata gets refreshed.
    #[inline]
    pub fn partition_ids(&'a self, topic: &str) -> Option<Vec<i32>> {
        self.state.topic_partitions.get(topic)
            .map(|tp| &tp.partitions)
            .map(|ps| ps.iter().map(|p| p.partition_id).collect())
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
    type Item=Topic<'a>;
    type IntoIter=TopicIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a> IntoIterator for Topics<'a> {
    type Item=Topic<'a>;
    type IntoIter=TopicIter<'a>;

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
            iter: state.topic_partitions.iter()
        }
    }
}

impl<'a> Iterator for TopicIter<'a> {
    type Item=Topic<'a>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|(name, tps) | Topic {
            state: self.state,
            name: &name[..],
            tp: tps,
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

    /// Retrieves the list of known partitions for this topic.
    #[inline]
    pub fn partitions(&self) -> Partitions<'a> {
        Partitions {
            state: self.state,
            tp: self.tp,
        }
    }

    /// Retrieves a snapshot/copy of the partition ids available for
    /// this topic.  Note that the returned copy may get out of date
    /// if the underlying client's metadata gets refreshed.
    #[inline]
    pub fn partition_ids(&'a self) -> Vec<i32> {
        self.tp.partitions.iter().map(|p| p.partition_id).collect()
    }
}

impl<'a> fmt::Debug for Topic<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Topic {{ name: {}, partitions: {:?} }}",
               self.name, self.partitions())
    }
}

/// Metadata relevant to partitions of a particular topic.
pub struct Partitions<'a> {
    state: &'a ClientState,
    tp: &'a TopicPartitions,
}

impl<'a> Partitions<'a> {
    /// Retrieves the number of the underlying partitions.
    #[inline]
    pub fn len(&self) -> usize {
        self.tp.partitions.len()
    }

    /// Tests for `.len() > 0`.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.tp.partitions.is_empty()
    }

    /// Retrieves an iterator of the partitions for the underlying topic.
    #[inline]
    pub fn iter(&self) -> PartitionIter<'a> {
        PartitionIter::new(self.state, self.tp.partitions.iter())
    }

    /// Finds a specified partition identified by its id.
    #[inline]
    pub fn partition(&self, partition_id: i32) -> Option<Partition<'a>> {
        self.tp.partition(partition_id).map(|p| Partition::new(self.state, p))
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
    type Item=Partition<'a>;
    type IntoIter=PartitionIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a> IntoIterator for Partitions<'a> {
    type Item=Partition<'a>;
    type IntoIter=PartitionIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        PartitionIter::new(self.state, self.tp.partitions.iter())
    }
}

/// An interator over a topic's partitions.
pub struct PartitionIter<'a> {
    state: &'a ClientState,
    iter: slice::Iter<'a, TopicPartition>
}

impl<'a> PartitionIter<'a> {
    fn new(state: &'a ClientState, iter: slice::Iter<'a, TopicPartition>) -> Self {
        PartitionIter { state: state, iter: iter }
    }
}

impl<'a> Iterator for PartitionIter<'a> {
    type Item=Partition<'a>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|tp| Partition::new(self.state, tp))
    }
}

/// Metadata about a particular topic partition.
pub struct Partition<'a> {
    state: &'a ClientState,
    partition: &'a TopicPartition,
}

impl<'a> Partition<'a> {
    fn new(state: &'a ClientState, partition: &'a TopicPartition) -> Partition<'a> {
        Partition { state: state, partition: partition }
    }

    /// Retrieves the identifier of this topic partition.
    #[inline]
    pub fn id(&self) -> i32 {
        self.partition.partition_id
    }

    /// Retrives the node_id of the current leader of this partition.
    #[inline]
    pub fn leader_id(&self) -> i32 {
        self.state.broker_by_partition(self.partition).node_id
    }

    /// Retrieves the host of the current leader of this partition.
    #[inline]
    pub fn leader_host(&self) -> &'a str {
        &self.state.broker_by_partition(self.partition).host
    }
}

impl<'a> fmt::Debug for Partition<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Partition {{ id: {}, leader_id: {}, leader_host: {} }}",
               self.id(), self.leader_id(), self.leader_host())
    }
}
