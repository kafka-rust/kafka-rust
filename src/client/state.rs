use std::u32;
use std::collections::hash_map::{HashMap, Entry};
use std::slice;

use error::Result;
use protocol;

#[derive(Debug)]
pub struct ClientState {
    // ~ the last correlation used when communicating with kafka
    // (see `#next_correlation_id`)
    correlation: i32,

    // ~ a list of known brokers referred to by the index in this
    // vector.  This index is also referred to as `BrokerRef` and is
    // enforced by this module.
    //
    // Note: loading of additional topic metadata must preserve
    // already present brokers in this vector at their position.
    // See `ClientState::update_metadata`
    brokers: Vec<Broker>,

    // ~ a mapping of topic to information about its partitions
    topic_partitions: HashMap<String, TopicPartitions>,
}

// --------------------------------------------------------------------

// ~ note: this type is re-exported to the crates public api through client::metadata
/// Describes a Kafka broker node `kafka-rust` is communicating with.
#[derive(Debug)]
pub struct Broker {
    /// The identifier of this broker as understood in a Kafka
    /// cluster.
    node_id: i32,
    /// "host:port" of this broker. This information is advertised by
    /// and originating from Kafka cluster itself.
    host: String,
}

impl Broker {
    /// Retrives the node_id of this broker as identified with the
    /// remote Kafka cluster.
    #[inline]
    pub fn id(&self) -> i32 {
        self.node_id
    }

    /// Retrieves the host:port of the this Kafka broker.
    #[inline]
    pub fn host(&self) -> &str {
        &self.host
    }
}

// See `Brokerref`
const UNKNOWN_BROKER_INDEX: u32 = u32::MAX;

/// ~ A custom identifier for a broker.  This type hides the fact that
/// a `TopicPartition` references a `Broker` indirectly, loosely
/// through an index, thereby being able to share broker data without
/// having to fallback to `Rc` or `Arc` or otherwise fighting the
/// borrowck.
// ~ The value `UNKNOWN_BROKER_INDEX` is artificial and represents a
// index to an unknown broker (aka the null value.) Code indexing
// `self.brokers` using a `BrokerRef` _must_ check against this
// constant and/or treat it conditionally.
#[derive(Debug, Copy, Clone)]
pub struct BrokerRef {
    _index: u32,
}

impl BrokerRef {
    // ~ private constructor on purpose
    fn new(index: u32) -> Self {
        BrokerRef { _index: index }
    }
}

// --------------------------------------------------------------------

/// A representation of partitions for a single topic.
#[derive(Debug)]
pub struct TopicPartitions {
    // ~ This list keeps information about each partition of the
    // corresponding topic - even about partitions currently without a
    // leader.  The index into this list specifies the partition
    // identifier.  (This works due to Kafka numbering partitions 0..N
    // where N is the number of partitions of the topic.)
    partitions: Vec<TopicPartition>,
}

impl TopicPartitions {
    fn new(partitions: Vec<TopicPartition>) -> TopicPartitions {
        TopicPartitions { partitions: partitions }
    }

    pub fn len(&self) -> usize {
        self.partitions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.partitions.is_empty()
    }

    pub fn partition(&self, partition_id: i32) -> Option<&TopicPartition> {
        self.partitions.get(partition_id as usize)
    }

    pub fn iter(&self) -> TopicPartitionIter {
        self.into_iter()
    }
}

impl<'a> IntoIterator for &'a TopicPartitions {
    type Item = (i32, &'a TopicPartition);
    type IntoIter = TopicPartitionIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        TopicPartitionIter { partition_id: 0, iter: self.partitions.iter() }
    }
}

/// Metadata for a single topic partition.
#[derive(Debug)]
pub struct TopicPartition {
    broker: BrokerRef,
}

impl TopicPartition {
    fn new(broker: BrokerRef) -> TopicPartition {
        TopicPartition { broker: broker }
    }

    pub fn broker<'a>(&self, state: &'a ClientState) -> Option<&'a Broker> {
        state.brokers.get(self.broker._index as usize)
    }
}

/// An iterator over a topic's partitions.
pub struct TopicPartitionIter<'a> {
    iter: slice::Iter<'a, TopicPartition>,
    partition_id: i32,
}

impl<'a> Iterator for TopicPartitionIter<'a> {
    type Item = (i32, &'a TopicPartition);
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|tp| {
            let partition_id = self.partition_id;
            self.partition_id += 1;
            (partition_id, tp)
        })
    }
}

// --------------------------------------------------------------------

impl ClientState {
    pub fn new() -> Self {
        ClientState {
            correlation: 0,
            brokers: Vec::new(),
            topic_partitions: HashMap::new(),
        }
    }

    pub fn num_topics(&self) -> usize {
        self.topic_partitions.len()
    }

    pub fn contains_topic(&self, topic: &str) -> bool {
        self.topic_partitions.contains_key(topic)
    }

    // exposed for the sake of the metadata module
    pub fn topic_partitions(&self) -> &HashMap<String, TopicPartitions> {
        &self.topic_partitions
    }

    pub fn partitions_for<'a>(&'a self, topic: &str) -> Option<&'a TopicPartitions> {
        self.topic_partitions.get(topic)
    }

    pub fn next_correlation_id(&mut self) -> i32 {
        self.correlation = (self.correlation + 1) % (1i32 << 30);
        self.correlation
    }

    pub fn find_broker<'a>(&'a self, topic: &str, partition_id: i32) -> Option<&'a str> {
        self.topic_partitions
            .get(topic)
            .and_then(|tp| tp.partition(partition_id))
            .and_then(|p| p.broker(self))
            .map(|b| &b.host[..])
    }

    /// Clears all metadata.
    pub fn clear_metadata(&mut self) {
        // ~ important to clear both since one references the other
        // through `BrokerIndex`
        self.topic_partitions.clear();
        self.brokers.clear();
    }

    // XXX unit test loading metadata into a clear ClientState
    // XXX unit test loading metadata into a ClientState containing already some data

    /// Loads new and updates existing metadata from the given
    /// metadata response.
    pub fn update_metadata(&mut self, md: protocol::MetadataResponse) -> Result<()> {
        debug!("updating metadata from: {:?}", md);

        // ~ build an index of the already loaded brokers -- if any
        let mut brokers = HashMap::<i32, BrokerRef>::with_capacity(md.brokers.len() +
                                                                   self.brokers.len());
        for (i, broker) in (0u32..).zip(self.brokers.iter()) {
            brokers.insert(broker.node_id, BrokerRef::new(i));
        }

        // ~ now add new brokers or updated existing ones while
        // keeping the above 'broker' index up-to-date
        for broker in md.brokers {
            let broker_host = format!("{}:{}", broker.host, broker.port);
            match brokers.entry(broker.node_id) {
                Entry::Occupied(e) => {
                    // ~ verify our information of the already tracked
                    // broker is up-to-date
                    let bref = *e.get();
                    let b = &mut self.brokers[bref._index as usize];
                    if b.host != broker_host {
                        b.host = broker_host;
                    }
                }
                Entry::Vacant(e) => {
                    // ~ insert the new broker
                    let new_index = self.brokers.len();
                    self.brokers.push(Broker {
                        node_id: broker.node_id,
                        host: broker_host,
                    });
                    // ~ track the pushed broker's index
                    e.insert(BrokerRef::new(new_index as u32));
                }
            }
        }

        // ~ at this point all known brokers are registered with
        // self.brokers and in the local 'brokers' index

        for t in md.topics {
            // ~ create an ordered vector of partition information
            // where the index in the vector corresponds to the id of
            // the partition.id; see `TopicPartitions::partitions`
            let mut nparts: Vec<_> =
                (0 .. t.partitions.len())
                // ~ negative BrokerRef specifies "no broker" for
                // the corresponding partition
                .map(|_| TopicPartition::new(BrokerRef::new(UNKNOWN_BROKER_INDEX)))
                .collect();
            // ~ XXX can be more efficient here to re-use the already
            // existing vector in `self.topic_partitions` if the
            // lengths of them are the same
            for partition in t.partitions {
                match brokers.get(&partition.leader) {
                    Some(bref) => {
                        nparts[partition.id as usize].broker = *bref;
                    }
                    None => {
                        debug!("unknown leader {} for topic-partition: {}:{}",
                               partition.leader,
                               t.topic,
                               partition.id);
                    }
                }
            }
            // XXX see comment above
            self.topic_partitions.insert(t.topic, TopicPartitions::new(nparts));
        }
        Ok(())
    }
}
