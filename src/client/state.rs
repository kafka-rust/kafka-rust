use std::collections::hash_map::{Entry, HashMap, Keys};
use std::convert::AsRef;
use std::slice;

use crate::protocol;

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

    // ~ a mapping of groups to their coordinators
    group_coordinators: HashMap<String, BrokerRef>,
}

// --------------------------------------------------------------------

// ~ note: this type is re-exported to the crate's public api through
// client::metadata
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
    /// Retrieves the `node_id` of this broker as identified with the
    /// remote Kafka cluster.
    #[inline]
    #[must_use]
    pub fn id(&self) -> i32 {
        self.node_id
    }

    /// Retrieves the host:port of the this Kafka broker.
    #[inline]
    #[must_use]
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
// ~ The value `UNKNOWN_BROKER_INDEX` is artificial and represents an
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

    fn index(self) -> usize {
        self._index as usize
    }

    fn set(&mut self, other: BrokerRef) {
        if self._index != other._index {
            self._index = other._index;
        }
    }

    fn set_unknown(&mut self) {
        self.set(BrokerRef::new(UNKNOWN_BROKER_INDEX));
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
    /// Creates a new partitions vector with all partitions leaderless
    fn new_with_partitions(n: usize) -> TopicPartitions {
        TopicPartitions {
            partitions: (0..n).map(|_| TopicPartition::new()).collect(),
        }
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

    pub fn iter(&self) -> TopicPartitionIter<'_> {
        self.into_iter()
    }
}

impl<'a> IntoIterator for &'a TopicPartitions {
    type Item = (i32, &'a TopicPartition);
    type IntoIter = TopicPartitionIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        TopicPartitionIter {
            partition_id: 0,
            iter: self.partitions.iter(),
        }
    }
}

/// Metadata for a single topic partition.
#[derive(Debug)]
pub struct TopicPartition {
    broker: BrokerRef,
}

impl TopicPartition {
    fn new() -> TopicPartition {
        TopicPartition {
            broker: BrokerRef::new(UNKNOWN_BROKER_INDEX),
        }
    }

    pub fn broker<'a>(&self, state: &'a ClientState) -> Option<&'a Broker> {
        state.brokers.get(self.broker.index())
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

// ~ note: this type is re-exported to the crate's public api through
// client::metadata
/// An iterator over the topic names.
pub struct TopicNames<'a> {
    iter: Keys<'a, String, TopicPartitions>,
}

impl<'a> Iterator for TopicNames<'a> {
    type Item = &'a str;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(AsRef::as_ref)
    }
}

impl Default for ClientState {
    fn default() -> Self {
        Self::new()
    }
}

impl ClientState {
    pub fn new() -> Self {
        ClientState {
            correlation: 0,
            brokers: Vec::new(),
            topic_partitions: HashMap::new(),
            group_coordinators: HashMap::new(),
        }
    }

    pub fn num_topics(&self) -> usize {
        self.topic_partitions.len()
    }

    pub fn contains_topic(&self, topic: &str) -> bool {
        self.topic_partitions.contains_key(topic)
    }

    pub fn contains_topic_partition(&self, topic: &str, partition_id: i32) -> bool {
        self.topic_partitions
            .get(topic)
            .map(|tp| tp.partition(partition_id))
            .is_some()
    }

    pub fn topic_names(&self) -> TopicNames<'_> {
        TopicNames {
            iter: self.topic_partitions.keys(),
        }
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

    /// Loads new and updates existing metadata from the given
    /// metadata response.
    pub fn update_metadata(&mut self, md: protocol::MetadataResponse) {
        debug!("updating metadata from: {:?}", md);

        // ~ register new brokers with self.brokers and obtain an
        // index over them by broker-node-id
        let brokers = self.update_brokers(&md);

        // ~ now update partitions
        for t in md.topics {
            // ~ get a mutable reference to the partitions vector
            // (maintained in self.topic_partitions) for the topic
            let tps = match self.topic_partitions.entry(t.topic) {
                Entry::Occupied(e) => {
                    let ps = &mut e.into_mut().partitions;
                    match (ps.len(), t.partitions.len()) {
                        (n, m) if n > m => ps.truncate(m),
                        (n, m) if n < m => {
                            ps.reserve(m);
                            for _ in 0..(m - n) {
                                ps.push(TopicPartition::new());
                            }
                        }
                        _ => {}
                    }
                    ps
                }
                Entry::Vacant(e) => {
                    &mut e
                        .insert(TopicPartitions::new_with_partitions(t.partitions.len()))
                        .partitions
                }
            };
            // ~ sync the partitions vector with the new information
            for partition in t.partitions {
                let tp = &mut tps[partition.id as usize];
                if let Some(bref) = brokers.get(&partition.leader) {
                    tp.broker.set(*bref);
                } else {
                    tp.broker.set_unknown();
                }
            }
        }
        // Ok(())
    }

    /// Updates self.brokers from the given metadata returning an
    /// index `NodeId -> BrokerRef`
    fn update_brokers(&mut self, md: &protocol::MetadataResponse) -> HashMap<i32, BrokerRef> {
        // ~ build an index of the already loaded brokers -- if any
        let mut brokers = HashMap::with_capacity(self.brokers.len() + md.brokers.len());
        for (i, broker) in (0u32..).zip(self.brokers.iter()) {
            brokers.insert(broker.node_id, BrokerRef::new(i));
        }

        // ~ now add new brokers or updated existing ones while
        // keeping the above 'broker' index up-to-date
        for broker in &md.brokers {
            let broker_host = format!("{}:{}", broker.host, broker.port);
            match brokers.entry(broker.node_id) {
                Entry::Occupied(e) => {
                    // ~ verify our information of the already tracked
                    // broker is up-to-date
                    let bref = *e.get();
                    let b = &mut self.brokers[bref.index()];
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
        brokers
    }

    /// ~ Retrieves the host:port of the coordinator for the specified
    /// group - if any.
    pub fn group_coordinator<'a>(&'a self, group: &str) -> Option<&'a str> {
        self.group_coordinators
            .get(group)
            .and_then(|b| self.brokers.get(b.index()))
            .map(|b| &b.host[..])
    }

    /// ~ Removes the current coordinator - if any - for the specified
    /// group.
    pub fn remove_group_coordinator(&mut self, group: &str) {
        self.group_coordinators.remove(group);
    }

    /// ~ Updates the coordinator for the specified group and returns
    /// the coordinator host as if `group_coordinator` would have
    /// been called subsequently.
    pub fn set_group_coordinator<'a>(
        &'a mut self,
        group: &str,
        gc: &protocol::GroupCoordinatorResponse,
    ) -> &'a str {
        debug!(
            "set_group_coordinator: registering coordinator for '{}': {:?}",
            group, gc
        );

        let group_host = format!("{}:{}", gc.host, gc.port);
        // ~ try to find an already existing broker
        let mut broker_ref = BrokerRef::new(UNKNOWN_BROKER_INDEX);
        for (i, broker) in (0u32..).zip(self.brokers.iter()) {
            if gc.broker_id == broker.node_id {
                if group_host != broker.host {
                    warn!(
                        "set_group_coordinator: coord_host({}) != broker_host({}) for \
                           broker_id({})!",
                        group_host, broker.host, broker.node_id
                    );
                }
                broker_ref._index = i;
                break;
            }
        }
        // ~ if not found, add it to the list of known brokers
        if broker_ref._index == UNKNOWN_BROKER_INDEX {
            broker_ref._index = self.brokers.len() as u32;
            self.brokers.push(Broker {
                node_id: gc.broker_id,
                host: group_host,
            });
        }
        if let Some(br) = self.group_coordinators.get_mut(group) {
            if br._index != broker_ref._index {
                br._index = broker_ref._index;
            }
        }
        self.group_coordinators.insert(group.to_owned(), broker_ref);
        &self.brokers[broker_ref.index()].host
    }
}

#[cfg(test)]
mod tests {
    use super::ClientState;
    use crate::protocol;
    use crate::protocol::metadata as md;

    fn new_partition(id: i32, leader: i32) -> md::PartitionMetadata {
        md::PartitionMetadata {
            error: 0,
            id,
            leader,
            replicas: vec![],
            isr: vec![],
        }
    }

    /// Utility to sort the given vector and return it.
    fn sorted<O: Ord>(mut xs: Vec<O>) -> Vec<O> {
        xs.sort();
        xs
    }

    // mock data for an initial kafka metadata response
    fn metadata_response_initial() -> protocol::MetadataResponse {
        protocol::MetadataResponse {
            header: protocol::HeaderResponse { correlation: 1 },
            brokers: vec![
                md::BrokerMetadata {
                    node_id: 10,
                    host: "gin1.dev".to_owned(),
                    port: 1234,
                },
                md::BrokerMetadata {
                    node_id: 50,
                    host: "gin2.dev".to_owned(),
                    port: 9876,
                },
                md::BrokerMetadata {
                    node_id: 30,
                    host: "gin3.dev".to_owned(),
                    port: 9092,
                },
            ],
            topics: vec![
                md::TopicMetadata {
                    error: 0,
                    topic: "tee-one".to_owned(),
                    partitions: vec![
                        new_partition(0, 50),
                        new_partition(1, 10),
                        new_partition(2, 30),
                        new_partition(3, -1),
                        new_partition(4, 50),
                    ],
                },
                md::TopicMetadata {
                    error: 0,
                    topic: "tee-two".to_owned(),
                    partitions: vec![
                        new_partition(0, 30),
                        new_partition(1, -1),
                        new_partition(2, -1),
                        new_partition(3, 10),
                    ],
                },
                md::TopicMetadata {
                    error: 0,
                    topic: "tee-three".to_owned(),
                    partitions: vec![],
                },
            ],
        }
    }

    fn assert_partitions(
        state: &ClientState,
        topic: &str,
        expected: &[(i32, Option<(i32, &str)>)],
    ) {
        let partitions = state.partitions_for(topic).unwrap();
        assert_eq!(expected.len(), partitions.len());
        assert_eq!(expected.is_empty(), partitions.is_empty());
        assert_eq!(
            expected,
            &partitions
                .iter()
                .map(|(id, tp)| {
                    let broker = tp.broker(state).map(|b| (b.id(), b.host()));
                    // ~ verify that find_broker delivers the same information
                    assert_eq!(broker.map(|b| b.1), state.find_broker(topic, id));
                    (id, broker)
                })
                .collect::<Vec<_>>()[..]
        );
    }

    fn assert_initial_metadata_load(state: &ClientState) {
        assert_eq!(
            vec!["tee-one", "tee-three", "tee-two"],
            sorted(state.topic_names().collect::<Vec<_>>())
        );
        assert_eq!(3, state.num_topics());

        assert_eq!(true, state.contains_topic("tee-one"));
        assert!(state.partitions_for("tee-one").is_some());

        assert_eq!(true, state.contains_topic("tee-two"));
        assert!(state.partitions_for("tee-two").is_some());

        assert_eq!(true, state.contains_topic("tee-three"));
        assert!(state.partitions_for("tee-three").is_some());

        assert_eq!(false, state.contains_topic("foobar"));
        assert!(state.partitions_for("foobar").is_none());

        assert_partitions(
            state,
            "tee-one",
            &[
                (0, Some((50, "gin2.dev:9876"))),
                (1, Some((10, "gin1.dev:1234"))),
                (2, Some((30, "gin3.dev:9092"))),
                (3, None),
                (4, Some((50, "gin2.dev:9876"))),
            ],
        );
        assert_partitions(
            state,
            "tee-two",
            &[
                (0, Some((30, "gin3.dev:9092"))),
                (1, None),
                (2, None),
                (3, Some((10, "gin1.dev:1234"))),
            ],
        );
        assert_partitions(state, "tee-three", &[]);
    }

    fn metadata_response_update() -> protocol::MetadataResponse {
        protocol::MetadataResponse {
            header: protocol::HeaderResponse { correlation: 2 },
            brokers: vec![
                md::BrokerMetadata {
                    node_id: 10,
                    host: "gin1.dev".to_owned(),
                    port: 1234,
                },
                // note: compared to the initial metadata
                // response this broker moved to a different
                // machine
                md::BrokerMetadata {
                    node_id: 50,
                    host: "aladin1.dev".to_owned(),
                    port: 9091,
                },
                md::BrokerMetadata {
                    node_id: 30,
                    host: "gin3.dev".to_owned(),
                    port: 9092,
                },
            ],
            // metadata for topic "tee-two" only
            topics: vec![md::TopicMetadata {
                error: 0,
                topic: "tee-two".to_owned(),
                partitions: vec![
                    new_partition(0, 10),
                    new_partition(1, 10),
                    new_partition(2, 50),
                    new_partition(3, -1),
                    new_partition(4, 30),
                ],
            }],
        }
    }

    fn assert_updated_metadata_load(state: &ClientState) {
        assert_eq!(
            vec!["tee-one", "tee-three", "tee-two"],
            sorted(state.topic_names().collect::<Vec<_>>())
        );
        assert_eq!(3, state.num_topics());

        assert_eq!(true, state.contains_topic("tee-one"));
        assert!(state.partitions_for("tee-one").is_some());

        assert_eq!(true, state.contains_topic("tee-two"));
        assert!(state.partitions_for("tee-two").is_some());

        assert_eq!(true, state.contains_topic("tee-three"));
        assert!(state.partitions_for("tee-three").is_some());

        assert_eq!(false, state.contains_topic("foobar"));
        assert!(state.partitions_for("foobar").is_none());

        assert_partitions(
            state,
            "tee-one",
            &[
                (0, Some((50, "aladin1.dev:9091"))),
                (1, Some((10, "gin1.dev:1234"))),
                (2, Some((30, "gin3.dev:9092"))),
                (3, None),
                (4, Some((50, "aladin1.dev:9091"))),
            ],
        );
        assert_partitions(
            state,
            "tee-two",
            &[
                (0, Some((10, "gin1.dev:1234"))),
                (1, Some((10, "gin1.dev:1234"))),
                (2, Some((50, "aladin1.dev:9091"))),
                (3, None),
                (4, Some((30, "gin3.dev:9092"))),
            ],
        );
        assert_partitions(state, "tee-three", &[]);
    }

    #[test]
    fn test_loading_metadata() {
        let mut state = ClientState::new();
        // Test loading metadata into a new, empty client state.
        state.update_metadata(metadata_response_initial());
        assert_initial_metadata_load(&state);

        // Test loading a metadata update into a client state with
        // already some initial metadata loaded.
        state.update_metadata(metadata_response_update());
        assert_updated_metadata_load(&state);
    }
}
