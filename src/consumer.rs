//! Kafka Consumer
//!
//! A consumer for a single Kafka topic on behalf of a specified group
//! providing help in offset management.  The consumer can be
//! optionally advised to consume only particular partitions of the
//! underlying topic.
//!
//! # Example
//! ```no_run
//! use kafka::client::{KafkaClient, FetchOffset};
//! use kafka::consumer::Consumer;
//!
//! let mut client = KafkaClient::new(vec!("localhost:9092".to_owned()));
//! client.load_metadata_all().unwrap();
//! let mut consumer = Consumer::new(client, "my-group".to_owned(), "my-topic".to_owned())
//!                     .with_partitions(&[0, 1])
//!                     .with_fallback_offset(FetchOffset::Earliest);
//! loop {
//!   for ms in consumer.poll().unwrap().iter() {
//!     for m in ms.messages() {
//!       println!("{:?}", m);
//!     }
//!     consumer.consume_messageset(ms);
//!   }
//!   consumer.commit_consumed();
//! }
//! ```
//!
//! A `.poll()` will ask for the next available "chunk of data" for
//! client code to process.  The returned data are `MessageSet`s - at
//! most one for each partition of the consumed topic.
//!
//! The consumer helps in keeping track of already consumed messages
//! by maintaining a map of the consumed offsets.  Messages can be
//! told "consumed" either through `consume_message` or
//! `consume_messages` methods.  Once these consumed messages are
//! committed to Kafka using `commit_consumed`, the consumer will
//! start fetching messages from here after restart. Since committing
//! is a certain overhead, it is up to the client to decide the
//! frequency of the commits.  The consumer will *not* commit any
//! messages to Kafka automatically.

use std::collections::hash_map::{HashMap,Entry};
use std::slice;

use client::{KafkaClient, FetchOffset, Topics};
use error::{Error, KafkaCode, Result};
use utils::{TopicPartition, TopicPartitionOffset, FetchPartition};

// public re-exports
pub use client::fetch::Message;
use client::fetch::{FetchResponse, TopicFetchResponse, PartitionFetchResponse};

/// The Kafka Consumer
///
/// See module level documentation.
pub struct Consumer {
    client: KafkaClient,
    state: State,
    config: Config,
}

#[derive(Debug)]
struct State {
    /// Contains the offsets which to fetch next for each partition
    fetch_offsets: HashMap<i32, i64>, // XXX might want to change into a Vec<i64> where index denotes the partition_id

    /// Contains the offsets of messages marked as "consumed" (to be
    /// committed)
    consumed_offsets: HashMap<i32, i64>, // XXX might want to change into a Vec<i64> where index denotes the partition_id
    /// `true` iff the consumed_offsets contain data which needs to be
    /// committed. Set to `false` after commit.
    consumed_offsets_dirty: bool,
}

struct Config {
    group: String,
    topic: String,
    partitions: Vec<i32>,
    fallback_offset: Option<FetchOffset>,
}

impl Consumer {

    /// Create a new consumer over the specified managing offsets on
    /// behalf of the specified group.
    pub fn new(client: KafkaClient, group: String, topic: String) -> Consumer {
        Consumer {
            client: client,
            state: State::new(),
            config: Config {
                group: group,
                topic: topic,
                partitions: Vec::new(),
                fallback_offset: None,
            },
        }
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
    pub fn with_fallback_offset(mut self, fallback_offset_time: FetchOffset) -> Consumer {
        self.config.fallback_offset = Some(fallback_offset_time);
        // XXX potentially reinitialize offsets and pre-fetched data
        self
    }

    /// Explicitely specifies the partitions to consume. This will
    /// override any previous assignments.
    ///
    /// If this function is never called, all available partitions of
    /// the underlying topic will be consumed assumed.
    pub fn with_partitions(mut self, partition: &[i32]) -> Consumer {
        self.config.partitions.extend_from_slice(partition);
        // XXX might need to reinitialize offsets and potentially pre-fetched data
        self
    }

    /// Destroys this consumer returning back the underlying kafka client.
    pub fn client(self) -> KafkaClient {
        self.client
    }

    /// Poll for the next available message data.
    pub fn poll(&mut self) -> Result<MessageSets> {
        if !self.state.initialized() {
            try!(self.state.initialize(&self.config, &mut self.client));
            debug!("initialized: (topic: {} / group: {} / state: {:?})",
                   self.config.topic, self.config.group, self.state);
        }

        let resps = try!(self.fetch_messages());

        // XXX in future, immediately issue one more fetch_messages in
        // the background such that the next time the client polls
        // that request's response will likely be ready for
        // consumption

        let mut empty = true;
        for resp in &resps {
            for t in resp.topics() {
                for p in t.partitions() {

                    // XXX when a partition is empty but has a higher
                    // highwatermark-offset than the one we fetched
                    // from ... try to increase the max-fetch-size in
                    // the next fetch request

                    // XXX for now, as soon as a partition has an
                    // error we fail to prevent client programs from
                    // not noticing.  however, in future we don't need
                    // to fail immediately, we can try to recover from
                    // certain errors and retry the fetch operation
                    // transparently for the caller.

                    match p.data() {
                        &Err(ref e) => return Err(e.clone()),
                        &Ok(ref data) => {
                            if let Some(last_msg) = data.messages().last() {
                                empty = false;
                                self.state.fetch_offsets.insert(p.partition(), last_msg.offset + 1);
                            }
                        }
                    }
                }
            }
        }

        Ok(MessageSets{ responses: resps, empty: empty })
    }

    /// Retrieves the offset of the last "consumed" message in the
    /// specified partition. Results in `None` if there is no such
    /// "consumed" message for this consumer's group in the underlying
    /// topic.
    pub fn last_consumed_message(&self, partition: i32) -> Option<i64> {
        self.state.consumed_offsets.get(&partition).cloned()
    }

    /// Marks the message at the specified offset in the specified
    /// partition as consumed by the caller.
    ///
    /// Note: a message with a "later/higher" offset automatically
    /// marks all preceeding messages as "consumed", this is messages
    /// with "earlier/lower" offsets in the same partition.
    /// Therefore, it is not neccessary to invoke this method for
    /// every consumed message.
    pub fn consume_message(&mut self, partition: i32, offset: i64) {
        match self.state.consumed_offsets.entry(partition) {
            Entry::Vacant(v) => {
                v.insert(offset);
                self.state.consumed_offsets_dirty = true;
            }
            Entry::Occupied(mut v) => {
                let o = v.get_mut();
                if offset > *o {
                     *o = offset;
                    self.state.consumed_offsets_dirty = true;
                }
            }
        }
    }

    /// A convience method to mark the given message set consumed as a
    /// whole by the caller.  This is equivalent to marking the last
    /// message of the given set as consumed.
    pub fn consume_messageset<'a>(&mut self, msgs: MessageSet<'a>) {
        debug_assert_eq!(msgs.topic, self.config.topic);

        if !msgs.messages.is_empty() {
            self.consume_message(
                msgs.partition, msgs.messages.last().unwrap().offset);
        }
    }

    /// Persists the so-far "marked as consumed" messages (on behalf
    /// of this consumer's group for the underlying topic.)
    ///
    /// See also `Consumer::consume_message` and `Consumer::consume_messetset`.
    // XXX offer an async version as well
    pub fn commit_consumed(&mut self) -> Result<()> {
        if !self.state.consumed_offsets_dirty {
            debug!("no new consumed offsets to commit.");
            return Ok(());
        }

        let client = &mut self.client;
        let (group, topic) = (&self.config.group, &self.config.topic);
        let offsets = &self.state.consumed_offsets;
        debug!("commiting consumed offsets (topic: {} / group: {} / offsets: {:?}",
               topic, group, offsets);
        try!(client.commit_offsets(
            group,
            offsets.iter()
                .map(|(&p, &o)| TopicPartitionOffset {
                    topic: topic,
                    partition: p,
                    offset: o,
                })));
        self.state.consumed_offsets_dirty = false;
        Ok(())
    }

    fn fetch_messages(&mut self) -> Result<Vec<FetchResponse>> {
        let topic = &self.config.topic;
        let fetch_offsets = &self.state.fetch_offsets;
        debug!("fetching messages: (topic: {} / fetch-offsets: {:?})", topic, fetch_offsets);
        let reqs = fetch_offsets.iter().map(|(&p, &o)| FetchPartition::new(topic, p, o));
        self.client.fetch_messages(reqs)
    }
}

// --------------------------------------------------------------------

impl State {
    fn new() -> State {
        State {
            fetch_offsets: HashMap::new(),
            consumed_offsets: HashMap::new(),
            consumed_offsets_dirty: false,
        }
    }

    fn initialized(&self) -> bool {
        !self.fetch_offsets.is_empty()
    }

    fn initialize(&mut self, config: &Config, client: &mut KafkaClient) -> Result<()> {
        let partitions = try!(determine_partitions(config, client.topics()));
        let consumed_offsets = try!(load_consumed_offsets(config, client, &partitions));
        let fetch_offsets = try!(load_fetch_offsets(config, client, &partitions, &consumed_offsets));

        self.fetch_offsets = fetch_offsets;
        self.consumed_offsets = consumed_offsets;
        self.consumed_offsets_dirty = false;
        Ok(())
    }
}

/// Determines the partitions to be consumed according to the
/// specified configuration. Returns a ordered list of the determined
/// partition ids.
fn determine_partitions(config: &Config, metadata: Topics) -> Result<Vec<i32>> {
    let avail_partitions = match metadata.partitions(&config.topic) {
        // ~ fail if the underlying topic is unkonwn to the given client
        None => {
            debug!("no such topic: {} (all metadata: {:?})", config.topic, metadata);
            return Err(Error::Kafka(KafkaCode::UnknownTopicOrPartition));
        }
        Some(tp) => tp,
    };

    let ps = if config.partitions.is_empty() {
        // ~ no partitions configured ... use all available
        avail_partitions.iter().map(|p| p.id()).collect()
    } else {
        // ~ validate that all partitions we're going to consume are
        // available
        let mut ps: Vec<i32> = Vec::with_capacity(config.partitions.len());
        for &p in config.partitions.iter() {
            match avail_partitions.partition(p) {
                None => {
                    debug!("no such partition: {} (all metadata for {}: {:?})",
                           p, config.topic, avail_partitions.as_slice());
                    return Err(Error::Kafka(KafkaCode::UnknownTopicOrPartition));
                }
                Some(_) => ps.push(p),
            };
        }
        ps.sort();
        ps.dedup();
        ps
    };

    Ok(ps)
}

// Fetches the so-far commited/consumed offsets for the configured
// group/topic/partitions.
fn load_consumed_offsets(config: &Config, client: &mut KafkaClient, partitions: &[i32])
                         -> Result<HashMap<i32, i64>>
{
    assert!(!partitions.is_empty());

    let topic = &config.topic;
    let tpos = try!(client.fetch_group_offsets_multi(
        &config.group,
        partitions.iter().map(|&p_id | TopicPartition::new(topic, p_id))));

    let mut offs = HashMap::with_capacity(partitions.len());
    for tpo in tpos {
        if tpo.error.is_none() && tpo.offset != -1 {
            offs.insert(tpo.partition, tpo.offset);
        }
    }
    Ok(offs)
}

/// Fetches the "next fetch" offsets based on the specified
/// configuration and the given consumed offsets.
fn load_fetch_offsets(config: &Config,
                      client: &mut KafkaClient,
                      partitions: &[i32],
                      consumed_offsets: &HashMap<i32, i64>)
                      -> Result<HashMap<i32, i64>>
{
    fn load_partition_offsets(client: &mut KafkaClient, topic: &str, offset: FetchOffset)
                              -> Result<HashMap<i32, i64>> {
        let offs = try!(client.fetch_topic_offset(topic, offset));
        let mut m = HashMap::with_capacity(offs.len());
        for off in offs {
            m.insert(off.partition, off.offset.unwrap_or(-1));
        }
        Ok(m)
    }

    // fetch the earliest and latest available offsets
    let latest = try!(load_partition_offsets(client, &config.topic, FetchOffset::Latest));
    let earliest = try!(load_partition_offsets(client, &config.topic, FetchOffset::Earliest));

    // ~ for each partition if we have a consumed_offset verify it is
    // in the earliest/latest range and use that consumed_offset+1 as
    // the fetch_message.
    let mut fetch_offsets = HashMap::new();
    for p in partitions {
        let (&l_off, &e_off) = (latest.get(p).unwrap_or(&-1), earliest.get(p).unwrap_or(&-1));
        fetch_offsets.insert(
            *p, match consumed_offsets.get(p) {
                Some(&co) if co >= e_off && co < l_off => co + 1,
                _ => {
                    match config.fallback_offset {
                        Some(FetchOffset::Latest) => l_off,
                        Some(FetchOffset::Earliest) => e_off,
                        _ => {
                            debug!("cannot determine fetch offset (group: {} / topic: {} / partition: {})",
                                   &config.group, &config.topic, p);
                            return Err(Error::Kafka(KafkaCode::Unknown));
                        }
                    }
                }
            }
            );
    }
    Ok(fetch_offsets)
}

// --------------------------------------------------------------------

/// Messages retrieved from kafka in one fetch request.  This is a
/// concatenation of blocks of messages successfully retrieved from
/// the consumed topic partitions.  Each such partitions is guaranteed
/// to be present at most once in this structure.
pub struct MessageSets {
    responses: Vec<FetchResponse>,

    /// Precomputed; Says whether there are some messages or whether
    /// the responses actually contain consumeable messages
    empty: bool
}

impl MessageSets {
    /// Determines efficiently whether there are any consumeable
    /// messages in this data set.
    pub fn is_empty(&self) -> bool {
        self.empty
    }

    /// Iterates over the message sets delivering the fetched message
    /// data of consumed topic partitions.
    pub fn iter(&self) -> MessageSetsIter {
        let mut responses = self.responses.iter();
        let mut topics = responses.next().map(|r| r.topics().iter());
        let (curr_topic, partitions) =
            topics.as_mut()
            .and_then(|t| t.next())
            .map_or((None, None), |t| (Some(t.topic()), Some(t.partitions().iter())));
        MessageSetsIter {
            responses: responses,
            topics: topics,
            curr_topic: curr_topic.unwrap_or(""),
            partitions: partitions,
        }
    }
}

/// A set of messages succesfully retrieved from a specific topic
/// partition.
pub struct MessageSet<'a> {
    topic: &'a str,
    partition: i32,
    messages: &'a [Message<'a>],
}

impl<'a> MessageSet<'a> {
    #[inline]
    pub fn topic(&self) -> &'a str {
        self.topic
    }

    #[inline]
    pub fn partition(&self) -> i32 {
        self.partition
    }

    #[inline]
    pub fn messages(&self) -> &'a [Message<'a>] {
        self.messages
    }
}

/// An iterator over the consumed topic partition message sets.
pub struct MessageSetsIter<'a> {
    responses: slice::Iter<'a, FetchResponse>,
    topics: Option<slice::Iter<'a, TopicFetchResponse<'a>>>,
    curr_topic: &'a str,
    partitions: Option<slice::Iter<'a, PartitionFetchResponse<'a>>>,
}

impl<'a> Iterator for MessageSetsIter<'a> {
    type Item = MessageSet<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // ~ then the next available partition
            if let Some(p) = self.partitions.as_mut().and_then(|p| p.next()) {
                // ~ skip errornous partitions
                // ~ skip empty partitions
                match p.data() {
                    &Err(_) => {
                        continue;
                    }
                    &Ok(ref pdata) => {
                        let msgs = pdata.messages();
                        if msgs.is_empty() {
                            continue;
                        } else {
                            return Some(MessageSet {
                                topic: self.curr_topic,
                                partition: p.partition(),
                                messages: msgs,
                            });
                        }
                    }
                }
            }
            // ~ then the next available topic
            if let Some(t) = self.topics.as_mut().and_then(|t| t.next()) {
                self.curr_topic = t.topic();
                self.partitions = Some(t.partitions().iter());
                continue;
            }
            // ~ then the next available response
            if let Some(r) = self.responses.next() {
                self.curr_topic = "";
                self.topics = Some(r.topics().iter());
                continue;
            }
            // ~ finally we know there's nothing available anymore
            return None;
        }
    }
}
