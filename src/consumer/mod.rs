//! Kafka Consumer - A higher-level API for consuming a kafka topic.
//!
//! A consumer for a single Kafka topic on behalf of a specified group
//! providing help in offset management.  The consumer can be
//! optionally advised to consume only particular partitions of the
//! underlying topic.
//!
//! # Example
//! ```no_run
//! use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
//!
//! let mut consumer =
//!    Consumer::from_hosts(vec!("localhost:9092".to_owned()),
//!                              "my-topic".to_owned())
//!       .with_partitions(&[0, 1])
//!       .with_fallback_offset(FetchOffset::Earliest)
//!       .with_group("my-group".to_owned())
//!       .with_offset_storage(GroupOffsetStorage::Kafka)
//!       .create()
//!       .unwrap();
//! loop {
//!   for ms in consumer.poll().unwrap().iter() {
//!     for m in ms.messages() {
//!       println!("{:?}", m);
//!     }
//!     consumer.consume_messageset(ms);
//!   }
//!   consumer.commit_consumed().unwrap();
//! }
//! ```
//!
//! A `.poll()` will ask for the next available "chunk of data" for
//! client code to process.  The returned data are `MessageSet`s - at
//! most one for each partition of the consumed topic.
//!
//! If the consumer is configured for a non-empty group, it helps in
//! keeping track of already consumed messages by maintaining a map of
//! the consumed offsets.  Messages can be told "consumed" either
//! through `consume_message` or `consume_messages` methods.  Once
//! these consumed messages are committed to Kafka using
//! `commit_consumed`, the consumer will start fetching messages from
//! here even after restart.  Since committing is a certain overhead,
//! it is up to the client to decide the frequency of the commits.
//! The consumer will *not* commit any messages to Kafka
//! automatically.
//!
//! The configuration of a group is optional.  If the consumer has no
//! group configured, it will behave as if it had one, only that
//! commiting consumed message offsets resolves into a void operation.

use std::collections::hash_map::{HashMap, Entry};
use std::collections::VecDeque;
use std::slice;
use std::hash::BuildHasherDefault;

use fnv::FnvHasher;

use client::{self, KafkaClient, FetchPartition, CommitOffset, FetchGroupOffset};
use client::metadata::Topics;
use error::{Error, KafkaCode, Result};
use client::fetch;

// public re-exports
pub use client::fetch::Message;
pub use client::FetchOffset;
pub use client::GroupOffsetStorage;

#[cfg(feature = "security")]
use client::SecurityConfig;

#[cfg(not(feature = "security"))]
type SecurityConfig = ();

/// The default value for `Builder::with_retry_max_bytes_limit`.
pub const DEFAULT_RETRY_MAX_BYTES_LIMIT: i32 = 0;

/// The default value for `Builder::with_fallback_offset`.
pub const DEFAULT_FALLBACK_OFFSET: FetchOffset = FetchOffset::Latest;

type PartitionHasher = BuildHasherDefault<FnvHasher>;

/// The Kafka Consumer
///
/// See module level documentation.
pub struct Consumer {
    client: KafkaClient,
    state: State,
    config: Config,
}

// The "fetch state" for a particular topci partition.
#[derive(Debug)]
struct FetchState {
    /// ~ specifies the offset which to fetch from
    offset: i64,
    /// ~ specifies the max_bytes to be fetched
    max_bytes: i32,
}

#[derive(Debug)]
struct State {
    /// Contains the information relevant for the next fetch operation
    /// on the corresponding partitions
    fetch_offsets: HashMap<i32, FetchState, PartitionHasher>,

    /// Specifies partitions to be fetched on their own in the next
    /// poll request.
    retry_partitions: VecDeque<i32>,

    /// Contains the offsets of messages marked as "consumed" (to be
    /// committed)
    consumed_offsets: HashMap<i32, i64, PartitionHasher>,

    /// `true` iff the consumed_offsets contain data which needs to be
    /// committed. Set to `false` after commit.
    consumed_offsets_dirty: bool,
}

struct Config {
    group: String,
    topic: String,
    partitions: Vec<i32>,
    fallback_offset: FetchOffset,
    retry_max_bytes_limit: i32,
}

// XXX 1) Support multiple topics
// XXX 2) Issue IO in a separate (background) thread and pre-fetch messagesets
// XXX 3) Allow returning to a previous offset (aka seeking)

impl Consumer {

    /// Starts building a consumer for the given topic using the given
    /// kafka client.
    pub fn from_client(client: KafkaClient, topic: String) -> Builder {
        Builder::new(Some(client), Vec::new()).with_topic(topic)
    }

    /// Starts building a consumer for the given topic bootstraping
    /// internally a new kafka client from the given kafka hosts.
    pub fn from_hosts(hosts: Vec<String>, topic: String) -> Builder {
        Builder::new(None, hosts).with_topic(topic)
    }

    /// Destroys this consumer returning back the underlying kafka client.
    pub fn client(self) -> KafkaClient {
        self.client
    }

    /// Polls for the next available message data.
    pub fn poll(&mut self) -> Result<MessageSets> {
        let (n, resps) = self.fetch_messages();
        self.process_fetch_responses(n, try!(resps))
    }

    /// Determines whether this consumer is set up to consume only a
    /// single partition.
    pub fn single_partition_consumer(&self) -> bool {
        self.state.fetch_offsets.len() == 1
    }

    /// Retrieves the group on which behalf this consumer is acting.
    /// The empty group name specifies a group-less consumer.
    pub fn group(&self) -> &str {
        &self.config.group
    }

    // ~ returns (number partitions queried, fecth responses)
    fn fetch_messages(&mut self) -> (u32, Result<Vec<fetch::Response>>) {
        let topic = &self.config.topic;
        let fetch_offsets = &self.state.fetch_offsets;
        // ~ if there's a retry partition ... fetch messages just for
        // that one. Otherwise try to fetch messages for all assigned
        // partitions.
        match self.state.retry_partitions.pop_front() {
            Some(partition) => {
                let s = match fetch_offsets.get(&partition) {
                    Some(fstate) => fstate,
                    None => return (1, Err(Error::Kafka(KafkaCode::UnknownTopicOrPartition))),
                };
                debug!("fetching messages: (topic: {} / fetch-offset: {{{}: {:?}}})",
                       topic, partition, s);
                (1, self.client.fetch_messages_for_partition(
                    &FetchPartition::new(topic, partition, s.offset)
                        .with_max_bytes(s.max_bytes)))
            }
            None => {
                debug!("fetching messages: (topic: {} / fetch-offsets: {:?})",
                       topic, fetch_offsets);
                let reqs = fetch_offsets.iter()
                    .map(|(&p, s)| {
                        FetchPartition::new(topic, p, s.offset)
                            .with_max_bytes(s.max_bytes)
                    });
                (fetch_offsets.len() as u32, self.client.fetch_messages(reqs))
            }
        }
    }

    // ~ post process a data retrieved through fetch_messages before
    // handing them out to client code
    //   - update the fetch state for the next fetch cycle
    // ~ num_partitions_queried: the original number of partitions requested/queried for
    //   the responses
    // ~ resps: the responses to post process
    fn process_fetch_responses(&mut self, num_partitions_queried: u32, resps: Vec<fetch::Response>)
                               -> Result<MessageSets>
    {
        let single_partition_consumer = self.single_partition_consumer();
        let mut empty = true;
        let mut retry_partitions = &mut self.state.retry_partitions;

        for resp in &resps {
            for t in resp.topics() {
                for p in t.partitions() {
                    let partition = p.partition();

                    // ~ for now, as soon as a partition has an error
                    // we fail to prevent client programs from not
                    // noticing.  however, in future we don't need to
                    // fail immediately, we can try to recover from
                    // certain errors and retry the fetch operation
                    // transparently for the caller.
                    let data = match p.data() {
                        // XXX need to prevent updating fetch_offsets in case we're gonna fail here
                        &Err(ref e) => return Err(e.clone()),
                        &Ok(ref data) => data,
                    };

                    let mut fetch_state = self.state.fetch_offsets
                            .get_mut(&partition)
                            .expect("non-requested partition");
                    // ~ book keeping
                    if let Some(last_msg) = data.messages().last() {
                        fetch_state.offset = last_msg.offset + 1;
                        empty = false;

                        // ~ reset the max_bytes again to its usual
                        // value if we had a retry request and finally
                        // got some data
                        if fetch_state.max_bytes != self.client.fetch_max_bytes_per_partition() {
                            let prev_max_bytes = fetch_state.max_bytes;
                            fetch_state.max_bytes = self.client.fetch_max_bytes_per_partition();
                            debug!("reset max_bytes for {}:{} from {} to {}",
                                   t.topic(), partition, prev_max_bytes, fetch_state.max_bytes);
                        }
                    } else {
                        debug!("no data received for {}:{} (max_bytes: {} / fetch_offset: {} / highwatermark_offset: {})",
                               t.topic(), partition, fetch_state.max_bytes, fetch_state.offset, data.highwatermark_offset());

                        // ~ when a partition is empty but has a
                        // highwatermark-offset equal to or greater
                        // than the one we tried to fetch ... we'll
                        // try to increase the max-fetch-size in the
                        // next fetch request
                        if fetch_state.offset < data.highwatermark_offset() {
                            if fetch_state.max_bytes < self.config.retry_max_bytes_limit {
                                // ~ try to double the max_bytes
                                let prev_max_bytes = fetch_state.max_bytes;
                                let incr_max_bytes = prev_max_bytes + prev_max_bytes;
                                if incr_max_bytes > self.config.retry_max_bytes_limit {
                                    fetch_state.max_bytes = self.config.retry_max_bytes_limit;
                                } else {
                                    fetch_state.max_bytes = incr_max_bytes;
                                }
                                debug!("increased max_bytes for {}:{} from {} to {}",
                                       t.topic(), partition, prev_max_bytes, fetch_state.max_bytes);
                            } else if num_partitions_queried == 1 {
                                // ~ this was a single partition
                                // request and we didn't get anything
                                // and we won't be increasing the max
                                // fetch size ... this is will fail
                                // forever ... signal the problem to
                                // the user
                                return Err(Error::Kafka(KafkaCode::MessageSizeTooLarge));
                            }
                            // ~ if this consumer is subscribed to one
                            // partition only, there's no need to push
                            // the partition to the 'retry_partitions'
                            // (this is just a small optimization)
                            if !single_partition_consumer {
                                // ~ mark this partition for a retry on its own
                                debug!("rescheduled for retry: {}:{}", t.topic(), partition);
                                retry_partitions.push_back(partition)
                            }
                        }
                    }
                }
            }
        }

        // XXX in future, issue one more fetch_messages request in the
        // background such that the next time the client polls that
        // request's response will likely be already ready for
        // consumption

        Ok(MessageSets{ responses: resps, empty: empty })
    }

    /// Retrieves the offset of the last "consumed" message in the
    /// specified partition. Results in `None` if there is no such
    /// "consumed" message for this consumer yet.
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
    /// of this consumer's group for the underlying topic - if any.)
    ///
    /// See also `Consumer::consume_message` and
    /// `Consumer::consume_messetset`.
    pub fn commit_consumed(&mut self) -> Result<()> {
        if self.config.group.is_empty() {
            debug!("commit_consumed: ignoring commit request since no group defined");
            return Ok(());
        }
        if !self.state.consumed_offsets_dirty {
            debug!("commit_consumed: no new consumed offsets to commit");
            return Ok(());
        }
        let client = &mut self.client;
        let (group, topic) = (&self.config.group, &self.config.topic);
        let offsets = &self.state.consumed_offsets;
        debug!("commit_consumed: commiting consumed offsets \
                (topic: {} / group: {} / offsets: {:?}",
               topic, group, offsets);
        try!(client.commit_offsets(
            group,
            offsets.iter().map(|(&p, &o)| CommitOffset::new(topic, p, o))));
        self.state.consumed_offsets_dirty = false;
        Ok(())
    }
}

// --------------------------------------------------------------------

impl State {
    fn new(client: &mut KafkaClient, config: &Config) -> Result<State> {
        let partitions = try!(determine_partitions(config, client.topics()));
        let consumed_offsets = try!(load_consumed_offsets(config, client, &partitions));
        let fetch_offsets = try!(load_fetch_states(config, client, &partitions, &consumed_offsets));

        Ok(State {
            fetch_offsets: fetch_offsets,
            retry_partitions: VecDeque::new(),
            consumed_offsets: consumed_offsets,
            consumed_offsets_dirty: false,
        })
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
                           p, config.topic, avail_partitions);
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
                         -> Result<HashMap<i32, i64, PartitionHasher>>
{
    assert!(!partitions.is_empty());

    let mut offs = HashMap::with_capacity_and_hasher(
        partitions.len(), PartitionHasher::default());
    // ~ no group, no persisted consumed offsets
    if config.group.is_empty() {
        return Ok(offs);
    }
    // ~ otherwise try load them for the group
    let topic = &config.topic;
    let tpos = try!(client.fetch_group_offsets(
        &config.group,
        partitions.iter().map(|&p_id| FetchGroupOffset::new(topic, p_id))));
    for tpo in tpos {
        match tpo.offset {
            Ok(o) if o != -1 => {
                offs.insert(tpo.partition, o);
            }
            _ => {}
        }
    }
    Ok(offs)
}

/// Fetches the "next fetch" offsets/states based on the specified
/// configuration and the given consumed offsets.
fn load_fetch_states(config: &Config,
                      client: &mut KafkaClient,
                      partitions: &[i32],
                      consumed_offsets: &HashMap<i32, i64, PartitionHasher>)
                      -> Result<HashMap<i32, FetchState, PartitionHasher>>
{
    fn load_partition_offsets(client: &mut KafkaClient, topic: &str, offset: FetchOffset)
                              -> Result<HashMap<i32, i64, PartitionHasher>>
    {
        let offs = try!(client.fetch_topic_offsets(topic, offset));
        let mut m = HashMap::with_capacity_and_hasher(offs.len(), PartitionHasher::default());
        for off in offs {
            m.insert(off.partition, off.offset.unwrap_or(-1));
        }
        Ok(m)
    }

    let max_bytes = client.fetch_max_bytes_per_partition();

    let mut fetch_offsets = HashMap::with_capacity_and_hasher(
        partitions.len(), PartitionHasher::default());


    if consumed_offsets.is_empty() {
        // ~ if there are no offsets on behalf of the consumer
        // group - if any - we can directly use fallback offsets.
        let offs = try!(load_partition_offsets(
            client, &config.topic, config.fallback_offset));
        for p in partitions {
            fetch_offsets.insert(*p, FetchState {
                offset: *offs.get(p).unwrap_or(&-1),
                max_bytes: max_bytes
            });
        }
    } else {
        // fetch the earliest and latest available offsets
        let latest = try!(load_partition_offsets(client, &config.topic, FetchOffset::Latest));
        let earliest = try!(load_partition_offsets(client, &config.topic, FetchOffset::Earliest));

        // ~ for each partition if we have a consumed_offset verify it is
        // in the earliest/latest range and use that consumed_offset+1 as
        // the fetch_message.
        for p in partitions {
            let (&l_off, &e_off) = (latest.get(p).unwrap_or(&-1), earliest.get(p).unwrap_or(&-1));
            let offset = match consumed_offsets.get(p) {
                Some(&co) if co >= e_off && co < l_off => co + 1,
                _ => {
                    match config.fallback_offset {
                        FetchOffset::Latest => l_off,
                        FetchOffset::Earliest => e_off,
                        _ => {
                            debug!("cannot determine fetch offset \
                                    (group: {} / topic: {} / partition: {})",
                                   &config.group, &config.topic, p);
                            return Err(Error::Kafka(KafkaCode::Unknown));
                        }
                    }
                }
            };
            fetch_offsets.insert(*p, FetchState{ offset: offset, max_bytes: max_bytes });
        }
    }
    Ok(fetch_offsets)
}

// --------------------------------------------------------------------

/// Messages retrieved from kafka in one fetch request.  This is a
/// concatenation of blocks of messages successfully retrieved from
/// the consumed topic partitions.  Each such partitions is guaranteed
/// to be present at most once in this structure.
pub struct MessageSets {
    responses: Vec<fetch::Response>,

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
    responses: slice::Iter<'a, fetch::Response>,
    topics: Option<slice::Iter<'a, fetch::Topic<'a>>>,
    curr_topic: &'a str,
    partitions: Option<slice::Iter<'a, fetch::Partition<'a>>>,
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

// --------------------------------------------------------------------

/// A Kafka Consumer builder easing the process of setting up various
/// configuration settings.
#[derive(Debug)]
pub struct Builder {
    client: Option<KafkaClient>,
    hosts: Vec<String>,
    group: String,
    topic: String,
    partitions: Vec<i32>,
    fallback_offset: FetchOffset,
    fetch_max_wait_time: i32,
    fetch_min_bytes: i32,
    fetch_max_bytes_per_partition: i32,
    retry_max_bytes_limit: i32,
    fetch_crc_validation: bool,
    security_config: Option<SecurityConfig>,
    group_offset_storage: GroupOffsetStorage,
}

impl Builder {
    fn new(client: Option<KafkaClient>, hosts: Vec<String>) -> Builder {
        let mut b = Builder {
            client: client,
            hosts: hosts,
            fetch_max_wait_time: client::DEFAULT_FETCH_MAX_WAIT_TIME,
            fetch_min_bytes: client::DEFAULT_FETCH_MIN_BYTES,
            fetch_max_bytes_per_partition: client::DEFAULT_FETCH_MAX_BYTES_PER_PARTITION,
            fetch_crc_validation: client::DEFAULT_FETCH_CRC_VALIDATION,
            retry_max_bytes_limit: DEFAULT_RETRY_MAX_BYTES_LIMIT,
            group: "".to_owned(),
            topic: "".to_owned(),
            partitions: Vec::new(),
            fallback_offset: DEFAULT_FALLBACK_OFFSET,
            security_config: None,
            group_offset_storage: client::DEFAULT_GROUP_OFFSET_STORAGE,
        };
        if let Some(ref c) = b.client {
            b.fetch_max_wait_time = c.fetch_max_wait_time();
            b.fetch_min_bytes = c.fetch_min_bytes();
            b.fetch_max_bytes_per_partition = c.fetch_max_bytes_per_partition();
            b.fetch_crc_validation = c.fetch_crc_validation();
            b.group_offset_storage = c.group_offset_storage();
        }
        b
    }

    /// Specifies the topic to consume.
    fn with_topic(mut self, topic: String) -> Builder {
        self.topic = topic;
        self
    }

    /// Specifies the group on whose behalf to maintain consumed
    /// message offsets.
    ///
    /// The group is allowed to be the empty string, in which case the
    /// resulting consumer will be group-less.
    pub fn with_group(mut self, group: String) -> Builder {
        self.group = group;
        self
    }

    /// Explicitely specifies the partitions to consume.
    ///
    /// If this function is never called, all available partitions of
    /// the underlying topic will be consumed assumed.
    pub fn with_partitions(mut self, partitions: &[i32]) -> Builder {
        self.partitions.extend_from_slice(partitions);
        self
    }

    /// Specifies the security config to use.
    /// See `KafkaClient::new_secure` for more info.
    #[cfg(feature = "security")]
    pub fn with_security(mut self, sec: SecurityConfig) -> Builder {
        self.security_config = Some(sec);
        self
    }

    /// Specifies the offset to use when none was committed for the
    /// underlying group yet or the consumer has no group configured.
    ///
    /// Running the underlying group for the first time against a
    /// topic or running the consumer without a group results in the
    /// question where to start reading from the topic, since it might
    /// already contain a lot of messages.  Common strategies are
    /// starting at the earliest available message (thereby consuming
    /// whatever is currently in the topic) or at the latest one
    /// (thereby staring to consume only newly arriving messages.)
    /// The "fallback offset" here corresponds to `time` in
    /// `KafkaClient::fetch_offsets`.
    pub fn with_fallback_offset(mut self, fallback_offset: FetchOffset) -> Builder {
        self.fallback_offset = fallback_offset;
        self
    }

    /// See `KafkaClient::set_fetch_max_wait_time`
    pub fn with_fetch_max_wait_time(mut self, max_wait_time: i32) -> Builder {
        self.fetch_max_wait_time = max_wait_time;
        self
    }

    /// See `KafkaClient::set_fetch_min_bytes`
    pub fn with_fetch_min_bytes(mut self, min_bytes: i32) -> Builder {
        self.fetch_min_bytes = min_bytes;
        self
    }

    /// See `KafkaClient::set_fetch_max_bytes_per_partition`
    pub fn with_fetch_max_bytes_per_partition(mut self, max_bytes_per_partition: i32) -> Builder {
        self.fetch_max_bytes_per_partition = max_bytes_per_partition;
        self
    }

    /// See `KafkaClient::set_fetch_crc_validation`
    pub fn with_fetch_crc_validation(mut self, validate_crc: bool) -> Builder {
        self.fetch_crc_validation = validate_crc;
        self
    }

    /// See `KafkaClient::set_group_offset_storage`
    pub fn with_offset_storage(mut self, storage: GroupOffsetStorage) -> Builder {
        self.group_offset_storage = storage;
        self
    }

    /// Specifies the upper bound of data bytes to allow fetching from
    /// a kafka partition when retrying a fetch request due to a too
    /// big message in the partition.
    ///
    /// By default, this consumer will fetch up to
    /// `KafkaClient::fetch_max_bytes_per_partition` data from each
    /// partition.  However, when it discovers that there are messages
    /// in an underlying partition which could not be delivered, the
    /// request to that partition might be retried a few times with an
    /// increased `fetch_max_bytes_per_partition`.  The value
    /// specified here defines a limit to this increment.
    ///
    /// A value smaller than the
    /// `KafkaClient::fetch_max_bytes_per_partition`, e.g. zero, will
    /// disable the retry feature of this consumer.  The default value
    /// for this setting is `DEFAULT_RETRY_MAX_BYTES_LIMIT`.
    ///
    /// Note: if the consumed topic partitions are known to host large
    /// messages it is much more efficient to set
    /// `KafkaClient::fetch_max_bytes_per_partition` appropriately
    /// instead of relying on the limit specified here.  This limit is
    /// just an upper bound for already additional retry requests.
    pub fn with_retry_max_bytes_limit(mut self, limit: i32) -> Builder {
        self.retry_max_bytes_limit = limit;
        self
    }

    #[cfg(not(feature = "security"))]
    fn new_kafka_client(hosts: Vec<String>, _: Option<SecurityConfig>) -> KafkaClient {
        KafkaClient::new(hosts)
    }

    #[cfg(feature = "security")]
    fn new_kafka_client(hosts: Vec<String>, security: Option<SecurityConfig>) -> KafkaClient {
        if let Some(security) = security {
            KafkaClient::new_secure(hosts, security)
        } else {
            KafkaClient::new(hosts)
        }
    }

    /// Finally creates/builds a new consumer based on the so far
    /// supplied settings.
    pub fn create(self) -> Result<Consumer> {
        let mut client = match self.client {
            Some(client) => client,
            None => {
                let mut client = Self::new_kafka_client(self.hosts, self.security_config);
                try!(client.load_metadata_all());
                client
            }
        };
        client.set_fetch_max_wait_time(self.fetch_max_wait_time);
        client.set_fetch_min_bytes(self.fetch_min_bytes);
        client.set_fetch_max_bytes_per_partition(self.fetch_max_bytes_per_partition);
        client.set_group_offset_storage(self.group_offset_storage);

        let config = Config {
            group: self.group,
            topic: self.topic,
            partitions: self.partitions,
            fallback_offset: self.fallback_offset,
            retry_max_bytes_limit: self.retry_max_bytes_limit,
        };

        let state = try!(State::new(&mut client, &config));

        debug!("initialized: (topic: {} / group: {} / state: {:?})",
               config.topic, config.group, state);
        Ok(Consumer{ client: client, state: state, config: config })
    }
}
