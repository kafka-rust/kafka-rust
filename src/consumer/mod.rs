//! Kafka Consumer - A higher-level API for consuming kafka topics.
//!
//! A consumer for Kafka topics on behalf of a specified group
//! providing help in offset management.  The consumer requires at
//! least one topic for consumption and allows consuming multiple
//! topics at the same time. Further, clients can restrict the
//! consumer to only specific topic partitions as demonstrated in the
//! following example.
//!
//! # Example
//! ```no_run
//! use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
//!
//! let mut consumer =
//!    Consumer::from_hosts(vec!("localhost:9092".to_owned()))
//!       .with_topic_partitions("my-topic".to_owned(), &[0, 1])
//!       .with_fallback_offset(FetchOffset::Earliest)
//!       .with_group("my-group".to_owned())
//!       .with_offset_storage(Some(GroupOffsetStorage::Kafka))
//!       .create()
//!       .unwrap();
//! loop {
//!   for ms in consumer.poll().unwrap().iter() {
//!     for m in ms.messages() {
//!       println!("{:?}", m);
//!     }
//!     consumer.consume_messageset(&ms);
//!   }
//!   consumer.commit_consumed().unwrap();
//! }
//! ```
//!
//! Please refer to the documentation of the individual "with" methods
//! used to set up the consumer. These contain further information or
//! links to such.
//!
//! A call to `.poll()` on the consumer will ask for the next
//! available "chunk of data" for the client code to process.  The
//! returned data are `MessageSet`s. There is at most one for each partition
//! of the consumed topics. Individual messages are embedded in the
//! retrieved messagesets and can be processed using the `messages()`
//! iterator.  Due to this embedding, an individual message's lifetime
//! is bound to the `MessageSet` it is part of. Typically, client
//! code accesses the raw data/bytes, parses it into custom data types,
//! and passes that along for further processing within the application.
//! Although inconvenient, this helps in reducing the number of
//! allocations within the pipeline of processing incoming messages.
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
//! committing consumed message offsets resolves into a void operation.

use std::collections::hash_map::{Entry, HashMap};
use std::slice;

use crate::client::fetch;
use crate::client::{CommitOffset, FetchPartition, KafkaClient};
use crate::error::{Error, KafkaCode, Result};
use crate::protocol;

// public re-exports
pub use self::builder::Builder;
pub use crate::client::fetch::Message;
pub use crate::client::FetchOffset;
pub use crate::client::GroupOffsetStorage;

mod assignment;
mod builder;
mod config;
mod state;

/// The default value for `Builder::with_retry_max_bytes_limit`.
pub const DEFAULT_RETRY_MAX_BYTES_LIMIT: i32 = 0;

/// The default value for `Builder::with_fallback_offset`.
pub const DEFAULT_FALLBACK_OFFSET: FetchOffset = FetchOffset::Latest;

/// The Kafka Consumer
///
/// See module level documentation.
#[derive(Debug)]
pub struct Consumer {
    client: KafkaClient,
    state: state::State,
    config: config::Config,
}

// XXX 1) Allow returning to a previous offset (aka seeking)
// XXX 2) Issue IO in a separate (background) thread and pre-fetch messagesets

impl Consumer {
    /// Starts building a consumer using the given kafka client.
    #[must_use]
    pub fn from_client(client: KafkaClient) -> Builder {
        builder::new(Some(client), Vec::new())
    }

    /// Starts building a consumer bootstraping internally a new kafka
    /// client from the given kafka hosts.
    #[must_use]
    pub fn from_hosts(hosts: Vec<String>) -> Builder {
        builder::new(None, hosts)
    }

    /// Borrows the underlying kafka client.
    #[must_use]
    pub fn client(&self) -> &KafkaClient {
        &self.client
    }

    /// Borrows the underlying kafka client as mut.
    #[must_use]
    pub fn client_mut(&mut self) -> &mut KafkaClient {
        &mut self.client
    }

    /// Destroys this consumer returning back the underlying kafka client.
    #[must_use]
    pub fn into_client(self) -> KafkaClient {
        self.client
    }

    /// Retrieves the topic partitions being currently consumed by
    /// this consumer.
    #[must_use]
    pub fn subscriptions(&self) -> HashMap<String, Vec<i32>> {
        // ~ current subscriptions are reflected by
        // `self.state.fetch_offsets` see `self.fetch_messages()`.
        // ~ the number of topics subscribed can be estimated from the
        // user specified assignments stored in `self.state.assignments`.
        let mut h: HashMap<String, Vec<i32>> =
            HashMap::with_capacity(self.state.assignments.as_slice().len());
        // ~ expand subscriptions to (topic-name, partition id)
        let tps = self
            .state
            .fetch_offsets
            .keys()
            .map(|tp| (self.state.topic_name(tp.topic_ref), tp.partition));
        // ~ group by topic-name
        for tp in tps {
            // ~ allocate topic-name only once per topic
            if let Some(ps) = h.get_mut(tp.0) {
                ps.push(tp.1);
                continue;
            }
            h.insert(tp.0.to_owned(), vec![tp.1]);
        }
        h
    }

    /// Polls for the next available message data.
    pub fn poll(&mut self) -> Result<MessageSets> {
        let (n, resps) = self.fetch_messages();
        self.process_fetch_responses(n, resps?)
    }

    /// Determines whether this consumer is set up to consume only a
    /// single topic partition.
    #[must_use]
    fn single_partition_consumer(&self) -> bool {
        self.state.fetch_offsets.len() == 1
    }

    /// Retrieves the group on which behalf this consumer is acting.
    /// The empty group name specifies a group-less consumer.
    #[must_use]
    pub fn group(&self) -> &str {
        &self.config.group
    }

    // ~ returns (number partitions queried, fecth responses)
    fn fetch_messages(&mut self) -> (u32, Result<Vec<fetch::Response>>) {
        // ~ if there's a retry partition ... fetch messages just for
        // that one. Otherwise try to fetch messages for all assigned
        // partitions.
        if let Some(tp) = self.state.retry_partitions.pop_front() {
            let Some(s) = self.state.fetch_offsets.get(&tp) else {
                return (1, Err(Error::Kafka(KafkaCode::UnknownTopicOrPartition)));
            };

            let topic = self.state.topic_name(tp.topic_ref);
            debug!(
                "fetching retry messages: (fetch-offset: {{\"{}:{}\": {:?}}})",
                topic, tp.partition, s
            );
            (
                1,
                self.client.fetch_messages_for_partition(
                    &FetchPartition::new(topic, tp.partition, s.offset).with_max_bytes(s.max_bytes),
                ),
            )
        } else {
            let client = &mut self.client;
            let state = &self.state;
            debug!(
                "fetching messages: (fetch-offsets: {:?})",
                state.fetch_offsets_debug()
            );
            let reqs = state.fetch_offsets.iter().map(|(tp, s)| {
                let topic = state.topic_name(tp.topic_ref);
                FetchPartition::new(topic, tp.partition, s.offset).with_max_bytes(s.max_bytes)
            });
            (
                state.fetch_offsets.len() as u32,
                client.fetch_messages(reqs),
            )
        }
    }

    // ~ post process a data retrieved through fetch_messages before
    // handing them out to client code
    //   - update the fetch state for the next fetch cycle
    // ~ num_partitions_queried: the original number of partitions requested/queried for
    //   the responses
    // ~ resps: the responses to post process
    fn process_fetch_responses(
        &mut self,
        num_partitions_queried: u32,
        resps: Vec<fetch::Response>,
    ) -> Result<MessageSets> {
        let single_partition_consumer = self.single_partition_consumer();
        let mut empty = true;
        let retry_partitions = &mut self.state.retry_partitions;

        for resp in &resps {
            for t in resp.topics() {
                let topic_ref = self
                    .state
                    .assignments
                    .topic_ref(t.topic())
                    .expect("unknown topic in response");

                for p in t.partitions() {
                    let tp = state::TopicPartition {
                        topic_ref,
                        partition: p.partition(),
                    };

                    // ~ for now, as soon as a partition has an error
                    // we fail to prevent client programs from not
                    // noticing.  however, in future we don't need to
                    // fail immediately, we can try to recover from
                    // certain errors and retry the fetch operation
                    // transparently for the caller.

                    // XXX need to prevent updating fetch_offsets in case we're gonna fail here
                    let data = p.data()?;

                    let fetch_state = self
                        .state
                        .fetch_offsets
                        .get_mut(&tp)
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
                            debug!(
                                "reset max_bytes for {}:{} from {} to {}",
                                t.topic(),
                                tp.partition,
                                prev_max_bytes,
                                fetch_state.max_bytes
                            );
                        }
                    } else {
                        debug!(
                            "no data received for {}:{} (max_bytes: {} / fetch_offset: {} / \
                                highwatermark_offset: {})",
                            t.topic(),
                            tp.partition,
                            fetch_state.max_bytes,
                            fetch_state.offset,
                            data.highwatermark_offset()
                        );

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
                                debug!(
                                    "increased max_bytes for {}:{} from {} to {}",
                                    t.topic(),
                                    tp.partition,
                                    prev_max_bytes,
                                    fetch_state.max_bytes
                                );
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
                                debug!("rescheduled for retry: {}:{}", t.topic(), tp.partition);
                                retry_partitions.push_back(tp);
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

        Ok(MessageSets {
            responses: resps,
            empty,
        })
    }

    /// Retrieves the offset of the last "consumed" message in the
    /// specified partition. Results in `None` if there is no such
    /// "consumed" message.
    #[must_use]
    pub fn last_consumed_message(&self, topic: &str, partition: i32) -> Option<i64> {
        self.state
            .topic_ref(topic)
            .and_then(|tref| {
                self.state.consumed_offsets.get(&state::TopicPartition {
                    topic_ref: tref,
                    partition,
                })
            })
            .map(|co| co.offset)
    }

    /// Marks the message at the specified offset in the specified
    /// topic partition as consumed by the caller.
    ///
    /// Note: a message with a "later/higher" offset automatically
    /// marks all preceding messages as "consumed", this is messages
    /// with "earlier/lower" offsets in the same partition.
    /// Therefore, it is not necessary to invoke this method for
    /// every consumed message.
    ///
    /// Results in an error if the specified topic partition is not
    /// being consumed by this consumer.
    pub fn consume_message(&mut self, topic: &str, partition: i32, offset: i64) -> Result<()> {
        let topic_ref = self
            .state
            .topic_ref(topic)
            .ok_or_else(|| Error::Kafka(KafkaCode::UnknownTopicOrPartition))?;

        let tp = state::TopicPartition {
            topic_ref,
            partition,
        };
        match self.state.consumed_offsets.entry(tp) {
            Entry::Vacant(v) => {
                v.insert(state::ConsumedOffset {
                    offset,
                    dirty: true,
                });
            }
            Entry::Occupied(mut v) => {
                let o = v.get_mut();
                if offset > o.offset {
                    o.offset = offset;
                    o.dirty = true;
                }
            }
        }
        Ok(())
    }

    /// A convenience method to mark the given message set consumed as a
    /// whole by the caller. This is equivalent to marking the last
    /// message of the given set as consumed.
    pub fn consume_messageset(&mut self, msgs: &MessageSet<'_>) -> Result<()> {
        if let Some(last) = msgs.messages().last() {
            self.consume_message(msgs.topic(), msgs.partition(), last.offset)
        } else {
            Ok(())
        }
    }

    /// Persists the so-far "marked as consumed" messages (on behalf
    /// of this consumer's group for the underlying topic - if any.)
    ///
    /// See also `Consumer::consume_message` and
    /// `Consumer::consume_messageset`.
    pub fn commit_consumed(&mut self) -> Result<()> {
        if self.config.group.is_empty() {
            return Err(Error::UnsetGroupId);
        }
        debug!(
            "commit_consumed: committing dirty-only consumer offsets (group: {} / offsets: {:?}",
            self.config.group,
            self.state.consumed_offsets_debug()
        );
        let (client, state) = (&mut self.client, &mut self.state);
        client.commit_offsets(
            &self.config.group,
            state
                .consumed_offsets
                .iter()
                .filter(|&(_, o)| o.dirty)
                .map(|(tp, o)| {
                    let topic = state.topic_name(tp.topic_ref);

                    // Note that the offset that is committed should be the
                    // offset of the next message a consumer should read, so
                    // add one to the consumed message's offset.
                    //
                    // https://kafka.apache.org/090/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html
                    CommitOffset::new(topic, tp.partition, o.offset + 1)
                }),
        )?;
        for co in state.consumed_offsets.values_mut() {
            if co.dirty {
                co.dirty = false;
            }
        }
        Ok(())
    }
}

// --------------------------------------------------------------------

/// Messages retrieved from kafka in one fetch request.  This is a
/// concatenation of blocks of messages successfully retrieved from
/// the consumed topic partitions.  Each such partitions is guaranteed
/// to be present at most once in this structure.
#[derive(Debug)]
pub struct MessageSets {
    responses: Vec<fetch::Response>,

    /// Precomputed; Says whether there are some messages or whether
    /// the responses actually contain consumeable messages
    empty: bool,
}

impl MessageSets {
    /// Determines efficiently whether there are any consumeable
    /// messages in this data set.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.empty
    }

    #[must_use]
    pub fn iter(&self) -> MessageSetsIter<'_> {
        <&Self as IntoIterator>::into_iter(self)
    }
}

/// Iterates over the message sets delivering the fetched message
/// data of consumed topic partitions.
impl<'a> IntoIterator for &'a MessageSets {
    type Item = MessageSet<'a>;
    type IntoIter = MessageSetsIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        let mut responses = self.responses.iter();
        let mut topics = responses.next().map(|r| r.topics().iter());
        let (curr_topic, partitions) = topics
            .as_mut()
            .and_then(Iterator::next)
            .map_or(("", None), |t| (t.topic(), Some(t.partitions().iter())));
        MessageSetsIter {
            responses,
            topics,
            curr_topic,
            partitions,
        }
    }
}

/// A set of messages successfully retrieved from a specific topic
/// partition.
pub struct MessageSet<'a> {
    topic: &'a str,
    partition: i32,
    messages: &'a [Message<'a>],
}

impl<'a> MessageSet<'a> {
    #[inline]
    #[must_use]
    pub fn topic(&self) -> &'a str {
        self.topic
    }

    #[inline]
    #[must_use]
    pub fn partition(&self) -> i32 {
        self.partition
    }

    #[inline]
    #[must_use]
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
            if let Some(p) = self.partitions.as_mut().and_then(Iterator::next) {
                // ~ skip erroneous partitions
                // ~ skip empty partitions
                if let Some(messages) = p
                    .data()
                    .ok()
                    .map(protocol::fetch::Data::messages)
                    .filter(|msgs| !msgs.is_empty())
                {
                    return Some(MessageSet {
                        topic: self.curr_topic,
                        partition: p.partition(),
                        messages,
                    });
                }
            }
            // ~ then the next available topic
            if let Some(t) = self.topics.as_mut().and_then(Iterator::next) {
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
