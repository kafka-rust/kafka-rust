// XXX module documentation

use std::collections::HashMap;
use std::slice;

use client::{zfetch, KafkaClient, FetchOffset};
use error::{Error, KafkaCode, Result};
use utils::{PartitionOffset, TopicPartitionOffset, TopicPartitionOffsetError};

// public re-exports
pub use client::zfetch::Message;

// XXX verify that the last committed messages is not consumed again upon another run of the client program

// XXX documentation
pub struct Consumer {
    client: KafkaClient,
    state: State,
    config: Config,
}

struct State {
    /// Contains the offset which to fetch next for each partition
    fetch_offsets: HashMap<i32, i64>, // XXX might want to change into a Vec<i64> where index denotes the partition_id
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
    // XXX make this accept brokers just like KafkaClient and avoid the client having to specify it; allocate one yourself
    pub fn new(client: KafkaClient, group: String, topic: String) -> Consumer {
        Consumer {
            client: client,
            state: State {
                fetch_offsets: HashMap::new(),
            },
            config: Config {
                group: group,
                topic: topic,
                partitions: Vec::new(),
                fallback_offset: None,
            },
        }
    }

    // XXX allow clients to obtain loaded metadata

    // XXX allow specifying fetch related settings (see KafkaClient)

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
    // XXX rename to set_fallback_offset
    pub fn fallback_offset(mut self, fallback_offset_time: FetchOffset) -> Consumer {
        self.config.fallback_offset = Some(fallback_offset_time);
        self
    }

    /// Explicitely specify which partitions to consume.
    ///
    /// If this function is never called, all partitions are assumed.
    /// This function call be called multiple times to add more than 1
    /// partitions.
    // XXX rename to assign_partitions(mut self, &[i32])
    pub fn partition(mut self, partition: i32) -> Consumer {
        self.config.partitions.push(partition);
        self
    }

    /// Poll for the next available message data.
    pub fn poll(&mut self) -> Result<MessageSets> {
        if self.state.fetch_offsets.is_empty() {
            self.state.fetch_offsets = try!(fetch_group_offsets(&self.config, &mut self.client));
            debug!("fetched group offsets: (topic: {} / group: {} / fetch-offset: {:?})",
                   self.config.topic, self.config.group, self.state.fetch_offsets);
        }

        let resps = try!(self.fetch_messages());

        // XXX in future, issue the next fetch_messages in the background

        let mut empty = true;
        for resp in &resps {
            for t in resp.topics() {
                for p in t.partitions() {
                    // XXX handle partitions with errors (will
                    // probably need to refetch metadata)

                    // XXX when a partition is empty but has a higher
                    // highwatermark-offset than the one we fetched
                    // from ... try to increase the max-fetch-size in
                    // the next fetch request

                    if let &Ok(ref data) = p.data() {
                        if let Some(last_msg) = data.messages().last() {
                            empty = false;
                            self.state.fetch_offsets.insert(p.partition(), last_msg.offset + 1);
                        }
                    }
                }
            }
        }

        Ok(MessageSets{ responses: resps, empty: empty })
    }

    // XXX provide a way for clients to mark messages as "consumed"

    // XXX provide a way for clients to commit "consumed" offsets

    fn fetch_messages(&mut self) -> Result<Vec<zfetch::FetchResponse>> {
        let topic = &self.config.topic;
        let fetch_offsets = &self.state.fetch_offsets;
        debug!("fetching messages: (topic: {} / fetch-offsets: {:?})", topic, fetch_offsets);
        let reqs = fetch_offsets.iter().map(|(&p, &o)| TopicPartitionOffset::new(topic, p, o));
        self.client.zfetch_messages_multi(reqs)
    }
}

fn fetch_group_offsets(config: &Config, client: &mut KafkaClient) -> Result<HashMap<i32, i64>> {
    let partitions = {
        let metadata = client.topics();
        let avail_partitions = match metadata.partitions(&config.topic) {
            // ~ fail if the underlying topic is unkonwn to the given client
            None => {
                debug!("no such topic: {} (all metadata: {:?})", config.topic, metadata);
                return Err(Error::Kafka(KafkaCode::UnknownTopicOrPartition));
            }
            Some(tp) => tp,
        };
        if config.partitions.is_empty() {
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
            ps
        }
    };

    // ~ fetch the so far commited group offsets
    let mut tpos = try!(client.fetch_group_offsets(&config.group, &config.topic));

    // ~ it might well that there were no group offsets committed
    // yet ... fallback to default offsets.
    try!(set_fallback_offsets(config, client, &mut tpos));

    // ~ now initialize the offset table from the fetched offsets
    let mut offsets = HashMap::new();
    for tpo in &tpos {
        if partitions.contains(&tpo.partition) {
            offsets.insert(tpo.partition, tpo.offset);
        }
    }

    Ok(offsets)
}

/// Try setting the "fallback offsets" for all of `tpos` where
/// `offset == -1`. Fails if retrieving the fallback offset is not
/// possible for some reason for the affected elements from
/// `tpos`.
fn set_fallback_offsets(config: &Config, client: &mut KafkaClient,
                        tpos: &mut [TopicPartitionOffsetError])
                        -> Result<()>
{
    // ~ it looks like kafka (0.8.2.1) is sending an error code
    // (even though it documents it won't:
    // https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetFetchResponse)
    // so let's check only for `offset == -1` (and don't verify `error == None`)
    let has_missing_offs = tpos.iter().filter(|tpo| tpo.offset == -1).next().is_some();
    if has_missing_offs {
        // ~ if the user specified a "fallback offset" strategy
        // will try to fetch the current offset for that strategy
        // and start consuming starting at those
        if let Some(fallback_offset) = config.fallback_offset {
            // ~ now fetch the offset according to the specified strategy
            let new_offs = try!(client.fetch_topic_offset(&config.topic, fallback_offset));
            // ehm ... not really fast (O(n^2))
            for tpo in tpos.iter_mut() {
                if tpo.offset == -1 {
                    if let Some(&PartitionOffset {offset: Ok(offset), ..})
                        = new_offs.iter().find(|pt| pt.partition == tpo.partition)
                    {
                        tpo.offset = offset;
                        tpo.error = None;
                    }
                }
            }
        } else {
            // XXX might want to produce some log message and return a dedicated error code
            return Err(Error::Kafka(KafkaCode::Unknown));
        }
    }
    Ok(())
}

// --------------------------------------------------------------------

/// A convenience helper for iterating "fetch messages" responses.
pub struct ResponseIter<'a> {
    responses: slice::Iter<'a, zfetch::FetchResponse>,
    topics: Option<slice::Iter<'a, zfetch::TopicFetchResponse<'a>>>,
    curr_topic: &'a str,
    partitions: Option<slice::Iter<'a, zfetch::PartitionFetchResponse<'a>>>,
}

/// A responce for a set of messages from a single topic partition.
pub struct Response<'a> {
    /// The name of the topic this response corresponds to.
    pub topic: &'a str,
    /// The partition this response correponds to.
    pub partition: i32,
    /// Either an error or the set of messages retrieved for the
    /// underlying topic partition.
    pub data: &'a Result<zfetch::PartitionData<'a>>,
}

/// Provide a partition level iterator over all specified fetch
/// responses. Since there might be failures on the level of
/// partitions, it is the lowest unit at which a response can be
/// processed. The returned iterator will iterate all partitions in
/// the given responses in their specified order.
pub fn iter_responses<'a>(responses: &'a [zfetch::FetchResponse]) -> ResponseIter<'a> {
    let mut responses = responses.iter();
    let mut topics = responses.next().map(|r| r.topics().iter());
    let (curr_topic, partitions) =
        topics.as_mut()
        .and_then(|t| t.next())
        .map_or((None, None), |t| (Some(t.topic()), Some(t.partitions().iter())));
    ResponseIter {
        responses: responses,
        topics: topics,
        curr_topic: curr_topic.unwrap_or(""),
        partitions: partitions,
    }
}

impl<'a> Iterator for ResponseIter<'a> {
    type Item = Response<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        // ~ then the next available partition
        if let Some(p) = self.partitions.as_mut().and_then(|p| p.next()) {
            return Some(Response {
                topic: self.curr_topic,
                partition: p.partition(),
                data: p.data(),
            });
        }
        // ~ then the next available topic
        if let Some(t) = self.topics.as_mut().and_then(|t| t.next()) {
            self.curr_topic = t.topic();
            self.partitions = Some(t.partitions().iter());
            return self.next();
        }
        // ~ then the next available response
        if let Some(r) = self.responses.next() {
            self.curr_topic = "";
            self.topics = Some(r.topics().iter());
            return self.next();
        }
        // ~ finally we know there's nothing available anymore
        None
    }
}

// --------------------------------------------------------------------

/// Messages retrieved from kafka in one fetch request.  This is a
/// concatenation of blocks of messages successfully retrieved from
/// the consumed topic partitions.  Each such partitions is guaranteed
/// to be present at most once in this structure.
pub struct MessageSets {
    responses: Vec<zfetch::FetchResponse>,

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

/// An iterator over the consumed topic partition message sets.
pub struct MessageSetsIter<'a> {
    responses: slice::Iter<'a, zfetch::FetchResponse>,
    topics: Option<slice::Iter<'a, zfetch::TopicFetchResponse<'a>>>,
    curr_topic: &'a str,
    partitions: Option<slice::Iter<'a, zfetch::PartitionFetchResponse<'a>>>,
}

/// A set of messages succesfully retrieved from a specific topic
/// partition.
pub struct MessageSet<'a> {
    pub topic: &'a str,
    pub partition: i32,
    pub messages: &'a [Message<'a>],
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
