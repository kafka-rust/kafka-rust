# Kafka Rust Client

[![Build Status](https://travis-ci.org/spicavigo/kafka-rust.svg?branch=master)](https://travis-ci.org/spicavigo/kafka-rust) [![](http://meritbadge.herokuapp.com/kafka)](https://crates.io/crates/kafka)

### Documentation

The [documentation](https://spicavigo.github.io/kafka-rust/) include
some examples, too.


### Installation

This crate works with Cargo and is on
[crates.io](https://crates.io/crates/kafka).  The API is currently
under heavy movement although we do follow semantic versioning.

```toml
[dependencies]
kafka = "0.2"
```

To build kafka-rust you'll need `libsnappy-dev` on your local
machine. If that library is not installed in the usual path, you can
export the `LD_LIBRARY_PATH` and `LD_RUN_PATH` environment variables
before issueing `cargo build`.


### Example:

As mentioned, the cargo generated documentation constains some
examples. Further, standalone, compilable example programs are
provided in the [examples directory of the
repository](https://github.com/spicavigo/kafka-rust/tree/master/examples).

[KafkaClient](https://spicavigo.github.io/kafka-rust/kafka/client/struct.KafkaClient.html)
is the main entry point into the API. This is a mid-level abstraction
for Kafka.  Its main methods are:

* [Loading metadata](https://spicavigo.github.io/kafka-rust/kafka/client/struct.KafkaClient.html#method.load_metadata_all)
* [Fetching topic offsets](https://spicavigo.github.io/kafka-rust/kafka/client/struct.KafkaClient.html#method.fetch_offsets)
* [Sending messages](https://spicavigo.github.io/kafka-rust/kafka/client/struct.KafkaClient.html#method.send_messages)
* [Fetching messages](https://spicavigo.github.io/kafka-rust/kafka/client/struct.KafkaClient.html#method.fetch_messages)
* [Committing a consumer group's offsets](https://spicavigo.github.io/kafka-rust/kafka/client/struct.KafkaClient.html#method.commit_offsets)
* [Fetching a consumer group's offsets](https://spicavigo.github.io/kafka-rust/kafka/client/struct.KafkaClient.html#method.fetch_group_topics_offset)

Many of these methods also have convenience partners acting on a
single topic or partition.


### [Consumer] (https://spicavigo.github.io/kafka-rust/kafka/consumer/index.html)

This is a slightly higher-level Consumer for Kafka. It provides convenient
offset management support on behalf of a specified group.


### [Creating a topic] (https://kafka.apache.org/08/quickstart.html)

Note unless otherwise explicitely stated in the documentation, this
library will ignore requests to topics which it doesn't know about. In
particular it will not send to or try to retrieve messages from not
yet existing topics.  Give your local kafka server installation you
can create topics if the following command (`kafka-topics.sh` is part
of the Kafka distribution):

```
kafka-topics.sh --topic my-topic --create --zookeeper localhost:2181  --partition 1 --replication-factor 1
```
