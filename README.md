# Kafka Rust Client

[![Build Status](https://travis-ci.org/spicavigo/kafka-rust.svg?branch=master)](https://travis-ci.org/spicavigo/kafka-rust) [![](http://meritbadge.herokuapp.com/kafka)](https://crates.io/crates/kafka)

### Documentation

[Kafka Rust Client Documentation](http://fauzism.co/rustdoc/kafka/index.html)

The documentation includes some examples too.


### Installation

This crate works with Cargo and is on [crates.io](https://crates.io/crates/kafka). I will be updating the package frequently till we move out of pre-release. So add this to your `Cargo.toml` (instead of a specific version):

```toml
[dependencies]
kafka = "*"
```

#### Usage:

##### Load topic metadata:

```rust
extern crate kafka;
use kafka::client::KafkaClient;
fn main() {
    let mut client = KafkaClient::new(&vec!("localhost:9092".to_string()));
    client.load_metadata(&vec!("my-topic".to_string()));
    // OR
    // client.load_metadata_all(); // Loads metadata for all topics
 }
```
##### Fetch Offsets:

```rust
extern crate kafka;
use kafka::client::KafkaClient;
fn main() {
    let mut client = KafkaClient::new(&vec!("localhost:9092".to_string()));
    client.load_metadata_all();
    println!("{:?}", client.fetch_topic_offset(&"my-topic".to_string()));
}
```
##### Produce:

```rust
extern crate kafka;
use kafka::client::KafkaClient;
fn main() {
    let mut client = KafkaClient::new(&vec!("localhost:9092".to_string()));
    client.load_metadata_all();
    // Currently only supports sending a single message
    client.send_message(
        &"my-topic".to_string(),        // Topic
        0,                              // Partition
        1,                              // Required Acks
        100,                            // Timeout
        &"b".to_string().into_bytes()   // Message
    )
}
```

##### Fetch Messages:

```rust
extern crate kafka;
use kafka::client::KafkaClient;
fn main() {
    let mut client = KafkaClient::new(&vec!("localhost:9092".to_string()));
    client.load_metadata_all();
    // Topic, partition, offset
    for om in client.fetch_messages(&"my-topic".to_string(), 0, 0) {
        println!("{:?}", om);
    };
}
```

#### TODO:

* Tests
* Missing functions for the implemented methods (eg. Fetch Message by multiple topic/partition)
* Offset Management functions
