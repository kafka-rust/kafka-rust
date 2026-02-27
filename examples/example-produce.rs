use std::time::Duration;

use kafka::error::Error as KafkaError;
use kafka::producer::{Producer, Record, RequiredAcks};
#[cfg(feature = "producer_timestamp")]
use kafka::producer::TimestampedRecord;

/// This program demonstrates sending single message through a
/// `Producer`.  This is a convenient higher-level client that will
/// fit most use cases.
fn main() {
    tracing_subscriber::fmt::init();

    let broker = "localhost:9092";
    let topic = "my-topic";

    let data = "hello, kafka".as_bytes();

    if let Err(e) = produce_message(data, topic, vec![broker.to_owned()]) {
        println!("Failed producing messages: {}", e);
    }

    #[cfg(feature = "producer_timestamp")]
    {
        let timestamp_ms = 1_700_000_000_000;
        if let Err(e) = produce_message_with_timestamp(data, topic, vec![broker.to_owned()], timestamp_ms)
        {
            println!("Failed producing timestamped message: {}", e);
        }
    }
}

fn produce_message(data: &[u8], topic: &str, brokers: Vec<String>) -> Result<(), KafkaError> {
    println!("About to publish a message at {:?} to: {}", brokers, topic);

    // ~ create a producer. this is a relatively costly operation, so
    // you'll do this typically once in your application and re-use
    // the instance many times.
    let mut producer = Producer::from_hosts(brokers)
        // ~ give the brokers one second time to ack the message
        .with_ack_timeout(Duration::from_secs(1))
        // ~ require only one broker to ack the message
        .with_required_acks(RequiredAcks::One)
        // ~ build the producer with the above settings
        .create()?;

    // ~ now send a single message.  this is a synchronous/blocking
    // operation.

    // ~ we're sending 'data' as a 'value'. there will be no key
    // associated with the sent message.

    // ~ we leave the partition "unspecified" - this is a negative
    // partition - which causes the producer to find out one on its
    // own using its underlying partitioner.
    producer.send(&Record {
        topic,
        partition: -1,
        key: (),
        value: data,
    })?;

    // ~ we can achieve exactly the same as above in a shorter way with
    // the following call
    producer.send(&Record::from_value(topic, data))?;

    Ok(())
}

#[cfg(feature = "producer_timestamp")]
fn produce_message_with_timestamp(
    data: &[u8],
    topic: &str,
    brokers: Vec<String>,
    timestamp_ms: i64,
) -> Result<(), KafkaError> {
    let mut producer = Producer::from_hosts(brokers)
        .with_ack_timeout(Duration::from_secs(1))
        .with_required_acks(RequiredAcks::One)
        .create()?;

    producer.send_timestamped(&TimestampedRecord::from_value(topic, data, timestamp_ms))?;

    Ok(())
}
