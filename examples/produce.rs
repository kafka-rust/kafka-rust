extern crate kafka;
extern crate env_logger;

use kafka::producer::{Producer, Record};
use kafka::error::Error as KafkaError;

/// This program demonstrates sending single message through a
/// `Producer`.  This is a convenient higher-level client that will
/// fit most use cases.
fn main() {
    env_logger::init().unwrap();

    let broker = "localhost:9092";
    let topic = "my-topic";

    let data = "hello, kafka".as_bytes();

    if let Err(e) = produce_message(data, topic, vec![broker.to_owned()]) {
        println!("Failed producing messages: {}", e);
    }
}

fn produce_message<'a, 'b>(data: &'a [u8], topic: &'b str, brokers: Vec<String>)
                   -> Result<(), KafkaError>
{
    println!("About to publish a message at {:?} to: {}", brokers, topic);

    // ~ create a producer. this is a relatively costly operation, so
    // you'll do this typically once in your application and re-use
    // the instance many times.
    let mut producer =
        try!(Producer::from_hosts(brokers)
             // ~ give the brokers one second time to ack the message
             .with_ack_timeout(1000)
             // ~ require only one broker to ack the message
             .with_required_acks(1)
             // ~ build the producer with the above settings
             .create());

    // ~ now send a single message.  this is a synchronous/blocking
    // operation.

    // ~ we're sending 'data' as a 'value'. there will be no key
    // associated with the sent message.

    // ~ we leave the partition "unspecified" - this is a negative
    // partition - which causes the producer to find out one on its
    // own using its underlying partitioner.
    try!(producer.send(&Record{
        topic: topic,
        partition: -1,
        key: (),
        value: data,
    }));

    // ~ we can achive exactly the same as above in a shorter way with
    // the following call
    try!(producer.send(&Record::from_value(topic, data)));

    Ok(())
}
