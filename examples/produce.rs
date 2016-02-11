extern crate kafka;

use kafka::producer::{Producer, ProduceRecord};
use kafka::error::Error as KafkaError;

/// This program demonstrates sending a single message through a
/// `Producer`. This is a convenient higher-level client that will fit
/// most use cases.
fn main() {
    let broker = "localhost:9092";
    let topic = "my-topic";

    let data = "hello, kafka".as_bytes();

    if let Err(e) = produce_message(data, topic, vec![broker.to_owned()]) {
        println!("Failed producing messages: {}", e);
    }
}

fn produce_message(data: &[u8], topic: &str, brokers: Vec<String>)
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
    // ~ we specify -1 as the target partition which will cause the
    // producer to find out one on its own using its underlygin
    // partitioner.  note, if we specified `partition >= 0` the
    // producer would respect our choice.
    // ~ we're sending 'data' as a 'value'. there will be no key
    // associated with the sent message.
    let r = try!(producer.send(&ProduceRecord::from_value(topic, data)));
    println!("message sent: {:?}", r);
    Ok(())
}
