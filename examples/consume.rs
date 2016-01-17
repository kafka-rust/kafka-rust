extern crate kafka;

use kafka::client::{KafkaClient, FetchOffset};
use kafka::consumer::Consumer;

/// This program demonstrates consuming messages through a `Consumer`.
/// This is a convenient client that will fit most use cases.  Note
/// that consumed messages are tracked by Kafka so you can only
/// consume them once.  This is what you want for most use cases, you
/// can look at `examples/fetch.rs` for a lower level API.
fn main() {
    let broker = "localhost:9092";
    let topic = "my-topic";

    println!("About to consume messages at {} from: {}", broker, topic);


    let mut client = KafkaClient::new(vec!(broker.to_owned()));
    if let Err(e) = client.load_metadata_all() {
        println!("Failed to load meta data from {}: {}", broker, e);
        return;
    }

    // ~ make sure to print out a warning message when the target
    // topic does not yet exist
    if !client.topics().contains(topic) {
        println!("No such topic at {}: {}", broker, topic);
        return;
    }

    let mut con = Consumer::new(client, "test-group".to_owned(), topic.to_owned())
        .with_fallback_offset(FetchOffset::Earliest);
    loop {
        match con.poll() {
            Err(e) => {
                println!("Error consuming messages: {}", e);
                break;
            }
            Ok(mss) => {
                if mss.is_empty() {
                    println!("No more messages.");
                    break;
                }
                for ms in mss.iter() {
                    for m in ms.messages() {
                        println!("{}:{}@{}: {:?}", ms.topic(), ms.partition(), m.offset, m.value);
                    }
                }
            }
        }
    }
}
