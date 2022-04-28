use kafka::client::{FetchPartition, KafkaClient};

/// This program demonstrates the low level api for fetching messages.
/// Please look at examles/consume.rs for an easier to use API.
fn main() {
    env_logger::init();

    let broker = "localhost:9092";
    let topic = "my-topic";
    let partition = 0;
    let offset = 0;

    println!(
        "About to fetch messages at {} from: {} (partition {}, offset {}) ",
        broker, topic, partition, offset
    );

    let mut client = KafkaClient::new(vec![broker.to_owned()]);
    if let Err(e) = client.load_metadata_all() {
        println!("Failed to load metadata from {}: {}", broker, e);
        return;
    }

    // ~ make sure to print out a warning message when the target
    // topic does not yet exist
    if !client.topics().contains(topic) {
        println!("No such topic at {}: {}", broker, topic);
        return;
    }

    match client.fetch_messages(&[FetchPartition::new(topic, partition, offset)]) {
        Err(e) => {
            println!("Failed to fetch messages: {}", e);
        }
        Ok(resps) => {
            for resp in resps {
                for t in resp.topics() {
                    for p in t.partitions() {
                        match p.data() {
                            Err(ref e) => {
                                println!("partition error: {}:{}: {}", t.topic(), p.partition(), e)
                            }
                            Ok(ref data) => {
                                println!(
                                    "topic: {} / partition: {} / latest available message \
                                          offset: {}",
                                    t.topic(),
                                    p.partition(),
                                    data.highwatermark_offset()
                                );
                                for msg in data.messages() {
                                    println!(
                                        "topic: {} / partition: {} / message.offset: {} / \
                                              message.len: {}",
                                        t.topic(),
                                        p.partition(),
                                        msg.offset,
                                        msg.value.len()
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
