# Kafka Rust Client


#### Usage:

##### Load topic metadata:
    extern crate kafka;
    use kafka::client::KafkaClient;
    fn main() {
        let mut client = KafkaClient::new(&vec!("localhost:9092".to_string()));
        client.load_metadata(&vec!("my-topic".to_string()));
        // OR
        // client.load_metadata_all(); // Loads metadata for all topics
     }

##### Fetch Offsets:
    extern crate kafka;
    use kafka::client::KafkaClient;
    fn main() {
        let mut client = KafkaClient::new(&vec!("localhost:9092".to_string()));
        client.load_metadata_all();
        println!("{:?}", client.fetch_topic_offset(&"my-topic".to_string()));
    }

##### Produce:
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


##### Fetch Messages:
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


#### TODO:

* Test
* Remove unnecessary imports and publics
* Tests
* Missing functions for the implemented methods (eg. Fetch Message by multiple topic/partition)
* Offset Management functions
* Documentation
