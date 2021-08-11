extern crate zstd;
extern crate kafka;
extern crate getopts;
extern crate env_logger;
#[macro_use]
extern crate error_chain;

use std::{env, process};
use std::fs::File;
use std::str::FromStr;
use std::io::{self, stdin, stderr, Write, BufRead, BufReader};
use std::ops::{Deref, DerefMut};
use std::time::Duration;

//use std::io::{Cursor, Read, Write};
use std::str::from_utf8;
use zstd::{Decoder, Encoder};
use kafka::compression::{self, zstandard};

use kafka::client::{KafkaClient, Compression, RequiredAcks, DEFAULT_CONNECTION_IDLE_TIMEOUT_MILLIS};
use kafka::producer::{AsBytes, Producer, Record, DEFAULT_ACK_TIMEOUT_MILLIS};
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use kafka::error::Error as KafkaError;

fn main() {
    env_logger::init();

    //check_basic_compression();

    let broker = "localhost:9092";
    let topic = "kafka-rust-test3";

    let data = "hello, kafka John Ward".as_bytes();

    match produce_message(data, topic, vec![broker.to_owned()]) {
        Ok(_a) => println!("OK"),
        Err(e) => println!("Error: {}", e),
    }

    let broker2 = "localhost:9092".to_owned();
    let topic2 = "kafka-rust-test3".to_owned();
    let group2 = "my-group".to_owned();

    match consume_messages(group2, topic2, vec![broker2]) {
        Ok(_a) => println!("OK"),
        Err(e) => println!("Error {}", e),
    }

    //test_produce_fetch_messages();
    // match produce_message(&cfg_produce) {
    //     Ok(_a) => println!("OK"),
    //     Err(_e) => println!("Error"),
    // }

    // match test_compression_consume(cfg_consume) {
    //     Ok(_a) => println!("OK"),
    //     Err(_e) => println!("Error"),
    // }
}

// --------------------------------------------------------------------

error_chain! {
    links {
        Kafka(kafka::error::Error, kafka::error::ErrorKind);
    }
    foreign_links {
        Io(io::Error);
        Opt(getopts::Fail);
    }
}

fn check_basic_compression() {
    let b = "This string will be read".as_bytes();

    //let buffer = zstd::encode_all(&b[..], 3).unwrap();
    let buffer = zstandard::compress(b, 3).unwrap();

    //let new_buffer = zstd::decode_all(&buffer[..]).unwrap();

    let new_buffer = zstandard::uncompress(&buffer[..]).unwrap();

    let buf_str = from_utf8(&new_buffer).unwrap();

    println!("{}",buf_str);
}

fn produce_message<'a, 'b>(
    data: &'a [u8],
    topic: &'b str,
    brokers: Vec<String>,
) -> Result<()> {
    println!("About to publish a message at {:?} to: {}", brokers, topic);

    // ~ create a producer. this is a relatively costly operation, so
    // you'll do this typically once in your application and re-use
    // the instance many times.
    let mut producer = Producer::from_hosts(brokers)
        // ~ give the brokers one second time to ack the message
        .with_ack_timeout(Duration::from_secs(1))
        // ~ require only one broker to ack the message
        .with_required_acks(RequiredAcks::One)
        .with_compression(Compression::ZSTANDARD)
        // ~ build the producer with the above settings
        .create()
        .unwrap();

    // ~ now send a single message.  this is a synchronous/blocking
    // operation.

    // ~ we're sending 'data' as a 'value'. there will be no key
    // associated with the sent message.

    // ~ we leave the partition "unspecified" - this is a negative
    // partition - which causes the producer to find out one on its
    // own using its underlying partitioner.
    producer.send(&Record {
        topic: topic,
        partition: -1,
        key: (),
        value: data,
    })?;

    // ~ we can achieve exactly the same as above in a shorter way with
    // the following call
    //producer.send(&Record::from_value(topic, data))?;

    Ok(())
}


fn consume_messages(group: String, topic: String, brokers: Vec<String>) -> Result<()> {
    let mut con = Consumer::from_hosts(brokers)
        .with_topic(topic)
        .with_group(group)
        .with_fallback_offset(FetchOffset::Earliest)
        .with_offset_storage(GroupOffsetStorage::Kafka)
        .create()
        .unwrap();

    loop {
        let mss = con.poll()?;
        if mss.is_empty() {
            //println!("No messages available right now.");
            //return Ok(());
        }

        for ms in mss.iter() {
            for m in ms.messages() {
                println!(
                    "{}:{}@{}: {:?}, {:?}",
                    ms.topic(),
                    ms.partition(),
                    m.offset,
                    m.value,
                    String::from_utf8_lossy(m.value)
                );

                let val_str = String::from_utf8_lossy(m.value);
                println!("{}", val_str);
            }
            let _ = con.consume_messageset(ms);
        }
        con.commit_consumed()?;
    }
}
