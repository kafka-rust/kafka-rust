use super::*;

use std::collections::HashMap;

use kafka::producer::Record;
use kafka::error;

use env_logger;
use rand;
use rand::Rng;

const RANDOM_MESSAGE_SIZE: usize = 32;

/// Tests that consuming one message works
#[test]
fn test_consumer_poll() {
    // poll once to set a position in the topic
    let mut consumer = test_consumer();
    let mut messages = consumer.poll().unwrap();
    assert!(messages.is_empty(), "messages was not empty: {:?}", messages);

    // send a message and then poll it and ensure it is the correct message
    let correct_message_contents = "test_consumer_poll".as_bytes();
    let mut producer = test_producer();
    producer
        .send(&Record::from_value(TEST_TOPIC_NAME, correct_message_contents))
        .unwrap();

    messages = consumer.poll().unwrap();
    let mut messages_iter = messages.iter();
    let message_set = messages_iter.next().unwrap();

    assert_eq!(1, message_set.messages().len(), "should only be one message");

    let message_content = message_set.messages()[0].value;
    assert_eq!(correct_message_contents, message_content, "incorrect message contents");
}

/// Test Consumer::commit_messageset
#[test]
fn test_consumer_commit_messageset() {
    env_logger::init().unwrap();

    let mut consumer = test_consumer();

    // get the offsets at the beginning of the test
    let start_offsets = {
        let mut client = new_ready_kafka_client();
        client
            .fetch_group_topic_offsets(TEST_GROUP_NAME, TEST_TOPIC_NAME)
            .unwrap()
            .iter()
            .map(|po| (po.partition, if po.offset != -1 { po.offset } else { 0 }))
            .collect::<HashMap<i32, i64>>()
    };

    debug!("start offsets: {:?}", start_offsets);

    // poll once to set a position in the topic
    let messages = consumer.poll().unwrap();
    assert!(messages.is_empty(), "messages was not empty: {:?}", messages);

    let mut random_message_buf = [0u8; RANDOM_MESSAGE_SIZE];
    let mut rng = rand::thread_rng();
    let mut producer = test_producer();

    // send some messages to the topic
    const NUM_MESSAGES: i64 = 100;

    for _ in 0..NUM_MESSAGES {
        rng.fill_bytes(&mut random_message_buf);
        producer
            .send(&Record::from_value(TEST_TOPIC_NAME, &random_message_buf[..]))
            .unwrap();
    }

    let mut num_messages = 0;

    'read: loop {
        for ms in consumer.poll().unwrap().iter() {
            let messages = ms.messages();
            num_messages += messages.len();
            consumer.consume_messageset(ms).unwrap();
        }

        consumer.commit_consumed().unwrap();

        if num_messages >= (NUM_MESSAGES as usize) {
            break 'read;
        }
    }

    assert_eq!(NUM_MESSAGES as usize, num_messages, "wrong number of messages");

    // get the latest offsets and make sure they add up to the number of messages
    let latest_offsets = {
        let mut client = consumer.into_client();
        client.load_metadata_all().unwrap();
        client
            .fetch_group_topic_offsets(TEST_GROUP_NAME, TEST_TOPIC_NAME)
            .unwrap()
            .iter()
            .map(|po| (po.partition, po.offset))
            .collect::<HashMap<i32, i64>>()
    };

    debug!("end offsets: {:?}", latest_offsets);

    // add up the differences
    let num_new_messages_committed: i64 = latest_offsets
        .iter()
        .map(|(partition, new_offset)| {
            let old_offset = start_offsets.get(partition).unwrap();
            new_offset - old_offset
        })
        .sum();

    assert_eq!(NUM_MESSAGES, num_new_messages_committed, "wrong number of messages committed");
}

/// Consuming from a non-existent topic should fail.
#[test]
fn test_consumer_non_existent_topic() {
    let consumer_err = test_consumer_builder()
        .with_topic_partitions("foo_topic".to_owned(), &TEST_TOPIC_PARTITIONS)
        .create()
        .unwrap_err();

    let error_code = match consumer_err {
        error::Error(error::ErrorKind::Kafka(code), _) => code,
        _ => panic!("Should have received Kafka error"),
    };

    let correct_error_code = error::KafkaCode::UnknownTopicOrPartition;
    assert_eq!(correct_error_code, error_code, "should have errored on non-existent topic");
}
