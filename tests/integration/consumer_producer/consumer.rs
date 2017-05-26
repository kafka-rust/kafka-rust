use super::*;
use kafka::producer::Record;
use kafka::error;

/// Tests that consuming one message works
#[test]
fn test_consumer_poll() {
    // poll once to set a position in the topic
    let mut consumer = test_consumer();
    let mut messages = consumer.poll().unwrap();
    assert!(messages.is_empty(), format!("messages was not empty: {:?}", messages));

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
