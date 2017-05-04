use kafka::client::Compression;
use kafka::producer::Record;

use super::*;

fn test_compression(compression: Compression) {
    extern crate env_logger;

    let _ = env_logger::init();

    let mut producer = test_producer_builder()
        .with_compression(compression)
        .create()
        .unwrap();
    let mut consumer = test_consumer();

    let records = &[Record::from_value(TEST_TOPIC_NAME, "foo".as_bytes()),
                    Record::from_value(TEST_TOPIC_NAME, "bar".as_bytes())];
    let confirms = producer.send_all(records).unwrap();

    for confirm in confirms {
        assert_eq!(TEST_TOPIC_NAME.to_owned(), confirm.topic);

        for partition_confirm in confirm.partition_confirms {
            assert!(partition_confirm.offset.is_ok(),
                    format!("should have sent successfully. Got: {:?}", partition_confirm.offset));
        }
    }

    let messages = consumer.poll().unwrap();
    assert!(!messages.is_empty(), format!("messages was empty: {:?}", messages));

    let mut messages_iter = messages.iter();
    let message_set = messages_iter.next().unwrap();

    assert_eq!(1, message_set.messages().len(), "should only be one message");

    let message_content = message_set.messages()[0].value;
    assert_eq!(message_content, b"bar", "incorrect message contents");

    let message_set = messages_iter.next().unwrap();

    assert_eq!(1, message_set.messages().len(), "should only be one message");

    let message_content = message_set.messages()[0].value;
    assert_eq!(message_content, b"foo", "incorrect message contents");

    consumer.commit_consumed().unwrap();
}

#[test]
fn test_none_compression() {
    test_compression(Compression::NONE);
}

#[test]
fn test_gzip_compression() {
    test_compression(Compression::GZIP);
}

#[test]
fn test_snappy_compression() {
    test_compression(Compression::SNAPPY);
}

#[test]
fn test_lz4_compression() {
    test_compression(Compression::LZ4);
}
