extern crate zstd;
extern crate kafka;

use std::io::{Cursor, Read, Write};
use std::str::from_utf8;
use zstd::{Decoder, Encoder};

use kafka::compression::{self, zstandard};

// use std::collections::HashMap;
// use std::collections::HashSet;
// use std::time::Duration;

//use env_logger;

use kafka::client::fetch::Response;
use kafka::client::{
    CommitOffset, FetchOffset, FetchPartition, PartitionOffset, ProduceMessage, RequiredAcks,
};

fn main() {
    check_basic_compression();

    //test_produce_fetch_messages();
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

// fn test_produce_fetch_messages() {
//     let _ = env_logger::try_init();
//     let mut client = new_ready_kafka_client();
//     let topics = [TEST_TOPIC_NAME, TEST_TOPIC_NAME_2];
//     let init_latest_offsets = client.fetch_offsets(&topics, FetchOffset::Latest).unwrap();

//     // first send the messages and verify correct confirmation responses
//     // from kafka
//     let req = vec![
//         ProduceMessage::new(TEST_TOPIC_NAME, 0, None, Some("a".as_bytes())),
//         ProduceMessage::new(TEST_TOPIC_NAME, 1, None, Some("b".as_bytes())),
//         ProduceMessage::new(TEST_TOPIC_NAME_2, 0, None, Some("c".as_bytes())),
//         ProduceMessage::new(TEST_TOPIC_NAME_2, 1, None, Some("d".as_bytes())),
//     ];

//     let resp = client
//         .produce_messages(RequiredAcks::All, Duration::from_millis(1000), req)
//         .unwrap();

//     assert_eq!(2, resp.len());

//     // need to keep track of the offsets so we can fetch them next
//     let mut fetches = Vec::new();

//     for confirm in &resp {
//         assert!(confirm.topic == TEST_TOPIC_NAME || confirm.topic == TEST_TOPIC_NAME_2);
//         assert_eq!(2, confirm.partition_confirms.len());

//         assert!(confirm
//             .partition_confirms
//             .iter()
//             .any(|part_confirm| { part_confirm.partition == 0 && part_confirm.offset.is_ok() }));

//         assert!(confirm
//             .partition_confirms
//             .iter()
//             .any(|part_confirm| { part_confirm.partition == 1 && part_confirm.offset.is_ok() }));

//         for part_confirm in confirm.partition_confirms.iter() {
//             fetches.push(FetchPartition::new(
//                 confirm.topic.as_ref(),
//                 part_confirm.partition,
//                 part_confirm.offset.unwrap(),
//             ));
//         }
//     }

//     // now fetch the messages back and verify that they are the correct
//     // messages
//     let fetch_resps = client.fetch_messages(fetches).unwrap();
//     let messages = flatten_fetched_messages(&fetch_resps);

//     let correct_messages = vec![
//         (TEST_TOPIC_NAME, 0, "a".as_bytes()),
//         (TEST_TOPIC_NAME, 1, "b".as_bytes()),
//         (TEST_TOPIC_NAME_2, 0, "c".as_bytes()),
//         (TEST_TOPIC_NAME_2, 1, "d".as_bytes()),
//     ];

//     assert!(correct_messages
//         .into_iter()
//         .all(|c_msg| { messages.contains(&c_msg) }));

//     let end_latest_offsets = client.fetch_offsets(&topics, FetchOffset::Latest).unwrap();

//     for (topic, begin_partition_offsets) in init_latest_offsets {
//         let begin_partition_offsets: HashMap<i32, i64> = begin_partition_offsets
//             .iter()
//             .map(|po| (po.partition, po.offset))
//             .collect();

//         let end_partition_offsets: HashMap<i32, i64> = end_latest_offsets
//             .get(&topic)
//             .unwrap()
//             .iter()
//             .map(|po| (po.partition, po.offset))
//             .collect();

//         for (partition, begin_offset) in begin_partition_offsets {
//             let end_offset = end_partition_offsets.get(&partition).unwrap();
//             assert_eq!(begin_offset + 1, *end_offset);
//         }
//     }
// }