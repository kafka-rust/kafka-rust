extern crate kafka;
use kafka::client::KafkaClient;

fn main() {


    let mut client = KafkaClient::new(&vec!("localhost:9092".to_string()));
    client.load_metadata_all();
    println!("{:?}", client.fetch_topic_offset(&"my-topic".to_string()));
    /*let p = ProduceRequest::new_single(&"my-topic".to_string(), 0, 1,
                      100, &"b".to_string().into_bytes(),
                      1, &"rust".to_string());
    println!("{:?}", p);
    let mut buf = vec!();
    p.encode(&mut buf);
    println!("{:?}", buf);
    client.send_message(&"my-topic".to_string(), 0, 1,
                      100, &"b".to_string().into_bytes());*/

    //for om in client.fetch_messages(&"my-topic".to_string(), 0, 0) {
    //    println!("{:?}", om);
    //};

}
