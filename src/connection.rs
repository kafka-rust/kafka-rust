
use std::io::prelude::*;
use std::net::TcpStream;

pub struct KafkaConnection {
    stream: TcpStream
}

impl KafkaConnection {

    pub fn send(&mut self, msg: & Vec<u8>) -> Result<usize>{
        println!("{:?}", &msg[..]);
        self.stream.write(&msg[..])
    }

    pub fn read(&mut self, size: u64, buffer: &mut Vec<u8>) {
        println!("Reading {} bytes", size);
        let mut buffer_: Vec<u8> = Vec::new();
        let mut s = (&self.stream).take(size);
        match s.read_to_end(&mut buffer_) {
            Err(_) => return,
            Ok(bytes_read) => {
                println!("Read {} bytes", bytes_read);
                for b in buffer_.iter() {
                    buffer.push(*b);
                }
            }
        }

    }

    pub fn new(host: &str) -> KafkaConnection {
        let _host = host.to_string();
        let stream = TcpStream::connect(host).unwrap();
        KafkaConnection{stream: stream}

    }
}
