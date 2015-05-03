
use std::io::prelude::*;
use std::net::TcpStream;
use std::io::Result;
use std::fmt;

pub struct KafkaConnection {
    host: String,
    timeout: i32,
    stream: TcpStream
}

impl fmt::Debug for KafkaConnection {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {

        write!(f, "KafkaConnection")
    }
}

impl KafkaConnection {

    pub fn send(&mut self, msg: & Vec<u8>) -> Result<usize> {
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

    pub fn clone(&self) -> KafkaConnection {
        KafkaConnection{stream: self.stream.try_clone().unwrap(), host: self.host.clone(), timeout:self.timeout}
    }

    pub fn new(host: &String, timeout: i32) -> KafkaConnection {
        let s: &str = &host;
        let stream = TcpStream::connect(s).unwrap();
        KafkaConnection{host: s.to_string(), timeout: timeout, stream: stream}

    }
}
