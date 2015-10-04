use std::io::prelude::*;
use std::net::TcpStream;
use std::fmt;

use error::Result;

pub struct KafkaConnection {
    host: String,
    timeout: i32,
    stream: TcpStream
}

impl fmt::Debug for KafkaConnection {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "KafkaConnection to {}", self.host)
    }
}

impl KafkaConnection {

    pub fn send(&mut self, msg: &Vec<u8>) -> Result<usize> {
        self.stream.write(&msg[..]).map_err(From::from)
    }

    pub fn read(&mut self, size: u64, buffer: &mut Vec<u8>) -> Result<usize>{
        let mut buffer_: Vec<u8> = Vec::new();
        let mut s = (&self.stream).take(size);
        let mut total_bytes_read: usize = 0;
        match s.read_to_end(&mut buffer_) {
            Err(err) => return Err(From::from(err)),
            Ok(bytes_read) => {
                total_bytes_read += bytes_read;
                for b in buffer_.iter() {
                    buffer.push(*b);
                }
            }
        }
        Ok(total_bytes_read)

    }

    pub fn new(host: &str, timeout: i32) -> Result<KafkaConnection> {
        let stream = try!(TcpStream::connect(host));
        Ok(KafkaConnection{host: host.to_owned(), timeout: timeout, stream: stream})
    }
}
