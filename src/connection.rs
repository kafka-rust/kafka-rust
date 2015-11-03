use std::fmt;
use std::io::{Read,Write};
use std::net::TcpStream;
use std::time::Duration;

use error::{Error, Result};

pub struct KafkaConnection {
    host: String,
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

    pub fn read_exact(&mut self, size: u64) -> Result<Vec<u8>> {
        let mut buffer: Vec<u8> = Vec::with_capacity(size as usize);
        let mut s = (&self.stream).take(size);
        let bytes_read = try!(s.read_to_end(&mut buffer));
        if bytes_read != size as usize {
            Err(Error::UnexpectedEOF)
        } else {
            Ok(buffer)
        }
    }

    pub fn new(host: &str, timeout_secs: i32) -> Result<KafkaConnection> {
        let stream = try!(TcpStream::connect(host));
        if timeout_secs > 0 {
            let t = Some(Duration::from_secs(timeout_secs as u64));
            stream.set_read_timeout(t).expect("Set connection read-timeout");
            stream.set_write_timeout(t).expect("Set connection write-timeout");
        }
        Ok(KafkaConnection{host: host.to_owned(), stream: stream})
    }
}
