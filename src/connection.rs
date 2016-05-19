use std::fmt;
use std::io::{self, Read,Write};
use std::net::TcpStream;
use std::time::Duration;

use openssl::ssl::{SslContext, SslStream};

use error::{Error, Result};

enum KafkaStream {
    Plain(TcpStream),
    Ssl(SslStream<TcpStream>)
}

impl KafkaStream {
    fn get_ref(&self) -> &TcpStream {
        match self {
            &KafkaStream::Plain(ref s) => s,
            &KafkaStream::Ssl(ref s) => s.get_ref()
        }
    }
}

impl Read for KafkaStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            &mut KafkaStream::Plain(ref mut s) => s.read(buf),
            &mut KafkaStream::Ssl(ref mut s) => s.read(buf),
        }
    }
}

impl Write for KafkaStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            &mut KafkaStream::Plain(ref mut s) => s.write(buf),
            &mut KafkaStream::Ssl(ref mut s) => s.write(buf),
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        match self {
            &mut KafkaStream::Plain(ref mut s) => s.flush(),
            &mut KafkaStream::Ssl(ref mut s) => s.flush(),
        }
    }
}

pub struct KafkaConnection {
    host: String,
    stream: KafkaStream
}

impl fmt::Debug for KafkaConnection {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "KafkaConnection to {}", self.host)
    }
}

impl KafkaConnection {

    pub fn send(&mut self, msg: &[u8]) -> Result<usize> {
        self.stream.write(&msg[..]).map_err(From::from)
    }

    pub fn read_exact(&mut self, size: u64) -> Result<Vec<u8>> {
        let mut buffer: Vec<u8> = Vec::with_capacity(size as usize);
        let mut s = (&mut self.stream).take(size);
        let bytes_read = try!(s.read_to_end(&mut buffer));
        if bytes_read != size as usize {
            Err(Error::UnexpectedEOF)
        } else {
            Ok(buffer)
        }
    }

    pub fn new(host: &str, timeout_secs: i32, security: Option<&SslContext>) -> Result<KafkaConnection> {
        let plain_stream = try!(TcpStream::connect(host));
        let stream = match security {
            Some(ctx) => KafkaStream::Ssl(try!(SslStream::connect(ctx, plain_stream))),
            None => KafkaStream::Plain(plain_stream),
        };
        if timeout_secs > 0 {
            let t = Some(Duration::from_secs(timeout_secs as u64));
            stream.get_ref().set_read_timeout(t).expect("Set connection read-timeout");
            stream.get_ref().set_write_timeout(t).expect("Set connection write-timeout");
        }
        Ok(KafkaConnection{host: host.to_owned(), stream: stream})
    }
}
