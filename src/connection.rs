use std::fmt;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

#[cfg(feature = "security")]
use openssl::ssl::{SslContext, SslStream};

use error::Result;

#[cfg(not(feature = "security"))]
type KafkaStream = TcpStream;

#[cfg(feature = "security")]
use self::openssled::KafkaStream;

#[cfg(feature = "security")]
mod openssled {
    use std::io::{self, Read, Write};
    use std::net::TcpStream;
    use std::time::Duration;

    use openssl::ssl::SslStream;

    pub enum KafkaStream {
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

        pub fn set_read_timeout(&self, dur: Option<Duration>) -> io::Result<()> {
            self.get_ref().set_read_timeout(dur)
        }

        pub fn set_write_timeout(&self, dur: Option<Duration>) -> io::Result<()> {
            self.get_ref().set_write_timeout(dur)
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
}

pub struct KafkaConnection {
    host: String,
    stream: KafkaStream,
}

impl fmt::Debug for KafkaConnection {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "KafkaConnection {{ host: {} }}", self.host)
    }
}

impl KafkaConnection {

    pub fn send(&mut self, msg: &[u8]) -> Result<usize> {
        self.stream.write(&msg[..]).map_err(From::from)
    }

    pub fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        (&mut self.stream).read_exact(buf).map_err(From::from)
    }

    pub fn read_exact_alloc(&mut self, size: u64) -> Result<Vec<u8>> {
        let size: usize = size as usize;
        let mut buffer: Vec<u8> = Vec::with_capacity(size);
        // this is safe actually: we are setting the len to the
        // buffers capacity and either fully populate it in the
        // following call to `read_exact` or discard the vector (in
        // the error case)
        unsafe { buffer.set_len(size) };
        try!((&mut self.stream).read_exact(buffer.as_mut_slice()));
        Ok(buffer)
    }

    fn from_stream(stream: KafkaStream, host: &str, timeout_secs: i32) -> KafkaConnection {
        if timeout_secs > 0 {
            let t = Some(Duration::from_secs(timeout_secs as u64));
            stream.set_read_timeout(t).expect("Set connection read-timeout");
            stream.set_write_timeout(t).expect("Set connection write-timeout");
        }
        KafkaConnection{host: host.to_owned(), stream: stream}
    }

    #[cfg(not(feature = "security"))]
    pub fn new(host: &str, timeout_secs: i32) -> Result<KafkaConnection> {
        Ok(KafkaConnection::from_stream(try!(TcpStream::connect(host)), host, timeout_secs))
    }

    #[cfg(feature = "security")]
    pub fn new(host: &str, timeout_secs: i32, security: Option<&SslContext>) -> Result<KafkaConnection> {
        let stream = try!(TcpStream::connect(host));
        let stream = match security {
            Some(ctx) => KafkaStream::Ssl(try!(SslStream::connect(ctx, stream))),
            None => KafkaStream::Plain(stream),
        };
        Ok(KafkaConnection::from_stream(stream, host, timeout_secs))
    }
}
