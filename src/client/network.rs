//! Network related functionality for `KafkaClient`.
//!
//! This module is crate private and not exposed to the public except
//! through re-exports of individual items from within
//! `kafka::client`.

use std::collections::HashMap;
use std::fmt;
use std::io::{Read, Write};
use std::mem;
use std::net::TcpStream;
use std::time::Duration;

#[cfg(feature = "security")]
use openssl::ssl::{SslContext, SslStream};

use error::Result;

// --------------------------------------------------------------------

/// Security relevant configuration options for KafkaClient.
// This will be expanded in the future. See #51.
#[cfg(feature = "security")]
#[derive(Debug)]
pub struct SecurityConfig(SslContext);

#[cfg(feature = "security")]
impl SecurityConfig {
    /// In the future this will also support a kerbos via #51.
    pub fn new(ssl: SslContext) -> SecurityConfig {
        SecurityConfig(ssl)
    }
}

// --------------------------------------------------------------------

#[derive(Debug)]
pub struct Connections {
    conns: HashMap<String, KafkaConnection>,
    rw_timeout: Option<Duration>,
    #[cfg(feature = "security")]
    security_config: Option<SecurityConfig>,
}

impl Connections {
    #[cfg(not(feature = "security"))]
    pub fn new(rw_timeout: Option<Duration>) -> Connections {
        ConnectionPool {
            conns: HashMap::new(),
            rw_timeout: rw_timeout,
        }
    }

    #[cfg(feature = "security")]
    pub fn new(rw_timeout: Option<Duration>) -> Connections {
        Self::new_with_security(rw_timeout, None)
    }

    #[cfg(feature = "security")]
    pub fn new_with_security(rw_timeout: Option<Duration>, security: Option<SecurityConfig>)
                             -> Connections
    {
        Connections {
            conns: HashMap::new(),
            rw_timeout: rw_timeout,
            security_config: security,
        }
    }

    pub fn get_conn<'a>(&'a mut self, host: &str) -> Result<&'a mut KafkaConnection> {
        if let Some(conn) = self.conns.get_mut(host) {
            // ~ decouple the lifetimes to make the borrowck happy;
            // this is safe since we're immediatelly returning the
            // reference and the rest of the code in this method is
            // not affected
            return Ok(unsafe { mem::transmute(conn) });
        }
        let conn = try!(self.new_conn(host));
        self.conns.insert(host.to_owned(), conn);
        Ok(self.conns.get_mut(host).unwrap())
    }

    pub fn get_conn_any<'a>(&'a mut self) -> Option<&'a mut KafkaConnection> {
        self.conns.iter_mut().next().map(|(_, conn)| conn)
    }

    #[cfg(not(feature = "security"))]
    fn new_conn(&self, host: &str) -> Result<KafkaConnection> {
        KafkaConnection::new(host, self.timeout)
    }

    #[cfg(feature = "security")]
    fn new_conn(&self, host: &str) -> Result<KafkaConnection> {
        KafkaConnection::new(host, self.rw_timeout, self.security_config.as_ref().map(|c| &c.0))
    }
}

// --------------------------------------------------------------------

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

/// A TCP stream to a remote Kafka broker.
pub struct KafkaConnection {
    host: String,
    stream: KafkaStream,
}

impl fmt::Debug for KafkaConnection {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "KafkaConnection {{ host: \"{}\" }}", self.host)
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
        try!(self.read_exact(buffer.as_mut_slice()));
        Ok(buffer)
    }

    fn from_stream(stream: KafkaStream, host: &str, rw_timeout: Option<Duration>)
                   -> Result<KafkaConnection>
    {
        try!(stream.set_read_timeout(rw_timeout));
        try!(stream.set_write_timeout(rw_timeout));
        Ok(KafkaConnection { host: host.to_owned(), stream: stream })
    }

    #[cfg(not(feature = "security"))]
    fn new(host: &str, rw_timeout: Option<Duration>) -> Result<KafkaConnection> {
        Ok(KafkaConnection::from_stream(try!(TcpStream::connect(host)), host, rw_timeout))
    }

    #[cfg(feature = "security")]
    fn new(host: &str, rw_timeout: Option<Duration>, security: Option<&SslContext>) -> Result<KafkaConnection> {
        let stream = try!(TcpStream::connect(host));
        let stream = match security {
            Some(ctx) => KafkaStream::Ssl(try!(SslStream::connect(ctx, stream))),
            None => KafkaStream::Plain(stream),
        };
        KafkaConnection::from_stream(stream, host, rw_timeout)
    }
}
