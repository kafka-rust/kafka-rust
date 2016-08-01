//! Network related functionality for `KafkaClient`.
//!
//! This module is crate private and not exposed to the public except
//! through re-exports of individual items from within
//! `kafka::client`.

use std::collections::HashMap;
use std::fmt;
use std::io::{Read, Write};
use std::mem;
use std::net::{TcpStream, Shutdown};
use std::time::{Instant, Duration};

#[cfg(feature = "security")]
use openssl::ssl::{SslContext, SslStream};

use error::Result;

// --------------------------------------------------------------------

/// Security relevant configuration options for `KafkaClient`.
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

struct Pooled<T> {
    last_checkout: Instant,
    item: T,
}

impl<T> Pooled<T> {
    fn new(last_checkout: Instant, item: T) -> Self {
        Pooled { last_checkout: last_checkout, item: item }
    }
}

impl<T: fmt::Debug> fmt::Debug for Pooled<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Pooled {{ last_checkout: {:?}, item: {:?} }}",
               self.last_checkout, self.item)
    }
}

#[derive(Debug)]
pub struct Config {
    rw_timeout: Option<Duration>,
    idle_timeout: Option<Duration>,
    #[cfg(feature = "security")]
    security_config: Option<SecurityConfig>,
}

impl Config {
    #[cfg(not(feature = "security"))]
    fn new_conn(&self, host: &str) -> Result<KafkaConnection> {
        debug!("Establishing connection to '{}'", host);
        KafkaConnection::new(host, self.timeout)
    }

    #[cfg(feature = "security")]
    fn new_conn(&self, host: &str) -> Result<KafkaConnection> {
        debug!("Establishing connection to '{}'", host);
        KafkaConnection::new(host,
                             self.rw_timeout,
                             self.security_config.as_ref().map(|c| &c.0))
    }
}

#[derive(Debug)]
pub struct Connections {
    conns: HashMap<String, Pooled<KafkaConnection>>,
    config: Config,
}

impl Connections {
    #[cfg(not(feature = "security"))]
    pub fn new(rw_timeout: Option<Duration>, idle_timeout: Option<Duration>)
               -> Connections
    {
        ConnectionPool {
            conns: HashMap::new(),
            rw_timeout: rw_timeout,
            idle_timeout: idle_timeout,
        }
    }

    #[cfg(feature = "security")]
    pub fn new(rw_timeout: Option<Duration>, idle_timeout: Option<Duration>)
               -> Connections
    {
        Self::new_with_security(rw_timeout, idle_timeout, None)
    }

    #[cfg(feature = "security")]
    pub fn new_with_security(rw_timeout: Option<Duration>,
                             idle_timeout: Option<Duration>,
                             security: Option<SecurityConfig>)
                             -> Connections
    {
        Connections {
            conns: HashMap::new(),
            config: Config {
                rw_timeout: rw_timeout,
                idle_timeout: idle_timeout,
                security_config: security,
            },
        }
    }

    pub fn get_conn<'a>(&'a mut self, host: &str, now: Instant)
                        -> Result<&'a mut KafkaConnection>
    {
        if let Some(conn) = self.conns.get_mut(host) {
            if let Some(idle_timeout) = self.config.idle_timeout {
                if now.duration_since(conn.last_checkout) >= idle_timeout {
                    debug!("Idle timeout ({:?}) reached for connection to '{}'",
                           idle_timeout, host);
                    let new_conn = try!(self.config.new_conn(host));
                    let _ = conn.item.shutdown();
                    conn.item = new_conn;
                }
            }
            conn.last_checkout = now;
            let kconn: &mut KafkaConnection = &mut conn.item;
            // ~ decouple the lifetimes to make the borrowck happy;
            // this is safe since we're immediatelly returning the
            // reference and the rest of the code in this method is
            // not affected
            return Ok(unsafe { mem::transmute(kconn) });
        }
        self.conns.insert(host.to_owned(),
                          Pooled::new(now, try!(self.config.new_conn(host))));
        Ok(&mut self.conns.get_mut(host).unwrap().item)
    }

    pub fn get_conn_any(&mut self, now: Instant) -> Option<&mut KafkaConnection> {
        for (host, conn) in &mut self.conns {
            if let Some(idle_timeout) = self.config.idle_timeout {
                if now.duration_since(conn.last_checkout) >= idle_timeout {
                    debug!("Idle timeout ({:?}) reached for connection to '{}'",
                           idle_timeout, host);
                    let new_conn = match self.config.new_conn(host.as_str()) {
                        Ok(new_conn) => {
                            let _ = conn.item.shutdown();
                            new_conn
                        }
                        Err(e) => {
                            debug!("Failed to establish connection to {}: {:?}",
                                   host, e);
                            continue;
                        }
                    };
                    conn.item = new_conn;
                }
            }
            conn.last_checkout = now;
            let kconn: &mut KafkaConnection = &mut conn.item;
            return Some(kconn);
        }
        None
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
    use std::net::{TcpStream, Shutdown};
    use std::time::Duration;

    use openssl::ssl::SslStream;

    pub enum KafkaStream {
        Plain(TcpStream),
        Ssl(SslStream<TcpStream>)
    }

    impl KafkaStream {
        fn get_ref(&self) -> &TcpStream {
            match *self {
                KafkaStream::Plain(ref s) => s,
                KafkaStream::Ssl(ref s) => s.get_ref()
            }
        }

        pub fn set_read_timeout(&self, dur: Option<Duration>) -> io::Result<()> {
            self.get_ref().set_read_timeout(dur)
        }

        pub fn set_write_timeout(&self, dur: Option<Duration>) -> io::Result<()> {
            self.get_ref().set_write_timeout(dur)
        }

        pub fn shutdown(&mut self, how: Shutdown) -> io::Result<()> {
            self.get_ref().shutdown(how)
        }
    }

    impl Read for KafkaStream {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            match *self {
                KafkaStream::Plain(ref mut s) => s.read(buf),
                KafkaStream::Ssl(ref mut s) => s.read(buf),
            }
        }
    }

    impl Write for KafkaStream {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            match *self {
                KafkaStream::Plain(ref mut s) => s.write(buf),
                KafkaStream::Ssl(ref mut s) => s.write(buf),
            }
        }
        fn flush(&mut self) -> io::Result<()> {
            match *self {
                KafkaStream::Plain(ref mut s) => s.flush(),
                KafkaStream::Ssl(ref mut s) => s.flush(),
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
        let r = self.stream.write(&msg[..]).map_err(From::from);
        debug!("Sent {} bytes to '{}' => {:?}", msg.len(), self.host, r);
        r
    }

    pub fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        let r = (&mut self.stream).read_exact(buf).map_err(From::from);
        debug!("Read {} bytes from '{}' => {:?}", buf.len(), self.host, r);
        r
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

    fn shutdown(&mut self) -> Result<()> {
        let r = self.stream.shutdown(Shutdown::Both);
        debug!("Shut down connection to '{}' => {:?}", self.host, r);
        r.map_err(From::from)
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
    fn new(host: &str, rw_timeout: Option<Duration>, security: Option<&SslContext>)
           -> Result<KafkaConnection>
    {
        let stream = try!(TcpStream::connect(host));
        let stream = match security {
            Some(ctx) => KafkaStream::Ssl(try!(SslStream::connect(ctx, stream))),
            None => KafkaStream::Plain(stream),
        };
        KafkaConnection::from_stream(stream, host, rw_timeout)
    }
}
