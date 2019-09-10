//! Network related functionality for `KafkaClient`.
//!
//! This module is crate private and not exposed to the public except
//! through re-exports of individual items from within
//! `kafka::client`.

use std::collections::HashMap;
use std::fmt;
use std::io::{Read, Write};
use std::mem;
use std::net::{Shutdown, TcpStream};
use std::time::{Duration, Instant};

#[cfg(feature = "security")]
use openssl::ssl::SslConnector;

use error::Result;

// --------------------------------------------------------------------

/// Security relevant configuration options for `KafkaClient`.
// This will be expanded in the future. See #51.
#[cfg(feature = "security")]
pub struct SecurityConfig {
    connector: SslConnector,
    verify_hostname: bool,
}

#[cfg(feature = "security")]
impl SecurityConfig {
    /// In the future this will also support a kerbos via #51.
    pub fn new(connector: SslConnector) -> Self {
        SecurityConfig {
            connector,
            verify_hostname: true,
        }
    }

    /// Initiates a client-side TLS session with/without performing hostname verification.
    pub fn with_hostname_verification(self, verify_hostname: bool) -> SecurityConfig {
        SecurityConfig {
            verify_hostname,
            ..self
        }
    }
}

#[cfg(feature = "security")]
impl fmt::Debug for SecurityConfig {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SecurityConfig {{ verify_hostname: {} }}",
            self.verify_hostname
        )
    }
}

// --------------------------------------------------------------------

struct Pooled<T> {
    last_checkout: Instant,
    item: T,
}

impl<T> Pooled<T> {
    fn new(last_checkout: Instant, item: T) -> Self {
        Pooled {
            last_checkout,
            item,
        }
    }
}

impl<T: fmt::Debug> fmt::Debug for Pooled<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Pooled {{ last_checkout: {:?}, item: {:?} }}",
            self.last_checkout, self.item
        )
    }
}

#[derive(Debug)]
pub struct Config {
    rw_timeout: Option<Duration>,
    idle_timeout: Duration,
    #[cfg(feature = "security")]
    security_config: Option<SecurityConfig>,
}

impl Config {
    #[cfg(not(feature = "security"))]
    fn new_conn(&self, id: u32, host: &str) -> Result<KafkaConnection> {
        KafkaConnection::new(id, host, self.rw_timeout).map(|c| {
            debug!("Established: {:?}", c);
            c
        })
    }

    #[cfg(feature = "security")]
    fn new_conn(&self, id: u32, host: &str) -> Result<KafkaConnection> {
        KafkaConnection::new(
            id,
            host,
            self.rw_timeout,
            self.security_config
                .as_ref()
                .map(|c| (c.connector.clone(), c.verify_hostname)),
        )
        .map(|c| {
            debug!("Established: {:?}", c);
            c
        })
    }
}

#[derive(Debug)]
struct State {
    num_conns: u32,
}

impl State {
    fn new() -> State {
        State { num_conns: 0 }
    }

    fn next_conn_id(&mut self) -> u32 {
        let c = self.num_conns;
        self.num_conns = self.num_conns.wrapping_add(1);
        c
    }
}

#[derive(Debug)]
pub struct Connections {
    conns: HashMap<String, Pooled<KafkaConnection>>,
    state: State,
    config: Config,
}

impl Connections {
    #[cfg(not(feature = "security"))]
    pub fn new(rw_timeout: Option<Duration>, idle_timeout: Duration) -> Connections {
        Connections {
            conns: HashMap::new(),
            state: State::new(),
            config: Config {
                rw_timeout,
                idle_timeout,
            },
        }
    }

    #[cfg(feature = "security")]
    pub fn new(rw_timeout: Option<Duration>, idle_timeout: Duration) -> Connections {
        Self::new_with_security(rw_timeout, idle_timeout, None)
    }

    #[cfg(feature = "security")]
    pub fn new_with_security(
        rw_timeout: Option<Duration>,
        idle_timeout: Duration,
        security: Option<SecurityConfig>,
    ) -> Connections {
        Connections {
            conns: HashMap::new(),
            state: State::new(),
            config: Config {
                rw_timeout,
                idle_timeout,
                security_config: security,
            },
        }
    }

    pub fn set_idle_timeout(&mut self, idle_timeout: Duration) {
        self.config.idle_timeout = idle_timeout;
    }

    pub fn idle_timeout(&self) -> Duration {
        self.config.idle_timeout
    }

    pub fn get_conn<'a>(&'a mut self, host: &str, now: Instant) -> Result<&'a mut KafkaConnection> {
        if let Some(conn) = self.conns.get_mut(host) {
            if now.duration_since(conn.last_checkout) >= self.config.idle_timeout {
                debug!("Idle timeout reached: {:?}", conn.item);
                let new_conn = try!(self.config.new_conn(self.state.next_conn_id(), host));
                let _ = conn.item.shutdown();
                conn.item = new_conn;
            }
            conn.last_checkout = now;
            let kconn: &mut KafkaConnection = &mut conn.item;
            // ~ decouple the lifetimes to make the borrowck happy;
            // this is safe since we're immediatelly returning the
            // reference and the rest of the code in this method is
            // not affected
            return Ok(unsafe { mem::transmute(kconn) });
        }
        let cid = self.state.next_conn_id();
        self.conns.insert(
            host.to_owned(),
            Pooled::new(now, try!(self.config.new_conn(cid, host))),
        );
        Ok(&mut self.conns.get_mut(host).unwrap().item)
    }

    pub fn get_conn_any(&mut self, now: Instant) -> Option<&mut KafkaConnection> {
        for (host, conn) in &mut self.conns {
            if now.duration_since(conn.last_checkout) >= self.config.idle_timeout {
                debug!("Idle timeout reached: {:?}", conn.item);
                let new_conn_id = self.state.next_conn_id();
                let new_conn = match self.config.new_conn(new_conn_id, host.as_str()) {
                    Ok(new_conn) => {
                        let _ = conn.item.shutdown();
                        new_conn
                    }
                    Err(e) => {
                        warn!("Failed to establish connection to {}: {:?}", host, e);
                        continue;
                    }
                };
                conn.item = new_conn;
            }
            conn.last_checkout = now;
            let kconn: &mut KafkaConnection = &mut conn.item;
            return Some(kconn);
        }
        None
    }
}

// --------------------------------------------------------------------

trait IsSecured {
    fn is_secured(&self) -> bool;
}

#[cfg(not(feature = "security"))]
type KafkaStream = TcpStream;

#[cfg(not(feature = "security"))]
impl IsSecured for KafkaStream {
    fn is_secured(&self) -> bool {
        false
    }
}

#[cfg(feature = "security")]
use self::openssled::KafkaStream;

#[cfg(feature = "security")]
mod openssled {
    use std::io::{self, Read, Write};
    use std::net::{Shutdown, TcpStream};
    use std::time::Duration;

    use openssl::ssl::SslStream;

    use super::IsSecured;

    pub enum KafkaStream {
        Plain(TcpStream),
        Ssl(SslStream<TcpStream>),
    }

    impl IsSecured for KafkaStream {
        fn is_secured(&self) -> bool {
            match *self {
                KafkaStream::Ssl(_) => true,
                _ => false,
            }
        }
    }

    impl KafkaStream {
        fn get_ref(&self) -> &TcpStream {
            match *self {
                KafkaStream::Plain(ref s) => s,
                KafkaStream::Ssl(ref s) => s.get_ref(),
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
    // a surrogate identifier to distinguish between
    // connections to the same host in debug messages
    id: u32,
    // "host:port"
    host: String,
    // the (wrapped) tcp stream
    stream: KafkaStream,
}

impl fmt::Debug for KafkaConnection {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "KafkaConnection {{ id: {}, secured: {}, host: \"{}\" }}",
            self.id,
            self.stream.is_secured(),
            self.host
        )
    }
}

impl KafkaConnection {
    pub fn send(&mut self, msg: &[u8]) -> Result<usize> {
        let r = self.stream.write(&msg[..]).map_err(From::from);
        trace!("Sent {} bytes to: {:?} => {:?}", msg.len(), self, r);
        r
    }

    pub fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        let r = (&mut self.stream).read_exact(buf).map_err(From::from);
        trace!("Read {} bytes from: {:?} => {:?}", buf.len(), self, r);
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
        debug!("Shut down: {:?} => {:?}", self, r);
        r.map_err(From::from)
    }

    fn from_stream(
        stream: KafkaStream,
        id: u32,
        host: &str,
        rw_timeout: Option<Duration>,
    ) -> Result<KafkaConnection> {
        try!(stream.set_read_timeout(rw_timeout));
        try!(stream.set_write_timeout(rw_timeout));
        Ok(KafkaConnection {
            id,
            host: host.to_owned(),
            stream,
        })
    }

    #[cfg(not(feature = "security"))]
    fn new(id: u32, host: &str, rw_timeout: Option<Duration>) -> Result<KafkaConnection> {
        KafkaConnection::from_stream(try!(TcpStream::connect(host)), id, host, rw_timeout)
    }

    #[cfg(feature = "security")]
    fn new(
        id: u32,
        host: &str,
        rw_timeout: Option<Duration>,
        security: Option<(SslConnector, bool)>,
    ) -> Result<KafkaConnection> {
        let stream = try!(TcpStream::connect(host));
        let stream = match security {
            Some((connector, verify_hostname)) => {
                if !verify_hostname {
                    connector
                        .configure()
                        .map_err(|err| {
                            let err: crate::error::Error =
                                crate::error::ErrorKind::Ssl(From::from(err)).into();
                            err
                        })?
                        .set_verify_hostname(false);
                }
                let domain = match host.rfind(':') {
                    None => host,
                    Some(i) => &host[..i],
                };
                let connection = try!(connector.connect(domain, stream));
                KafkaStream::Ssl(connection)
            }
            None => KafkaStream::Plain(stream),
        };
        KafkaConnection::from_stream(stream, id, host, rw_timeout)
    }
}
