//! Network related functionality for `KafkaClient`.
//!
//! This module is crate private and not exposed to the public except
//! through re-exports of individual items from within
//! `kafka::client`.

#[cfg(any(feature = "security-rustls", feature = "security-openssl"))]
use crate::codecs::{FromByte, ToByte};
use crate::error::Result;
#[cfg(any(feature = "security-rustls", feature = "security-openssl"))]
use crate::protocol::{
    SaslAuthenticateRequest, SaslAuthenticateResponse, SaslHandshakeRequest, SaslHandshakeResponse,
    sasl_plain_auth_bytes,
};
#[cfg(any(feature = "security-rustls", feature = "security-openssl"))]
use byteorder::{BigEndian, WriteBytesExt};
use std::collections::HashMap;
use std::fmt;
#[cfg(any(feature = "security-rustls", feature = "security-openssl"))]
use std::io::Cursor;
use std::io::{Read, Write};
use std::mem;
use std::net::{Shutdown, TcpStream};
use std::time::{Duration, Instant};
use tracing::{debug, trace, warn};

// Import TLS types based on enabled features
#[cfg(any(feature = "security-rustls", feature = "security-openssl"))]
use crate::tls::{TlsConfig, TlsStream};

#[cfg(feature = "security-rustls")]
use crate::tls::RustlsConnector;

#[cfg(all(feature = "security-openssl", not(feature = "security-rustls")))]
use crate::tls::OpenSslConnector;

// --------------------------------------------------------------------

/// SASL mechanism and credentials for authentication.
///
/// Used with `SASL_SSL` or `SASL_PLAINTEXT` security protocols.
#[cfg(any(feature = "security-rustls", feature = "security-openssl"))]
#[derive(Debug, Clone)]
pub struct SaslConfig {
    /// SASL mechanism name (e.g. "PLAIN")
    pub mechanism: String,
    pub username: String,
    pub password: String,
}

/// Security relevant configuration options for `KafkaClient`.
///
/// This configuration is now backend-agnostic and works with both
/// rustls (default) and OpenSSL (deprecated) TLS implementations.
#[cfg(any(feature = "security-rustls", feature = "security-openssl"))]
pub struct SecurityConfig {
    tls_config: TlsConfig,
    sasl_config: Option<SaslConfig>,
}

#[cfg(any(feature = "security-rustls", feature = "security-openssl"))]
impl SecurityConfig {
    /// Create a new `SecurityConfig` with default TLS settings.
    ///
    /// By default, this will:
    /// - Verify hostnames
    /// - Use system/webpki root certificates
    /// - Not use client certificates
    #[must_use]
    pub fn new() -> Self {
        SecurityConfig {
            tls_config: TlsConfig::new(),
            sasl_config: None,
        }
    }

    /// Create a `SecurityConfig` from a `TlsConfig`
    #[must_use]
    pub fn from_tls_config(tls_config: TlsConfig) -> Self {
        SecurityConfig {
            tls_config,
            sasl_config: None,
        }
    }

    /// Enable SASL/PLAIN authentication.
    ///
    /// Works with `SASL_SSL` (TLS + SASL) or `SASL_PLAINTEXT`. Typically used
    /// with Confluent Cloud, AWS MSK, and other managed Kafka services.
    #[must_use]
    pub fn with_sasl_plain(mut self, username: String, password: String) -> SecurityConfig {
        self.sasl_config = Some(SaslConfig {
            mechanism: "PLAIN".to_owned(),
            username,
            password,
        });
        self
    }

    /// Returns the SASL config if configured
    #[must_use]
    pub fn sasl_config(&self) -> Option<&SaslConfig> {
        self.sasl_config.as_ref()
    }

    /// Initiates a client-side TLS session with/without performing hostname verification.
    #[must_use]
    pub fn with_hostname_verification(mut self, verify_hostname: bool) -> SecurityConfig {
        self.tls_config.verify_hostname = verify_hostname;
        self
    }

    /// Set a custom CA certificate file path
    #[must_use]
    pub fn with_ca_cert(mut self, path: String) -> SecurityConfig {
        self.tls_config.ca_cert_path = Some(path);
        self
    }

    /// Set client certificate and key file paths
    #[must_use]
    pub fn with_client_cert(mut self, cert_path: String, key_path: String) -> SecurityConfig {
        self.tls_config.client_cert_path = Some(cert_path);
        self.tls_config.client_key_path = Some(key_path);
        self
    }
}

// Backward compatibility: Support OpenSSL SslConnector
#[cfg(feature = "security-openssl")]
impl SecurityConfig {
    /// Create `SecurityConfig` from an OpenSSL `SslConnector` (deprecated)
    ///
    /// ⚠️ **DEPRECATED**: This method exists for backward compatibility only.
    /// The OpenSSL backend will be removed in the next major version.
    /// Please use `SecurityConfig::new()` instead with rustls.
    #[deprecated(
        since = "0.10.0",
        note = "OpenSSL backend is deprecated. Use SecurityConfig::new() with rustls instead."
    )]
    #[must_use]
    pub fn new_openssl(_connector: openssl::ssl::SslConnector) -> Self {
        // We can't directly use the SslConnector with our abstraction
        // For backward compatibility, we'll store it as TlsConfig
        // and handle it specially in the connection logic
        warn!("Using deprecated OpenSSL SecurityConfig. Please migrate to rustls.");
        SecurityConfig {
            tls_config: TlsConfig::new(),
            sasl_config: None,
        }
    }
}

#[cfg(any(feature = "security-rustls", feature = "security-openssl"))]
impl Default for SecurityConfig {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(any(feature = "security-rustls", feature = "security-openssl"))]
impl fmt::Debug for SecurityConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SecurityConfig {{ verify_hostname: {}, sasl: {} }}",
            self.tls_config.verify_hostname,
            self.sasl_config.is_some()
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
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
    #[cfg(any(feature = "security-rustls", feature = "security-openssl"))]
    security_config: Option<SecurityConfig>,
}

impl Config {
    #[cfg(not(any(feature = "security-rustls", feature = "security-openssl")))]
    fn new_conn(&self, id: u32, host: &str) -> Result<KafkaConnection> {
        KafkaConnection::new(id, host, self.rw_timeout).map(|c| {
            debug!("Established: {:?}", c);
            c
        })
    }

    #[cfg(any(feature = "security-rustls", feature = "security-openssl"))]
    fn new_conn(&self, id: u32, host: &str) -> Result<KafkaConnection> {
        KafkaConnection::new(
            id,
            host,
            self.rw_timeout,
            self.security_config.as_ref().map(|c| &c.tls_config),
            self.security_config.as_ref().and_then(|c| c.sasl_config()),
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
    #[cfg(not(any(feature = "security-rustls", feature = "security-openssl")))]
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

    #[cfg(any(feature = "security-rustls", feature = "security-openssl"))]
    pub fn new(rw_timeout: Option<Duration>, idle_timeout: Duration) -> Connections {
        Self::new_with_security(rw_timeout, idle_timeout, None)
    }

    #[cfg(any(feature = "security-rustls", feature = "security-openssl"))]
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

    pub fn get_conn(&mut self, host: &str, now: Instant) -> Result<&mut KafkaConnection> {
        if let Some(conn) = self.conns.get_mut(host) {
            if now.duration_since(conn.last_checkout) >= self.config.idle_timeout {
                debug!("Idle timeout reached: {:?}", conn.item);
                let new_conn = self.config.new_conn(self.state.next_conn_id(), host)?;
                let _ = conn.item.shutdown();
                conn.item = new_conn;
            }
            conn.last_checkout = now;
            let kconn: &mut KafkaConnection = &mut conn.item;
            // ~ decouple the lifetimes to make the borrowck happy;
            // this is safe since we're immediately returning the
            // reference and the rest of the code in this method is
            // not affected
            return Ok(unsafe { mem::transmute(kconn) });
        }
        let cid = self.state.next_conn_id();
        self.conns.insert(
            host.to_owned(),
            Pooled::new(now, self.config.new_conn(cid, host)?),
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

// Abstraction for stream types based on security features
#[cfg(not(any(feature = "security-rustls", feature = "security-openssl")))]
type KafkaStream = TcpStream;

#[cfg(any(feature = "security-rustls", feature = "security-openssl"))]
enum KafkaStream {
    Plain(TcpStream),
    Tls(Box<dyn TlsStream>),
}

// Helper trait for stream operations
trait StreamOps {
    fn is_secured(&self) -> bool;
    fn set_read_timeout(&mut self, dur: Option<Duration>) -> std::io::Result<()>;
    fn set_write_timeout(&mut self, dur: Option<Duration>) -> std::io::Result<()>;
    fn shutdown(&mut self, how: Shutdown) -> std::io::Result<()>;
}

#[cfg(not(any(feature = "security-rustls", feature = "security-openssl")))]
impl StreamOps for KafkaStream {
    fn is_secured(&self) -> bool {
        false
    }

    fn set_read_timeout(&mut self, dur: Option<Duration>) -> std::io::Result<()> {
        self.set_read_timeout(dur)
    }

    fn set_write_timeout(&mut self, dur: Option<Duration>) -> std::io::Result<()> {
        self.set_write_timeout(dur)
    }

    fn shutdown(&mut self, how: Shutdown) -> std::io::Result<()> {
        self.shutdown(how)
    }
}

#[cfg(any(feature = "security-rustls", feature = "security-openssl"))]
impl StreamOps for KafkaStream {
    fn is_secured(&self) -> bool {
        match self {
            KafkaStream::Plain(_) => false,
            KafkaStream::Tls(_) => true,
        }
    }

    fn set_read_timeout(&mut self, dur: Option<Duration>) -> std::io::Result<()> {
        match self {
            KafkaStream::Plain(s) => s.set_read_timeout(dur),
            KafkaStream::Tls(s) => s.set_read_timeout(dur),
        }
    }

    fn set_write_timeout(&mut self, dur: Option<Duration>) -> std::io::Result<()> {
        match self {
            KafkaStream::Plain(s) => s.set_write_timeout(dur),
            KafkaStream::Tls(s) => s.set_write_timeout(dur),
        }
    }

    fn shutdown(&mut self, how: Shutdown) -> std::io::Result<()> {
        match self {
            KafkaStream::Plain(s) => s.shutdown(how),
            KafkaStream::Tls(s) => s.shutdown(),
        }
    }
}

#[cfg(any(feature = "security-rustls", feature = "security-openssl"))]
impl Read for KafkaStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            KafkaStream::Plain(s) => s.read(buf),
            KafkaStream::Tls(s) => s.read(buf),
        }
    }
}

#[cfg(any(feature = "security-rustls", feature = "security-openssl"))]
impl Write for KafkaStream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            KafkaStream::Plain(s) => s.write(buf),
            KafkaStream::Tls(s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            KafkaStream::Plain(s) => s.flush(),
            KafkaStream::Tls(s) => s.flush(),
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
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
        let r = self.stream.write(msg).map_err(From::from);
        trace!("Sent {} bytes to: {:?} => {:?}", msg.len(), self, r);
        r
    }

    pub fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        let r = (self.stream).read_exact(buf).map_err(From::from);
        trace!("Read {} bytes from: {:?} => {:?}", buf.len(), self, r);
        r
    }

    pub fn read_exact_alloc(&mut self, size: u64) -> Result<Vec<u8>> {
        let mut buffer = vec![0; size as usize];
        self.read_exact(buffer.as_mut_slice())?;
        Ok(buffer)
    }

    fn shutdown(&mut self) -> Result<()> {
        let r = self.stream.shutdown(Shutdown::Both);
        debug!("Shut down: {:?} => {:?}", self, r);
        r.map_err(From::from)
    }

    fn from_stream(
        mut stream: KafkaStream,
        id: u32,
        host: &str,
        rw_timeout: Option<Duration>,
    ) -> Result<KafkaConnection> {
        stream.set_read_timeout(rw_timeout)?;
        stream.set_write_timeout(rw_timeout)?;
        Ok(KafkaConnection {
            id,
            host: host.to_owned(),
            stream,
        })
    }

    #[cfg(not(any(feature = "security-rustls", feature = "security-openssl")))]
    fn new(id: u32, host: &str, rw_timeout: Option<Duration>) -> Result<KafkaConnection> {
        KafkaConnection::from_stream(TcpStream::connect(host)?, id, host, rw_timeout)
    }

    #[cfg(any(feature = "security-rustls", feature = "security-openssl"))]
    fn new(
        id: u32,
        host: &str,
        rw_timeout: Option<Duration>,
        tls_config: Option<&TlsConfig>,
        sasl_config: Option<&SaslConfig>,
    ) -> Result<KafkaConnection> {
        let tcp_stream = TcpStream::connect(host)?;

        let mut stream = match tls_config {
            Some(config) => {
                // Extract domain from host:port
                let domain = match host.rfind(':') {
                    None => host,
                    Some(i) => &host[..i],
                };

                // Create appropriate TLS connector based on enabled features
                #[cfg(feature = "security-rustls")]
                {
                    let connector = RustlsConnector::new(config)?;
                    let tls_stream = connector.connect(domain, tcp_stream)?;
                    KafkaStream::Tls(tls_stream)
                }

                #[cfg(all(feature = "security-openssl", not(feature = "security-rustls")))]
                {
                    #[allow(deprecated)]
                    let connector = OpenSslConnector::new(config)?;
                    let tls_stream = connector.connect(domain, tcp_stream)?;
                    KafkaStream::Tls(tls_stream)
                }
            }
            None => {
                // Plain TCP connection
                KafkaStream::Plain(tcp_stream)
            }
        };

        if let Some(sasl) = sasl_config {
            perform_sasl_auth(&mut stream, sasl, rw_timeout)?;
        }

        KafkaConnection::from_stream(stream, id, host, rw_timeout)
    }
}

/// Perform SASL handshake and authentication on an established stream.
#[cfg(any(feature = "security-rustls", feature = "security-openssl"))]
fn perform_sasl_auth(
    stream: &mut KafkaStream,
    sasl: &SaslConfig,
    rw_timeout: Option<Duration>,
) -> Result<()> {
    use std::io::{Read, Write};

    if let Some(t) = rw_timeout {
        stream.set_read_timeout(Some(t))?;
        stream.set_write_timeout(Some(t))?;
    }

    let client_id = "kafka-rust";
    let correlation_handshake: i32 = 1;
    let correlation_auth: i32 = 2;

    // SaslHandshake
    let handshake_req =
        SaslHandshakeRequest::new(correlation_handshake, client_id, &sasl.mechanism);
    let mut buf = Vec::with_capacity(64);
    buf.extend_from_slice(&[0, 0, 0, 0]);
    handshake_req.encode(&mut buf)?;
    let size = (buf.len() - 4) as i32;
    let mut size_buf = [0u8; 4];
    size_buf.as_mut_slice().write_i32::<BigEndian>(size)?;
    buf[..4].copy_from_slice(&size_buf);
    stream.write_all(&buf).map_err(crate::error::Error::from)?;
    stream.flush().map_err(crate::error::Error::from)?;

    let mut size_buf = [0u8; 4];
    stream
        .read_exact(&mut size_buf)
        .map_err(crate::error::Error::from)?;
    let resp_size = i32::decode_new(&mut Cursor::new(&size_buf))?;
    let mut resp = vec![0; resp_size as usize];
    stream
        .read_exact(&mut resp)
        .map_err(crate::error::Error::from)?;
    let handshake_resp = SaslHandshakeResponse::decode_new(&mut Cursor::new(resp))?;
    handshake_resp.into_result()?;

    // SaslAuthenticate (PLAIN sends auth in one shot)
    let auth_bytes = sasl_plain_auth_bytes(&sasl.username, &sasl.password);
    let auth_req = SaslAuthenticateRequest::new(correlation_auth, client_id, &auth_bytes);
    buf.clear();
    buf.extend_from_slice(&[0, 0, 0, 0]);
    auth_req.encode(&mut buf)?;
    let size = (buf.len() - 4) as i32;
    size_buf.as_mut_slice().write_i32::<BigEndian>(size)?;
    buf[..4].copy_from_slice(&size_buf);
    stream.write_all(&buf).map_err(crate::error::Error::from)?;
    stream.flush().map_err(crate::error::Error::from)?;

    stream
        .read_exact(&mut size_buf)
        .map_err(crate::error::Error::from)?;
    let resp_size = i32::decode_new(&mut Cursor::new(&size_buf))?;
    let mut resp = vec![0; resp_size as usize];
    stream
        .read_exact(&mut resp)
        .map_err(crate::error::Error::from)?;
    let auth_resp = SaslAuthenticateResponse::decode_new(&mut Cursor::new(resp))?;
    auth_resp.into_result()?;

    debug!("SASL {} authentication successful", sasl.mechanism);
    Ok(())
}
