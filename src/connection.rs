use std::fmt;
use std::path::Path;
use std::io::{self, Read,Write};
use std::net::TcpStream;
use std::time::Duration;

use openssl::ssl::{Ssl, SslContext, SslStream, SslMethod, SSL_VERIFY_NONE};
use openssl::x509::X509FileType;

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

    pub fn new(host: &str, timeout_secs: i32) -> Result<KafkaConnection> {
        let stream = KafkaStream::Plain(try!(TcpStream::connect(host)));
        if timeout_secs > 0 {
            let t = Some(Duration::from_secs(timeout_secs as u64));
            stream.get_ref().set_read_timeout(t).expect("Set connection read-timeout");
            stream.get_ref().set_write_timeout(t).expect("Set connection write-timeout");
        }
        Ok(KafkaConnection{host: host.to_owned(), stream: stream})
    }

    pub fn ssl<C,K>(&mut self, cert: C, key: K) -> Result<()>
    where C: AsRef<Path>, K: AsRef<Path> {
        let mut context = try!(SslContext::new(SslMethod::Sslv23));
        try!(context.set_cipher_list("DEFAULT"));
        try!(context.set_certificate_file(cert.as_ref(), X509FileType::PEM));
        try!(context.set_private_key_file(key.as_ref(), X509FileType::PEM));
        context.set_verify(SSL_VERIFY_NONE, None);
        let ssl = try!(Ssl::new(&context));

        let plain_stream = match self.stream {
            KafkaStream::Plain(ref s) => try!(s.try_clone()),
            KafkaStream::Ssl(_) => return Err(Error::AlreadySecure),
        };
        let stream = try!(SslStream::connect(ssl, plain_stream));
        self.stream = KafkaStream::Ssl(stream);
        Ok(())
    }
}
