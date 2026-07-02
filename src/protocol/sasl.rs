//! SASL authentication protocol (API 17 `SaslHandshake`, API 36 `SaslAuthenticate`).
//!
//! Used for SASL/PLAIN and other SASL mechanisms. The handshake negotiates the
//! mechanism; the authenticate request sends the actual auth bytes.
//!
//! SASL/PLAIN wire format: `\x00{username}\x00{password}` (NUL-separated, no length prefix).

use std::io::{Read, Write};

use crate::codecs::{FromByte, ToByte};
use crate::error::{Error, Result};
use byteorder::{BigEndian, ReadBytesExt};

use super::{HeaderRequest, HeaderResponse};

// --------------------------------------------------------------------
// API key constants (from Kafka protocol)
// --------------------------------------------------------------------

pub const API_KEY_SASL_HANDSHAKE: i16 = 17;
pub const API_KEY_SASL_AUTHENTICATE: i16 = 36;
pub const SASL_HANDSHAKE_VERSION: i16 = 1; // v1 uses SaslAuthenticate for tokens
pub const SASL_AUTHENTICATE_VERSION: i16 = 0;

// --------------------------------------------------------------------
// SaslHandshake
// --------------------------------------------------------------------

#[derive(Debug)]
pub struct SaslHandshakeRequest<'a> {
    pub header: HeaderRequest<'a>,
    pub mechanism: &'a str,
}

impl<'a> SaslHandshakeRequest<'a> {
    pub fn new(correlation_id: i32, client_id: &'a str, mechanism: &'a str) -> Self {
        SaslHandshakeRequest {
            header: HeaderRequest::new(
                API_KEY_SASL_HANDSHAKE,
                SASL_HANDSHAKE_VERSION,
                correlation_id,
                client_id,
            ),
            mechanism,
        }
    }
}

impl ToByte for SaslHandshakeRequest<'_> {
    fn encode<W: Write>(&self, buffer: &mut W) -> Result<()> {
        self.header.encode(buffer)?;
        self.mechanism.encode(buffer)?;
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct SaslHandshakeResponse {
    pub header: HeaderResponse,
    pub error_code: i16,
    pub mechanisms: Vec<String>,
}

impl SaslHandshakeResponse {
    /// Returns `Err` if the handshake failed (e.g. mechanism not supported)
    pub fn into_result(self) -> Result<Self> {
        if let Some(e) = Error::from_protocol(self.error_code) {
            return Err(e);
        }
        Ok(self)
    }
}

impl FromByte for SaslHandshakeResponse {
    type R = SaslHandshakeResponse;

    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        self.header.decode(buffer)?;
        self.error_code.decode(buffer)?;
        self.mechanisms.decode(buffer)?;
        Ok(())
    }
}

// --------------------------------------------------------------------
// SaslAuthenticate
// --------------------------------------------------------------------

#[derive(Debug)]
pub struct SaslAuthenticateRequest<'a> {
    pub header: HeaderRequest<'a>,
    pub auth_bytes: &'a [u8],
}

impl<'a> SaslAuthenticateRequest<'a> {
    pub fn new(correlation_id: i32, client_id: &'a str, auth_bytes: &'a [u8]) -> Self {
        SaslAuthenticateRequest {
            header: HeaderRequest::new(
                API_KEY_SASL_AUTHENTICATE,
                SASL_AUTHENTICATE_VERSION,
                correlation_id,
                client_id,
            ),
            auth_bytes,
        }
    }
}

impl ToByte for SaslAuthenticateRequest<'_> {
    fn encode<W: Write>(&self, buffer: &mut W) -> Result<()> {
        self.header.encode(buffer)?;
        self.auth_bytes.encode(buffer)?;
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct SaslAuthenticateResponse {
    pub header: HeaderResponse,
    pub error_code: i16,
    pub error_message: Option<String>,
    pub auth_bytes: Vec<u8>,
}

impl SaslAuthenticateResponse {
    /// Returns `Err` if authentication failed
    pub fn into_result(self) -> Result<Self> {
        if let Some(e) = Error::from_protocol(self.error_code) {
            return Err(e);
        }
        Ok(self)
    }
}

impl FromByte for SaslAuthenticateResponse {
    type R = SaslAuthenticateResponse;

    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        self.header.decode(buffer)?;
        self.error_code.decode(buffer)?;
        decode_nullable_string(buffer, &mut self.error_message)?;
        self.auth_bytes.decode(buffer)?;
        Ok(())
    }
}

/// Decode a Kafka nullable string: -1 (i16) means null, else length + bytes
fn decode_nullable_string<T: Read>(buffer: &mut T, dest: &mut Option<String>) -> Result<()> {
    let length = buffer
        .read_i16::<BigEndian>()
        .map_err(|_| Error::CodecError)?;
    if length == -1 {
        *dest = None;
        return Ok(());
    }
    if length < 0 {
        return Err(Error::CodecError);
    }
    let mut s = String::new();
    let _ = buffer.take(length as u64).read_to_string(&mut s);
    if s.len() != length as usize {
        return Err(Error::UnexpectedEOF);
    }
    *dest = Some(s);
    Ok(())
}

// --------------------------------------------------------------------
// SASL/PLAIN
// --------------------------------------------------------------------

/// Build SASL/PLAIN auth bytes: `\x00{username}\x00{password}`.
///
/// The PLAIN mechanism sends the identity as a single blob with NUL-separated
/// authorization id, authentication id (username), and password.
/// For Kafka we use empty authorization id.
#[must_use]
pub fn sasl_plain_auth_bytes(username: &str, password: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(2 + username.len() + password.len());
    out.push(0);
    out.extend_from_slice(username.as_bytes());
    out.push(0);
    out.extend_from_slice(password.as_bytes());
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sasl_plain_auth_bytes() {
        let b = sasl_plain_auth_bytes("alice", "secret");
        assert_eq!(
            b,
            &[
                0, b'a', b'l', b'i', b'c', b'e', 0, b's', b'e', b'c', b'r', b'e', b't'
            ]
        );
    }

    #[test]
    fn test_sasl_plain_empty_password() {
        let b = sasl_plain_auth_bytes("user", "");
        assert_eq!(b, &[0, b'u', b's', b'e', b'r', 0]);
    }
}
