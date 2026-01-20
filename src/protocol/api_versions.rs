//! API Versions Request/Response for protocol version negotiation
//!
//! This module implements the ApiVersions API (API Key 18) which allows
//! clients to discover which versions of various Kafka APIs are supported
//! by the broker.

use std::io::{Read, Write};

use crate::codecs::{FromByte, ToByte};
use crate::error::{Error, Result};

use super::{HeaderRequest, HeaderResponse, API_KEY_API_VERSIONS};

/// ApiVersions request to discover supported API versions
#[derive(Debug)]
pub struct ApiVersionsRequest<'a> {
    pub header: HeaderRequest<'a>,
}

impl<'a> ApiVersionsRequest<'a> {
    /// Creates a new ApiVersionsRequest
    ///
    /// Note: ApiVersions uses API version 0 initially for compatibility
    pub fn new(correlation_id: i32, client_id: &'a str) -> ApiVersionsRequest<'a> {
        ApiVersionsRequest {
            header: HeaderRequest::new(API_KEY_API_VERSIONS, 0, correlation_id, client_id),
        }
    }
}

impl<'a> ToByte for ApiVersionsRequest<'a> {
    fn encode<W: Write>(&self, buffer: &mut W) -> Result<()> {
        self.header.encode(buffer)
    }
}

/// Response from ApiVersions request
#[derive(Debug, Default, Clone)]
pub struct ApiVersionsResponse {
    pub header: HeaderResponse,
    pub error_code: i16,
    pub api_versions: Vec<ApiVersion>,
}

/// Information about a supported API version range
#[derive(Debug, Default, Clone)]
pub struct ApiVersion {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
}

impl ApiVersionsResponse {
    /// Converts the response into a Result, returning an error if error_code is non-zero
    pub fn into_result(self) -> Result<Self> {
        match Error::from_protocol(self.error_code) {
            Some(e) => Err(e),
            None => Ok(self),
        }
    }
}

impl FromByte for ApiVersionsResponse {
    type R = ApiVersionsResponse;

    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.header.decode(buffer),
            self.error_code.decode(buffer),
            self.api_versions.decode(buffer)
        )
    }
}

impl FromByte for ApiVersion {
    type R = ApiVersion;

    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try_multi!(
            self.api_key.decode(buffer),
            self.min_version.decode(buffer),
            self.max_version.decode(buffer)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_api_versions_request_creation() {
        let req = ApiVersionsRequest::new(123, "test-client");
        assert_eq!(req.header.api_key, API_KEY_API_VERSIONS);
        assert_eq!(req.header.api_version, 0);
        assert_eq!(req.header.correlation_id, 123);
        assert_eq!(req.header.client_id, "test-client");
    }

    #[test]
    fn test_api_version_decode() {
        // Test data: api_key=0, min=0, max=5
        let data: Vec<u8> = vec![
            0, 0, // api_key = 0
            0, 0, // min_version = 0
            0, 5, // max_version = 5
        ];
        let mut cursor = std::io::Cursor::new(data);
        let mut api_version = ApiVersion::default();
        api_version.decode(&mut cursor).unwrap();

        assert_eq!(api_version.api_key, 0);
        assert_eq!(api_version.min_version, 0);
        assert_eq!(api_version.max_version, 5);
    }
}
