#[cfg(feature = "gzip")]
pub mod gzip;

#[cfg(feature = "snappy")]
pub mod snappy;

#[cfg(feature = "zstandard")]
pub mod zstandard;

/// Compression types supported by kafka. The numeral values of this
/// enumeration correspond to the compression encoding in the
/// attributes of a Message in the protocol.
#[derive(Debug, Copy, Clone)]
pub enum Compression {
    NONE = 0,
    #[cfg(feature = "gzip")]
    GZIP = 1,
    #[cfg(feature = "snappy")]
    SNAPPY = 2,
    #[cfg(feature = "zstandard")]
    ZSTANDARD = 4,
}

impl Default for Compression {
    fn default() -> Self {
        Compression::NONE
    }
}
