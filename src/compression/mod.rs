#[cfg(feature = "gzip")]
pub mod gzip;

#[cfg(feature = "snappy")]
pub mod snappy;

/// Compression types supported by kafka. The numeral values of this
/// enumeration correspond to the compression encoding in the
/// attributes of a Message in the protocol.
#[derive(Debug, Copy, Clone, Default)]
pub enum Compression {
    #[default]
    NONE = 0,
    #[cfg(feature = "gzip")]
    GZIP = 1,
    #[cfg(feature = "snappy")]
    SNAPPY = 2,
}
