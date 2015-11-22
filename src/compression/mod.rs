pub mod snappy;
pub mod gzip;

/// Compression types supported by kafka. The numeral values of this
/// enumeration correspond to the compression encoding in the
/// attributes of a Message in the protocol.
#[derive(Debug, Copy, Clone)]
pub enum Compression {
    NONE = 0,
    GZIP = 1,
    SNAPPY = 2
}

impl Default for Compression {
    fn default() -> Self {
        Compression::NONE
    }
}
