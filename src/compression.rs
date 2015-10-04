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

pub use super::{gzip, snappy};
