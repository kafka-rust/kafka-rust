use client::FetchOffset;

#[derive(Debug)]
pub struct Config {
    pub group: String,
    pub fallback_offset: FetchOffset,
    pub retry_max_bytes_limit: i32,
}
