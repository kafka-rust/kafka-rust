use std::io::{self, Read};

use byteorder::{BigEndian, ByteOrder};
use snap;

use error::{Result, Error, ErrorKind};

pub fn compress(src: &[u8]) -> Result<Vec<u8>> {
    let mut buf = vec![0; snap::max_compress_len(src.len())];

    snap::Encoder::new()
        .compress(src, &mut buf)
        .map(|len| {
            buf.truncate(len);
            buf
        })
        .map_err(|err| ErrorKind::InvalidSnappy(err).into())
}

fn uncompress_to(src: &[u8], dst: &mut Vec<u8>) -> Result<()> {
    snap::decompress_len(src)
        .and_then(|min_len| {
            if min_len > 0 {
                let off = dst.len();
                dst.resize(off + min_len, 0);
                let uncompressed_len = {
                    let buf = &mut dst.as_mut_slice()[off..off + min_len];
                    try!(snap::Decoder::new().decompress(src, buf))
                };
                dst.truncate(off + uncompressed_len);
            }
            Ok(())
        })
        .map_err(|err| ErrorKind::InvalidSnappy(err).into())
}

// --------------------------------------------------------------------

const MAGIC: &'static [u8] = &[0x82, b'S', b'N', b'A', b'P', b'P', b'Y', 0];

// ~ reads a i32 valud and "advances" the given slice by four bytes;
// assumes "slice" is a mutable reference to a &[u8].
macro_rules! next_i32 {
    ($slice:expr) => {{
        if $slice.len() < 4 {
            bail!(ErrorKind::UnexpectedEOF);
        }
        { let n = BigEndian::read_i32($slice);
          $slice = &$slice[4..];
          n
        }
    }}
}

/// Validates the expected header at the beginning of the
/// stream. Further, checks the version and compatibility of the
/// stream indicating we can parse the stream. Returns the rest of the
/// stream following the validated header.
fn validate_stream(mut stream: &[u8]) -> Result<&[u8]> {
    // ~ check the "header magic"
    if stream.len() < MAGIC.len() {
        bail!(ErrorKind::UnexpectedEOF);
    }
    if &stream[..MAGIC.len()] != MAGIC {
        bail!(ErrorKind::InvalidSnappy(snap::Error::Header));
    }
    stream = &stream[MAGIC.len()..];
    // ~ let's be assertive and (for the moment) restrict ourselves to
    // version == 1 and compatibility == 1.
    let version = next_i32!(stream);
    if version != 1 {
        bail!(ErrorKind::InvalidSnappy(snap::Error::Header));
    }
    let compat = next_i32!(stream);
    if compat != 1 {
        bail!(ErrorKind::InvalidSnappy(snap::Error::Header));
    }
    Ok(stream)
}

#[test]
fn test_validate_stream() {
    let header = [
        0x82,
        0x53,
        0x4e,
        0x41,
        0x50,
        0x50,
        0x59,
        0x00,
        0,
        0,
        0,
        1,
        0,
        0,
        0,
        1,
        0x56,
    ];
    // ~ this must not result in a panic
    let rest = validate_stream(&header).unwrap();
    // ~ the rest of the input after the parsed header must be
    // correctly delivered
    assert_eq!(rest, &[0x56]);
}

// ~ An implementation of a reader over a stream of snappy compressed
// chunks as produced by org.xerial.snappy.SnappyOutputStream
// (https://github.com/xerial/snappy-java/ version: 1.1.1.*)
pub struct SnappyReader<'a> {
    // the compressed data itself
    compressed_data: &'a [u8],

    // a pointer into `uncompressed_chunk` indicating the next data
    // byte to serve
    uncompressed_pos: usize,
    // the uncompressed chunk of data available for consumption
    uncompressed_chunk: Vec<u8>,
}

impl<'a> SnappyReader<'a> {
    pub fn new(mut stream: &[u8]) -> Result<SnappyReader> {
        stream = try!(validate_stream(stream));
        Ok(SnappyReader {
            compressed_data: stream,
            uncompressed_pos: 0,
            uncompressed_chunk: Vec::new(),
        })
    }

    fn _read(&mut self, buf: &mut [u8]) -> Result<usize> {
        if self.uncompressed_pos < self.uncompressed_chunk.len() {
            self.read_uncompressed(buf)
        } else if try!(self.next_chunk()) {
            self.read_uncompressed(buf)
        } else {
            Ok(0)
        }
    }

    fn read_uncompressed(&mut self, buf: &mut [u8]) -> Result<usize> {
        let n = try!((&self.uncompressed_chunk[self.uncompressed_pos..]).read(buf));
        self.uncompressed_pos += n;
        Ok(n)
    }

    fn next_chunk(&mut self) -> Result<bool> {
        if self.compressed_data.is_empty() {
            return Ok(false);
        }
        self.uncompressed_pos = 0;
        let chunk_size = next_i32!(self.compressed_data);
        if chunk_size <= 0 {
            bail!(ErrorKind::InvalidSnappy(snap::Error::UnsupportedChunkLength {
                len: chunk_size as u64,
                header: false,
            }));
        }
        let chunk_size = chunk_size as usize;
        self.uncompressed_chunk.clear();
        try!(uncompress_to(&self.compressed_data[..chunk_size], &mut self.uncompressed_chunk));
        self.compressed_data = &self.compressed_data[chunk_size..];
        Ok(true)
    }

    fn _read_to_end(&mut self, buf: &mut Vec<u8>) -> Result<usize> {
        let init_len = buf.len();
        // ~ first consume already uncompressed and unconsumed data - if any
        if self.uncompressed_pos < self.uncompressed_chunk.len() {
            let rest = &self.uncompressed_chunk[self.uncompressed_pos..];
            buf.extend_from_slice(rest);
            self.uncompressed_pos += rest.len();
        }
        // ~ now decompress data directly to the output target
        while !self.compressed_data.is_empty() {
            let chunk_size = next_i32!(self.compressed_data);
            if chunk_size <= 0 {
                bail!(ErrorKind::InvalidSnappy(snap::Error::UnsupportedChunkLength {
                    len: chunk_size as u64,
                    header: false,
                }));
            }
            let (c1, c2) = self.compressed_data.split_at(chunk_size as usize);
            try!(uncompress_to(c1, buf));
            self.compressed_data = c2;
        }
        Ok(buf.len() - init_len)
    }
}

macro_rules! to_io_error {
    ($expr:expr) => {
        match $expr {
            Ok(n) => Ok(n),
            // ~ pass io errors through directly
            Err(Error(ErrorKind::Io(io_error), _)) => Err(io_error),
            // ~ wrapp our other errors
            Err(e) => Err(io::Error::new(io::ErrorKind::Other, e.description())),
        }
    }
}

impl<'a> Read for SnappyReader<'a> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        to_io_error!(self._read(buf))
    }

    fn read_to_end(&mut self, buf: &mut Vec<u8>) -> io::Result<usize> {
        to_io_error!(self._read_to_end(buf))
    }
}

// --------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::str;
    use std::io::Read;

    use error::{Error, ErrorKind, Result};
    use super::{compress, uncompress_to, SnappyReader};

    fn uncompress(src: &[u8]) -> Result<Vec<u8>> {
        let mut v = Vec::new();
        match uncompress_to(src, &mut v) {
            Ok(_) => Ok(v),
            Err(e) => Err(e),
        }
    }

    #[test]
    fn test_compress() {
        let msg = "This is test".as_bytes();
        let compressed = compress(msg).unwrap();
        let expected = &[
            12,
            44,
            84,
            104,
            105,
            115,
            32,
            105,
            115,
            32,
            116,
            101,
            115,
            116,
        ];
        assert_eq!(&compressed, expected);
    }

    #[test]
    fn test_uncompress() {
        // The vector should uncompress to "This is test"
        let compressed = &[
            12,
            44,
            84,
            104,
            105,
            115,
            32,
            105,
            115,
            32,
            116,
            101,
            115,
            116,
        ];
        let uncompressed = String::from_utf8(uncompress(compressed).unwrap()).unwrap();
        assert_eq!(&uncompressed, "This is test");
    }

    #[test]
    fn test_uncompress_invalid_input() {
        // The vector is an invalid snappy message (second byte modified on purpose)
        let compressed = &[
            12,
            42,
            84,
            104,
            105,
            115,
            32,
            105,
            115,
            32,
            116,
            101,
            115,
            116,
        ];
        let uncompressed = uncompress(compressed);
        assert!(uncompressed.is_err());
        assert!(if let Some(Error(ErrorKind::InvalidSnappy(_), _)) = uncompressed.err() {
            true
        } else {
            false
        });
    }

    static ORIGINAL: &'static str = include_str!("../../test-data/fetch1.txt");
    static COMPRESSED: &'static [u8] = include_bytes!("../../test-data/fetch1.snappy.chunked.4k");

    #[test]
    fn test_snappy_reader_read() {
        let mut buf = Vec::new();
        let mut r = SnappyReader::new(COMPRESSED).unwrap();

        let mut tmp_buf = [0u8; 1024];
        loop {
            match r.read(&mut tmp_buf).unwrap() {
                0 => break,
                n => buf.extend_from_slice(&tmp_buf[..n]),
            }
        }
        assert_eq!(ORIGINAL, str::from_utf8(&buf[..]).unwrap());
    }

    #[test]
    fn test_snappy_reader_read_to_end() {
        let mut buf = Vec::new();
        let mut r = SnappyReader::new(COMPRESSED).unwrap();
        r.read_to_end(&mut buf).unwrap();
        assert_eq!(ORIGINAL, str::from_utf8(&buf[..]).unwrap());
    }
}
