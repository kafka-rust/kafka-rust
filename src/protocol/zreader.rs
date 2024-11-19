use std::str;

use crate::{error::Result, Error};
use byteorder::{BigEndian, ByteOrder};

static EMPTY_STR: &str = "";

pub struct ZReader<'a> {
    data: &'a [u8],
}

// ~ a helper macro to hide away the used byte order
macro_rules! dec {
    ($method:ident, $src:expr) => {
        BigEndian::$method($src)
    };
}

impl<'a> ZReader<'a> {
    pub fn new(data: &[u8]) -> ZReader<'_> {
        ZReader { data }
    }

    /// ~ Consumes `n_bytes` from the underlying slice while returning
    /// the consumed bytes. The returned slice is guaranteed to be
    /// `n_bytes` long. This operation either succeeds or fails as a
    /// whole. Upon failure the reader will _not_ advance.
    pub fn read<'b>(&'b mut self, n_bytes: usize) -> Result<&'a [u8]> {
        if n_bytes > self.data.len() {
            Err(Error::UnexpectedEOF)
        } else {
            let (x, rest) = self.data.split_at(n_bytes);
            self.data = rest;
            Ok(x)
        }
    }

    /// ~ Retrieves the rest of the underlying slice without advancing
    /// this reader.
    pub fn rest(&self) -> &[u8] {
        self.data
    }

    /// ~ Determines whether there are still some bytes available for
    /// consumption.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn read_i8(&mut self) -> Result<i8> {
        self.read(1).map(|x| unsafe { *x.get_unchecked(0) as i8 })
    }

    pub fn read_i16(&mut self) -> Result<i16> {
        self.read(2).map(|x| dec!(read_i16, x))
    }

    pub fn read_i32(&mut self) -> Result<i32> {
        self.read(4).map(|x| dec!(read_i32, x))
    }

    pub fn read_i64(&mut self) -> Result<i64> {
        self.read(8).map(|x| dec!(read_i64, x))
    }

    /// Reads a string as defined by the Kafka Protocol. The 'null'
    /// string is delivered as the empty string.
    pub fn read_str<'b>(&'b mut self) -> Result<&'a str> {
        let len = self.read_i16()?;
        if len <= 0 {
            Ok(EMPTY_STR)
        } else {
            // alternatively: str::from_utf8_unchecked(..)
            match str::from_utf8(self.read(len as usize)?) {
                Ok(s) => Ok(s),
                Err(_) => Err(Error::StringDecodeError),
            }
        }
    }

    /// Reads 'bytes' as defined by the Kafka Protocol. The 'null'
    /// bytes are delivered as an empty slice.
    pub fn read_bytes<'b>(&'b mut self) -> Result<&'a [u8]> {
        let len = self.read_i32()?;
        if len <= 0 {
            Ok(&self.data[0..0])
        } else {
            self.read(len as usize)
        }
    }

    /// Reads the size of an array as defined by the Kafka
    /// Protocol. The size of 'null' array will be returned as the
    /// size an array of an empty array.
    pub fn read_array_len(&mut self) -> Result<usize> {
        let len = self.read_i32()?;
        Ok(if len < 0 { 0 } else { len as usize })
    }
}

#[test]
fn test_read() {
    let data = &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9];

    // ~ consume the reader in small chunks
    let mut r = ZReader::new(data);
    assert_eq!(&[0, 1], r.read(2).unwrap());
    assert_eq!(&[2], r.read(1).unwrap());
    assert_eq!(&[3, 4, 5, 6, 7], r.read(5).unwrap());
    assert_eq!(&[8, 9], r.read(2).unwrap());
    assert!(r.read(1).is_err());

    // ~ consume the whole available input
    r = ZReader::new(data);
    assert_eq!(data, r.read(data.len()).unwrap());

    r = ZReader::new(data);
    // ~ consume too much
    assert!(r.read(11).is_err());
    // ~ validate that the reader did not advance in the previous
    // operation
    assert_eq!(data, r.read(10).unwrap());
}

#[test]
fn test_read_i8() {
    let data = &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
    let mut r = ZReader::new(data);
    for &i in data {
        assert_eq!(i as i8, r.read_i8().unwrap());
    }
    assert!(r.read_i8().is_err());

    let data = &[0xff, 0xfe];
    let mut r = ZReader::new(data);
    assert_eq!(-1, r.read_i8().unwrap());
    assert_eq!(-2, r.read_i8().unwrap());
    assert!(r.read_i8().is_err());
}

#[test]
fn test_read_i16() {
    let data = &[1, 2, 16, 1];
    let mut r = ZReader::new(data);
    assert_eq!(258, r.read_i16().unwrap());
    assert_eq!(4097, r.read_i16().unwrap());
    assert!(r.read_i16().is_err());

    let data = &[0xff, 0xff, 0xff, 0xfe];
    let mut r = ZReader::new(data);
    assert_eq!(-1, r.read_i16().unwrap());
    assert_eq!(-2, r.read_i16().unwrap());
    assert!(r.read_i16().is_err());
}

#[test]
fn test_read_i32() {
    let data = &[1, 2, 3, 4];
    let mut r = ZReader::new(data);
    assert_eq!(16_909_060, r.read_i32().unwrap());
    assert!(r.read_i32().is_err());

    let data = &[0xff, 0xff, 0xff, 0xfd];
    let mut r = ZReader::new(data);
    assert_eq!(-3, r.read_i32().unwrap());
    assert!(r.read_i32().is_err());
}

#[test]
fn test_read_i64() {
    let data = &[1, 2, 3, 4, 5, 6, 7, 8];
    let mut r = ZReader::new(data);
    assert_eq!(72_623_859_790_382_856, r.read_i64().unwrap());
    assert!(r.read_i64().is_err());

    let data = &[0, 0, 0, 0, 0, 0, 0, 1];
    let mut r = ZReader::new(data);
    assert_eq!(1, r.read_i64().unwrap());
    assert!(r.read_i64().is_err());

    let data = &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfd];
    let mut r = ZReader::new(data);
    assert_eq!(-3, r.read_i64().unwrap());
    assert!(r.read_i64().is_err());
}

#[test]
fn test_read_str() {
    let data = &[
        0u8, 5, b'h', b'e', b'l', b'l', b'o', 0u8, 7, b',', b' ', b'w', b'o', b'r', b'l', b'd',
        255, /* a "null" string */
        28,
    ]; // some byte

    let mut r = ZReader::new(data);
    assert_eq!("hello", r.read_str().unwrap());
    assert_eq!(", world", r.read_str().unwrap());
    assert_eq!("", r.read_str().unwrap());
    // reading the last byte (> 0) as a string is invalid as more
    // chars would be expected
    assert!(r.read_str().is_err());
}

/// Verify we can advance the reader while holding on to a previously
/// returned slice/string.
#[test]
fn test_mutability_lifetimes() {
    let data = &[0, 2, b'h', b'i', 0, 2, b'h', b'o'];

    let mut r = ZReader::new(data);
    let x = r.read_str().unwrap();
    let y = r.read_str().unwrap();

    assert_eq!("hi", x);
    assert_eq!("ho", y);
}
