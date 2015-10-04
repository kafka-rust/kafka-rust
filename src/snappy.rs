extern crate libc;
use self::libc::{c_int, size_t};
use std::io::Read;

use codecs::FromByte;
use error::{Result, Error};

#[link(name = "snappy")]
extern {
    fn snappy_compress(input: *const u8,
                       input_length: size_t,
                       compressed: *mut u8,
                       compressed_length: *mut size_t) -> c_int;
    fn snappy_max_compressed_length(source_length: size_t) -> size_t;
    fn snappy_uncompress(compressed: *const u8,
                             compressed_length: size_t,
                             uncompressed: *mut u8,
                             uncompressed_length: *mut size_t) -> c_int;
    fn snappy_uncompressed_length(compressed: *const u8,
                                  compressed_length: size_t,
                                  result: *mut size_t) -> c_int;
}

#[derive(Default, Debug, Clone)]
pub struct SnappyHeader {
    pub marker: i8,
    // TODO - Its a c-string of 6 bytes not 6 independent chars
    pub c1: i8,
    pub c2: i8,
    pub c3: i8,
    pub c4: i8,
    pub c5: i8,
    pub c6: i8,
    pub pad: i8,
    pub version: i32,
    pub compat: i32
}

#[derive(Default, Debug, Clone)]
pub struct SnappyMessage {
    pub message: Vec<u8>
}

impl FromByte for SnappyHeader {
    type R = SnappyHeader;

    #[allow(unused_must_use)]
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        self.marker.decode(buffer);
        // TODO - decode a fixed size array instead of byte by byte. I mean, make it elegant
        try!(self.c1.decode(buffer));
        try!(self.c2.decode(buffer));
        try!(self.c3.decode(buffer));
        try!(self.c4.decode(buffer));
        try!(self.c5.decode(buffer));
        try!(self.c6.decode(buffer));
        try!(self.pad.decode(buffer));
        try!(self.version.decode(buffer));
        try!(self.compat.decode(buffer));
        if (self.marker, self.c1, self.c2, self.c3, self.c4, self.c5, self.c6, self.pad) !=
            (-126, b'S' as i8, b'N' as i8, b'A' as i8, b'P' as i8, b'P' as i8, b'Y' as i8, 0) {
            Err(Error::InvalidInputSnappy)
        } else {
            Ok(())
        }
    }
}

impl FromByte for SnappyMessage {
    type R = SnappyMessage;

    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try!(self.message.decode(buffer));
        Ok(())
    }
}

pub fn uncompress(src: Vec<u8>) -> Result<Vec<u8>> {
    unsafe {
        let (_, x) = src.split_at(0);
        let srclen = x.len() as size_t;
        let psrc = x.as_ptr();
        let mut dstlen: size_t = 0;
        snappy_uncompressed_length(psrc, srclen, &mut dstlen);
        let mut dst = Vec::with_capacity(dstlen as usize);
        let pdst = dst.as_mut_ptr();

        if snappy_uncompress(psrc, srclen, pdst, &mut dstlen) == 0 {
            dst.set_len(dstlen as usize);
            Ok(dst)
        } else {
            Err(Error::InvalidInputSnappy) // SNAPPY_INVALID_INPUT
        }
    }
}

pub fn compress(src: &[u8]) -> Result<Vec<u8>> {
    unsafe {
        let (_, x) = src.split_at(0);
        let srclen = x.len() as size_t;
        let psrc = x.as_ptr();
        let mut dstlen = snappy_max_compressed_length(srclen);
        let mut dst = Vec::with_capacity(dstlen as usize);
        let pdst = dst.as_mut_ptr();
        if snappy_compress(psrc, srclen, pdst, &mut dstlen) == 0 {
            dst.set_len(dstlen as usize);
            Ok(dst)
        } else {
            Err(Error::InvalidInputSnappy)
        }
    }
}

#[test]
fn test_compress() {
    let msg = "This is test".as_bytes();
    let comp_msg = compress(msg).unwrap();
    let expected: Vec<u8> = vec!(12, 44, 84, 104, 105, 115, 32, 105, 115, 32, 116, 101, 115, 116);
    assert_eq!(comp_msg, expected);
}

#[test]
fn test_uncompress() {
    // The vector should uncompress to "This is test"
    let msg: Vec<u8> = vec!(12, 44, 84, 104, 105, 115, 32, 105, 115, 32, 116, 101, 115, 116);
    let uncomp_msg = String::from_utf8(uncompress(msg).unwrap()).unwrap();
    assert_eq!(&uncomp_msg[..], "This is test");
}

#[test]
#[should_panic]
fn test_uncompress_panic() {
    let msg: Vec<u8> = vec!(12, 42, 84, 104, 105, 115, 32, 105, 115, 32, 116, 101, 115, 116);
    let uncomp_msg = String::from_utf8(uncompress(msg).unwrap()).unwrap();
    assert_eq!(&uncomp_msg[..], "This is test");
}
