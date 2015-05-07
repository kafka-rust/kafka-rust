extern crate libc;
use self::libc::{c_int, size_t};
use std::io::{Read, Write};
use byteorder::{ByteOrder, BigEndian, ReadBytesExt, WriteBytesExt, Result, Error};
use super::codecs::*;

#[link(name = "snappy")]
extern {
    fn snappy_compress(input: *const u8,
                           input_length: size_t,
                           compressed: *mut u8,
                           compressed_length: *mut size_t) -> c_int;
    fn snappy_uncompress(compressed: *const u8,
                             compressed_length: size_t,
                             uncompressed: *mut u8,
                             uncompressed_length: *mut size_t) -> c_int;
    fn snappy_max_compressed_length(source_length: size_t) -> size_t;
    fn snappy_uncompressed_length(compressed: *const u8,
                                  compressed_length: size_t,
                                  result: *mut size_t) -> c_int;
    fn snappy_validate_compressed_buffer(compressed: *const u8,
                                         compressed_length: size_t) -> c_int;
}

#[derive(Default)]
#[derive(Debug)]
#[derive(Clone)]
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

#[derive(Default)]
#[derive(Debug)]
#[derive(Clone)]
pub struct SnappyMessage {
    pub message: Vec<u8>
}

impl FromByte for SnappyHeader {
    type R = SnappyHeader;

    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        self.marker.decode(buffer);
        // TODO - decode a fixed size array instead of byte by byte. I mean, make it elegant
        self.c1.decode(buffer);
        self.c2.decode(buffer);
        self.c3.decode(buffer);
        self.c4.decode(buffer);
        self.c5.decode(buffer);
        self.c6.decode(buffer);
        self.pad.decode(buffer);
        self.version.decode(buffer);
        self.compat.decode(buffer);
        Ok(())
    }
}

impl FromByte for SnappyMessage {
    type R = SnappyMessage;

    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        try!(self.message.decode(buffer));
        Ok(())
    }
}
pub fn validate_compressed_buffer(src: &[u8]) -> bool {
    unsafe {
        snappy_validate_compressed_buffer(src.as_ptr(), src.len() as size_t) == 0
    }
}


pub fn compress(src: &[u8]) -> Vec<u8> {
    unsafe {
        let srclen = src.len() as size_t;
        let psrc = src.as_ptr();

        let mut dstlen = snappy_max_compressed_length(srclen);
        let mut dst = Vec::with_capacity(dstlen as usize);
        let pdst = dst.as_mut_ptr();

        snappy_compress(psrc, srclen, pdst, &mut dstlen);
        dst.set_len(dstlen as usize);
        dst
    }
}

pub fn uncompress(src: &Vec<u8>) -> Option<Vec<u8>> {
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
            Some(dst)
        } else {
            None // SNAPPY_INVALID_INPUT
        }
    }
}
