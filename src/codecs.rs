use std::io::{Read, Write};
use std::default::Default;

use num::traits::ToPrimitive;
use byteorder::{ByteOrder, BigEndian, ReadBytesExt, WriteBytesExt};


use error::{Result, Error};


pub trait ToByte {
    fn encode<T: Write>(&self, buffer: &mut T) -> Result<()>;
    fn encode_nolen<T: Write>(&self, buffer: &mut T)  -> Result<()> {
        self.encode(buffer)
    }
}

pub trait FromByte {
    type R: Default + FromByte;

    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()>;
    fn decode_new<T: Read>(buffer: &mut T) -> Result<Self::R> {
        let mut temp: Self::R = Default::default();
        match temp.decode(buffer) {
            Ok(_) => Ok(temp),
            Err(e) => Err(e)
        }
    }
}

impl ToByte for i8 {
    fn encode<T:Write>(&self, buffer: &mut T) -> Result<()> {
        buffer.write_i8(*self).or_else(|e| Err(From::from(e)))
    }
}

impl ToByte for i16 {
    fn encode<T:Write>(&self, buffer: &mut T) -> Result<()> {
        buffer.write_i16::<BigEndian>(*self).or_else(|e| Err(From::from(e)))
    }
}

impl ToByte for i32 {
    fn encode<T:Write>(&self, buffer: &mut T) -> Result<()> {
        buffer.write_i32::<BigEndian>(*self).or_else(|e| Err(From::from(e)))
    }
}

impl ToByte for i64 {
    fn encode<T:Write>(&self, buffer: &mut T) -> Result<()> {
        buffer.write_i64::<BigEndian>(*self).or_else(|e| Err(From::from(e)))
    }
}

impl ToByte for String {
    fn encode<T: Write>(&self, buffer: &mut T) -> Result<()> {
        let s: &str = self;
        s.encode(buffer)
    }
}

impl ToByte for str {
    fn encode<T: Write>(&self, buffer: &mut T) -> Result<()> {
        let l = try!(self.len()
                        .to_i16()
                        .ok_or(Error::CodecError));
        try!(buffer.write_i16::<BigEndian>(l));
        buffer.write_all(self.as_bytes())
                             .or_else(|e| Err(From::from(e)))
    }
}

impl <V: ToByte> ToByte for Vec<V> {
    fn encode<T:Write>(&self, buffer: &mut T) -> Result<()> {
        let l = try!(self.len()
                        .to_i32()
                        .ok_or(Error::CodecError));
        try!(buffer.write_i32::<BigEndian>(l));
        for e in self {
            try!(e.encode(buffer));
        }
        Ok(())
    }
    fn encode_nolen<T:Write>(&self, buffer: &mut T) -> Result<()> {
        for e in self {
            try!(e.encode(buffer));
        }
        Ok(())
    }
}

impl ToByte for Vec<u8> {
    fn encode<T: Write>(&self, buffer: &mut T) -> Result<()> {
        let l = try!(self.len()
                        .to_i32()
                        .ok_or(Error::CodecError));
        try!(buffer.write_i32::<BigEndian>(l));
        buffer.write_all(self).or_else(|e| Err(From::from(e)))
    }
    fn encode_nolen<T: Write>(&self, buffer: &mut T) -> Result<()> {
        buffer.write_all(self).or_else(|e| Err(From::from(e)))
    }
}

macro_rules! dec_helper {
    ($val: expr, $dest:expr) => ({
        match $val {
            Ok(val) => {
                *$dest = val;
                Ok(())
                },
            Err(e) => Err(From::from(e))
        }
    })
}
macro_rules! decode {
    ($src:expr, $dest:expr) => ({
        dec_helper!($src.read_i8(), $dest)

    });
    ($src:expr, $method:ident, $dest:expr) => ({
        dec_helper!($src.$method::<BigEndian>(), $dest)

    });
}

impl FromByte for i8 {
    type R = i8;

    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        decode!(buffer, self)
    }
}

impl FromByte for i16 {
    type R = i16;

    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        decode!(buffer, read_i16, self)
    }
}

impl FromByte for i32 {
    type R = i32;

    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        decode!(buffer, read_i32, self)
    }
}

impl FromByte for i64 {
    type R = i64;
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        decode!(buffer, read_i64, self)
    }
}

impl FromByte for String {
    type R = String;
    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        let mut length: i16 = 0;
        if let Err(e) = decode!(buffer, read_i16, &mut length) {
            return Err(e);
        }
        if length <= 0 { return Ok(()); }
        self.reserve(length as usize);
        let _ = buffer.take(length as u64).read_to_string(self);
        if self.len() != length as usize {
            return Err(Error::UnexpectedEOF);
        }
        Ok(())
    }
}

impl <V: FromByte + Default> FromByte for Vec<V>{
    type R = Vec<V>;

    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        let mut length: i32 = 0;
        if let Err(e) = decode!(buffer, read_i32, &mut length) {
            return Err(e);
        }
        if length <= 0 { return Ok(()); }
        self.reserve(length as usize);
        for _ in 0..length {
            let mut e: V = Default::default();
            try!(e.decode(buffer));
            self.push(e);
        }
        Ok(())
    }
}

impl FromByte for Vec<u8>{
    type R = Vec<u8>;

    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        let mut length: i32 = 0;
        match decode!(buffer, read_i32, &mut length) {
            Ok(_) => {},
            Err(e) => return Err(e)
        }
        if length <= 0 {return Ok(());}
        self.reserve(length as usize);
        match buffer.take(length as u64).read_to_end(self) {
            Ok(size) => {
                if size < length as usize {
                    return Err(Error::UnexpectedEOF)
                } else {
                    Ok(())
                }
            },
            Err(e) => Err(From::from(e))
        }
    }
}

#[test]
fn codec_i8() {
    use std::io::Cursor;
    let mut buf = vec!();
    let orig: i8 = 5;

    // Encode into buffer
    orig.encode(&mut buf).unwrap();
    assert_eq!(buf, [5]);

    // Read from buffer into existing variable
    let mut dec1: i8 = 0;
    dec1.decode(&mut Cursor::new(&buf)).unwrap();
    assert_eq!(dec1, orig);

    // Read from buffer into new variable
    let dec2 = i8::decode_new(&mut Cursor::new(&buf)).unwrap();
    assert_eq!(dec2, orig);
}

#[test]
fn codec_i16() {
    use std::io::Cursor;
    let mut buf = vec!();
    let orig: i16 = 5;

    // Encode into buffer
    orig.encode(&mut buf).unwrap();
    assert_eq!(buf, [0, 5]);

    // Read from buffer into existing variable
    let mut dec1: i16 = 0;
    dec1.decode(&mut Cursor::new(&buf)).unwrap();
    assert_eq!(dec1, orig);

    // Read from buffer into new variable
    let dec2 = i16::decode_new(&mut Cursor::new(&buf)).unwrap();
    assert_eq!(dec2, orig);
}

#[test]
fn codec_32() {
    use std::io::Cursor;
    let mut buf = vec!();
    let orig: i32 = 5;

    // Encode into buffer
    orig.encode(&mut buf).unwrap();
    assert_eq!(buf, [0, 0, 0, 5]);

    // Read from buffer into existing variable
    let mut dec1: i32 = 0;
    dec1.decode(&mut Cursor::new(&buf)).unwrap();
    assert_eq!(dec1, orig);

    // Read from buffer into new variable
    let dec2 = i32::decode_new(&mut Cursor::new(&buf)).unwrap();
    assert_eq!(dec2, orig);
}

#[test]
fn codec_i64() {
    use std::io::Cursor;
    let mut buf = vec!();
    let orig: i64 = 5;

    // Encode into buffer
    orig.encode(&mut buf).unwrap();
    assert_eq!(buf, [0, 0, 0, 0, 0, 0, 0, 5]);

    // Read from buffer into existing variable
    let mut dec1: i64 = 0;
    dec1.decode(&mut Cursor::new(&buf)).unwrap();
    assert_eq!(dec1, orig);

    // Read from buffer into new variable
    let dec2 = i64::decode_new(&mut Cursor::new(&buf)).unwrap();
    assert_eq!(dec2, orig);
}

#[test]
fn codec_string() {
    use std::io::Cursor;
    let mut buf = vec!();
    let orig = "test".to_owned();

    // Encode into buffer
    orig.encode(&mut buf).unwrap();
    assert_eq!(buf, [0, 4, 116, 101, 115, 116]);

    // Read from buffer into existing variable
    let mut dec1 = String::new();
    dec1.decode(&mut Cursor::new(&buf)).unwrap();
    assert_eq!(dec1, orig);

    // Read from buffer into new variable
    let dec2 = String::decode_new(&mut Cursor::new(&buf)).unwrap();
    assert_eq!(dec2, orig);
}

#[test]
fn codec_vec_u8() {
    use std::io::Cursor;
    let mut buf = vec!();
    let orig: Vec<u8> = vec!(1, 2, 3);

    // Encode into buffer
    orig.encode(&mut buf).unwrap();
    assert_eq!(buf, [0, 0, 0, 3, 1, 2, 3]);

    // Read from buffer into existing variable
    let mut dec1: Vec<u8> = vec!();
    dec1.decode(&mut Cursor::new(&buf)).unwrap();
    assert_eq!(dec1, orig);

    // Read from buffer into new variable
    let dec2 = Vec::<u8>::decode_new(&mut Cursor::new(&buf)).unwrap();
    assert_eq!(dec2, orig);
}

#[test]
fn codec_vec_strings() {
    use std::io::Cursor;

    let orig: Vec<String> = vec!["abc".to_owned(), "defg".to_owned()];

    // Encode into buffer
    let mut buf = Vec::new();
    orig.encode(&mut buf).unwrap();
    assert_eq!(buf, [0, 0, 0, 2,
                     0, 3, b'a', b'b', b'c',
                     0, 4, b'd', b'e', b'f', b'g']);

    // Decode from buffer into existing value
    {
        let mut dec: Vec<String> = Vec::new();
        dec.decode(&mut Cursor::new(&buf)).unwrap();
        assert_eq!(dec, orig);
    }

    // Read from buffer into new variable
    {
        let dec = Vec::<String>::decode_new(&mut Cursor::new(&buf)).unwrap();
        assert_eq!(dec, orig);
    }
}
