use num::traits::ToPrimitive;
use std::io::{Read, Write};
use byteorder::{ByteOrder, BigEndian, ReadBytesExt, WriteBytesExt, Result, Error};
use std::default::Default;

pub trait ToByte {
    fn encode<T: Write>(&self, buffer: &mut T);
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
    fn encode<T:Write>(&self, buffer: &mut T) {
        buffer.write_i8(*self);
    }
}

impl ToByte for i16 {
    fn encode<T:Write>(&self, buffer: &mut T) {
        buffer.write_i16::<BigEndian>(*self);
    }
}
impl ToByte for i32 {
    fn encode<T:Write>(&self, buffer: &mut T) {
        buffer.write_i32::<BigEndian>(*self);
    }
}
impl ToByte for i64 {
    fn encode<T:Write>(&self, buffer: &mut T) {
        buffer.write_i64::<BigEndian>(*self);
    }
}
impl ToByte for String {
    fn encode<T:Write>(&self, buffer: &mut T) {
        buffer.write_i16::<BigEndian>(self.len().to_i16().unwrap());
        buffer.write_all(self.as_bytes());
    }
}

impl <V:ToByte> ToByte for Vec<V> {
    fn encode<T:Write>(&self, buffer: &mut T) {
        buffer.write_i32::<BigEndian>(self.len().to_i32().unwrap());
        for e in self {
            e.encode(buffer);
        }
    }
}

impl ToByte for Vec<u8>{
    fn encode<T: Write>(&self, buffer: &mut T) {
        buffer.write_i32::<BigEndian>(self.len().to_i32().unwrap());
        buffer.write_all(self);
    }
}

macro_rules! dec_helper {
    ($val: expr, $dest:expr) => ({
        match $val {
            Ok(val) => {
                *$dest = val;
                Ok(())
                }
            Err(e) => Err(e)
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
        try!(decode!(buffer, read_i16, &mut length));// = buffer.read_i16::<BigEndian>().unwrap();
        let mut client = String::new();
        let _ = buffer.take(length as u64).read_to_string(self);
        if (self.len() != length as usize) {
            return Err(Error::UnexpectedEOF);
        }
        Ok(())
    }
}

impl <V: FromByte + Default> FromByte for Vec<V>{
    type R = Vec<V>;

    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        let mut length: i32 = 0;
        try!(decode!(buffer, read_i32, &mut length));// = buffer.read_i32::<BigEndian>().unwrap();
        println!("Length = {}", length);
        for i in 0..length {
            let mut e: V = Default::default();
            try!(e.decode(buffer));
            self.push(e);
        }
        println!("Got Length = {}", length);
        Ok(())
    }
}

impl FromByte for Vec<u8>{
    type R = Vec<u8>;

    fn decode<T: Read>(&mut self, buffer: &mut T) -> Result<()> {
        let mut length: i32 = 0;
        try!(decode!(buffer, read_i32, &mut length));// = buffer.read_i32::<BigEndian>().unwrap();
        println!("\tLength = {}", length);
        if (length <= 0) {return Ok(());}
        match buffer.take(length as u64).read_to_end(self) {
            Ok(size) => {
                if size < length as usize {
                    return Err(Error::UnexpectedEOF)
                } else {
                    Ok(())
                }
            },
            Err(e) => Err(Error::Io(e))
        }
    }
}
