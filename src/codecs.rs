use num::traits::ToPrimitive;
use std::io::{Read, Write};
use byteorder::{ByteOrder, BigEndian, ReadBytesExt, WriteBytesExt};
use std::default::Default;

pub trait ToByte {
    fn encode<T: Write>(&self, buffer: &mut T);
}

pub trait FromByte {
    type R: Default + FromByte;

    fn decode<T: Read>(&mut self, buffer: &mut T);
    fn decode_new<T: Read>(buffer: &mut T) -> Self::R {
        let mut temp: Self::R = Default::default();
        temp.decode(buffer);
        temp
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
impl <T:Write> ToByte for i64 {
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


impl FromByte for i16 {
    type R = i16;

    fn decode<T: Read>(&mut self, buffer: &mut T) {
        *self = buffer.read_i16::<BigEndian>().unwrap();
    }
}
impl FromByte for i32 {
    type R = i32;

    fn decode<T: Read>(&mut self, buffer: &mut T) {
        *self = buffer.read_i32::<BigEndian>().unwrap();
    }
}
impl FromByte for i64 {
    type R = i64;
    fn decode<T: Read>(&mut self, buffer: &mut T) {
        *self = buffer.read_i64::<BigEndian>().unwrap();
    }
}
impl FromByte for String {
    type R = String;
    fn decode<T: Read>(&mut self, buffer: &mut T) {
        let length = buffer.read_i16::<BigEndian>().unwrap();
        let mut client = String::new();
        let _ = buffer.take(length as u64).read_to_string(self);
    }
}

impl <V: FromByte + Default> FromByte for Vec<V>{
    type R = Vec<V>;

    fn decode<T: Read>(&mut self, buffer: &mut T) {
        let length = buffer.read_i32::<BigEndian>().unwrap();
        for i in 0..length {
            let mut e: V = Default::default();
            e.decode(buffer);
            self.push(e);
        }
    }
}
