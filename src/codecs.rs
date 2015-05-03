use num::traits::ToPrimitive;
use std::io::{Read, Write};
use byteorder::{ByteOrder, BigEndian, ReadBytesExt, WriteBytesExt};
use std::default::Default;

pub trait ToByte <T: Write> {
    fn encode(&self, buffer: &mut T);
}

pub trait FromByte <T: Read> {
    type R: Default + FromByte<T>;

    fn decode(&mut self, buffer: &mut T);
    fn decode_new(buffer: &mut T) -> Self::R {
        let mut temp: Self::R = Default::default();
        temp.decode(buffer);
        temp
    }
}

impl <T:Write> ToByte<T> for i16 {
    fn encode(&self, buffer: &mut T) {
        buffer.write_i16::<BigEndian>(*self);
    }
}
impl <T:Write> ToByte<T> for i32 {
    fn encode(&self, buffer: &mut T) {
        buffer.write_i32::<BigEndian>(*self);
    }
}
impl <T:Write> ToByte<T> for i64 {
    fn encode(&self, buffer: &mut T) {
        buffer.write_i64::<BigEndian>(*self);
    }
}
impl <T:Write> ToByte<T> for String {
    fn encode(&self, buffer: &mut T) {
        buffer.write_i16::<BigEndian>(self.len().to_i16().unwrap());
        buffer.write_all(self.as_bytes());
    }
}

impl <T:Write, V:ToByte<T>> ToByte<T> for Vec<V> {
    fn encode(&self, buffer: &mut T) {
        buffer.write_i32::<BigEndian>(self.len().to_i32().unwrap());
        for e in self {
            e.encode(buffer);
        }
    }
}


impl <T: Read> FromByte<T> for i16 {
    type R = i16;

    fn decode(&mut self, buffer: &mut T) {
        *self = buffer.read_i16::<BigEndian>().unwrap();
    }
}
impl <T: Read> FromByte<T> for i32 {
    type R = i32;

    fn decode(&mut self, buffer: &mut T) {
        *self = buffer.read_i32::<BigEndian>().unwrap();
    }
}
impl <T: Read> FromByte<T> for i64 {
    type R = i64;
    fn decode(&mut self, buffer: &mut T) {
        *self = buffer.read_i64::<BigEndian>().unwrap();
    }
}
impl <T: Read> FromByte<T> for String {
    type R = String;
    fn decode(&mut self, buffer: &mut T) {
        let length = buffer.read_i16::<BigEndian>().unwrap();
        let mut client = String::new();
        let _ = buffer.take(length as u64).read_to_string(self);
    }
}

impl <T:Read, V: FromByte<T> + Default> FromByte<T> for Vec<V>{
    type R = Vec<V>;

    fn decode(&mut self, buffer: &mut T) {
        let length = buffer.read_i32::<BigEndian>().unwrap();
        for i in 0..length {
            let mut e: V = Default::default();
            e.decode(buffer);
            self.push(e);
        }
    }
}
