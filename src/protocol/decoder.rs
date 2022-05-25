use std::{io, num, result, string};

use integer_encoding::{VarInt, VarIntReader};
use thiserror::Error as ThisError;
use uuid::Uuid;

#[derive(Debug, ThisError)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    TryFromInt(#[from] num::TryFromIntError),
    #[error(transparent)]
    FromUtf8(#[from] string::FromUtf8Error),
}

type Result<T> = result::Result<T, Error>;

macro_rules! decode_num {
    ($dec:ident,$t:ty) => {
        $dec.take_bytes_const().map(<$t>::from_be_bytes)
    };
}

pub struct Decoder<R> {
    rdr: R,
}

// XXX: it would be more efficient to implement using a cust Read that allows to inspect
// the inner slice [u8]: https://github.com/bincode-org/bincode/blob/6ec69a38b5698876662e93ca928f6f41183611d5/src/de/read.rs
impl<R: io::Read> Decoder<R> {
    pub fn new(rdr: R) -> Self {
        Self { rdr }
    }
    pub fn take_bytes(&mut self, n: usize) -> io::Result<Vec<u8>> {
        let buf = vec![0; n];
        self.rdr.read_exact(&mut buf)?;
        Ok(buf)
    }
    pub fn take_bytes_const<const N: usize>(&mut self) -> io::Result<[u8; N]> {
        let buf = [0; N];
        self.rdr.read_exact(&mut buf)?;
        Ok(buf)
    }
    pub fn decode_var<T: VarInt>(&mut self) -> io::Result<T> {
        self.rdr.read_varint()
    }
    pub fn decode_uuid(&mut self) -> Result<Uuid> {
        Ok(Uuid::from_bytes(self.take_bytes_const()?))
    }
    pub fn decode_string(&mut self) -> Result<String> {
        let len = decode_num!(self, i16)?;
        Ok(String::from_utf8(self.take_bytes(len.try_into()?)?)?)
    }
    pub fn decode_array<T>(
        &mut self,
        decode_next_elem: fn(&mut Decoder<R>) -> Result<T>,
    ) -> Result<Vec<T>> {
        let len = decode_num!(self, i32)?;
        let arr = Vec::with_capacity(len.try_into()?);
        for _ in 0..len {
            arr.push(decode_next_elem(self)?);
        }
        Ok(arr)
    }
}
