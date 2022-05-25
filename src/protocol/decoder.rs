use std::{array, num, result, str};

use integer_encoding::VarInt;
use num_enum::TryFromPrimitive;
use thiserror::Error as ThisError;
use uuid::Uuid;

use super::api_key::ApiKey;

#[derive(Debug, ThisError)]
pub enum Error {
    #[error("unexpected eof")]
    UnexpectedEof,
    #[error(transparent)]
    TryFromInt(#[from] num::TryFromIntError),
    #[error(transparent)]
    Utf8(#[from] str::Utf8Error),
    #[error(transparent)]
    TryFromSlice(#[from] array::TryFromSliceError),
    #[error(transparent)]
    DecodeApiKey(#[from] num_enum::TryFromPrimitiveError<ApiKey>),
    #[error("failed to decode var")]
    DecodeVar,
}

type Result<T> = result::Result<T, Error>;

macro_rules! decode_num {
    ($decoder:ident,$t:ty) => {
        $decoder.take_bytes_const().map(<$t>::from_be_bytes)
    };
}

pub struct Decoder<'storage> {
    bytes: &'storage [u8],
}

impl<'storage> Decoder<'storage> {
    pub fn new(bytes: &'storage [u8]) -> Self {
        Self { bytes }
    }

    pub fn take_bytes(&mut self, n: usize) -> Result<&'storage [u8]> {
        if n < self.bytes.len() {
            let (lhs, rhs) = self.bytes.split_at(n);
            self.bytes = rhs;
            Ok(lhs)
        } else {
            Err(Error::UnexpectedEof)
        }
    }

    pub fn take_bytes_const<const N: usize>(&mut self) -> Result<[u8; N]> {
        if N < self.bytes.len() {
            let (lhs, rhs) = self.bytes.split_at(N);
            self.bytes = rhs;
            Ok(lhs.try_into()?)
        } else {
            Err(Error::UnexpectedEof)
        }
    }

    pub fn decode_boolean(&mut self) -> Result<bool> {
        Ok(self.take_bytes_const::<1>()? != 0)
    }
    pub fn decode_int8(&mut self) -> Result<i8> {
        decode_num!(self, i8)
    }
    pub fn decode_int16(&mut self) -> Result<i16> {
        decode_num!(self, i16)
    }
    pub fn decode_int32(&mut self) -> Result<i32> {
        decode_num!(self, i32)
    }

    fn decode_var<T: VarInt>(&mut self) -> Result<T> {
        if let Some((value, size)) = T::decode_var(self.bytes) {
            let _ = self.take_bytes(size)?;
            Ok(value)
        } else {
            Err(Error::DecodeVar)
        }
    }
    pub fn decode_varint(&mut self) -> Result<i32> {
        self.decode_var()
    }
    pub fn decode_unsigned_varint(&mut self) -> Result<u32> {
        self.decode_var()
    }
    pub fn decode_varlong(&mut self) -> Result<i64> {
        self.decode_var()
    }
    pub fn decode_unsigned_varlong(&mut self) -> Result<u64> {
        self.decode_var()
    }
    pub fn decode_uuid(&mut self) -> Result<Uuid> {
        Ok(Uuid::from_bytes(self.take_bytes_const()?))
    }
    pub fn decode_string(&mut self) -> Result<&'storage str> {
        let len = self.decode_int16()?;
        Ok(str::from_utf8(self.take_bytes(len.try_into()?)?)?)
    }
    pub fn decode_compact_string(&mut self) -> Result<&'storage str> {
        let len: usize = self.decode_unsigned_varint()?.try_into()?;
        Ok(str::from_utf8(self.take_bytes(len - 1)?)?)
    }
    pub fn decode_nullable_string(&mut self) -> Result<Option<&'storage str>> {
        let len = self.decode_int16()?;
        if len == -1 {
            Ok(None)
        } else {
            let len: usize = len.try_into()?;
            Ok(Some(str::from_utf8(self.take_bytes(len)?)?))
        }
    }
    pub fn decode_compact_nullable_string(&mut self) -> Result<Option<&'storage str>> {
        let len = self.decode_var()?;
    }
    pub fn decode_array<T>(
        &mut self,
        decode_next_elem: fn(&mut Self) -> Result<T>,
    ) -> Result<Vec<T>> {
        let len = self.decode_int32()?;
        let arr = Vec::with_capacity(len.try_into()?);
        for _ in 0..len {
            arr.push(decode_next_elem(self)?);
        }
        Ok(arr)
    }

    pub fn decode_api_key(&mut self) -> Result<ApiKey> {
        Ok(ApiKey::try_from_primitive(self.decode_int16()?)?)
    }
}
