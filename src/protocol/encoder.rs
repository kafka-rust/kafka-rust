use std::io;

use num_enum::IntoPrimitive;
use thiserror::Error as ThisError;

use super::api_key::ApiKey;

#[derive(Debug, ThisError)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] io::Error),
}

macro_rules! encode_num {
    ($encoder:ident,$v:ident) => {
        $encoder.writer.write_all(&$v.to_be_bytes())
    };
}

pub struct Encoder<W> {
    writer: W,
}
impl<W: io::Write> Encoder<W> {
    pub fn new(buf: &'storage [u8]) -> Self {
        Self { writer: buf }
    }
    pub fn encode_boolean(&mut self, value: bool) -> io::Result<()> {
        if value {
            self.writer.write_all(&[1])
        } else {
            self.writer.write_all(&[0])
        }
    }
    pub fn encode_int8(&mut self, value: i8) -> io::Result<()> {
        encode_num!(self, value)
    }
    pub fn encode_int16(&mut self, value: i16) -> io::Result<()> {
        encode_num!(self, value)
    }
    pub fn encode_api_key(&mut self, value: ApiKey) -> io::Result<()> {
        value.into_primitive()
    }
}
