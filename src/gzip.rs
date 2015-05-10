use std::io::prelude::*;
use flate2::read::GzDecoder;

use error::Result;

pub fn uncompress<T: Read>(src: T) -> Result<Vec<u8>> {
    let mut d = try!(GzDecoder::new(src));

    let mut buffer: Vec<u8> = Vec::new();
    match d.read_to_end(&mut buffer) {
        Err(err) =>  Err(From::from(err)),
        Ok(_) => Ok(buffer)
    }
}
