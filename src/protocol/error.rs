use std::fmt::Debug;

use num_enum::TryFromPrimitive;
use thiserror::Error as ThisError;

use super::decoder;

#[derive(Debug, ThisError)]
pub enum Error {
    #[error(transparent)]
    Decoder(#[from] decoder::Error),
}

#[derive(Debug, ThisError)]
#[error("Request {request:?} caused server error with code {code}")]
pub struct Server<'a, R: Debug> {
    code: i16,
    request: &'a R,
}
