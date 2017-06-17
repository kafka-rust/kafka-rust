//! A crate private module to expose `KafkaClient` internals for use
//! within this crate but not outside of it.

use client::ProduceMessage;
use producer::ProduceConfirm;
use error::Result;

pub trait KafkaClientInternals {
    fn internal_produce_messages<'a, 'b, I, J>(
        &mut self,
        required_acks: i16,
        ack_timeout: i32,
        messages: I,
    ) -> Result<Vec<ProduceConfirm>>
    where
        J: AsRef<ProduceMessage<'a, 'b>>,
        I: IntoIterator<Item = J>;
}
