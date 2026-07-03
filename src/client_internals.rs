//! A crate private module to expose `KafkaClient` internals for use
//! within this crate but not outside of it.

use crate::client::ProduceMessage;
#[cfg(feature = "producer_timestamp")]
use crate::client::TimestampedProduceMessage;
use crate::error::Result;
use crate::producer::ProduceConfirm;

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

    #[cfg(feature = "producer_timestamp")]
    fn internal_produce_messages_with_timestamps<'a, 'b, I, J>(
        &mut self,
        required_acks: i16,
        ack_timeout: i32,
        messages: I,
    ) -> Result<Vec<ProduceConfirm>>
    where
        J: AsRef<TimestampedProduceMessage<'a, 'b>>,
        I: IntoIterator<Item = J>;
}
