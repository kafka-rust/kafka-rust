
#[derive(Default)]
#[derive(Debug)]
#[derive(Clone)]
pub struct OffsetMessage {
    pub offset: i64,
    pub message: Vec<u8>
}
