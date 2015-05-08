
#[derive(Default)]
#[derive(Debug)]
#[derive(Clone)]
pub struct OffsetMessage {
    pub offset: i64,
    pub message: Vec<u8>
}

#[derive(Default)]
#[derive(Debug)]
#[derive(Clone)]
pub struct PartitionOffset {
    pub partition: i32,
    pub offset: i64
}

#[derive(Default)]
#[derive(Debug)]
#[derive(Clone)]
pub struct TopicPartitionOffset {
    pub topic: String,
    pub partition: i32,
    pub offset: i64
}
