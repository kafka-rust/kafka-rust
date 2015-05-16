
#[derive(Clone, Debug)]
pub struct OffsetMessage {
    pub offset: i64,
    pub message: Vec<u8>
}

#[derive(Clone, Debug)]
pub struct ProduceMessage {
    pub topic: String,
    pub partition: i32,
    pub message: Vec<u8>
}


#[derive(Debug)]
pub struct PartitionOffset {
    pub partition: i32,
    pub offset: i64
}

#[derive(Debug)]
pub struct TopicPartitionOffset {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub error: i16
}

#[derive(Debug)]
pub struct TopicPartitions {
    pub topic: String,
    pub partitions: Vec<i32>
}
