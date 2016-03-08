# Index

* fetch1.txt
  - the input to various topic setups
  - one message per line
* fetch1.snappy.chunked.4k
  - snappy compressed fetch1.txt using SnappyOutputStream (snappy-java)
  - chunk size 4096
* fetch1.mytopic.1p.nocompression.kafka.0821
  - fetch response data (omitting the leading 4 byte size field)
  - snapshot from a 0.8.2.1 kafka server
  - input records: fetch1.txt
  - topic: "my-topic"
  - partitions: 1
  - messages were sent to the topic without any compression
* fetch1.mytopic.1p.snappy.kafka.0821
  - fetch response data (omitting the leading 4 byte size field)
  - snapshot from a 0.8.2.1 kafka server
  - input records: fetch1.txt
  - topic: "my-topic"
  - partitions: 1
  - messages were sent to the topic compressed using snappy compression
* fetch1.mytopic.1p.snappy.kafka.0822
  - fetch response data (omitting the leading 4 byte size field)
  - snapshot from a 0.8.2.2 kafka server
  - input records: fetch1.txt
  - topic: "my-topic"
  - partitions: 1
  - messages were sent to the topic compressed using snappy compression

* fetch2.txt
  - the input to various topic setups
  - exactly message only; one line
* fetch2.mytopic.nocompression.kafka.0900
  - fetch response data (omitting the leading 4 byte size field)
  - snapshot from a 0.9.0.0 kafka server
  - input records: fetch2.txt
  - topic: "my-topic"
  - partitions: 1
  - messages where sent to the topic in clear test (no compression)
* fetch2.mytopic.nocompression.invalid_crc.kafka.0900
  - same as fetch2.mytopic.nocompression.kafka.0900 except with a
    modified crc field, such that it does _not_ correspond to the
    message's data
