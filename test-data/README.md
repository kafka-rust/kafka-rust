# Index

* fetch1.txt
  - the input to various topic set-ups
  - one message per line
* fetch1.snappy.chunked.4k
  - snappy compressed fetch1.txt using SnappyOutputStream (snappy-java)
  - chunk size 4096
* fetch1.mytopic.1p.nocompression.kafka.0821
  - the fetch response data (omit the leading 4 byte size field)
  - snapshot from a 0.8.2.1 kafka server
  - input records: fetch1.txt
  - topic: "my-topic"
  - partitions: 1
  - messages were sent to the topic without any compression
* fetch1.mytopic.1p.snappy.kafka.0821
  - the fetch response data (omit the leading 4 byte size field)
  - snapshot from a 0.8.2.1 kafka server
  - input records: fetch1.txt
  - topic: "my-topic"
  - partitions: 1
  - messages were sent to the topic compressed using snappy compression
* fetch1.mytopic.1p.snappy.kafka.0822
  - the fetch response data (omit the leading 4 byte size field)
  - snapshot from a 0.8.2.2 kafka server
  - input records: fetch1.txt
  - topic: "my-topic"
  - partitions: 1
  - messages were sent to the topic compressed using snappy compression
