# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.8.0] 2019-09-10

* Upgrade openssl to v0.10. This may be a **breaking change** for your
  application code, since openssl v0.10 is a breaking change. Thanks to @l4l!
* Run integration tests on various configurations with compression and
  encryption.

## [0.7.0] 2017-10-17

### Fixed

* [**BREAKING**] Fixed #101. The `Consumer` was erroneously committing the offset
  of the *last consumed message* instead of the next offset that it should read,
  which is what [the Kafka protocol specifies it should
  be](https://kafka.apache.org/documentation.html#theconsumer). This means that:

  * When you upgrade, your consumers will read the last message it consumed again.
  * The consumers will now be committing one offset past where they were before.
    If you've come to rely on this behavior in any way, you should correct it.
