# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.11.0] 2026-07-02

### Added

- **SASL/PLAIN authentication over TLS (SASL_SSL)**: New `SaslConfig` type and `SecurityConfig::with_sasl_plain()` builder method. After the TLS handshake, the client performs a `SaslHandshake` (API 17) + `SaslAuthenticate` (API 36) exchange before entering normal Kafka protocol traffic. Useful for Confluent Cloud, AWS MSK, and other managed Kafka services that require SASL/PLAIN.
- **rustls TLS backend (default)**: kafka-rust now uses rustls as the default TLS implementation, providing a pure-Rust, secure, and portable solution for TLS connections.
  - New `security-rustls` feature flag (enabled by default via `security` feature)
  - **Updated to rustls 0.23.35** with latest improvements and optimizations
  - Support for webpki-roots 1.0 and rustls-native-certs 0.8 for certificate validation
  - Support for custom CA certificates, client certificates, and hostname verification control
  - Works out-of-the-box on musl, alpine, and other cross-compilation targets without native dependencies

### Changed

- **BREAKING (Minor)**: The `security` feature now maps to `security-rustls` instead of directly depending on OpenSSL.
  - Default TLS backend is now rustls (pure Rust)
  - Existing code using `SecurityConfig::new()` will automatically use rustls
  - No API changes required for most users
- **Updated rustls dependencies**:
  - rustls: 0.21 → 0.23.35 (latest stable with performance improvements)
  - webpki-roots: 0.25 → 1.0
  - rustls-pemfile: 1.0 → 2.2
  - rustls-native-certs: 0.6 → 0.8

### Deprecated

- **OpenSSL backend**: The OpenSSL-based TLS implementation is now deprecated and will be removed in the next major version.
  - Use `security-openssl` feature flag to continue using OpenSSL temporarily
  - Migrate to rustls by using the default `security` feature
  - See `examples/example-rustls.rs` for rustls usage examples

### Migration Guide

For most users, no changes are required. The default build will now use rustls instead of OpenSSL.

If you explicitly need OpenSSL (not recommended):
```toml
kafka = { version = "0.11", default-features = false, features = ["snappy", "gzip", "security-openssl"] }
```

To use rustls explicitly (recommended):
```toml
kafka = { version = "0.11", features = ["security"] }
# or
kafka = { version = "0.11", features = ["security-rustls"] }
```

Benefits of migrating to rustls:
- No native OpenSSL dependencies required
- Better cross-compilation support
- Consistent TLS behavior across platforms
- Modern TLS 1.2+ only
- Pure Rust implementation

## [0.9.0] 2022-04-29

- Updated to support Rust 2021
- Brought all of the dependencies up to date, so this could cause **breaking changes**
- Removed the try! methods
- Updated the error mechanism to use thiserror and anyhow.
- Removed error-chain, as it is deprecated
- This is a non breaking change, but I wanted to bump the version as it has been over two years since the last release.
  Contributors:
- Thank you to midnightexigent and tshepang for your contributions
- Thank you to dead10ck for your support.

## [0.8.0] 2019-09-10

- Upgrade openssl to v0.10. This may be a **breaking change** for your
  application code, since openssl v0.10 is a breaking change. Thanks to @l4l!
- Run integration tests on various configurations with compression and
  encryption.

## [0.7.0] 2017-10-17

### Fixed

- [**BREAKING**] Fixed #101. The `Consumer` was erroneously committing the offset
  of the _last consumed message_ instead of the next offset that it should read,
  which is what [the Kafka protocol specifies it should
  be](https://kafka.apache.org/documentation.html#theconsumer). This means that:

  - When you upgrade, your consumers will read the last message it consumed again.
  - The consumers will now be committing one offset past where they were before.
    If you've come to rely on this behavior in any way, you should correct it.
