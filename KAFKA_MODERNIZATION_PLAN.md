# Kafka-Rust Protocol Modernization Plan

## Executive Summary

This document outlines the comprehensive plan to modernize the `kafka-rust` library to support Kafka 3.x protocol features while maintaining 100% backward compatibility. The plan is divided into 7 phases spanning approximately 12-15 months.

**Current State:** API Version 0 (Kafka 0.8/0.9 era), Message Format v0  
**Target State:** Kafka 3.x compatibility with modern features  
**Approach:** Incremental, backward-compatible updates

---

## Current State Assessment

### What We Have
- **Protocol Version**: API_VERSION = 0 (Kafka 0.8/0.9 era)
- **Message Format**: Magic byte 0 only
- **Compression**: NONE, GZIP, SNAPPY
- **Error Codes**: Up to code 35 (missing ~50+ modern codes)
- **Features**: Basic producer/consumer, no idempotency, no transactions

### What's Missing
- Protocol version negotiation
- Message format v2/v3 (RecordBatch)
- Modern compression (LZ4, Zstandard)
- Idempotent/transactional producer
- Modern API versions for Produce/Fetch/Metadata
- Extended error codes (36-90+)

---

## Phase 1: Protocol Version Negotiation ✅ COMPLETE

**Duration:** 2-3 months  
**Status:** ✅ Completed

### Objectives
Implement infrastructure for dynamic protocol version negotiation with Kafka brokers.

### Deliverables
- ✅ ApiVersions Request/Response (API Key 18)
- ✅ Version storage in ClientState
- ✅ Automatic negotiation in KafkaClient
- ✅ Graceful fallback to version 0

### Files Modified
- `src/protocol/api_versions.rs` (NEW, 117 lines)
- `src/protocol/mod.rs` (+22 lines)
- `src/client/state.rs` (+33 lines)
- `src/client/mod.rs` (+68 lines)
- `PHASE1_COMPLETE.md` (NEW, documentation)

### Key Features
- Discovers broker-supported API versions
- Caches versions per broker for O(1) lookup
- Falls back to version 0 for Kafka < 0.10
- Zero runtime cost when not used
- 100% backward compatible

### Testing
- ✅ 2 unit tests for ApiVersions protocol
- ✅ All existing tests pass
- ✅ Compiles without errors

---

## Phase 2: Extended Error Codes

**Duration:** 1 month  
**Status:** 🔜 Pending  
**Dependencies:** None (independent)

### Objectives
Extend error handling to support modern Kafka error codes.

### Deliverables
- [ ] Extend `KafkaCode` enum with codes 36-90+
- [ ] Update `KafkaCode::from_protocol()` for extended range
- [ ] Add documentation for new error codes
- [ ] Update error handling tests

### New Error Codes (Partial List)
```rust
TopicAlreadyExists = 36,
InvalidPartitions = 37,
InvalidReplicationFactor = 38,
InvalidReplicaAssignment = 39,
InvalidConfig = 40,
NotController = 41,
InvalidRequest = 42,
UnsupportedForMessageFormat = 43,
PolicyViolation = 44,
OutOfOrderSequenceNumber = 45,
DuplicateSequenceNumber = 46,
InvalidProducerEpoch = 47,
InvalidTxnState = 48,
// ... (50+ more codes)
```

### Files to Modify
- `src/error.rs` - Add new error codes
- `src/protocol/mod.rs` - Update from_protocol range check

### Impact
- Better error messages and debugging
- Required for idempotent producer (Phase 5)
- Improved compatibility with modern Kafka

---

## Phase 3: Message Format v2/v3 Support

**Duration:** 3-4 months  
**Status:** 🔜 Pending  
**Dependencies:** Phase 1 (protocol negotiation)

### Objectives
Implement RecordBatch format (message format v2) for Kafka 0.11+.

### Deliverables
- [ ] RecordBatch encoding/decoding (`src/protocol/record_batch.rs`)
- [ ] Variable-length integer encoding (varint/varlong)
- [ ] Message headers support
- [ ] Timestamp delta encoding
- [ ] Producer updates for v2 format
- [ ] Consumer updates to parse v2 format
- [ ] Backward compatibility with v0 format

### RecordBatch Structure
```rust
pub struct RecordBatch {
    pub base_offset: i64,
    pub batch_length: i32,
    pub partition_leader_epoch: i32,
    pub magic: i8,              // 2 for v2
    pub crc: u32,
    pub attributes: i16,
    pub last_offset_delta: i32,
    pub first_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub records: Vec<Record>,
}

pub struct Record {
    pub length: i32,            // varint encoded
    pub attributes: i8,
    pub timestamp_delta: i64,   // varlong encoded
    pub offset_delta: i32,      // varint encoded
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<Header>,
}
```

### Key Changes
- Producer sends RecordBatch for API v3+
- Consumer detects magic byte (0, 1, or 2) and parses accordingly
- Headers support in public API (additive)
- More efficient wire protocol

### Files to Create/Modify
- `src/protocol/record_batch.rs` (NEW)
- `src/protocol/produce.rs` (update)
- `src/protocol/fetch.rs` (update)
- `src/producer.rs` (update)
- `src/consumer/mod.rs` (update)

### Testing
- Unit tests for RecordBatch encoding/decoding
- Integration tests with Kafka 3.x
- Backward compatibility tests with v0 format

---

## Phase 4: Additional Compression Formats

**Duration:** 2 months  
**Status:** 🔜 Pending  
**Dependencies:** None (can be done in parallel with Phase 3)

### Objectives
Add LZ4 and Zstandard compression support.

### Deliverables
- [ ] LZ4 compression/decompression
- [ ] Zstandard compression/decompression
- [ ] Feature flags for optional compilation
- [ ] Update Compression enum
- [ ] Producer/Consumer integration

### Implementation

#### LZ4 Support
```rust
// src/compression/lz4.rs
pub fn compress(data: &[u8]) -> Result<Vec<u8>>
pub fn uncompress(data: &[u8]) -> Result<Vec<u8>>
```

#### Zstandard Support
```rust
// src/compression/zstd.rs
pub fn compress(data: &[u8]) -> Result<Vec<u8>>
pub fn uncompress(data: &[u8]) -> Result<Vec<u8>>
```

#### Updated Compression Enum
```rust
pub enum Compression {
    NONE = 0,
    GZIP = 1,
    SNAPPY = 2,
    LZ4 = 3,      // NEW
    ZSTD = 4,     // NEW
}
```

### Files to Create/Modify
- `src/compression/lz4.rs` (NEW)
- `src/compression/zstd.rs` (NEW)
- `src/compression/mod.rs` (update)
- `src/protocol/produce.rs` (update)
- `src/protocol/fetch.rs` (update)
- `Cargo.toml` (add dependencies)

### Dependencies to Add
```toml
[dependencies]
lz4 = { version = "1.24", optional = true }
zstd = { version = "0.13", optional = true }

[features]
default = ["snappy", "gzip", "lz4", "zstd", "security"]
lz4 = ["dep:lz4"]
zstd = ["dep:zstd"]
```

---

## Phase 5: Idempotent Producer

**Duration:** 2-3 months  
**Status:** 🔜 Pending  
**Dependencies:** Phase 1 (negotiation), Phase 3 (RecordBatch)

### Objectives
Implement idempotent producer with exactly-once semantics.

### Deliverables
- [ ] InitProducerId Request/Response (API Key 22)
- [ ] Producer ID (PID) management
- [ ] Sequence number tracking per topic-partition
- [ ] Producer configuration for idempotence
- [ ] Error handling for duplicate/out-of-order sequences

### Architecture

#### Idempotence State
```rust
// src/producer/idempotence.rs
pub struct IdempotenceState {
    pub producer_id: i64,
    pub producer_epoch: i16,
    // topic -> partition -> sequence_number
    pub sequence_numbers: HashMap<String, HashMap<i32, i32>>,
}
```

#### InitProducerId Protocol
```rust
// src/protocol/init_producer_id.rs
pub struct InitProducerIdRequest<'a> {
    pub header: HeaderRequest<'a>,
    pub transactional_id: Option<&'a str>,
    pub transaction_timeout_ms: i32,
}

pub struct InitProducerIdResponse {
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub producer_id: i64,
    pub producer_epoch: i16,
}
```

#### Updated Produce Request
```rust
// Produce API v3+ includes:
pub struct ProduceRequest {
    // ... existing fields
    pub transactional_id: Option<String>,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,  // per partition
}
```

### Producer API Changes
```rust
impl Builder {
    // NEW: Enable idempotent producer
    pub fn with_idempotence(self, enable: bool) -> Self
}
```

### Error Handling
- `OutOfOrderSequenceNumber` → retry with correct sequence
- `DuplicateSequenceNumber` → treat as success
- `UnknownProducerId` → re-initialize producer ID

### Files to Create/Modify
- `src/protocol/init_producer_id.rs` (NEW)
- `src/producer/idempotence.rs` (NEW)
- `src/producer.rs` (update)
- `src/producer/builder.rs` (update)
- `src/protocol/produce.rs` (update for v3+)

### Testing
- Unit tests for sequence number tracking
- Integration tests with Kafka 3.x
- Test duplicate detection
- Test sequence recovery

---

## Phase 6: Update Protocol Request Versions

**Duration:** 2-3 months  
**Status:** 🔜 Pending  
**Dependencies:** Phase 1, 3, 5

### Objectives
Update all protocol APIs to use latest versions with dynamic negotiation.

### Deliverables
- [ ] Update Produce API to v7/v8
- [ ] Update Fetch API to v11/v12
- [ ] Update Metadata API to v9+
- [ ] Update OffsetFetch API to v5+
- [ ] Update OffsetCommit API to v7+
- [ ] Use negotiated versions from Phase 1

### API Version Updates

#### Produce API (v3 → v8)
```rust
// Use negotiated version
let api_version = client.get_api_version(host, API_KEY_PRODUCE);
let header = HeaderRequest::with_version(
    API_KEY_PRODUCE,
    api_version,  // Dynamic: 0-8 depending on broker
    correlation_id,
    client_id
);
```

**Version Features:**
- v3: Transactional/idempotent support, RecordBatch
- v7: Delivery timeout
- v8: Record errors

#### Fetch API (v0 → v11)
**Version Features:**
- v3: Max bytes limit
- v4: Isolation level for transactions
- v11: Rack awareness

#### Metadata API (v0 → v9)
**Version Features:**
- v1: Broker rack info
- v5: Offline replicas
- v8: Cluster ID, controller ID
- v9: Topic authorization flag

#### OffsetFetch API (v0 → v5)
**Version Features:**
- v3: Max timestamp field
- v5: Batch multiple groups

#### OffsetCommit API (v0 → v7)
**Version Features:**
- v2: Retention time
- v7: Group instance ID (static membership)

### Implementation Strategy
1. Add version-specific encoding/decoding
2. Use `get_api_version()` to select version
3. Fall back to v0 for old brokers
4. Maintain backward compatibility

### Files to Modify
- `src/protocol/produce.rs`
- `src/protocol/fetch.rs`
- `src/protocol/metadata.rs`
- `src/protocol/consumer.rs`
- `src/client/mod.rs`

---

## Phase 7: Testing and Validation

**Duration:** 1-2 months  
**Status:** 🔜 Pending  
**Dependencies:** All previous phases

### Objectives
Comprehensive testing against multiple Kafka versions.

### Deliverables
- [ ] Integration tests against Kafka 3.x
- [ ] Backward compatibility tests (Kafka 0.8.2 - 3.x)
- [ ] Performance benchmarks
- [ ] Update documentation
- [ ] Update examples
- [ ] Migration guide

### Test Matrix
```
Kafka Versions: 0.8.2, 0.9.0, 2.0.0, 3.0.0, 3.4.0, 3.8.0
Compression: NONE, GZIP, SNAPPY, LZ4, ZSTD
Security: Plain, SSL
Message Format: v0, v2
Producer: Standard, Idempotent
```

### Performance Testing
- Benchmark message format v0 vs v2
- Benchmark compression algorithms
- Compare with previous library version
- Measure negotiation overhead

### Documentation Updates
- Update README.md
- Update CHANGELOG.md
- Update API documentation
- Add migration guide
- Add examples for new features

### Files to Create/Modify
- `tests/integration/**` (update)
- `benches/**` (NEW)
- `README.md` (update)
- `CHANGELOG.md` (update)
- `MIGRATION_GUIDE.md` (NEW)
- `examples/**` (update)

---

## Implementation Timeline

### Detailed Schedule

| Phase | Duration | Start | End | Status |
|-------|----------|-------|-----|--------|
| Phase 1: Protocol Negotiation | 2-3 months | Month 1 | Month 3 | ✅ Complete |
| Phase 2: Error Codes | 1 month | Month 3 | Month 4 | 🔜 Next |
| Phase 3: Message Format v2 | 3-4 months | Month 4 | Month 7 | 🔜 Pending |
| Phase 4: Compression | 2 months | Month 5 | Month 7 | 🔜 Pending |
| Phase 5: Idempotent Producer | 2-3 months | Month 7 | Month 10 | 🔜 Pending |
| Phase 6: Protocol Updates | 2-3 months | Month 10 | Month 12 | 🔜 Pending |
| Phase 7: Testing | 1-2 months | Month 12 | Month 14 | 🔜 Pending |

**Total Duration:** 13-18 months (target: 12-15 months with parallel work)

### Parallel Work Opportunities
- Phase 2 can start immediately (independent)
- Phase 4 can overlap with Phase 3 (independent)
- Testing throughout all phases

---

## Backward Compatibility Strategy

### Core Principles
1. **No Breaking Changes:** All existing public APIs remain functional
2. **Opt-In Features:** New features enabled via configuration
3. **Graceful Fallback:** Automatic fallback to old versions for old brokers
4. **Feature Detection:** Automatic detection of broker capabilities

### Compatibility Matrix

| Feature | Old Kafka (< 0.10) | Mid Kafka (0.10-2.x) | New Kafka (3.x) |
|---------|-------------------|---------------------|-----------------|
| Protocol Negotiation | Fallback to v0 | ✅ Supported | ✅ Supported |
| Message Format v0 | ✅ Supported | ✅ Supported | ✅ Supported |
| Message Format v2 | ❌ Not Available | ✅ Supported | ✅ Supported |
| Idempotent Producer | ❌ Not Available | ✅ Supported | ✅ Supported |
| LZ4/ZSTD Compression | ❌ Not Available | Partial | ✅ Supported |

### API Stability Guarantees
- Existing method signatures unchanged
- New methods added via builder pattern
- Configuration changes opt-in only
- Default behavior preserved

---

## Success Metrics

### Functional Goals
- ✅ Support Kafka 0.8.2 through 3.x
- ✅ All existing tests pass
- ✅ No breaking changes to public API
- ✅ New features opt-in only

### Performance Goals
- ≤ 5% overhead for protocol negotiation (one-time cost)
- ≥ 20% improvement with message format v2
- ≥ 30% improvement with LZ4/ZSTD compression

### Quality Goals
- 100% test coverage for new code
- Zero critical bugs at release
- Comprehensive documentation
- Working examples for all features

---

## Risk Assessment

### Technical Risks

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Breaking backward compatibility | Low | High | Extensive testing, feature flags |
| Performance regression | Medium | Medium | Benchmarking at each phase |
| Protocol incompatibility | Low | High | Test against multiple Kafka versions |
| Complexity explosion | Medium | Medium | Incremental development, code reviews |

### Schedule Risks

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Phases take longer than estimated | Medium | Medium | Parallel work where possible |
| Dependency on upstream changes | Low | Low | Work on independent phases first |
| Testing takes longer | Low | Medium | Start integration tests early |

---

## Resources and Dependencies

### Development Resources
- 1 primary developer (you)
- Community contributors (optional)
- Code reviewers
- CI/CD infrastructure

### External Dependencies
- Kafka 3.x broker for testing
- Docker for integration tests
- Rust toolchain (stable channel)
- Cargo dependencies (lz4, zstd, etc.)

### Testing Infrastructure
- Docker Compose for Kafka cluster
- Multiple Kafka versions (0.8.2 - 3.8.0)
- CI pipeline for automated testing

---

## Communication Plan

### Documentation
- **CHANGELOG.md:** Document all changes
- **Migration Guide:** Help users upgrade
- **API Documentation:** Keep docs.rs updated
- **Blog Posts:** Announce major milestones

### Community Engagement
- GitHub issues for bug reports
- Pull requests for contributions
- Release notes for each phase
- Examples and tutorials

---

## Post-Implementation

### Future Phases (Phase 8+)

#### Phase 8: Transactional Producer (Optional)
- Full transaction support
- Multi-partition atomic writes
- Transaction coordinator integration

#### Phase 9: Admin API (Optional)
- Topic management (create/delete/configure)
- Consumer group management
- Cluster operations

#### Phase 10: Advanced Features (Optional)
- Schema registry integration
- Exactly-once semantics (EOS)
- KRaft support (ZooKeeper-less)

### Maintenance
- Regular dependency updates
- Security patches
- Bug fixes
- Performance optimizations

---

## Conclusion

This modernization plan provides a clear roadmap to bring kafka-rust up to date with Kafka 3.x while maintaining stability and backward compatibility. The phased approach allows for:

1. **Incremental Progress:** Each phase delivers value independently
2. **Low Risk:** Backward compatibility maintained throughout
3. **Flexibility:** Phases can be adjusted based on feedback
4. **Quality:** Extensive testing at each stage

**Phase 1 Status:** ✅ **COMPLETE** - Ready to proceed to Phase 2!

---

## Appendix A: API Version Reference

### Complete API Version Matrix

| API | Key | Current | Target | Key Features |
|-----|-----|---------|--------|--------------|
| Produce | 0 | 0 | 8 | Idempotency, transactions, RecordBatch |
| Fetch | 1 | 0 | 12 | Rack awareness, isolation level |
| ListOffsets | 2 | 0 | 7 | Timestamp-based offset lookup |
| Metadata | 3 | 0 | 12 | Cluster info, offline replicas |
| LeaderAndIsr | 4 | - | - | Internal (not implemented) |
| StopReplica | 5 | - | - | Internal (not implemented) |
| UpdateMetadata | 6 | - | - | Internal (not implemented) |
| ControlledShutdown | 7 | - | - | Internal (not implemented) |
| OffsetCommit | 8 | 0/1 | 8 | Retention, static membership |
| OffsetFetch | 9 | 0/1 | 8 | Batch groups |
| FindCoordinator | 10 | 0 | 4 | Group/transaction coordinator |
| JoinGroup | 11 | - | 9 | Consumer group protocol |
| Heartbeat | 12 | - | 4 | Keep-alive |
| LeaveGroup | 13 | - | 5 | Graceful exit |
| SyncGroup | 14 | - | 5 | Assignment sync |
| DescribeGroups | 15 | - | 5 | Group inspection |
| ListGroups | 16 | - | 4 | List all groups |
| SaslHandshake | 17 | - | 1 | Authentication |
| ApiVersions | 18 | **NEW** | 3 | Version negotiation |
| CreateTopics | 19 | - | 7 | Admin: create topics |
| DeleteTopics | 20 | - | 6 | Admin: delete topics |
| DeleteRecords | 21 | - | 2 | Delete old records |
| InitProducerId | 22 | - | 4 | Idempotent producer setup |
| ... | ... | ... | ... | ... |

---

## Appendix B: Message Format Evolution

### Message Format v0 (Kafka 0.8/0.9)
```
MessageSet:
  Offset: int64
  MessageSize: int32
  Message:
    CRC: int32
    MagicByte: int8 (0)
    Attributes: int8
    Key: bytes
    Value: bytes
```

### Message Format v1 (Kafka 0.10)
```
Same as v0 but:
  MagicByte: int8 (1)
  + Timestamp: int64
```

### Message Format v2 (Kafka 0.11+) - RecordBatch
```
RecordBatch:
  BaseOffset: int64
  Length: int32
  PartitionLeaderEpoch: int32
  Magic: int8 (2)
  CRC: uint32
  Attributes: int16
  LastOffsetDelta: int32
  FirstTimestamp: int64
  MaxTimestamp: int64
  ProducerId: int64
  ProducerEpoch: int16
  BaseSequence: int32
  Records: [Record]

Record:
  Length: varint
  Attributes: int8
  TimestampDelta: varlong
  OffsetDelta: varint
  Key: bytes
  Value: bytes
  Headers: [Header]

Header:
  KeyLength: varint
  Key: string
  ValueLength: varint
  Value: bytes
```

**Benefits of v2:**
- Smaller message overhead
- Efficient batch compression
- Headers support
- Producer ID for idempotency
- Better timestamp handling

---

**Document Version:** 1.0  
**Last Updated:** January 20, 2026  
**Status:** Phase 1 Complete, Phase 2 Next
