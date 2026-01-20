# Phase 1: Protocol Version Negotiation - COMPLETE

## Summary

Phase 1 has been successfully implemented, providing the infrastructure for dynamic protocol version negotiation with Kafka brokers.

## Implemented Components

### 1. ApiVersions Request/Response (src/protocol/api_versions.rs)

Created a new module implementing the ApiVersions API (API Key 18):

- `ApiVersionsRequest`: Request structure for discovering supported API versions
- `ApiVersionsResponse`: Response structure containing supported version ranges
- `ApiVersion`: Structure representing min/max version for each API key
- Proper error handling with `into_result()` method

### 2. Version Storage in ClientState (src/client/state.rs)

Extended `ClientState` to store negotiated API versions:

- Added `api_versions: HashMap<String, HashMap<i16, i16>>` field
  - Maps broker_host -> api_key -> max_supported_version
- `get_api_version(host, api_key)`: Retrieves negotiated version with fallback to 0
- `set_api_versions(host, versions)`: Stores negotiated versions for a broker
- `has_api_versions(host)`: Checks if versions have been negotiated

### 3. Version Negotiation in KafkaClient (src/client/mod.rs)

Added automatic API version negotiation:

- `negotiate_api_versions(host)`: Negotiates with a broker (idempotent, cached)
  - Skips if already negotiated
  - Falls back gracefully if broker doesn't support ApiVersions (Kafka < 0.10)
  - Handles errors without breaking backward compatibility
- `get_api_version(host, api_key)`: Gets version with automatic negotiation
  - Attempts negotiation if not done yet
  - Returns version 0 as safe fallback

### 4. Protocol Infrastructure Updates (src/protocol/mod.rs)

Updated protocol module:

- Added `API_KEY_API_VERSIONS` constant (18)
- Re-exported `ApiVersion`, `ApiVersionsRequest`, `ApiVersionsResponse`
- Added `HeaderRequest::with_version()` for explicit version control
- Documented version fallback behavior

## Backward Compatibility

✅ **Fully backward compatible** - All changes are additive:

- Existing code continues to use API version 0
- New infrastructure is opt-in
- Graceful fallback for brokers that don't support ApiVersions
- No breaking changes to public API

## How Version Negotiation Works

1. **First Request to Broker**: 
   - Client attempts ApiVersions request (API version 0, universally supported)
   - If successful, stores supported versions in ClientState
   - If fails, falls back to version 0 for all APIs

2. **Subsequent Requests**:
   - `get_api_version(host, api_key)` returns cached negotiated version
   - Or version 0 if negotiation failed/not available

3. **Future API Usage** (Phase 6):
   - Individual APIs will use `get_api_version()` to select best version
   - Example: Produce API can use v7/v8 if broker supports it, otherwise v0

## Testing Considerations

The implementation includes:
- ✅ Unit tests for ApiVersions request creation
- ✅ Unit tests for ApiVersion decoding
- ✅ Proper error handling throughout
- ✅ Debug logging for troubleshooting

## Integration Points

The infrastructure is ready for Phase 6 (Protocol Version Updates):

```rust
// Future usage example in ProduceRequest:
let api_version = client.get_api_version(host, API_KEY_PRODUCE);
let header = HeaderRequest::with_version(
    API_KEY_PRODUCE,
    api_version,  // Dynamic version instead of hardcoded 0
    correlation_id,
    client_id
);
```

## Files Modified

1. `src/protocol/api_versions.rs` - **NEW** - ApiVersions protocol implementation
2. `src/protocol/mod.rs` - Added api_versions module and exports
3. `src/client/state.rs` - Added version storage and retrieval methods
4. `src/client/mod.rs` - Added negotiation methods

## Next Steps (Phase 2-7)

Phase 1 provides the foundation for:
- Phase 2: Extended error codes (independent)
- Phase 3: Message format v2 (will use negotiated versions)
- Phase 5: Idempotent producer (will use negotiated versions for InitProducerId)
- Phase 6: Update all APIs to use negotiated versions instead of hardcoded v0

## Status

✅ **COMPLETE** - All Phase 1 objectives achieved with full backward compatibility.



