Cleanup:
    - [x] Update dependencies
    - [ ] Swap error-chain for failure crate
    - [ ] Comb through crate for poor design patterns
    - [ ] consider replacing openssl lib with rustls or update to openssl 0.10
    - [ ] use the Default trait
    - [ ] Investigate Error::clone(), why was non-exhaustive
    - [ ] Revisit: clippy::if_same_then_else