# Fuzzing

One libFuzzer target, `session`, drives fuzzer-chosen bytes through the
server's real connection handler: version detection, the MQTT 3.1.1 and
5.0 decoders, the CONNECT decision, `dispatch`, and the reply encoder.
A panic on that path, or one oversized allocation, is a remote denial of
service, so the target fails on either. Builds use cargo-fuzz's defaults:
AddressSanitizer, debug assertions and overflow checks.

Last verified: cargo-fuzz 0.13.2 with rustc 1.100.0-nightly (17fd5b8a3 2026-08-28)

## Input format

Byte 0 caps how many bytes each socket read returns (`0` = no cap); the
remaining bytes are what the client sends. The cap reaches the decoder
states that only partial socket reads produce.

## Flags

- `-max_len=131073`: one prefix byte plus one packet at the default
  inbound packet-size bound (131 072 bytes), so the fuzzer can build
  packets up to the bound.
- `-malloc_limit_mb=16`: any single allocation of 16 MiB or more is a
  crash. Under the 131 072-byte inbound bound the largest legitimate
  decode allocation is a topic-filter vector of about 2.5 MiB: a v3
  SUBSCRIBE of 43 689 zero-length filters (3 wire bytes each) grows
  `Vec<(ByteString, QoS)>` to 65 536 entries of 40 bytes. 16 MiB clears
  that with room to spare, while a declared Remaining Length that
  bypassed the bound reserves up to 256 MiB, so the oracle still catches
  the F2.4 reservation denial of service.

## Commands

Run from the repository root. cargo-fuzz needs a nightly toolchain.

```sh
cargo install cargo-fuzz --version 0.13.2 --locked
cargo +nightly fuzz build session
mkdir -p fuzz/corpus/session
# Fuzz until stopped; new inputs go to fuzz/corpus/session (gitignored).
cargo +nightly fuzz run session fuzz/corpus/session tests/fuzz_seeds/session -- -max_len=131073 -malloc_limit_mb=16
# The one-hour check (epic uring-ingest, F4.2 Done-when).
cargo +nightly fuzz run session fuzz/corpus/session tests/fuzz_seeds/session -- -max_total_time=3600 -max_len=131073 -malloc_limit_mb=16
# Replay the committed seeds with the default toolchain; CI runs it on stable.
cargo test --lib fuzz::
```

The one-hour check passes when cargo-fuzz exits 0, libFuzzer prints
`Done <N> runs in <S> second(s)` with S of at least 3600, and no new file
appears under `fuzz/artifacts/session/`.

If a later nightly fails to build the target, install the toolchain named
on the "Last verified" line and run with `+<that toolchain>`.

## Seeds and crashes

- `tests/fuzz_seeds/session/` holds hand-written seeds. Each one has a
  row in `SEED_TRACES` (`src/broker/fuzz.rs`) recording the outcome,
  bytes written and publishes delivered, and `cargo test` replays them.
- `fuzz/corpus/` (generated inputs) and `fuzz/artifacts/` (crash
  inputs) are gitignored.
- When a crash is fixed, its input moves into `tests/fuzz_seeds/session/`
  with a `SEED_TRACES` row, so the fix stays covered on stable CI.