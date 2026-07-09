# moomoo-adaptor

A Rust client that speaks the moomoo OpenD wire protocol directly (no C++ SDK) to stream live trade-by-trade (buy/sell) data for a security and print it to stdout. This is the first working slice of a larger pipeline intended to eventually write into QuestDB.

## What it does right now

Connects to a locally running OpenD instance, performs the `InitConnect` handshake, subscribes to the **Ticker** (trade-by-trade) feed for one hardcoded security, then loops forever printing each trade as it arrives:

```
connected: conn_id=123456789 keep_alive_interval=10s
subscribed to AAPL for Ticker pushes
AAPL BUY seq=48213 price=213.5 volume=100
AAPL SELL seq=48214 price=213.48 volume=200
```

## Prerequisites

1. **A moomoo account** with market data permissions for whatever you want to watch.
2. **OpenD running and logged in.** OpenD is moomoo's local gateway process — this Rust program talks to it over TCP on `127.0.0.1:11111`, not to moomoo's servers directly. Start OpenD (GUI or command-line build) and confirm it shows as logged in before running this.
   - Login credentials live in OpenD's config (`OpenD.xml` if you're using the command-line build). Use `login_pwd_md5` (an MD5 hash of your password), not the plaintext `login_pwd` field — don't check plaintext credentials into version control.
3. **Rust** (edition 2024 — a recent stable toolchain; `cargo --version` should be 1.85+).
4. **`protoc`** (the Protocol Buffers compiler) installed and on your `PATH` — `prost-build` shells out to it at compile time to turn the vendored `.proto` files into Rust code. Verify with `protoc --version`.

## Building

```
cargo build
```

This runs `build.rs`, which globs every `.proto` file in `proto/` and compiles them via `prost-build` into `OUT_DIR`. `src/mods.rs` then `include!`s the specific generated files it needs — if you add a new message type, you'll need both a matching `.proto` file already vendored in `proto/` (there are 164, covering the whole OpenD API surface) and a new `pub mod your_message { include!(...) }` block in `mods.rs`.

## Configuring what to watch

The security and market are currently hardcoded in `src/main.rs`:

```rust
engine::stream_ticker(11, "AAPL").await
```

The first argument is the `QotMarket` code (`11` = US equities, `1` = HK equities — see `Qot_Common.proto`'s `QotMarket` enum for the full list). The second is the ticker symbol as moomoo expects it. Change these and rebuild to watch something else — there's no config file or CLI argument parsing yet.

## Running

With OpenD already running and logged in:

```
cargo run
```

If everything's wired correctly, you'll see the `connected:` and `subscribed to:` lines within a second or two, followed by a `BUY`/`SELL`/`NEUTRAL` line per trade as they happen. Outside market hours, expect silence after the subscribe confirmation — OpenD only pushes on real activity (you may get at most one snapshot push, then nothing until the market opens).

## Troubleshooting

- **Connection refused** — OpenD isn't running, or isn't listening on port `11111`. Check its config's `<api_port>`.
- **`InitConnect failed: ...`** — printed `ret_msg` will say why; most commonly OpenD isn't logged into moomoo yet.
- **Garbage-looking numbers immediately after connecting** (huge `body_len`, nonsensical `proto_id` inside the panic/error) — this points at a wire-format byte-order mismatch. The framing code in `src/frame.rs` currently assumes little-endian integers, which is an educated guess, not a confirmed fact from moomoo's docs; if this happens, flip the `to_le_bytes`/`from_le_bytes` calls in `frame.rs` to their `_be_` equivalents and retry.
- **`Sub failed: ...`** — check the printed `ret_msg`; a common cause is exceeding your account's subscription quota (unsubscribe from things in the moomoo app, or wait for quota to free up).
- **Nothing prints after "subscribed to"** — either the market's closed, or the symbol/market pair doesn't match what moomoo expects for that ticker.

## Known limitations (this is an early slice, not the full pipeline)

- **No heartbeat.** OpenD expects a periodic `KeepAlive` message; this program doesn't send one yet, so expect the connection to eventually be dropped on long runs.
- **No reconnect logic.** A dropped connection currently just ends the program (the `?` in the read loop propagates the I/O error out of `main`).
- **Sequential handshake, not a dispatcher.** The `InitConnect` and `Sub` steps assume the very next frame off the socket is their reply. If OpenD sends an unrelated push in between (possible since `InitConnect` requests notification delivery), it'd be misread. Only the main loop after subscribing has real proto-ID-based dispatch.
- **Hardcoded serial numbers** (`1`, `2`) — fine for exactly two sequential requests, not yet a real counter.
- **stdout only** — nothing is persisted anywhere yet; the QuestDB sink hasn't been built.
- **One security, one data type** — only Ticker (trade prints) for a single hardcoded symbol. `BasicQot`/`OrderBook`/`KL`/`RT`/`Broker` push types are already vendored and partially wired (`qot_update_basic_qot` module exists) but not yet consumed anywhere.
