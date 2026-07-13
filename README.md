# moomoo-adaptor

A Rust client that speaks the moomoo OpenD wire protocol directly (no C++ SDK) to stream live trade-by-trade (buy/sell) data for one or more securities and persist it into QuestDB, with automatic reconnect/backoff and a heartbeat to keep the OpenD session alive.

## What it does right now

Connects to a locally running OpenD instance, performs the `InitConnect` handshake, and for each configured security: checks its market state, subscribes to the **Ticker** (trade-by-trade) feed, and streams pushes indefinitely — printing each trade to stdout and writing it into QuestDB via the Influx Line Protocol. Every security runs as its own independent, concurrently-scheduled task (`tokio::spawn`), each with its own OpenD connection, heartbeat, and reconnect/backoff loop, so one ticker's connection trouble doesn't affect the others. A dropped connection is retried with exponential backoff up to a configurable attempt limit; a connection that's stayed healthy for a while resets the backoff on its next failure.

```
connected: conn_id=123456789 keep_alive_interval=10s
market with id:11 and name:US has status2
proceeding to subscribe to ticker
subscribed to AAPL for Ticker pushes
AAPL BUY seq=48213 price=213.5 volume=100
AAPL SELL seq=48214 price=213.48 volume=200
```

## Prerequisites

1. **A moomoo account** with market data permissions for whatever you want to watch.
2. **OpenD running and logged in** — see [Installing and running OpenD](#installing-and-running-opend) below.
3. **QuestDB running** — see [Setting up QuestDB](#setting-up-questdb) below. Every ticker's ticks are persisted here.
4. **Rust** (edition 2024 — a recent stable toolchain; `cargo --version` should be 1.85+).
5. **`protoc`** (the Protocol Buffers compiler) installed and on your `PATH` — `prost-build` shells out to it at compile time to turn the vendored `.proto` files into Rust code. Verify with `protoc --version`.

## Installing and running OpenD

OpenD is moomoo's own local gateway process — this Rust program talks to *it* over TCP on `127.0.0.1:11111` by default, not to moomoo's servers directly, so nothing here works until OpenD is running and logged in. Download it from moomoo's official OpenAPI site; the download includes two variants (GUI app and command-line build), either works.

This project only has the **Ubuntu build** downloaded (see the `OpenD/` folder alongside this repo) — there's no Windows-native OpenD here. That means running it means either grabbing the Windows build instead if you'd rather run it natively, or running the Ubuntu build inside WSL2, which is the setup this project has actually been tested against. For the WSL2 path (command-line build):

1. Copy the `moomoo_OpenD_..._Ubuntu18.04/` folder into your WSL2 filesystem (e.g. `~/opend/`) — don't run the Linux binary directly off a Windows-mounted path.
2. Edit `OpenD.xml`: set `<login_account>` to your moomoo account, and `<login_pwd_md5>` to the MD5 hash of your password. **Never put a plaintext password in `<login_pwd>`, and never commit `OpenD.xml` anywhere** — it holds your account identifier and credential hash. Leave `<api_port>` at `11111` to match this project's default `OPEND_ADDR`.
3. `chmod +x OpenD`, then run it: `./OpenD`
4. Watch its log output for a successful login confirmation before starting `moomoo-adaptor` — connecting before OpenD has finished logging in will fail immediately.

WSL2 forwards `127.0.0.1` automatically between Windows and the WSL2 instance, so a service listening on `127.0.0.1:11111` inside WSL2 is reachable at the same address from Windows — no extra networking setup needed.

For the GUI variant, the equivalent is launching the `.AppImage`, logging in through its own interface, and confirming the same `api_port` in its settings panel.

## Setting up QuestDB

Any QuestDB instance reachable over its HTTP ILP endpoint (default port `9000`) works. The quickest way to get one running locally is Docker:

```
docker run -p 9000:9000 -p 9009:9009 -p 8812:8812 questdb/questdb
```

The `9000` HTTP port is what this client writes to; the QuestDB web console for browsing ingested data lives at `http://localhost:9000`. No table needs to be created up front — the client's writer creates the `ticker_ticks` table on first write.

## Building

**`proto/` is not in this repo — fetch it first.** It's gitignored (moomoo's schema files aren't ours to redistribute), so a fresh clone is missing it entirely and `cargo build` will fail with no `.proto` files to compile. Download the proto file bundle from moomoo's official OpenAPI documentation site (moomoo publishes these specifically so developers can build their own clients against the OpenD protocol — the version used here was `MMAPIProtoFiles_10.8.6808`) and place all 164 `.proto` files flat inside a `proto/` folder at the repo root, alongside `Cargo.toml`.

```
cargo build
```

This runs `build.rs`, which globs every `.proto` file in `proto/` and compiles them via `prost-build` into `OUT_DIR`. `src/mods.rs` then `include!`s the specific generated files it needs — if you add a new message type, you'll need both a matching `.proto` file already vendored in `proto/` (there are 164, covering the whole OpenD API surface) and a new `pub mod your_message { include!(...) }` block in `mods.rs`.

## Configuration

Everything is driven by environment variables, loaded via `Config::from_env()` in `src/config.rs`. For local development, drop a `.env` file in the repo root — it's loaded automatically at startup (via `dotenvy`) and is already gitignored, so it's a safe place to keep local settings.

| Variable | Default | Meaning |
|---|---|---|
| `OPEND_ADDR` | `127.0.0.1:11111` | Address of the locally running OpenD instance. |
| `MOOMOO_SECURITIES` | `91:BTC` | Comma-separated `market:code` pairs, one per ticker to watch — e.g. `91:BTC,11:AAPL`. Each entry gets its own connection and task. See `QotMarket` in `proto/Qot_Common.proto` for market codes (`1`=HK, `11`=US, `21`=CNSH, `22`=CNSZ, `31`=SG, `41`=JP, `51`=AU, `61`=MY, `71`=CA, `81`=FX, `91`=Crypto). |
| `QDB_CLIENT_CONF` | `http::addr=localhost:9000;` | QuestDB ILP connection string, in `questdb-rs`'s own config-string format. |
| `RETRY_INITIAL_BACKOFF_SECS` | `1` | Delay before the first reconnect attempt after a connection failure. |
| `RETRY_MAX_BACKOFF_SECS` | `60` | Cap on the exponentially-growing reconnect delay. |
| `RETRY_MAX_ATTEMPTS` | `5` | Consecutive failures allowed before a ticker's task gives up entirely. |
| `RETRY_HEALTHY_THRESHOLD_SECS` | `30` | A connection that stays up at least this long resets the backoff/attempt counters on its next failure, so a flaky-but-mostly-working connection doesn't exhaust its retry budget. |

A value that's set but fails to parse is a hard startup error rather than a silent fallback to the default — an override that's silently ignored is worse than one that fails loudly. An unset variable falls back to its default above.

## Running

With OpenD and QuestDB already running:

```
cargo run
```

If everything's wired correctly, you'll see a `connected:` line and a `subscribed to ... for Ticker pushes` line per configured security within a second or two, followed by interleaved `BUY`/`SELL`/`NEUTRAL` lines as trades happen across all of them, each also being written into QuestDB. Outside market hours for a given security, expect silence after its subscribe confirmation — OpenD only pushes on real activity (crypto trades around the clock; equities won't).

## Troubleshooting

- **Connection refused (OpenD)** — OpenD isn't running, or isn't listening on the port `OPEND_ADDR` points at. Check its config's `<api_port>`.
- **`init_connect failed: ...`** — printed `ret_msg` will say why; most commonly OpenD isn't logged into moomoo yet.
- **Garbage-looking numbers immediately after connecting** (huge `body_len`, nonsensical `proto_id` inside the panic/error) — this points at a wire-format byte-order mismatch. The framing code in `src/frame.rs` currently assumes little-endian integers, which is an educated guess, not a confirmed fact from moomoo's docs; if this happens, flip the `to_le_bytes`/`from_le_bytes` calls in `frame.rs` to their `_be_` equivalents and retry.
- **`Sub failed: ...`** — check the printed `ret_msg`; a common cause is exceeding your account's subscription quota (unsubscribe from things in the moomoo app, or wait for quota to free up).
- **Nothing prints after "subscribed to"** — either the market's closed, or the symbol/market pair doesn't match what moomoo expects for that ticker.
- **`Config parse error: ...` or connection errors immediately followed by a reconnect storm from a ticker that otherwise looks fine** — check `QDB_CLIENT_CONF`; a malformed QuestDB connection string currently causes ticker writes to fail in a way that tears down and retries that ticker's whole OpenD session.
- **A ticker task keeps retrying and eventually logs `gave up: ...`** — it exhausted `RETRY_MAX_ATTEMPTS` consecutive failures without a `RETRY_HEALTHY_THRESHOLD_SECS`-long healthy stretch in between. Other tickers are unaffected and keep running independently.

## Known limitations

- **Sequential handshake, not a dispatcher, during setup.** The `InitConnect`, `GetMarketState`, and `Sub` steps each assume the very next frame off the socket is their reply. If OpenD sends an unrelated push in between, it'd be misread. Only the streaming loop after subscribing has real proto-ID-based dispatch (via a dedicated reader task and channel).
- **Market state is checked but not enforced.** `GetMarketState` is queried and printed before subscribing, but its result doesn't currently gate whether the subscription proceeds.
- **QuestDB write failures are not isolated from the OpenD session.** A failed write to the shared QuestDB writer currently propagates as an error on that ticker's connection, triggering a full reconnect rather than just dropping the affected tick.
- **Log lines don't consistently identify which ticker they're about.** With multiple tickers streaming concurrently, some log lines (e.g. the initial `connected:` line, reconnect/retry messages, keep-alive replies) don't include the ticker's symbol, which can make interleaved output ambiguous.
- **One data type.** Only Ticker (trade prints) is consumed. `BasicQot`/`OrderBook`/`KL`/`RT`/`Broker` push types are already vendored and partially wired (`qot_update_basic_qot` module exists) but not yet consumed anywhere.
