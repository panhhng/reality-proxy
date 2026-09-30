# Alternate-Reality Network

`reality` is an application-level TCP proxy for testing how a client behaves
when the network is slow or unreliable. Point an application at the local
listener, and the proxy forwards its TCP stream to the real server while
applying configurable conditions.

## Quick start

```sh
cargo run -- run \
  --listen 127.0.0.1:9000 \
  --target 127.0.0.1:8080 \
  --latency 300ms \
  --bandwidth 2mbit \
  --drop-rate 5% \
  --seed 42
```

Configure the application to connect to `127.0.0.1:9000` instead of the server
at `127.0.0.1:8080`. `--listen` defaults to `127.0.0.1:9000`. Add
`--disconnect-after 30s` to close each accepted session after 30 seconds.

## Simulated conditions

- `--latency 300ms` adds a one-way delay before the first bytes in each
  direction are forwarded. Both directions are delayed, so the added
  request/response round-trip time is approximately twice this value.
- `--bandwidth 2mbit` caps each direction independently. Rates support bit
  suffixes such as `mbit`/`Mbps` and byte suffixes such as `KB/s`/`MB/s`.
- `--drop-rate 5%` rejects a proportion of new TCP connections. `--seed`
  makes those accept/reject decisions repeatable for the same connection order.
- `--disconnect-after 30s` closes each established proxied connection after
  the requested duration.

This version operates on TCP byte streams, not IP packets. Connection rejection
is not packet loss, and it does not implement packet duplication, reordering,
UDP, DNS faults, or scenario files yet. Bandwidth pacing is applied to chunks
after they are forwarded, so it is intended for application testing rather
than precise transport benchmarking.

## Build

```sh
cargo build --release
```