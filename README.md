# Quorium

A threshold-signature (MPC) node for blockchain wallets, written in Rust.

A private key is never assembled in one place. Instead `n` nodes each hold a share, and any `t` of
them can jointly produce a signature without ever reconstructing the key — so compromising fewer
than `t` nodes yields nothing usable.

## Architecture

### Coordination state lives in NATS

An MPC cluster needs a small amount of state that every node agrees on: who the members are, which
of them are up right now, and how each wallet's key was generated. The conventional answer is to
run a dedicated coordination service such as Consul or etcd next to the message bus. Quorium does
not. All of it goes into NATS JetStream KV — the same system that already carries the messages.

| State               | Contents                                    | Storage                            |
| ------------------- | ------------------------------------------- | ---------------------------------- |
| Peer roster         | node name to node ID                        | KV bucket, written by an operator  |
| Presence            | which nodes are currently up                | KV bucket, per-key TTL and a watch |
| Wallet key metadata | participants, threshold, version per wallet | KV bucket, persistent              |

Reasons to fold this into the message bus rather than run a second system:

- **One less distributed system to operate and secure.** A coordination service is a consensus
  cluster in its own right, with its own ports, ACLs and TLS to get right. In practice its ACLs
  are often left off outside production, which would leave the peer roster and wallet metadata
  writable by anything that can reach the network.
- **Watch instead of polling.** Presence changes arrive as events. Polling a list endpoint on a
  timer trades latency against load and never wins at both.
- **TTL instead of manual eviction.** A node renews its own presence key; if the process dies the
  key expires by itself. There is no separate bookkeeping to resign on shutdown or to have peers
  agree on when to evict an unresponsive member.
- **One source of truth for liveness.** Nodes establish that a peer is alive by exchanging messages
  with it. Recording that conclusion in a different system means two views that can disagree, and
  disagreement here is what forces debouncing and retry heuristics.

The trade-off is that NATS becomes the only piece of infrastructure holding state, so it has to be
deployed as a real cluster with mTLS and per-node credentials rather than as a lightweight message
pipe.

None of this state is secret. Key shares are encrypted and held locally by each node and never go
into KV, so the integrity of the system rests on the threshold cryptography and on per-message
signatures — not on the store. An attacker who can write to KV can disrupt a cluster, but cannot
forge a signature.

### Design principles

- **Errors are values.** No fallible path aborts the process on its own; operations return typed
  errors and the binary decides what is fatal.
- **Subscriptions are streams, not callbacks.** A NATS subscriber already is a `Stream`, and a
  stream composes with `tokio::select!` for shutdown without boxing a closure into an `Arc`.
- **Identities are parsed, not stringly typed.** Peer IDs are validated at the boundary rather than
  passed around as bare strings.

## Getting started

### Prerequisites

- Rust 1.85 or newer (the crate uses edition 2024)
- A NATS server **with JetStream enabled** — `nodes-backup` creates a KV bucket, which JetStream
  provides

```bash
docker run --rm -p 4222:4222 nats:latest -js
```

The `-js` flag is not optional; without it KV bucket creation fails.

### Bootstrap the peer list

```bash
cargo run --bin nodes-backup
```

On an empty bucket this generates three peer IDs, writes them to the `mpc-peers` KV bucket, and
mirrors them to `peers.json` (which is gitignored). On a populated bucket it just prints what is
there. Note that this binary currently hardcodes `nats://127.0.0.1:4222` and ignores `NATS_URL`.

### Run a node

Because the crate ships two binaries, `cargo run` needs to be told which one:

```bash
cargo run --bin quorium
```

It connects to NATS, subscribes to `mpc:generate`, and logs whatever arrives until interrupted. To
see it do something, publish to that subject from another terminal:

```bash
nats pub 'mpc:generate' '{"wallet_id":"demo"}'
```

## Development

```bash
cargo build --all-targets
cargo test
cargo clippy --all-targets
cargo fmt --check
```

Doc comments follow RFC 1574 conventions, and comments are reserved for constraints the code
cannot express on its own rather than restating it.
