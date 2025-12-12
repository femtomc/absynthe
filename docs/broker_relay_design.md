# Broker & Relay Design (Syndicate Protocol)

This document sketches how to add a network-facing Syndicate broker/relay to Absynthe, mirroring the shape of `syndicate-rs` while fitting OTP/Elixir. It is meant to guide the milestone issues and give reviewers a single source for scope and non-goals.

## Goals
- Enable remote peers to exchange asserts/retracts/messages/syncs with a local dataspace via the Syndicate protocol.
- Support both TCP and Unix domain sockets so local clients can use low-latency sockets and remote nodes can traverse networks.
- Preserve Syndicate semantics: handle/facet lifetimes, assertion reference-counting, capability attenuation on import/export, and fate-sharing.
- Keep operational ergonomics close to `syndicate-rs`: config-driven services, logging/tracing, and example workloads (ping/pong, presence).

## Non-goals (initially)
- Persistence or durable message storage.
- Horizontal sharding of the dataspace; we focus on a single broker node.
- Complex HTTP integration beyond parity with the Rust server’s minimal router.

## Architecture Overview
- **Broker OTP app**: top-level supervisor hosting:
  - A broker actor (Absynthe actor) with one or more dataspaces (e.g., `config`, `log`, `default`).
  - Listener supervisors for TCP and Unix sockets; each accepted socket spawns a relay worker.
  - Optional services (config watcher, HTTP router, debt reporter) modeled after `syndicate-server`.
- **Relay worker (per connection)**:
  - Speaks the Syndicate wire protocol (see `syndicate-rs` `syndicate/src/relay.rs`).
  - Maintains membranes mapping remote object IDs ↔ local `Ref`s and refcounts.
  - Forwards turn events to/from the local actor: assert/retract/message/sync.
  - Encodes/decodes Preserves packets; supports binary and text framing.
  - Applies attenuation on re-import so capabilities cannot amplify authority.
- **Transports**:
  - TCP listener(s) bound to configured host/port(s).
  - Unix listener(s) bound to configured filesystem paths (or abstract namespace on Linux).
  - Common accept loop + relay spawn; configurable backlog and idle timeouts.
- **Capability auth (Gatekeeper/SturdyRef)**:
  - Validate incoming sturdy refs using `Absynthe.Core.SturdyRef`.
  - Gatekeeper entity issues/attenuates sturdy refs and resolves them to live `Ref`s for the relay.
  - Optional Noise handshake (parity with `syndicate-rs` resolution/noise services) as a follow-on.
- **Flow control**:
  - Integrate with credit-based flow control (absynthe-fa7) so relays charge debt to origin accounts and can pause socket reads when debt is high.
  - Debt reporter service to surface backpressure metrics.
- **Observability**:
  - Structured logging (with timestamps akin to `tai64n` option), per-connection IDs, and dataspace-backed log assertions.
  - Telemetry events for relay lifecycle, bytes in/out, debt, and assertion counts.

## Protocol Handling (relay)
- **Framing**: read/write `protocol::Packet` equivalents over the socket. Support binary packed Preserves; optionally expose text mode for debugging.
- **Turn event mapping**:
  - `Assert`: map remote handle → local handle, insert into dataspace, remember handle→assertion for retraction.
  - `Retract`: retract by handle; drop membrane refs when refcount hits zero.
  - `Message`: deliver to target ref.
  - `Sync`: respond with `Synced` after local delivery completes.
- **Membranes/refs**:
  - Exported objects get local oids and refcounts; imported objects get remote oids; keep attenuation records to prevent amplification on re-import.
  - Garbage-collect wire symbols when refcounts hit zero.
- **Error paths**: on framing/protocol error, close relay and retract outstanding assertions attributed to that relay’s handles.

## Configuration & Operations
- CLI and/or config file (Preserves `.pr` similar to `syndicate-server`) to declare:
  - TCP listeners (`host`, `port`, gatekeeper).
  - Unix listeners (`path`, gatekeeper).
  - Gatekeeper keys/attenuation policy.
  - Logging options (timestamps, color).
  - Debt reporter/metrics settings.
- Defaults: no listeners by default; explicit `--port`/`--socket` to open endpoints.
- Hot-reload config watcher (optional parity with Rust).

## Testing & Examples
- Cross-node ping/pong example: one node runs broker + dataspace, another connects and exercises turn events.
- Presence example: client asserts/retracts `Online` records and observers on the server see notifications.
- Property/integration tests: round-trip handles/caps across relay, attenuation enforcement, debt throttling under load.

## Milestones (proposed issues)
1. **Relay core**: implement relay worker (membranes, turn-event forwarding, Preserves framing) and in-process broker actor wiring. Ship a Unix-socket-only happy path with a ping/pong integration test.
2. **Transports & config**: add TCP and Unix listeners, CLI/config wiring, supervised accept loops, idle timeouts, and service assertions in the broker dataspace.
3. **Gatekeeper & sturdy refs**: validate/mint sturdy refs, resolve to live refs for relay imports, and optionally stage Noise handshake. Include a cross-node auth example.
4. **Observability & examples**: telemetry/logging, debt reporter hooks, and runnable examples (cross-node ping/pong, presence) plus docs.

