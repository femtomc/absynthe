# Market Stress Test Specification

**Issue**: absynthe-4l5.1 - Spec load scenario + acceptance for stress example

This document defines the stress test scenario to intentionally flush bugs in
Absynthe's Syndicated Actor Model implementation.

## Goal

Ship a full end-to-end stress example (multi-tenant market over relay +
gatekeeper + dataspace + dataflow) that hammers Absynthe and flushes latent
bugs.

## Architecture Overview

```
                           ┌─────────────────────────┐
                           │       Test Driver       │
                           │  (metrics, chaos ctrl)  │
                           └───────────┬─────────────┘
                                       │
        ┌──────────────────────────────┼──────────────────────────────┐
        │                              │                              │
        ▼                              ▼                              ▼
┌───────────────┐            ┌───────────────┐            ┌───────────────┐
│   Client 1    │            │   Client N    │            │   Monitor     │
│ (Trader Bot)  │            │ (Trader Bot)  │            │  (Analytics)  │
└───────┬───────┘            └───────┬───────┘            └───────┬───────┘
        │ TCP/Noise                  │                            │
        │ + Sturdy Ref               │                            │
        ▼                            ▼                            ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                              Broker                                     │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐              │
│  │   Listener   │    │  Gatekeeper  │    │  Relay Pool  │              │
│  │ (TCP/Unix)   │───▶│ (Auth/Attn)  │───▶│  (per-conn)  │              │
│  └──────────────┘    └──────────────┘    └──────────────┘              │
│                                                │                        │
│                           ┌────────────────────┴────────────────────┐   │
│                           ▼                                         ▼   │
│  ┌─────────────────────────────────────────────────────────────────────┐│
│  │                          Dataspace                                   ││
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────────┐  ││
│  │  │  Skeleton   │  │    Bag      │  │      Observers              │  ││
│  │  │  (indexes)  │  │ (ref-count) │  │  (pattern subscriptions)    │  ││
│  │  └─────────────┘  └─────────────┘  └─────────────────────────────┘  ││
│  └─────────────────────────────────────────────────────────────────────┘│
│                                                                         │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │                   Market Entities                                 │  │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────────┐  │  │
│  │  │ OrderBook│  │ Matcher  │  │ History  │  │ Analytics Entity │  │  │
│  │  │ (per sym)│  │          │  │          │  │ (dataflow fields)│  │  │
│  │  └──────────┘  └──────────┘  └──────────┘  └──────────────────┘  │  │
│  └──────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
```

## SAM Component Coverage

### Actors

| Actor | Location | Lifecycle |
|-------|----------|-----------|
| **Broker Actor** | Server | Long-lived, hosts Dataspace + Gatekeeper + Market entities |
| **Relay Actor** | Server (per connection) | Created on connect, terminated on disconnect |
| **Client Actor** | Client | Created per trader bot, terminated on test end |

### Entities

| Entity | Facet | Purpose | Key Operations |
|--------|-------|---------|----------------|
| **Dataspace** | Broker:root | Assertion routing | Route assertions/retractions to observers |
| **Gatekeeper** | Broker:root | Auth + attenuation | Validate sturdy refs, enforce caveats |
| **OrderBook** | Broker:market | Per-symbol order state | Track bids/asks as assertions |
| **Matcher** | Broker:market | Trade execution | Match orders, emit fills, retract matched |
| **History** | Broker:market | Trade history | Observe fills, maintain audit log |
| **Analytics** | Broker:analytics | Reactive metrics | Dataflow fields for volume, VWAP, spread |
| **RelayProxy** | Relay:session | Remote ref proxy | Import/export membrane translation |
| **SessionEntity** | Client:session | Relay client | Translate remote assertions to local |
| **OrderManager** | Client:orders | Order lifecycle | Place/cancel orders, track fills |
| **MarketObserver** | Client:data | Market data feed | Subscribe to order book, fills |

### Facets

| Facet Name | Parent | Entities Owned | Termination Trigger |
|------------|--------|----------------|---------------------|
| **Broker:root** | - | Dataspace, Gatekeeper | Broker shutdown |
| **Broker:market** | Broker:root | OrderBook (per symbol), Matcher, History | Market close (test end) |
| **Broker:analytics** | Broker:root | Analytics | Dataflow cleanup |
| **Relay:session** | - | RelayProxy entities | Connection close |
| **Client:session** | - | SessionEntity | Disconnect |
| **Client:orders** | Client:session | OrderManager | Session end (fate-sharing) |
| **Client:data** | Client:session | MarketObserver | Session end (fate-sharing) |

### Turns

| Turn Type | Trigger | Actions | Atomicity Guarantee |
|-----------|---------|---------|---------------------|
| **Assert** | Order placement | Add to Bag, notify observers | All-or-nothing |
| **Retract** | Order cancel/fill | Remove from Bag, notify observers | All-or-nothing |
| **Message** | Fill notification | Route to target entity | Delivered once |
| **Spawn** | New subscription | Create child entity | Entity exists or not |
| **Sync** | Barrier request | Complete pending work | All prior work visible |

### Membrane Interactions (Relay)

| Direction | Trigger | Translation |
|-----------|---------|-------------|
| **Import (peer → local)** | Receive WireRef | Allocate OID, create proxy entity |
| **Export (local → peer)** | Send Ref | Allocate OID, track in membrane |
| **Retract import** | Peer sends retract | Remove from import table |
| **Retract export** | Local facet terminates | Send retract, remove from export table |

## Assertion Schemas

All assertions use Preserves record format with explicit lifecycle:

```preserves
# Order assertions (asserted while order is live, retracted on cancel/fill)
<Order {
  id: symbol,           # Unique order identifier (e.g., `order-12345`)
  symbol: symbol,       # Trading pair (e.g., `BTC-USD`)
  side: <bid> | <ask>,  # Order direction
  price: integer,       # Price in smallest unit (cents, satoshi)
  qty: integer,         # Remaining quantity
  trader: symbol,       # Trader identity
  seq: integer          # Monotonic sequence for ordering
}>

# Cancel request (message, not assertion)
<Cancel {
  order_id: symbol,     # Order to cancel
  trader: symbol,       # Must match order owner
  seq: integer          # For ordering
}>

# Fill notification (ephemeral message, delivered exactly once)
<Fill {
  fill_id: symbol,      # Unique fill identifier
  order_id: symbol,     # Order that was filled
  symbol: symbol,       # Trading pair
  side: <bid> | <ask>,  # Fill direction
  price: integer,       # Execution price
  qty: integer,         # Filled quantity
  counterparty: symbol, # Other trader
  timestamp: integer,   # Monotonic nanoseconds
  seq: integer          # Fill sequence for ordering
}>

# Partial fill update (retract old Order, assert new with reduced qty)
# This is NOT a separate assertion type - use Order retract+assert

# Market data subscription (Observe pattern per Preserves conventions)
<Observe {
  pattern: <Order {symbol: $sym, ...}>,  # Capture symbol
  observer: #embedded-ref                 # Entity to notify
}>

# Session presence (asserted while connected, retracted on disconnect)
<Session {
  id: symbol,           # Session identifier
  trader: symbol,       # Authenticated trader
  connected_at: integer,# Connection timestamp (monotonic ns)
  version: integer      # Protocol version for compat
}>

# Analytics snapshot (dataflow-driven, retract+assert on update)
<MarketStats {
  symbol: symbol,
  volume_24h: integer,  # Volume in base units
  vwap: integer,        # Volume-weighted avg price (scaled by 1e6)
  spread: integer,      # Best ask - best bid
  last_price: integer,  # Most recent fill price
  updated_at: integer,  # Timestamp of last update
  seq: integer          # Monotonic sequence for causality
}>
```

### Assertion Lifecycle

| Assertion | Asserted When | Retracted When |
|-----------|---------------|----------------|
| Order | Placed by trader | Cancelled, fully filled, or session disconnect |
| Session | Client connects + auth | Client disconnects (facet terminates) |
| MarketStats | First trade | Never (updated via retract+assert) |
| Observe | Subscription start | Unsubscribe or session disconnect |

## Attenuation Rules

Sturdy refs use caveats to restrict capabilities. The gatekeeper enforces these
on every materialized ref.

### Caveat Types

| Caveat | Effect | Test Scenario |
|--------|--------|---------------|
| **ReadOnly** | Block assert/message, allow observe | Market data subscriber |
| **SymbolFilter** | Restrict to specific symbols | Single-symbol trader |
| **RateLimit** | Max N operations per window | Prevent spam |
| **VolumeCap** | Max order size | Risk limit |
| **Deny** | Block all operations | Revoked credentials |

### Attenuation Test Cases

1. **Read-only client**: Attempts to assert Order, should receive rejection
2. **Symbol-filtered trader**: Places order for wrong symbol, should fail
3. **Rate-limited trader**: Exceeds rate, should see pause/retry
4. **Volume-capped order**: Order qty > cap, should be rejected
5. **Revoked session**: Previously valid ref now denied

```elixir
# Example caveat structure
%Caveat{
  type: :rewrite,
  pattern: {:record, {{:symbol, "Order"}, [{:symbol, "symbol"}, ...]}},
  template: {:record, {{:symbol, "Order"}, [{:symbol, "BTC-USD"}, ...]}},
}
```

## Load Profiles

### Profile 1: Steady State (Baseline)

| Parameter | Value |
|-----------|-------|
| Clients | 10 |
| Symbols | 3 |
| Orders/sec/client | 5 |
| Order lifetime | 1-10s (uniform random, seed=42) |
| Warmup | 10s ramp from 0 to target rate |
| Duration | 60s (after warmup) |
| Random seed | 42 (reproducible) |

### Profile 2: Burst Traffic

| Parameter | Value |
|-----------|-------|
| Clients | 50 |
| Symbols | 5 |
| Base rate | 5 orders/sec/client |
| Burst rate | 50 orders/sec/client |
| Burst pattern | 5s burst, 25s idle, repeat |
| Warmup | 15s |
| Duration | 120s (4 burst cycles) |
| Random seed | 123 |

### Profile 3: High Churn

| Parameter | Value |
|-----------|-------|
| Clients | 20 |
| Symbols | 50 (high cardinality) |
| Assert/retract rate | 100/sec/client |
| Observer churn | Subscribe/unsubscribe every 2s per client |
| Facet lifecycle | Create child facet, spawn 5 entities, terminate after 3s |
| Facet churn rate | 1 facet cycle/sec/client |
| Warmup | 20s |
| Duration | 180s |
| Random seed | 456 |

### Profile 4: Chaos Mode

| Parameter | Value |
|-----------|-------|
| Clients | 30 |
| Symbols | 20 |
| Base load | Profile 2 burst pattern |
| Random disconnects | 5% chance per client per 10s |
| Facet crashes | Inject RuntimeError in 2% of entity callbacks |
| Network delays | Random 10-100ms delay on 10% of packets |
| Gatekeeper denials | 5% of sturdy ref resolutions return rejection |
| Flow control stress | Deliberately fill accounts to trigger pause |
| Warmup | 30s |
| Duration | 300s |
| Random seed | 789 |

### Profile 5: Negative Path

| Parameter | Value |
|-----------|-------|
| Clients | 10 |
| Purpose | Test rejection/error paths |
| Invalid sturdy refs | 20% of resolve requests |
| Attenuated denials | 30% of operations violate caveats |
| Duplicate order IDs | 5% of orders reuse existing ID |
| Invalid cancel targets | 10% of cancels for non-existent orders |
| Malformed assertions | 2% of assertions with wrong schema |
| Duration | 60s |
| Random seed | 999 |

**Profile 5 Pass Criteria**:
- Invalid sturdy refs: 100% result in `[:absynthe, :gatekeeper, :resolve, :failure]` telemetry
- Attenuated denials: 100% result in `[:absynthe, :gatekeeper, :attenuation, :denied]` telemetry
- Duplicate order IDs: Handled gracefully (no crash, logged warning, second order ignored or rejected)
- Invalid cancels: Return error or no-op (no crash, no matching retract)
- Malformed assertions: Rejected by dataspace or entity (no crash, `[:stress, :client, :error]` logged)
- **Zero crashes**: All actors remain alive despite invalid input
- **No bypass**: Zero successful operations for attenuated/invalid requests

## Acceptance Criteria

### Pass Conditions (with measurable thresholds)

1. **No crashes**: Zero actor crashes during test (supervisor restart count = 0)
2. **Assertion consistency**: All assertions retracted within 5s of source disconnect
   - Measured: `count(orphan_assertions) == 0` at test end
3. **Observer notification completeness**:
   - Every assert has matching retract delivered: `delivered_retracts >= delivered_asserts`
   - Counter per observer: `missing_retracts = asserts_seen - retracts_seen == 0`
4. **No duplicate delivery**: Each handle delivered at most once per observer
   - Counter: `duplicate_notifications == 0`
5. **Flow control engagement**: At least 1 pause+resume cycle in Profile 2-4
   - Counter: `pause_events > 0 && resume_events > 0`
6. **Memory bounded**:
   - ETS table size: `max(skeleton_size) < 100_000` entries
   - Process heap: `max(heap_size) < 50MB` per actor
   - No monotonic growth: `heap_size(t_end) <= heap_size(t_start) * 1.5`
7. **Latency SLOs** (measured at turn commit):
   - p50 turn latency < 1ms
   - p99 turn latency < 50ms
   - p999 turn latency < 500ms
8. **Matching correctness**:
   - Price-time priority: Older orders at same price fill first
   - Conservation: `sum(filled_qty) == sum(matched_qty)` per symbol
   - No partial orphans: Every partial fill has corresponding qty reduction

### Fail Conditions

1. **Orphan assertions**: `count(assertions_after_cleanup) > 0` (timeout 5s)
2. **Lost retractions**: Observer missing_retracts counter > 0
3. **Double delivery**: duplicate_notifications counter > 0
4. **Memory leak**: Heap grows > 2x during steady state (Profile 1)
5. **Deadlock**: Any turn latency > 10s (watchdog timeout)
6. **Skeleton corruption**: `skeleton_index_size != bag_assertion_count`
7. **Gatekeeper bypass**: Attenuated operation succeeds when it should fail
8. **Flow control stuck**: Paused > 30s without resume

## Telemetry to Record

### Flow Control

```elixir
[:absynthe, :flow_control, :account, :borrow]   # {cost, new_debt, account_id}
[:absynthe, :flow_control, :account, :repay]    # {cost, new_debt, account_id}
[:absynthe, :flow_control, :account, :pause]    # {debt, high_water, account_id}
[:absynthe, :flow_control, :account, :resume]   # {debt, low_water, account_id}
```

### Turn Execution

```elixir
[:absynthe, :actor, :turn, :start]     # {actor_id, entity_id, event_type}
[:absynthe, :actor, :turn, :commit]    # {actor_id, action_count}
[:absynthe, :actor, :turn, :duration]  # {actor_id, duration_us}
[:absynthe, :actor, :turn, :error]     # {actor_id, error, stacktrace}
```

### Dataspace Operations

```elixir
[:absynthe, :dataspace, :assert]       # {handle, assertion_type}
[:absynthe, :dataspace, :retract]      # {handle}
[:absynthe, :dataspace, :notify]       # {observer_count, assertion_type}
[:absynthe, :dataspace, :bag, :size]   # {count} (gauge)
[:absynthe, :dataspace, :skeleton, :size]  # {count} (gauge)
[:absynthe, :dataspace, :observer, :count] # {count} (gauge)
```

### Relay/Membrane

```elixir
[:absynthe, :relay, :packet, :received]    # {packet_type, size_bytes}
[:absynthe, :relay, :packet, :sent]        # {packet_type, size_bytes}
[:absynthe, :relay, :connection, :established] # {relay_id}
[:absynthe, :relay, :connection, :closed]  # {relay_id, reason}
[:absynthe, :relay, :membrane, :import]    # {oid, ref_id}
[:absynthe, :relay, :membrane, :export]    # {oid, ref_id}
[:absynthe, :relay, :membrane, :balance]   # {import_count, export_count} (gauge)
```

### Gatekeeper

```elixir
[:absynthe, :gatekeeper, :resolve, :success] # {oid, trader}
[:absynthe, :gatekeeper, :resolve, :failure] # {oid, reason}
[:absynthe, :gatekeeper, :attenuation, :applied] # {caveat_type}
[:absynthe, :gatekeeper, :attenuation, :denied]  # {caveat_type, operation}
```

### Facet Lifecycle

```elixir
[:absynthe, :facet, :create]     # {facet_id, parent_id, actor_id}
[:absynthe, :facet, :terminate]  # {facet_id, reason, child_count}
[:absynthe, :facet, :entity, :spawn]  # {facet_id, entity_id}
[:absynthe, :facet, :entity, :stop]   # {facet_id, entity_id}
```

### Market Domain

```elixir
[:stress, :order, :placed]     # {order_id, symbol, side, qty}
[:stress, :order, :cancelled]  # {order_id, reason}
[:stress, :order, :filled]     # {order_id, fill_qty, remaining_qty}
[:stress, :match, :executed]   # {bid_id, ask_id, price, qty}
[:stress, :match, :depth]      # {symbol, bid_depth, ask_depth} (gauge)
```

### Load Generator

```elixir
[:stress, :client, :connected]     # {client_id}
[:stress, :client, :disconnected]  # {client_id, reason}
[:stress, :client, :sent]          # {client_id, operation_type}
[:stress, :client, :acked]         # {client_id, operation_type, latency_us}
[:stress, :client, :error]         # {client_id, operation_type, error}
```

### System Resources

```elixir
[:stress, :system, :ets, :memory]     # {table_name, bytes} (gauge, sampled every 5s)
[:stress, :system, :process, :heap]   # {actor_id, bytes} (gauge, sampled every 5s)
[:stress, :system, :process, :count]  # {count} (gauge)
[:stress, :system, :scheduler, :util] # {scheduler_id, percent} (gauge)
```

## Instrumentation for Acceptance Counters

The harness must maintain the following counters to verify acceptance criteria.
These are computed from telemetry events, not from internal state inspection.

### Observer Notification Tracking

```elixir
# Per-observer state (ETS or Agent)
%{
  observer_id => %{
    handles_seen: MapSet.t(),           # All handles ever published
    handles_retracted: MapSet.t(),      # All handles ever retracted
    duplicate_publishes: integer,       # Same handle published twice
    orphan_handles: MapSet.t()          # published - retracted at end
  }
}

# Computed from telemetry:
# - [:absynthe, :dataspace, :notify] with event_type: :publish -> add to handles_seen
# - [:absynthe, :dataspace, :notify] with event_type: :retract -> add to handles_retracted
# - At test end: orphan_handles = handles_seen - handles_retracted
# - missing_retracts = MapSet.size(orphan_handles)
# - duplicate_notifications = count of handles in handles_seen appearing twice
```

### Price-Time Priority Verification

```elixir
# Per-symbol order book state (reconstructed from telemetry)
%{
  symbol => %{
    bids: [{price, timestamp, order_id}],  # sorted by price desc, time asc
    asks: [{price, timestamp, order_id}]   # sorted by price asc, time asc
  }
}

# On [:stress, :match, :executed] {bid_id, ask_id, price, qty}:
# - Look up bid_id and ask_id in order book
# - Verify bid was at best_bid position (or better price)
# - Verify ask was at best_ask position (or better price)
# - Verify no older order at same price was skipped
# - Increment priority_violations if check fails
```

### Flow Control Tracking

```elixir
# Per-account state
%{
  account_id => %{
    current_debt: integer,
    max_debt: integer,
    pause_count: integer,
    resume_count: integer,
    pause_start: timestamp | nil,
    max_pause_duration: integer,
    negative_debt_events: integer,    # debt went below 0
    oscillation_events: integer       # pause within 100ms of resume
  }
}

# Computed from telemetry:
# - [:absynthe, :flow_control, :account, :borrow] -> update debt
# - [:absynthe, :flow_control, :account, :repay] -> update debt, check for negative
# - [:absynthe, :flow_control, :account, :pause] -> record pause_start
# - [:absynthe, :flow_control, :account, :resume] -> check oscillation, clear pause_start
```

### Rejection/Error Tracking (Profile 5)

```elixir
# Rejection counters
%{
  invalid_sturdy_refs: %{sent: integer, rejected: integer},
  attenuated_operations: %{sent: integer, denied: integer},
  duplicate_orders: %{sent: integer, handled: integer},
  invalid_cancels: %{sent: integer, handled: integer},
  malformed_assertions: %{sent: integer, rejected: integer},
  bypass_attempts: integer  # operations that should have failed but succeeded
}

# Computed from:
# - Load generator tracks what it sent ([:stress, :client, :sent])
# - Gatekeeper telemetry tracks rejections
# - Compare: if sent.invalid > rejected.failure -> bypass detected
```

## Invariants to Assert

### Core SAM Invariants

1. **Bag reference counts**: `refcount >= 0`, and `refcount == 0` means removed
2. **Handle uniqueness**: No two live assertions share a handle
3. **Facet tree integrity**: `child.parent.alive? || child.terminated?`
4. **Turn atomicity**: No partial commits visible to observers
5. **Fate-sharing**: Child facet terminates within 1 turn of parent termination

### Dataspace Invariants

6. **Skeleton-Bag alignment**: `skeleton.count == bag.unique_assertions`
7. **Observer notification completeness**: Every bag change triggers observer notify
8. **Observer notification ordering**: Publishes before retracts for same handle

### Relay/Membrane Invariants

9. **Import-export balance**: On clean disconnect, `imports == exports == 0`
10. **No stale refs**: Retracted refs not resolvable
11. **Membrane monotonic OIDs**: OIDs increase monotonically per direction

### Gatekeeper Invariants

12. **Attenuation enforcement**: Every operation checked against caveats
13. **No capability amplification**: Attenuated ref cannot gain capabilities
14. **Signature validity**: Invalid signatures always rejected

### Market Domain Invariants

15. **Order conservation**: `sum(placed) == sum(filled) + sum(cancelled) + sum(live)`
16. **Qty conservation**: `sum(fill_qty for order) <= order.original_qty`
17. **Price-time priority**: Fills respect order book ordering
18. **No duplicate order IDs**: Each order_id used exactly once

### Flow Control Invariants

19. **Non-negative debt**: `account.debt >= 0` at all times
20. **Debt consistency**: `account.debt == sum(outstanding_loan_costs)`
21. **Pause-resume hysteresis**: No pause within 100ms of resume (oscillation detection)
22. **Bounded pause duration**: `pause_duration < 30s` (stuck pause detection)
23. **Limit enforcement**: `account.debt <= account.limit + max_single_event_cost`

## Failure Modes and Edge Cases

### Connection Lifecycle

1. **Partial handshake**: Client disconnects during Noise handshake
2. **Reconnect storm**: Multiple clients reconnect simultaneously
3. **Stale session**: Client uses old sturdy ref after server restart
4. **Replay attack**: Client replays old Turn packet
5. **Idle timeout**: Connection closes due to inactivity

### Assertion Lifecycle

6. **Rapid assert-retract**: Assert and retract same value within single turn
7. **Duplicate assertion**: Same value asserted twice (ref-counted)
8. **Retract unknown**: Retract for non-existent handle
9. **Orphan on crash**: Entity crashes before retracting
10. **Partial fill cascade**: Fill triggers retract+assert in same turn

### Flow Control

11. **Stuck pause**: Account paused but resume never fires
12. **Rapid pause-resume**: Oscillation between pause and resume
13. **Debt overflow**: More work than account limit
14. **Negative debt**: Repay more than borrowed

### Matching Engine

15. **Self-trade**: Trader's bid matches own ask
16. **Price inversion**: Best bid > best ask after match
17. **Qty mismatch**: Fill qty doesn't match order reduction
18. **Stale match**: Match uses retracted order

### Gatekeeper

19. **Invalid signature**: Tampered sturdy ref
20. **Expired caveat**: Time-based attenuation expired
21. **Nested attenuation**: Multiple caveat layers
22. **Denial of service**: Flood of invalid resolve requests

## Failure Mode to Profile Mapping

Each failure mode is exercised by specific load profiles and chaos hooks:

| Failure Mode | Profile(s) | Chaos Hook / Injection |
|--------------|------------|------------------------|
| Partial handshake | 4 (Chaos) | `chaos.disconnect_during_handshake: 5%` |
| Reconnect storm | 4 (Chaos) | `chaos.simultaneous_reconnect: true` on disconnect |
| Stale session | 4, 5 | `chaos.use_stale_ref: 2%` after disconnect |
| Replay attack | 5 (Negative) | `inject.replay_old_turn: 1%` |
| Idle timeout | 4 (Chaos) | `chaos.idle_clients: 10%` (no activity for 30s) |
| Rapid assert-retract | 3 (Churn) | Normal churn behavior at 100/sec |
| Duplicate assertion | 3, 5 | `inject.duplicate_assert: 5%` |
| Retract unknown | 5 (Negative) | `inject.invalid_retract: 10%` |
| Orphan on crash | 4 (Chaos) | `chaos.entity_crash: 2%` |
| Partial fill cascade | 2, 3 | Normal matching at high rate |
| Stuck pause | 4 (Chaos) | `chaos.block_repay: 0.5%` (delay repay by 60s) |
| Rapid pause-resume | 2 (Burst), 4 | Burst pattern triggers natural oscillation |
| Debt overflow | 4 (Chaos) | `chaos.flood_work: true` (send 10x normal rate) |
| Negative debt | 5 (Negative) | `inject.double_repay: 1%` |
| Self-trade | 2, 3 | Normal trading (self-trade prevention check) |
| Price inversion | 2, 3 | Normal matching at high rate |
| Qty mismatch | 5 (Negative) | `inject.wrong_fill_qty: 1%` |
| Stale match | 4 (Chaos) | Race between retract and match |
| Invalid signature | 5 (Negative) | `inject.tampered_ref: 20%` |
| Expired caveat | 5 (Negative) | `inject.expired_caveat: 5%` |
| Nested attenuation | 5 (Negative) | `inject.multi_layer_caveat: 5%` |
| Denial of service | 5 (Negative) | `inject.flood_invalid_resolve: 30%` |

### Chaos Hook Configuration

```elixir
# Profile 4 chaos hooks
%{
  disconnect_during_handshake: 0.05,  # 5% of new connections
  simultaneous_reconnect: true,        # All disconnected clients reconnect at once
  use_stale_ref: 0.02,                 # 2% of operations use old ref
  idle_clients: 0.10,                  # 10% of clients go idle
  entity_crash: 0.02,                  # 2% of entity callbacks raise
  block_repay: 0.005,                  # 0.5% of repays delayed 60s
  flood_work: true                     # Periodic 10x rate spike
}

# Profile 5 injection hooks
%{
  replay_old_turn: 0.01,
  duplicate_assert: 0.05,
  invalid_retract: 0.10,
  double_repay: 0.01,
  wrong_fill_qty: 0.01,
  tampered_ref: 0.20,
  expired_caveat: 0.05,
  multi_layer_caveat: 0.05,
  flood_invalid_resolve: 0.30
}
```

## Runbook

### Prerequisites

```bash
# Ensure dependencies
mix deps.get
mix compile

# Verify base tests pass
mix test

# Check system resources
ulimit -n 65536  # Increase file descriptor limit
```

### Running Stress Tests

```bash
# Profile 1: Steady state baseline
MIX_ENV=test mix run examples/stress/runner.exs --profile steady --seed 42

# Profile 2: Burst traffic
MIX_ENV=test mix run examples/stress/runner.exs --profile burst --seed 123

# Profile 3: High churn
MIX_ENV=test mix run examples/stress/runner.exs --profile churn --seed 456

# Profile 4: Chaos mode
MIX_ENV=test mix run examples/stress/runner.exs --profile chaos --seed 789

# Profile 5: Negative path
MIX_ENV=test mix run examples/stress/runner.exs --profile negative --seed 999

# Full suite with report
MIX_ENV=test mix run examples/stress/runner.exs --all --report stress_report.json
```

### Analyzing Results

```bash
# View telemetry summary
mix run examples/stress/analyze.exs stress_report.json

# Check for invariant violations
mix run examples/stress/check_invariants.exs stress_report.json

# Generate latency histograms
mix run examples/stress/latency_report.exs stress_report.json
```

### Debugging Failures

1. Enable trace logging: `ABSYNTHE_LOG=debug mix run ...`
2. Enable process tracing: Add `:sys.trace(pid, true)` to suspect actors
3. Capture ETS snapshots: `Absynthe.Dataspace.Skeleton.dump(dataspace_pid)`
4. Memory profiling: `:recon.proc_count(:memory, 10)`
5. Scheduler utilization: `:scheduler.utilization(5000)`

### Invariant Violation Triage

| Violation | Likely Cause | Debug Steps |
|-----------|--------------|-------------|
| Orphan assertions | Facet teardown bug | Check facet terminate callback, trace retract path |
| Lost retractions | Observer notify bug | Trace skeleton notify, check observer state |
| Skeleton-Bag mismatch | Index corruption | Dump both, compare handles |
| Membrane imbalance | Import/export leak | Trace membrane operations, check refcount |
| Flow stuck paused | Repay not called | Trace turn completion, check loan tracking |

## Known Bug Signals

Patterns that indicate bugs discovered during stress:

1. **Slow observer notification**: Skeleton index not being used (full scan)
2. **Assertion handle leak**: Bag grows without bound
3. **Flow control stuck**: Paused but never resumes
4. **Facet teardown incomplete**: Assertions linger after facet stop
5. **Relay membrane drift**: Import/export counts diverge
6. **Pattern capture loss**: Observe callbacks missing bindings
7. **Gatekeeper bypass**: Attenuated operation succeeds
8. **Match engine drift**: Order book depth doesn't match assertion count

## Deliverables

1. `examples/stress/runner.exs` - Main test harness
2. `examples/stress/entities/` - Market entities (order_book, matcher, etc.)
3. `examples/stress/client.exs` - Trader bot implementation
4. `examples/stress/telemetry.exs` - Telemetry handler + reporter
5. `examples/stress/analyze.exs` - Post-run analysis script
6. `examples/stress/check_invariants.exs` - Invariant checker
7. Bug reports filed via `bd create` for any issues found
