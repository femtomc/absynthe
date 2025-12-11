# Absynthe

An implementation of [Tony Garnock-Jones' Syndicated Actor Model](https://syndicate-lang.org/) for Elixir, with full support for the [Preserves](https://preserves.gitlab.io/preserves/preserves.html) data format.

## What is the Syndicated Actor Model?

The Syndicated Actor Model (SAM) extends traditional actors with:

- **Assertions** - Persistent facts that remain until retracted (vs ephemeral messages)
- **Dataspaces** - Shared tuple spaces where actors publish and observe assertions
- **Pattern matching** - Subscribe to assertions matching patterns with wildcards/captures
- **Turns** - Atomic transactions ensuring all-or-nothing updates
- **Facets** - Scoped conversations with fate-sharing (parent dies, children die)

## Installation

Add `absynthe` to your dependencies:

```elixir
def deps do
  [
    {:absynthe, git: "https://github.com/femtomc/absynthe.git"}
  ]
end
```

## Quick Start

### 1. Define an Entity

Entities are the computational objects within actors. They implement the `Entity` protocol:

```elixir
defmodule Counter do
  defstruct count: 0
end

defimpl Absynthe.Core.Entity, for: Counter do
  def on_message(%{count: n} = counter, {:add, amount}, turn) do
    IO.puts("Counter: #{n} + #{amount} = #{n + amount}")
    {%{counter | count: n + amount}, turn}
  end

  def on_message(counter, :get, turn) do
    IO.puts("Counter value: #{counter.count}")
    {counter, turn}
  end

  # Required callbacks
  def on_publish(entity, _assertion, _handle, turn), do: {entity, turn}
  def on_retract(entity, _handle, turn), do: {entity, turn}
  def on_sync(entity, _peer, turn), do: {entity, turn}
end
```

### 2. Start an Actor and Spawn Entities

```elixir
# Start an actor
{:ok, actor} = Absynthe.start_actor(id: :counter_actor)

# Spawn a counter entity in the root facet
{:ok, counter_ref} = Absynthe.spawn_entity(actor, :root, %Counter{count: 0})

# Send messages
Absynthe.send_to(actor, counter_ref, {:add, 5})
Absynthe.send_to(actor, counter_ref, {:add, 3})
Absynthe.send_to(actor, counter_ref, :get)
# Output:
# Counter: 0 + 5 = 5
# Counter: 5 + 3 = 8
# Counter value: 8
```

### 3. Using Dataspaces

Dataspaces are shared spaces where actors publish and subscribe to assertions:

```elixir
defmodule PresenceObserver do
  defstruct users: []
end

defimpl Absynthe.Core.Entity, for: PresenceObserver do
  def on_publish(observer, {:record, {{:symbol, "Online"}, [{:string, username}]}}, _handle, turn) do
    IO.puts("User came online: #{username}")
    {%{observer | users: [username | observer.users]}, turn}
  end

  def on_retract(observer, _handle, turn) do
    IO.puts("A user went offline")
    {observer, turn}
  end

  def on_message(entity, _msg, turn), do: {entity, turn}
  def on_sync(entity, _peer, turn), do: {entity, turn}
end

# Create dataspace and observer
{:ok, actor} = Absynthe.start_actor(id: :presence)
dataspace = Absynthe.new_dataspace()
{:ok, ds_ref} = Absynthe.spawn_entity(actor, :root, dataspace)
{:ok, obs_ref} = Absynthe.spawn_entity(actor, :root, %PresenceObserver{})

# Subscribe observer to Online records
observe_assertion = Absynthe.observe(
  Absynthe.record(:Online, [Absynthe.wildcard()]),
  obs_ref
)
{:ok, _} = Absynthe.assert_to(actor, ds_ref, observe_assertion)

# Publish presence
online = Absynthe.record(:Online, ["alice"])
{:ok, handle} = Absynthe.assert_to(actor, ds_ref, online)
# Output: User came online: alice

# Retract when going offline
Absynthe.retract_from(actor, handle)
# Output: A user went offline
```

## Core Concepts

### Preserves Values

Absynthe uses Preserves as its data format. Values are tagged tuples:

```elixir
# Atomic types
{:boolean, true}
{:integer, 42}
{:double, 3.14}
{:string, "hello"}
{:symbol, "MySymbol"}
{:binary, <<1, 2, 3>>}

# Compound types
{:sequence, [{:integer, 1}, {:integer, 2}]}
{:set, MapSet.new([{:integer, 1}, {:integer, 2}])}
{:dictionary, %{{:symbol, "key"} => {:string, "value"}}}
{:record, {{:symbol, "Person"}, [{:string, "Alice"}, {:integer, 30}]}}

# Embedded references (for entity refs)
{:embedded, some_ref}
```

Helper functions make construction easier:

```elixir
Absynthe.symbol(:Person)           # {:symbol, "Person"}
Absynthe.record(:Person, ["Alice", 30])  # Full record with auto-conversion
```

### Encoding/Decoding

```elixir
# Binary format (compact, for wire)
binary = Absynthe.encode!({:integer, 42})
{:integer, 42} = Absynthe.decode!(binary)

# Text format (human readable)
text = Absynthe.encode!({:string, "hello"}, :text)
# => "\"hello\""
```

### Actors and Entities

- **Actor**: A GenServer process hosting entities
- **Entity**: A struct implementing `Absynthe.Core.Entity` protocol
- **Ref**: A capability/reference to send messages to an entity

```elixir
{:ok, actor} = Absynthe.start_actor(id: :my_actor, name: MyApp.Actor)
{:ok, ref} = Absynthe.spawn_entity(actor, :root, %MyEntity{})
```

### Facets (Conversations)

Facets provide scoped lifetimes with fate-sharing:

```elixir
# Create a child facet
:ok = Absynthe.create_facet(actor, :root, :session_123)

# Spawn entities in the facet
{:ok, ref} = Absynthe.spawn_entity(actor, :session_123, entity)

# Terminate facet (kills all entities in it)
:ok = Absynthe.terminate_facet(actor, :session_123)
```

### Pattern Matching

Use patterns to subscribe to specific assertions:

```elixir
# Match any Person record
Absynthe.record(:Person, [Absynthe.wildcard(), Absynthe.wildcard()])

# Capture the name field
Absynthe.record(:Person, [Absynthe.capture(:name), Absynthe.wildcard()])
```

### Reactive Dataflow

Fields are reactive cells that automatically propagate changes:

```elixir
alias Absynthe.Dataflow.Field

# Simple fields
x = Field.new(10)
y = Field.new(20)

# Computed field (auto-updates when dependencies change)
sum = Field.computed(fn ->
  Field.get(x) + Field.get(y)
end)

Field.get(sum)  # => 30

# Update via turn
turn = Absynthe.Core.Turn.new(:actor, :facet)
turn = Field.set(x, 15, turn)
Field.commit_field_updates(turn)

Field.get(sum)  # => 35 (automatically recomputed)
```

## Architecture

```
Absynthe
├── Core
│   ├── Actor      - GenServer-based actor process
│   ├── Entity     - Protocol for message/assertion handling
│   ├── Turn       - Atomic transaction context
│   ├── Facet      - Scoped conversation management
│   └── Ref        - Entity references/capabilities
├── Dataspace
│   ├── Dataspace  - Assertion routing entity
│   ├── Skeleton   - ETS-based pattern index
│   ├── Pattern    - Pattern compilation
│   └── Observer   - Subscription handling
├── Dataflow
│   ├── Field      - Reactive cells
│   └── Registry   - ETS-based field storage
├── Preserves
│   ├── Value      - Tagged union types
│   ├── Compare    - Total ordering
│   ├── Pattern    - Pattern matching
│   └── Encoder/Decoder - Binary & text formats
└── Protocol
    └── Event      - Assert/Retract/Message/Sync
```

## References

- [Syndicate Lang](https://syndicate-lang.org/) - Official project site
- [PhD Dissertation](https://syndicate-lang.org/tonyg-dissertation/html/) - Tony Garnock-Jones' thesis
- [Preserves Spec](https://preserves.gitlab.io/preserves/preserves.html) - Data format specification
- [syndicate-rs](https://git.syndicate-lang.org/syndicate-lang/syndicate-rs) - Rust implementation

## License

MIT License - see [LICENSE](LICENSE) for details.
