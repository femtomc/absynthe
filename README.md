# absynthe

<p align="center">
  <img src="assets/logo.png" alt="Absynthe Logo" width="400" height="400">
</p>

Syndicated Actor Model (SAM) for Elixir. `absynthe` follows Tony Garnock-Jones' work on [Syndicate](https://syndicate-lang.org/about/) and [Synit](https://synit.org/): actors exchange persistent assertions via shared dataspaces, reason with patterns, and run in atomic turns.

`absynthe` does the following:
- Implements SAM semantics on the BEAM: assertions, Observe subscriptions, turns, and fate-sharing facets.
- Implements the Preserves data format with binary and text encoders/decoders.

## Project status

- Experimental/alpha; APIs may change.
- Single-node only; dataspaces are local entities, no distribution or persistence yet.
- Best for learning/experimentation with SAM concepts; see `test/` and `examples/` for coverage of current behavior.

## Syndicate primer (concepts & mapping)

- `Actor` (GenServer) hosts entities and processes events inside *turns*.
- `Entity` implements `Absynthe.Core.Entity` callbacks: `on_message/3`, `on_publish/4`, `on_retract/3`, `on_sync/3`.
- `Assertion` is a persistent fact; identified by a `Absynthe.Assertions.Handle`. Assertions remain until fully retracted.
- `Dataspace` routes assertions to observers. Subscriptions are `Observe` assertions with a pattern and an observer `Ref`.
- `Facet` is a conversational scope; create via `Absynthe.create_facet/3`, terminate via `Absynthe.terminate_facet/2` (fate-sharing).
- `Turn` is the atomic unit of execution; handlers add actions and the actor commits or discards them together.
- `Preserves` is the data model: tagged tuples with records, sets, sequences, and embedded refs. Patterns use `wildcard/0` and `capture/1`.

## Installation

Add `absynthe` to your dependencies:

```elixir
def deps do
  [
    {:absynthe, git: "https://github.com/femtomc/absynthe.git"}
  ]
end
```

## Quick start

### Messages inside one actor

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

  def on_publish(entity, _assertion, _handle, turn), do: {entity, turn}
  def on_retract(entity, _handle, turn), do: {entity, turn}
  def on_sync(entity, _peer, turn), do: {entity, turn}
end

{:ok, actor} = Absynthe.start_actor(id: :counter_actor)
{:ok, counter_ref} = Absynthe.spawn_entity(actor, :root, %Counter{})

Absynthe.send_to(actor, counter_ref, {:add, 5})
Absynthe.send_to(actor, counter_ref, {:add, 3})
Absynthe.send_to(actor, counter_ref, :get)
```

### Dataspace presence (assert/observe)

```elixir
defmodule PresenceObserver do
  defstruct users: []

  defimpl Absynthe.Core.Entity do
    def on_publish(observer, {:record, {{:symbol, "Online"}, [{:string, name}]}}, _h, turn) do
      IO.puts("Online: #{name}")
      {%{observer | users: [name | observer.users]}, turn}
    end

    def on_retract(observer, _handle, turn) do
      IO.puts("Someone went offline")
      {observer, turn}
    end

    def on_message(entity, _msg, turn), do: {entity, turn}
    def on_sync(entity, _peer, turn), do: {entity, turn}
  end
end

{:ok, actor} = Absynthe.start_actor(id: :presence)
dataspace = Absynthe.new_dataspace()
{:ok, ds_ref} = Absynthe.spawn_entity(actor, :root, dataspace)
{:ok, obs_ref} = Absynthe.spawn_entity(actor, :root, %PresenceObserver{})

# Subscribe to Online records
observe = Absynthe.observe(Absynthe.record(:Online, [Absynthe.wildcard()]), obs_ref)
{:ok, _} = Absynthe.assert_to(actor, ds_ref, observe)

# Publish/retract presence
{:ok, handle} = Absynthe.assert_to(actor, ds_ref, Absynthe.record(:Online, ["alice"]))
:ok = Absynthe.retract_from(actor, handle)
```

See `examples/presence.exs` and `examples/ping_pong.exs` for runnable versions (use `MIX_ENV=test mix run ...` to avoid protocol consolidation).

## Working in entity callbacks

Entity callbacks receive a `Turn`. Use the helpers to queue work that will commit atomically:

```elixir
defimpl Absynthe.Core.Entity, for: MySession do
  alias Absynthe.Protocol.Event
  alias Absynthe.Core.Turn

  def on_message(state, {:ping, reply_to}, turn) do
    turn = Absynthe.send_message(turn, reply_to, :pong)
    {state, turn}
  end

  def on_message(%{dataspace: ds, last_handle: handle} = state, :leave, turn) do
    # Retract a prior assertion you stored
    turn = Absynthe.retract_value(turn, ds, handle)
    {state, turn}
  end
end
```

From outside an actor, use the high-level API (`assert_to/3`, `retract_from/2`, `send_to/3`) and keep the returned handles if you need to retract later.

## Patterns and Observe assertions

Patterns are Preserves values with wildcards and captures:

```elixir
Absynthe.record(:Person, [Absynthe.capture(:name), Absynthe.wildcard()])
```

Subscriptions are assertions of the form:

```elixir
observe = Absynthe.observe(pattern, observer_ref)
{:ok, _handle} = Absynthe.assert_to(actor, dataspace_ref, observe)
```

The dataspace compiles patterns for fast routing and reference-counts assertions so repeated asserts/retracts behave predictably.

## Preserves cheat sheet

```elixir
{:integer, 42}
{:string, "hi"}
{:boolean, true}
{:symbol, "Online"}
{:sequence, [{:integer, 1}, {:integer, 2}]}
{:record, {{:symbol, "Person"}, [{:string, "Alice"}, {:integer, 30}]}}
{:embedded, ref} # for entity refs/capabilities

Absynthe.record(:Person, ["Alice", 30])   # helper
Absynthe.encode!({:integer, 42})          # binary format
Absynthe.encode!({:string, "hello"}, :text) # readable text format
Absynthe.decode!(binary)
```

## Reactive dataflow (optional)

`Absynthe.Dataflow.Field` provides reactive cells that integrate with turns:

```elixir
alias Absynthe.Dataflow.Field
alias Absynthe.Core.Turn

x = Field.new(2)
y = Field.new(3)
sum = Field.computed(fn -> Field.get(x) + Field.get(y) end)

turn = Turn.new(:actor, :facet) |> Field.set(x, 10)
Field.commit_field_updates(turn)
Field.get(sum) # => 13
```

## Running and testing

- Run the examples: `MIX_ENV=test mix run examples/ping_pong.exs` or `examples/presence.exs`.
- Interactive shell: `iex -S mix` (compile once with `MIX_ENV=test` if you need dynamic protocol implementations).
- Test suite: `mix test`.

## Architecture (high level)

- `Absynthe.Core.*`: Actor, Entity protocol, Ref, Facet, Turn (transaction boundary).
- `Absynthe.Dataspace.*`: Dataspace entity, pattern compiler, observer skeleton.
- `Absynthe.Assertions.*`: Handles and reference-counted assertion bag.
- `Absynthe.Preserves.*`: Data model, encoders/decoders, pattern matching.
- `Absynthe.Dataflow.*`: Reactive fields and registry.

## References

- [Synit](https://synit.org/) – overview of the Syndicated Interaction Toolkit.
- [Syndicate](https://syndicate-lang.org/about/) – background on the Syndicated Actor Model.
- [Tony Garnock-Jones' dissertation](https://syndicate-lang.org/tonyg-dissertation/html/).
- [Preserves specification](https://preserves.gitlab.io/preserves/preserves.html).
- [syndicate-rs](https://git.syndicate-lang.org/syndicate-lang/syndicate-rs) – Rust implementation.

## License

MIT License – see `LICENSE`.
