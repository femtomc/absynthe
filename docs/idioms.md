# Absynthe Idioms and Patterns

This guide collects practical patterns for building systems with Absynthe. It focuses on how to structure entities, use assertions and dataspaces effectively, and apply capability attenuation for safe APIs.

## Mental model

- An actor is a GenServer process.
- Entities are plain structs stored inside the actor state.
- Events are processed sequentially; turns commit atomically.
- Actions from a turn are delivered asynchronously in later turns.
- Assertions are persistent facts addressed by handles; messages are one-shot.
- Dataspaces are entities that route assertions and messages by pattern.

## Entity structure

Entities are state machines. Keep mutable state in the struct and return a new struct each callback. Avoid long or blocking work in callbacks.

```elixir
defimpl Absynthe.Core.Entity, for: MyEntity do
  def on_message(state, {:set, value}, turn) do
    {%{state | value: value}, turn}
  end
end
```

## Store handles for retraction

`assert_to/3` returns a handle. Store it so you can retract later.

```elixir
def on_message(state, {:publish, value}, turn) do
  {:ok, handle} = Absynthe.assert_to(state.actor, state.ds, value)
  {%{state | last_handle: handle}, turn}
end

def on_message(%{last_handle: handle} = state, :withdraw, turn) do
  :ok = Absynthe.retract_from(state.actor, handle)
  {%{state | last_handle: nil}, turn}
end
```

## Subscriptions are assertions

Observations are just `Observe` assertions. Keep the Observe handle so you can unsubscribe.

```elixir
pattern = Absynthe.record(:Online, [Absynthe.wildcard()])
observe = Absynthe.observe(pattern, observer_ref)
{:ok, obs_handle} = Absynthe.assert_to(actor, ds_ref, observe)
```

## Use turns for atomic updates

If you need "retract old + assert new" as one transaction, use `update_value/4` or enqueue both actions in the same turn.

```elixir
{turn, new_handle} = Absynthe.update_value(turn, old_handle, ds_ref, new_value)
```

## Request/response with reply refs

Messages are one-shot. Use a reply ref to implement request/response.

```elixir
# requester
Absynthe.send_to(actor, service_ref, {:get_status, reply_ref})

# service entity
def on_message(state, {:get_status, reply_ref}, turn) do
  turn = Absynthe.send_message(turn, reply_ref, {:status, state.status})
  {state, turn}
end
```

## Facets for scoped lifetimes

Use facets to model sessions, agents, or tools. When a facet terminates, all owned entities and assertions are cleaned up.

- One facet per conversation or client.
- Spawn related entities in that facet.
- Terminate the facet to clean up assertions and observers together.

## Capability attenuation for tool access

Refs are capabilities. Use attenuation to restrict what a caller can assert or message.

```elixir
alias Absynthe.Core.{Ref, Caveat}

pattern = Absynthe.record(:ToolCall, [Absynthe.capture(:name), Absynthe.capture(:args)])
template = {:record, {{:symbol, "ToolCall"}, [{:ref, 0}, {:ref, 1}]}}

{:ok, limited_ref} = Ref.attenuate(tool_ref, Caveat.rewrite(pattern, template))
```

This lets you hand out a safe ref that only permits certain shapes of requests.

## Synth example: agents and tool capabilities

In synth, agents and tools are entities. A tool router can accept `ToolCall` messages
with a reply ref embedded in the payload:

```elixir
call = Absynthe.record(:ToolCall, ["fs.read", %{path: "/etc/hosts"}, {:embedded, reply_ref}])
Absynthe.send_to(actor, tool_ref, call)
```

To hand an agent a limited capability, attenuate the tool router ref to only allow
specific tool names:

```elixir
alias Absynthe.Core.{Ref, Caveat}

fs_read_pattern =
  Absynthe.record(:ToolCall, ["fs.read", Absynthe.capture(:args), Absynthe.capture(:reply)])

fs_read_template =
  {:record, {:lit, {:symbol, "ToolCall"}},
   [{:lit, {:string, "fs.read"}}, {:ref, 0}, {:ref, 1}]}

web_search_pattern =
  Absynthe.record(:ToolCall, ["web.search", Absynthe.capture(:args), Absynthe.capture(:reply)])

web_search_template =
  {:record, {:lit, {:symbol, "ToolCall"}},
   [{:lit, {:string, "web.search"}}, {:ref, 0}, {:ref, 1}]}

caveat =
  Caveat.alts([
    Caveat.rewrite(fs_read_pattern, fs_read_template),
    Caveat.rewrite(web_search_pattern, web_search_template)
  ])

{:ok, limited_tool_ref} = Ref.attenuate(tool_router_ref, caveat)
```

Any other tool name or message shape is rejected by the attenuation chain. The tool
can respond by sending a message to the embedded `reply_ref`.

## Offload long work

Callbacks should be fast and deterministic. For long work:

- Spawn a Task.
- Send the result back as a message.
- Store a request id in state to drop stale results.

## Messages vs assertions

- Use assertions for shared, persistent state or subscriptions.
- Use messages for commands, queries, and one-off responses.

## Asynchronous delivery

Because delivery is asynchronous, do not expect immediate visibility of actions within the same callback. If you need a barrier, use `sync/3` and handle `on_sync/3` responses.

## Suggested reading

- `README.md` for the conceptual primer.
- `lib/absynthe/core/actor.ex` for turn and delivery semantics.
- `examples/` for runnable patterns.
