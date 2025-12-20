# Absynthe - Syndicated Actor Model for Elixir

An implementation of Tony Garnock-Jones' Syndicated Actor Model with Preserves data format, dataspaces, and reactive dataflow.

## Quick Reference

```bash
# Build & test
mix compile          # Compile the project
mix test             # Run tests
mix format           # Format code (always run before commits)
mix docs             # Generate documentation

# Issue tracking (tissue)
tissue list              # List all issues
tissue show <id>         # Show issue details
tissue new "title"       # Create new issue
tissue status <id> in_progress
tissue status <id> closed
tissue ready             # Show ready work (no blockers)
```

## Project Structure

```
lib/
  absynthe/
    application.ex       # OTP application
    core/
      actor.ex           # Actor implementation
      entity.ex          # Entity behaviour
      facet.ex           # Facet lifecycle
      turn.ex            # Turn-based execution
      ref.ex             # Actor references
    dataspace/
      dataspace.ex       # Dataspace coordination
      skeleton.ex        # Skeleton indexing
      pattern.ex         # Pattern matching
      observer.ex        # Observation handlers
    dataflow/
      field.ex           # Reactive fields
      registry.ex        # Field registry
    preserves/
      value.ex           # Preserves values
      compare.ex         # Value comparison
      pattern.ex         # Pattern syntax
      encoder/           # Binary/text encoding
      decoder/           # Binary/text decoding
    protocol/
      event.ex           # Event types
    assertions/
      handle.ex          # Assertion handles
      bag.ex             # Assertion bag
test/
  # Test files mirror lib/ structure
examples/
  # Usage examples
```

## Issue Tracking with `tissue`

This project uses `tissue` for issue tracking. Issues are stored in `.tissue/`.

### Common Workflows

```bash
# View issues
tissue list                      # All open issues
tissue list --status closed      # Closed issues
tissue list --tag bug            # Filter by tag
tissue ready                     # Issues ready to work on

# Create issues
tissue new "Add feature X" -t feature -p 1
tissue new "Fix bug Y" -t bug -p 1

# Work on issues
tissue status absynthe-abc in_progress
tissue comment absynthe-abc -m "Working on this now"
tissue status absynthe-abc closed

# Dependencies
tissue dep add absynthe-abc blocks absynthe-xyz
tissue deps absynthe-abc         # Show issue dependencies
```

### Issue ID Format

Issues use prefix + hash format: `absynthe-abc123xy`, `absynthe-xyz789ab`

## Architecture Overview

### Syndicated Actor Model

The Syndicated Actor Model is a variation of the actor model that emphasizes:

1. **Dataspaces**: Shared tuple spaces where actors can assert facts and observe patterns
2. **Assertions**: Facts that actors assert into dataspaces (not messages)
3. **Facets**: Lifecycle units within actors that group related assertions
4. **Turns**: Atomic units of computation that process events

### Key Concepts

- **Actor**: An isolated process that communicates via assertions
- **Dataspace**: A coordination medium where actors share assertions
- **Assertion**: A fact that exists as long as its facet is alive
- **Pattern**: A template for matching assertions in a dataspace
- **Turn**: A batch of operations executed atomically

### Preserves Data Format

Absynthe uses the Preserves data format for structured data:

- Self-describing binary and text formats
- Rich type system (atoms, strings, bytes, sequences, sets, maps, records)
- Pattern matching support
- Canonical comparison

## Elixir Style

See `.claude/ELIXIR_STYLE.md` for comprehensive Elixir style guidelines.

Key points:
- Always run `mix format` before commits
- Follow module organization order (behaviours, use, import, require, alias, attrs, struct, types, callbacks, public, private)
- Use pattern matching over conditionals
- Return `{:ok, result}` / `{:error, reason}` tuples
- Place `@spec` immediately before `def`

## Testing

Tests use ExUnit.

```bash
mix test                          # Run all tests
mix test test/absynthe/core/      # Run core tests only
mix test --only integration       # Tagged tests
```

### Test Structure

- Tests mirror the `lib/` directory structure
- Use descriptive test names that explain the behavior
- Group related assertions with `describe` blocks

### Writing Tests

```elixir
defmodule Absynthe.Core.ActorTest do
  use ExUnit.Case, async: true

  describe "spawn/2" do
    test "creates a new actor with the given behavior" do
      # test implementation
    end
  end
end
```

## Development Tips

### Debugging

```elixir
# In IEx
iex -S mix

# Inspect dataspace state
Absynthe.Dataspace.debug(dataspace_pid)
```

### Common Issues

**Mix compile errors**
```bash
mix deps.get
mix deps.compile
```

**Pattern matching issues**
- Check that patterns use the correct Preserves syntax
- Verify capture variables are bound correctly

## External Resources

- [Syndicate Lang](https://syndicate-lang.org/) - Original Syndicated Actor Model
- [Preserves Spec](https://preserves.gitlab.io/preserves/preserves.html) - Data format specification
- [Tony Garnock-Jones' Papers](http://www.ccs.neu.edu/home/tonyg/) - Academic background
