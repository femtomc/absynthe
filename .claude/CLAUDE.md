# Absynthe - Syndicated Actor Model for Elixir

An implementation of Tony Garnock-Jones' Syndicated Actor Model with Preserves data format, dataspaces, and reactive dataflow.

## Quick Reference

```bash
# Build & test
mix compile          # Compile the project
mix test             # Run tests
mix format           # Format code (always run before commits)
mix docs             # Generate documentation

# Issue tracking (bd/beads)
bd list              # List all issues
bd show <id>         # Show issue details
bd create "title"    # Create new issue
bd update <id> --status in_progress
bd close <id>        # Close an issue
bd ready             # Show ready work (no blockers)
bd sync              # Sync with git remote
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

## Issue Tracking with `bd` (Beads)

This project uses `bd` for issue tracking. Issues are stored in `.beads/issues.jsonl` and sync with git.

### Common Workflows

```bash
# View issues
bd list                    # All open issues
bd list --status closed    # Closed issues
bd list --label bug        # Filter by label
bd ready                   # Issues ready to work on

# Create issues
bd create "Add feature X" -t feature -p 1
bd create "Fix bug Y" -t bug -p 0 --label urgent

# Work on issues
bd update absynthe-abc --status in_progress
bd comment absynthe-abc "Working on this now"
bd close absynthe-abc

# Dependencies
bd dep add absynthe-abc --depends-on absynthe-xyz
bd blocked                 # Show blocked issues

# Sync (use --flush-only, see bug below)
bd sync --flush-only       # Safe: export only, no import
bd info                    # Show database info
```

### Issue ID Format

Issues use prefix + hash format: `absynthe-abc`, `absynthe-xyz`

### Known Bug: `bd sync` deletes newly created issues

**Problem:** `bd sync` runs `git-history-backfill` during import which incorrectly
deletes newly added issues, treating them as "recovered from history" and adding
them to `deletions.jsonl`. This happens even when `--no-git-history` flag or
config settings are used.

**Workaround:** Use `--flush-only` instead of full sync:

```bash
# Safe sync workflow
bd sync --flush-only       # Export only, no destructive import

# If you need to import new issues from JSONL:
bd import --ignore-deletions --no-git-history < .beads/issues.jsonl
bd sync --flush-only
```

**Config settings (set but not honored by sync):**
```bash
bd config set sync.no_git_history true
bd config set import.no_git_history true
```

This is a bug in beads v0.29.0 - the config keys exist but aren't passed through
to the sync command's internal import.

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
