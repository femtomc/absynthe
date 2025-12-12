# Work on Issues

You are working on the absynthe project. Follow this workflow:

## Phase 1: Select Issue

1. Run `bd ready` to see issues with no blockers
2. Select the highest priority issue (P0 > P1 > P2)
3. Run `bd show <issue-id>` to get full context
4. Run `bd update <issue-id> --status in_progress` to claim it

## Phase 2: Implement

1. Understand the issue requirements fully
2. Explore the codebase to understand relevant code
3. Plan your implementation approach
4. Implement the solution:
   - Write clean, idiomatic Elixir code
   - Follow the style guide in `.claude/ELIXIR_STYLE.md`
   - Run `mix format` before considering work complete
   - Run `mix test` to ensure tests pass
   - Add tests if the change warrants them

## Phase 3: Verify

When you believe the implementation is complete:

1. Run `mix compile` and `mix test` one final time
2. Run `mix format` to ensure formatting is correct
3. Review your changes for:
   - Correctness and edge cases
   - Code quality and Elixir conventions
   - Test coverage
   - Documentation updates if needed

## Phase 4: Close and Complete

1. Run `bd close <issue-id>` to close the issue
2. Commit and push your changes:
   - `git add <files>` - stage the files you changed
   - `git commit -m "fix: <description>"` - commit with a descriptive message
   - `bd sync` - sync beads changes
   - `git push` - push to remote
3. Summarize what was accomplished
4. Stop - do not pick up another issue unless requested

## Important Rules

- Work on ONE issue at a time
- If you get stuck, add a comment with `bd comment <issue-id> "description of blocker"` and stop
- If tests fail repeatedly, investigate the root cause before continuing
- Always run `mix format` before marking work complete
