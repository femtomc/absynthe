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

## Phase 3: Review Cycle

When you believe the implementation is complete:

1. Run `mix compile` and `mix test` one final time
2. Track which files you modified during implementation
3. Request a Codex review by running:
   ```bash
   codex review 'Review changes for issue <issue-id>: "<issue title>".

   Files modified:
   - path/to/file1.ex
   - path/to/file2.ex

   Be critical and thorough:
   - Look for bugs, logic errors, and incorrect assumptions
   - Question whether the approach is correct for the problem domain
   - Identify missing edge cases in tests (boundary conditions, error paths, concurrency)
   - Check for performance issues (unnecessary allocations, N+2 queries, inefficient algorithms)
   - Verify consistency with existing patterns in the codebase
   - Point out any untested code paths or missing test coverage
   - Challenge if the implementation matches specifications/semantics
   - Look for potential race conditions or process interaction issues'
   ```

   **Key principle**: Encourage Codex to be **skeptical and critical**, not just verify correctness.

   **Bad example** (too passive):
   ```bash
   codex review 'Check if this code is correct.'
   ```

   **Good example** (encourages critical thinking):
   ```bash
   codex review 'Review set pattern matching fix.

   Be critical:
   - Is using sorted order correct for unordered sets?
   - Are we missing size mismatch checks?
   - What happens with nested sets or duplicate elements?
   - Is sorting on every extraction a performance issue?'
   ```

4. Parse the review response:
   - If review finds **no issues**: Proceed to Phase 4
   - If review finds **issues**: Address the feedback and repeat Phase 3

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
- Do not skip the review cycle
- If you get stuck, add a comment with `bd comment <issue-id> "description of blocker"` and stop
- If tests fail repeatedly, investigate the root cause before continuing
- Always run `mix format` before requesting review
