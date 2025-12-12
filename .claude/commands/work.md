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
   codex exec --dangerously-bypass-approvals-and-sandbox 'You are reviewing code changes for issue <issue-id>: "<issue title>".

   Files modified:
   - path/to/file1.ex
   - path/to/file2.ex

   Your task:
   1. Run `git diff HEAD~1` to see the changes
   2. Read the modified files to understand the full context
   3. Be critical and thorough in your review:
      - Look for bugs, logic errors, and incorrect assumptions
      - Question whether the approach is correct for the problem domain
      - Identify missing edge cases in tests (boundary conditions, error paths, concurrency)
      - Check for performance issues (unnecessary allocations, N+2 queries, inefficient algorithms)
      - Verify consistency with existing patterns in the codebase
      - Point out any untested code paths or missing test coverage
      - Challenge if the implementation matches specifications/semantics
      - Look for potential race conditions or process interaction issues
   4. Write your review summary to the issue:
      - If you find issues: `bd comment <issue-id> "REVIEW: [issues found]\n\n<your detailed review>"`
      - If no issues: `bd comment <issue-id> "REVIEW: LGTM\n\n<brief summary of what was reviewed>"`'
   ```

   **Key principle**: Encourage Codex to be **skeptical and critical**, not just verify correctness.

   **Good example** (encourages critical thinking):
   ```bash
   codex exec --dangerously-bypass-approvals-and-sandbox 'You are reviewing code changes for issue absynthe-xyz: "Fix set pattern matching".

   Files modified:
   - lib/absynthe/dataspace/pattern.ex
   - test/absynthe/dataspace/pattern_test.exs

   Be especially critical about:
   - Is using sorted order correct for unordered sets?
   - Are we missing size mismatch checks?
   - What happens with nested sets or duplicate elements?
   - Is sorting on every extraction a performance issue?

   Write your findings to the issue with `bd comment absynthe-xyz "REVIEW: ..."`'
   ```

4. Check the review:
   - Run `bd show <issue-id>` to see the review comment
   - If review finds **issues**: Address the feedback and repeat Phase 3
   - If review says **LGTM**: Proceed to Phase 4

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
