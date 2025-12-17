#!/usr/bin/env bash
# triple-review.sh - Run all three code reviewers in parallel
#
# Usage: ./scripts/triple-review.sh <issue-id>
#
# This script:
# 1. Gets issue details and modified files automatically
# 2. Spawns Codex, Gemini, and Claude reviewers in parallel
# 3. Waits for all to complete
# 4. Shows the review results
#
# Each reviewer posts their findings via `bd comment`.

set -euo pipefail

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <issue-id>"
    echo "Example: $0 absynthe-abc"
    exit 1
fi

ISSUE_ID="$1"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== Triple Review for ${ISSUE_ID} ===${NC}"

# Get issue title
echo -e "${YELLOW}Fetching issue details...${NC}"
ISSUE_TITLE=$(bd show "$ISSUE_ID" 2>&1 | grep -v '^$' | head -1 | sed 's/^[^ ]* //')
if [[ -z "$ISSUE_TITLE" ]]; then
    echo -e "${RED}Error: Could not fetch issue ${ISSUE_ID}${NC}"
    exit 1
fi
echo "Issue: $ISSUE_TITLE"

# Get modified files (staged + unstaged vs HEAD, or vs HEAD~1 if nothing staged)
echo -e "${YELLOW}Detecting modified files...${NC}"
MODIFIED_FILES=$(git diff --name-only HEAD 2>/dev/null || true)
if [[ -z "$MODIFIED_FILES" ]]; then
    # Try comparing to previous commit if working tree is clean
    MODIFIED_FILES=$(git diff --name-only HEAD~1 2>/dev/null || true)
fi

if [[ -z "$MODIFIED_FILES" ]]; then
    echo -e "${RED}Error: No modified files detected${NC}"
    echo "Make sure you have uncommitted changes or a recent commit to review."
    exit 1
fi

# Format files as bullet list
FILES_LIST=$(echo "$MODIFIED_FILES" | sed 's/^/- /')
echo "Files to review:"
echo "$FILES_LIST"
echo ""

# Create temp directory for logs
TEMP_DIR=$(mktemp -d)
trap "rm -rf $TEMP_DIR" EXIT

#############################################
# CODEX REVIEW
#############################################
codex_review() {
    local prompt='You are a senior code reviewer performing a rigorous review for issue '"$ISSUE_ID"': "'"$ISSUE_TITLE"'".

Files modified:
'"$FILES_LIST"'

## Your Mindset

Adopt an ADVERSARIAL mindset. Your job is to find problems, not to approve code. Assume the implementation has bugs until proven otherwise. Do not rubber-stamp. A quick "LGTM" without deep analysis is a failure of your responsibility.

## Your Process

1. **Understand the context deeply**
   - Run `git diff HEAD~1` (or `git diff` if uncommitted) to see the changes
   - Read the FULL content of each modified file, not just the diff
   - Understand how this code integrates with the rest of the system
   - Read `.claude/ELIXIR_STYLE.md` to understand project conventions

2. **Think step-by-step about correctness**
   - Trace through the logic manually with concrete examples
   - Ask yourself: "What inputs would break this?"
   - Consider: Is the algorithm fundamentally correct for this problem?
   - Question every assumption the code makes

3. **Analyze deeply for these issues**

   **Logic & Correctness:**
   - Off-by-one errors in recursion and list operations
   - Pattern matching gaps (unhandled cases)
   - Incorrect boolean logic or inverted conditions
   - Race conditions in concurrent code
   - Nil/empty handling issues

   **Elixir-Specific Concerns:**
   - Missing error handling in `with` chains
   - Improper use of `!` functions without rescue
   - GenServer state corruption possibilities
   - Supervisor strategy appropriateness
   - Protocol implementation completeness
   - Improper use of `send`/`receive` patterns

   **Memory & Resources:**
   - Unbounded list/binary growth
   - ETS table leaks
   - Process leaks (spawned but never terminated)
   - Large binaries in process heap vs binary heap

   **Edge Cases & Boundaries:**
   - Empty inputs (empty lists, empty strings, nil)
   - Maximum values (huge lists, deep recursion)
   - Malformed or adversarial input
   - Unicode edge cases in string handling

   **Test Coverage:**
   - Are all code paths tested?
   - Are edge cases covered?
   - Do tests actually assert the right behavior?
   - Could tests pass while code is still broken?

4. **Write your review**
   Think carefully, then write your findings:
   - If issues found: `bd comment '"$ISSUE_ID"' "CODEX REVIEW: CHANGES REQUESTED\n\n<detailed findings with specific line references and explanations>"`
   - If genuinely no issues after deep analysis: `bd comment '"$ISSUE_ID"' "CODEX REVIEW: LGTM\n\n<summary of what you verified and why you are confident>"`'

    echo -e "${BLUE}[CODEX]${NC} Starting review..." >&2
    if codex exec --dangerously-bypass-approvals-and-sandbox "$prompt" > "$TEMP_DIR/codex.log" 2>&1; then
        echo -e "${GREEN}[CODEX]${NC} Review complete" >&2
    else
        echo -e "${RED}[CODEX]${NC} Review failed (exit code $?)" >&2
        cat "$TEMP_DIR/codex.log" >&2
    fi
}

#############################################
# GEMINI REVIEW
#############################################
gemini_review() {
    local prompt='You are a senior code reviewer performing a rigorous review for issue '"$ISSUE_ID"': "'"$ISSUE_TITLE"'".

Files modified:
'"$FILES_LIST"'

## CRITICAL CONSTRAINTS

**YOU ARE A REVIEWER ONLY. DO NOT:**
- Use `write_file` or `replace` tools
- Create, edit, or modify ANY files
- Run `git commit`, `git add`, or any git write operations
- Make ANY changes to the codebase
- Work on other issues or tasks

**YOU MAY ONLY:**
- Use `read_file` to read file contents
- Use `glob` and `search_file_content` to find and search files
- Run `git diff`, `git status`, `git log` via shell
- Run `bd comment` to post your review
- Run `bd show` to read issue details

If you find yourself wanting to fix something, STOP. Your job is to report issues, not fix them.

## Your Mindset

Be DEEPLY SKEPTICAL. Your purpose is to catch mistakes before they reach production. Do not be polite at the expense of correctness. Challenge the implementation. Ask "why?" at every decision point. A superficial review that misses bugs is worse than no review at all.

## Your Process

1. **Build complete understanding first**
   - Run `git diff HEAD~1` (or `git diff` if uncommitted) to see the changes
   - Read the ENTIRE content of each modified file for full context
   - Understand the problem being solved and whether this solution is appropriate
   - Check `.claude/ELIXIR_STYLE.md` for project conventions

2. **Reason carefully about the logic**
   - Walk through the code with specific test cases in your head
   - Ask: "What is the worst-case input? What happens then?"
   - Ask: "What invariants must hold? Are they maintained?"
   - Ask: "Is this the right abstraction? Is it over/under-engineered?"

3. **Investigate these areas thoroughly**

   **Algorithmic Correctness:**
   - Is the approach fundamentally sound?
   - Are there subtle logic errors in conditionals or pattern matches?
   - Could reordering operations cause issues?
   - Are recursive functions properly bounded?

   **Elixir/OTP Safety:**
   - Pattern matches must be exhaustive or explicitly handle fallback
   - `with` chains must handle all error paths
   - GenServer callbacks must not crash on unexpected messages
   - Supervisors must have appropriate restart strategies
   - No blocking calls in GenServer that could cause timeouts

   **Error Handling:**
   - Are all error cases handled appropriately?
   - Is error propagation correct through `with`/`case`?
   - Are error tuples `{:error, reason}` used consistently?
   - No silently swallowed errors

   **Robustness:**
   - Empty/nil/zero inputs
   - Extremely large inputs (lists, binaries)
   - Malicious or malformed inputs
   - Concurrent access concerns
   - Process crash recovery

   **Test Quality:**
   - Do tests cover the happy path AND error paths?
   - Are boundary conditions tested?
   - Could a buggy implementation still pass these tests?
   - Are assertions checking the right things?

4. **Deliver your verdict**
   After thorough analysis:
   - If problems found: `bd comment '"$ISSUE_ID"' "GEMINI REVIEW: CHANGES REQUESTED\n\n<specific issues with file:line references and clear explanations>"`
   - If confident after deep review: `bd comment '"$ISSUE_ID"' "GEMINI REVIEW: LGTM\n\n<explanation of what you verified and your reasoning>"`'

    echo -e "${BLUE}[GEMINI]${NC} Starting review..." >&2
    if gemini --model gemini-3-pro-preview --allowed-tools read_file,glob,search_file_content,list_directory,run_shell_command "$prompt" > "$TEMP_DIR/gemini.log" 2>&1; then
        echo -e "${GREEN}[GEMINI]${NC} Review complete" >&2
    else
        echo -e "${RED}[GEMINI]${NC} Review failed (exit code $?)" >&2
        cat "$TEMP_DIR/gemini.log" >&2
    fi
}

#############################################
# CLAUDE REVIEW (Design/Organization)
#############################################
claude_review() {
    local prompt='You are a senior architect reviewing code organization for issue '"$ISSUE_ID"': "'"$ISSUE_TITLE"'".

Files modified:
'"$FILES_LIST"'

## CRITICAL CONSTRAINTS

**YOU ARE A REVIEWER ONLY. DO NOT:**
- Create, edit, or modify ANY files
- Run `git commit`, `git add`, or any git write operations
- Make ANY changes to the codebase
- Work on other issues or tasks

**YOU MAY ONLY:**
- Read files to understand code structure
- Run `git diff`, `git status`, `git log`
- Run `bd comment` to post your review
- Run `bd show` to read issue details

## Your Mindset

Focus on SOFTWARE DESIGN, not correctness. Assume the code works—your job is to evaluate whether it is well-structured, maintainable, and appropriately abstracted. Be critical of complexity. Simple solutions that work are better than clever solutions.

## Your Process

1. **Understand the change in context**
   - Run `git diff HEAD~1` (or `git diff` if uncommitted) to see what changed
   - Read the full modified files to understand structure
   - Look at surrounding code to understand existing patterns
   - Check `.claude/ELIXIR_STYLE.md` for project conventions

2. **Evaluate organization and modularity**

   **Module Structure:**
   - Does each module have a clear, single responsibility?
   - Are related functions grouped logically?
   - Is the public API minimal and well-defined?
   - Could this be split or should it be merged with something else?

   **Abstraction Quality:**
   - Is the abstraction level appropriate for the problem?
   - Are there unnecessary layers of indirection?
   - Is there premature generalization (YAGNI violation)?
   - Is there under-abstraction causing duplication?

   **Coupling & Cohesion:**
   - Do modules depend on implementation details they should not?
   - Are dependencies explicit and minimal?
   - Do things that change together live together?

   **Interface Design:**
   - Are function signatures clear and minimal?
   - Do types and specs communicate intent?
   - Are there too many parameters (struct/keyword list needed)?
   - Is the API easy to use correctly, hard to misuse?

   **Code Duplication:**
   - Is there copy-paste that should be extracted?
   - Conversely, is there forced reuse that hurts clarity?

3. **Deliver your verdict**
   After thorough analysis:
   - If design issues found: `bd comment '"$ISSUE_ID"' "CLAUDE REVIEW (DESIGN): CHANGES REQUESTED\n\n<specific issues with suggestions for restructuring>"`
   - If design is sound: `bd comment '"$ISSUE_ID"' "CLAUDE REVIEW (DESIGN): LGTM\n\n<summary of what makes the design good>"`'

    echo -e "${BLUE}[CLAUDE]${NC} Starting review..." >&2
    if claude --dangerously-skip-permissions "$prompt" > "$TEMP_DIR/claude.log" 2>&1; then
        echo -e "${GREEN}[CLAUDE]${NC} Review complete" >&2
    else
        echo -e "${RED}[CLAUDE]${NC} Review failed (exit code $?)" >&2
        cat "$TEMP_DIR/claude.log" >&2
    fi
}

#############################################
# MAIN - Run all three in parallel
#############################################
echo -e "${YELLOW}Starting all three reviewers in parallel...${NC}"
echo ""

# Run all three in background
codex_review &
CODEX_PID=$!

gemini_review &
GEMINI_PID=$!

claude_review &
CLAUDE_PID=$!

# Wait for all to complete
echo "Waiting for reviews to complete..."
echo "  Codex PID: $CODEX_PID"
echo "  Gemini PID: $GEMINI_PID"
echo "  Claude PID: $CLAUDE_PID"
echo ""

wait $CODEX_PID
CODEX_STATUS=$?

wait $GEMINI_PID
GEMINI_STATUS=$?

wait $CLAUDE_PID
CLAUDE_STATUS=$?

echo ""
echo -e "${BLUE}=== Review Summary ===${NC}"

# Report status
if [[ $CODEX_STATUS -eq 0 ]]; then
    echo -e "  ${GREEN}✓${NC} Codex review completed"
else
    echo -e "  ${RED}✗${NC} Codex review failed"
fi

if [[ $GEMINI_STATUS -eq 0 ]]; then
    echo -e "  ${GREEN}✓${NC} Gemini review completed"
else
    echo -e "  ${RED}✗${NC} Gemini review failed"
fi

if [[ $CLAUDE_STATUS -eq 0 ]]; then
    echo -e "  ${GREEN}✓${NC} Claude review completed"
else
    echo -e "  ${RED}✗${NC} Claude review failed"
fi

echo ""
echo -e "${YELLOW}Fetching review comments...${NC}"
echo ""

# Show the issue with all comments
bd show "$ISSUE_ID"

echo ""
echo -e "${BLUE}=== Next Steps ===${NC}"
echo "1. Read the reviews above"
echo "2. If any reviewer requested changes, address them and re-run:"
echo "   ./scripts/triple-review.sh $ISSUE_ID"
echo "3. If all three say LGTM, proceed to commit"
