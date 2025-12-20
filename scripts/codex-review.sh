#!/usr/bin/env bash
# codex-review.sh - Run Codex code reviewer
#
# Usage: ./scripts/codex-review.sh <issue-id> [commit]

set -euo pipefail

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <issue-id> [commit]"
    exit 1
fi

ISSUE_ID="$1"
COMMIT_ARG="${2:-}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}=== Codex Review for ${ISSUE_ID} ===${NC}"

# Get issue title
ISSUE_TITLE=$(tissue show "$ISSUE_ID" 2>&1 | sed -n 's/^Title: //p' || true)
if [[ -z "$ISSUE_TITLE" ]]; then
    echo -e "${RED}Error: Could not fetch issue ${ISSUE_ID}${NC}"
    exit 1
fi
echo "Issue: $ISSUE_TITLE"

# Determine commit SHA and diff spec
if [[ -n "$COMMIT_ARG" ]]; then
    COMMIT_SHA="$COMMIT_ARG"
    MODIFIED_FILES=$(git diff --name-only "${COMMIT_SHA}^..${COMMIT_SHA}" 2>/dev/null || true)
    DIFF_SPEC="${COMMIT_SHA}^..${COMMIT_SHA}"
else
    COMMIT_SHA=$(git rev-parse --short HEAD)
    MODIFIED_FILES=$(git diff --name-only HEAD 2>/dev/null || true)
    DIFF_SPEC="HEAD"
    if [[ -z "$MODIFIED_FILES" ]]; then
        MODIFIED_FILES=$(git diff --name-only HEAD~1 2>/dev/null || true)
        DIFF_SPEC="HEAD~1"
    fi
fi

if [[ -z "$MODIFIED_FILES" ]]; then
    echo -e "${RED}Error: No modified files detected${NC}"
    exit 1
fi

FILES_LIST=$(echo "$MODIFIED_FILES" | sed 's/^/- /')
FILES_JSON=$(echo "$MODIFIED_FILES" | jq -R -s -c 'split("\n") | map(select(length > 0))')

echo "Files: $MODIFIED_FILES"

# Temp file for output
TEMP_FILE=$(mktemp)
trap "rm -f $TEMP_FILE" EXIT

# Build prompt
PROMPT='You are a senior code reviewer for issue '"$ISSUE_ID"': "'"$ISSUE_TITLE"'" at commit '"$COMMIT_SHA"'.

Files modified:
'"$FILES_LIST"'

## Process

0. Review the issue, and think about it.
1. Run `git diff '"$DIFF_SPEC"'` to see changes
2. Read full content of modified files
3. Check `.claude/ELIXIR_STYLE.md` for conventions
4. Carefully think about the contributed code: your goal is to find problems and issues.

## Output Format

=== TISSUE_REVIEW_START ===
{
  "verdict": "LGTM" or "CHANGES_REQUESTED",
  "reviewer": "codex",
  "issue_id": "'"$ISSUE_ID"'",
  "commit": "'"$COMMIT_SHA"'",
  "timestamp": "<ISO-8601>",
  "summary": "<1-2 sentences>",
  "issues": [{"severity": "error|warning|info", "file": "<path>", "line": <n>, "message": "<desc>", "suggestion": "<fix>"}],
  "files_reviewed": '"$FILES_JSON"'
}
=== TISSUE_REVIEW_END ==='

echo -e "${YELLOW}Running Codex review...${NC}"
if codex exec --dangerously-bypass-approvals-and-sandbox "$PROMPT" > "$TEMP_FILE" 2>&1; then
    echo -e "${GREEN}Review complete${NC}"
else
    echo -e "${RED}Review failed${NC}"
    cat "$TEMP_FILE"
    exit 1
fi

# Extract JSON (get first block only, join lines for jq)
JSON=$(awk '/=== TISSUE_REVIEW_START ===/{found=1; next} /=== TISSUE_REVIEW_END ===/{if(found) exit} found' "$TEMP_FILE" | jq -c '.' 2>/dev/null || true)

if [[ -z "$JSON" ]]; then
    echo -e "${RED}Failed to extract review JSON${NC}"
    tail -50 "$TEMP_FILE"
    exit 1
fi

VERDICT=$(echo "$JSON" | jq -r '.verdict')
tissue comment "$ISSUE_ID" -m "[REVIEW:codex] $JSON"

echo -e "${BLUE}=== Result ===${NC}"
echo "Verdict: $VERDICT"

if [[ "$VERDICT" == "LGTM" ]]; then
    echo -e "${GREEN}Approved!${NC}"
    exit 0
else
    echo -e "${YELLOW}Changes requested. View: tissue show $ISSUE_ID${NC}"
    exit 1
fi
