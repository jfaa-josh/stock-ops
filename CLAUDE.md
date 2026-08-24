# Claude Code Instructions — stocks-ops

## Memory & Context Preservation

**Save context proactively during every conversation.** Context compression is aggressive — do not wait until the end of a session to save important information.

### When to save to memory:
- After any significant decision, architectural choice, or design discussion
- When the user explains *why* something is done a certain way
- When a bug is diagnosed and root-caused (save the cause, not just the fix)
- When the user corrects your approach or confirms it was right
- When starting a new feature or significant task (save the goal and approach)
- At natural milestones (e.g., after completing a subtask)

### What to save:
- User preferences and working style corrections → `feedback_*.md`
- Project decisions, goals, and motivations → `project_*.md`
- External resources and where to find things → `reference_*.md`

Memory files live in:
`C:\Users\JoshFody\.claude\projects\c--Python-Repositories-Backed-Up-to-Git-stocks-ops\memory\`

**Do not rely on chat history being available as your primary context source.** Write memories as if the next conversation starts completely cold — the mechanism below is a fallback for recovering an interrupted session, not a substitute for saving memory.

## Recovering context after a restart (chat-history fallback)

A Stop hook ([.claude/hooks/save_transcript.py](.claude/hooks/save_transcript.py)) copies the full transcript to `.claude/chat-history/<session_id>.jsonl` (and to `latest.jsonl`) every time a response finishes. **This is write-only** — nothing feeds it back into context automatically at session start.

If the user references prior work, a previous conversation, or says something implying continuity was broken (e.g. "after I rebooted", "we were just discussing...", "pick up where we left off"), and your own memory/context doesn't already cover it:

1. List `.claude/chat-history/` and find the most recent `*.jsonl` file that is **not** the current session's ID (check timestamps — `ls -la`).
2. These files are large; do not `Read` one in full. Use `Grep` for relevant keywords first, or parse it (e.g. via PowerShell `ConvertFrom-Json` line-by-line) to extract just the `message.content` text fields from the tail of the file.
3. Treat it as a raw transcript to mine for the specific facts you need — not something to summarize wholesale or announce reading in full.

## Project
See memory files for project context. Key files to be aware of:
- `src/stockops/` — main package
- `local_workflows/` — local (non-Docker) workflow scripts
- `pyproject.toml` — project metadata and dependencies
