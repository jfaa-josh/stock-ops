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

**Do not rely on chat history being available.** Write memories as if the next conversation starts completely cold.

## Project
See memory files for project context. Key files to be aware of:
- `src/stockops/` — main package
- `local_workflows/` — local (non-Docker) workflow scripts
- `pyproject.toml` — project metadata and dependencies
