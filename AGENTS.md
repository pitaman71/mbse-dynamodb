# Guide for AI agents

An in-memory store journaled to DynamoDB, each committed transaction a journal entry, with occasional snapshots (VISUALIZATION.md, FEA-006, CAP-013). Python, in `python3/mbse/Persistence/DynamoDB` (`mbse.Persistence.DynamoDB`).

## Invariants

- **Use mbse-schemas as it stands.** A gap found is reported to the user, not worked around here.
- **Siblings are checkouts beside this repository**, pinned by commit in `siblings.json`. Other agents may be working
  in them: never edit a sibling from here.
- **What this repository builds is specified in the workspace's `VISUALIZATION.md`**, by feature and capability ID;
  cite those IDs rather than restate them.
- **Tests run with pytest** from `python3/`: `uv sync && uv run pytest`. Use uv, never pip.
