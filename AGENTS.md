# Guide for AI agents

An in-memory store journaled to DynamoDB, each committed transaction a journal entry, with occasional snapshots (mbse-basis [FEA-006: persistence](../mbse-basis/docs/features/FEA-006.md), [CAP-013: an in-memory store journaled to DynamoDB](../mbse-basis/docs/capabilities/CAP-013.md)). Python, in `python3/mbse/Persistence/DynamoDB` (`mbse.Persistence.DynamoDB`).

## Read before any work

Agents must read these mbse-basis documents before doing any work:

- [The mbse basis](../mbse-basis/docs/README.md)
- [Principles](../mbse-basis/docs/PRINCIPLES.md)
- [Features](../mbse-basis/docs/features/README.md)
- [Capabilities](../mbse-basis/docs/capabilities/README.md)

These may be read selectively, on demand: [Vocabulary](../mbse-basis/docs/VOCABULARY.md) and
[Qualities](../mbse-basis/docs/QUALITIES.md).

Never write to `~/.claude/projects/*/memory/` or any hidden memory: what is worth keeping goes in the repository.

## This repository

- `mbse/Persistence/DynamoDB/Plugin.py` implements mbse-rest's persistence plugin protocol,
  `mbse.Services.Rest.Persistence.Plugin` (`load`, `journal`, `snapshot`).

## Commands

```sh
python3 ../mbse-basis/scripts/polyrepo.py pin   # the siblings beside this repository, pinned in siblings.json
cd python3 && uv sync && uv run pytest          # Python: use uv, never pip; DynamoDB mocked by moto
MBSE_DYNAMODB_LOCAL=http://localhost:8000 uv run pytest tests/test_local.py   # against DynamoDB Local
```
