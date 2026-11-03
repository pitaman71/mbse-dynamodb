# Guide for AI agents

An in-memory store journaled to DynamoDB, each committed transaction a journal entry, with occasional snapshots (mbse-basis [FEA-006: persistence](https://github.com/pitaman71/mbse-basis/blob/main/docs/features/FEA-006.md), [CAP-013: an in-memory store journaled to DynamoDB](https://github.com/pitaman71/mbse-basis/blob/main/docs/capabilities/CAP-013.md)). Python, in `python3/mbse/Persistence/DynamoDB` (`mbse.Persistence.DynamoDB`).

## Read before any work

Agents must read these mbse-basis documents before doing any work:

- [The mbse basis](https://github.com/pitaman71/mbse-basis/blob/main/docs/README.md)
- [Principles](https://github.com/pitaman71/mbse-basis/blob/main/docs/PRINCIPLES.md)
- [Features](https://github.com/pitaman71/mbse-basis/blob/main/docs/features/README.md)
- [Capabilities](https://github.com/pitaman71/mbse-basis/blob/main/docs/capabilities/README.md)

These may be read selectively, on demand: [Vocabulary](https://github.com/pitaman71/mbse-basis/blob/main/docs/VOCABULARY.md) and
[Qualities](https://github.com/pitaman71/mbse-basis/blob/main/docs/QUALITIES.md).

Never write to `~/.claude/projects/*/memory/` or any hidden memory: what is worth keeping goes in the repository.

Resolve a link written `mbse:<repo>/<path>`, as skills and source comments write a link into another mbse
repository, to the first of these that exists: in a polyrepo checkout, `<repo root>/../<repo>/<path>`, where `<repo
root>` is this repository's root; for a link into the installed package's own repository, `<path>` without its
language directory (`python3/` or `typescript5/`), under the installed package's root (Python: the `site-packages`
directory that holds `mbse/`; npm: the package's directory), where the package ships that file; otherwise
`https://github.com/pitaman71/<repo>/blob/main/<path>`.

## This repository

- `mbse/Persistence/DynamoDB/Plugin.py` implements mbse-rest's persistence plugin protocol,
  `mbse.Services.Rest.Persistence.Plugin` (`load`, `journal`, `snapshot`).

## Commands

```sh
python3 ../mbse-basis/scripts/polyrepo.py pin   # the siblings beside this repository, pinned in siblings.json
cd python3 && uv sync && uv run pytest          # Python: use uv, never pip; DynamoDB mocked by moto
MBSE_DYNAMODB_LOCAL=http://localhost:8000 uv run pytest tests/test_local.py   # against DynamoDB Local
```
