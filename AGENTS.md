# Guide for AI agents

An in-memory store journaled to DynamoDB, each committed transaction a journal entry, with occasional snapshots (mbse-basis [FEA-006: persistence](../mbse-basis/docs/features/FEA-006.md), [CAP-013: an in-memory store journaled to DynamoDB](../mbse-basis/docs/capabilities/CAP-013.md)). Python, in `python3/mbse/Persistence/DynamoDB` (`mbse.Persistence.DynamoDB`).

## Invariants

The conventions every mbse repository shares are stated once in mbse-basis; this file cites them and keeps
only what differs here.

- **Vocabulary**: [VOC-007: property](../mbse-basis/docs/VOCABULARY.md#VOC-007) and [VOC-026: schema](../mbse-basis/docs/VOCABULARY.md#VOC-026), each qualified with whose it is where
  it meets a language's own term ([the vocabulary](../mbse-basis/docs/VOCABULARY.md), its introduction).
- **Models and packages**: [PRI-019: fluent builder syntax](../mbse-basis/docs/PRINCIPLES.md#PRI-019) and [PRI-030: one package layout](../mbse-basis/docs/PRINCIPLES.md#PRI-030).
- **Documents**: navigation lines, the README opening with why, and what this file holds ([Rules for every document](../mbse-basis/docs/README.md#rules-for-every-document)).
- **Work**: [PRI-029: parallel work in polyrepo workspaces](../mbse-basis/docs/PRINCIPLES.md#PRI-029), [PRI-031: the version is the contract](../mbse-basis/docs/PRINCIPLES.md#PRI-031), [PRI-032: a sibling's gap is reported, not worked around](../mbse-basis/docs/PRINCIPLES.md#PRI-032) and [PRI-033: work lands by branch and pull request](../mbse-basis/docs/PRINCIPLES.md#PRI-033).
- **What this repository builds is specified in mbse-basis' features and capabilities**, cited by ID ([Detail files](../mbse-basis/docs/README.md#detail-files)).
- **It implements mbse-rest's persistence plugin**, `mbse.Services.Rest.Persistence.Plugin` (`load`, `journal`,
  `snapshot`), as `mbse/Persistence/DynamoDB/Plugin.py`; [CAP-013: an in-memory store journaled to DynamoDB](../mbse-basis/docs/capabilities/CAP-013.md#the-persistence-plugin), as built, is the contract
  and `Persistence.Memory` the reference it must agree with.
- Never write to `~/.claude/projects/*/memory/` or any hidden memory: what is worth keeping goes in the repository.

## Commands

```sh
python3 ../mbse-basis/scripts/polyrepo.py pin   # the siblings beside this repository, pinned in siblings.json
cd python3 && uv sync && uv run pytest          # Python: use uv, never pip; DynamoDB mocked by moto
MBSE_DYNAMODB_LOCAL=http://localhost:8000 uv run pytest tests/test_local.py   # against DynamoDB Local
```
