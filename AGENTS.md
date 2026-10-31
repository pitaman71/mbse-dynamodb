# Guide for AI agents

An in-memory store journaled to DynamoDB, each committed transaction a journal entry, with occasional snapshots (mbse-basis [FEA-006: persistence](../mbse-basis/docs/features/FEA-006.md), [CAP-013: an in-memory store journaled to DynamoDB](../mbse-basis/docs/capabilities/CAP-013.md)). Python, in `python3/mbse/Persistence/DynamoDB` (`mbse.Persistence.DynamoDB`).

## Invariants

The conventions every mbse repository shares are stated once in mbse-basis; this file cites them and keeps
only what differs here.

- **Vocabulary**: [VOC-007: property](../mbse-basis/docs/VOCABULARY.md#VOC-007) and [VOC-026: schema](../mbse-basis/docs/VOCABULARY.md#VOC-026) for schema-governed things, qualified with whose it is where
  it meets a language's own term ([the vocabulary](../mbse-basis/docs/VOCABULARY.md), its introduction).
- **Models as data** are written in the builders' fluent syntax ([PRI-019: fluent builder syntax](../mbse-basis/docs/PRINCIPLES.md#PRI-019)); packages are laid out and
  capitalized alike ([PRI-030: one package layout](../mbse-basis/docs/PRINCIPLES.md#PRI-030)).
- **Documents**: navigation lines, the README opening with why, and what this file holds ([Rules for every document](../mbse-basis/docs/README.md#rules-for-every-document)).
- **Work**: [PRI-029: parallel work in polyrepo workspaces](../mbse-basis/docs/PRINCIPLES.md#PRI-029), so a sibling is changed only through a workspace that edits it, never from this
  checkout; [PRI-031: the version is the contract](../mbse-basis/docs/PRINCIPLES.md#PRI-031); [PRI-032: a sibling's gap is reported, not worked around](../mbse-basis/docs/PRINCIPLES.md#PRI-032) (mbse-schemas used as it stands); and [PRI-033: work lands by branch and pull request](../mbse-basis/docs/PRINCIPLES.md#PRI-033).
- **What this repository builds is specified in mbse-basis' features and capabilities**, cited by ID; where one is
  silent, decide and record it as the basis says ([Detail files](../mbse-basis/docs/README.md#detail-files)).
- **Siblings are checkouts beside this repository**, pinned by version and commit in `siblings.json`
  (`python3 ../mbse-basis/scripts/polyrepo.py pin`).
- **Tests run with pytest** from `python3/`: `uv sync && uv run pytest`. Use uv, never pip. `tests/test_plugin.py`
  runs the plugin's own cases over the WorkTracker in moto (no AWS account, no network); `tests/test_rest.py`, mbse-rest's
  persistence cases (`mbse.Services.Rest.PersistenceCases`), each given a maker of fresh plugins over tables of their
  own, and mbse-rest's service as the environment configures it, selecting this plugin by name, over one table twice,
  a first start and a restart; `tests/test_local.py`, against DynamoDB Local, run where `MBSE_DYNAMODB_LOCAL` gives
  its endpoint, skipped otherwise.
- **It implements mbse-rest's persistence plugin**, `mbse.Services.Rest.Persistence.Plugin` (`load`, `journal`,
  `snapshot`), as `mbse/Persistence/DynamoDB/Plugin.py`; [CAP-013: an in-memory store journaled to DynamoDB](../mbse-basis/docs/capabilities/CAP-013.md#the-persistence-plugin), as built, is the contract
  and `Persistence.Memory` the reference it must agree with. mbse-rest is a sibling checkout, pinned in `siblings.json`.
- Never write to `~/.claude/projects/*/memory/` or any hidden memory: what is worth keeping goes in the repository.
