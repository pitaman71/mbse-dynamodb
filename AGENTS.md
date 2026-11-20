# Guide for AI agents

mbse-schemas stores kept in DynamoDB: a store kept in DynamoDB, the lowest layer of a stack, which interprets each
transaction with an interpreter of its own over its index, its journal the record, no snapshot (mbse-basis [FEA-006: persistence](https://github.com/pitaman71/mbse-basis/blob/main/docs/features/FEA-006.md), [CAP-289: journals kept in DynamoDB](https://github.com/pitaman71/mbse-basis/blob/main/docs/capabilities/README.md#CAP-289)), which
mbse-rest's service serves ([CAP-290: a service's store stacked, with no snapshot](https://github.com/pitaman71/mbse-basis/blob/main/docs/capabilities/README.md#CAP-290)). Python, in `python3/mbse/Persistence/DynamoDB`
(`mbse.Persistence.DynamoDB`).

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

- `mbse/Persistence/DynamoDB/Store.py` is the store kept in DynamoDB, a `Builders.Store` holding the schemas alone,
  whose transaction is the table's, committed in one DynamoDB transaction: how the table holds it is its module
  docstring. `mbse/Persistence/DynamoDB/Layer.py` is its interpreter, a class of mbse-schemas'
  `Transactions.Interpreter`, coupled with it, whose hooks read the index within the transaction and hold its writes.
  Both use mbse-schemas' public interface alone, what every layer shares among it (README, Decided by an agent): no
  private name of it, nor a subclass overriding one (mbse-basis [CAP-306: a public store-below interface](https://github.com/pitaman71/mbse-basis/blob/main/docs/capabilities/README.md#CAP-306)),
  which `tests/test_layer.py` checks.
- `mbse/Persistence/DynamoDB/Table.py` holds the settings (`MBSE_DYNAMODB_*`) and the table.
- `tests/test_below.py` runs a sibling's notebook suite, mbse-schemas' BLW, from its checkout, case by case
  (`tests/notebooks.py`), the store kept in DynamoDB its layer below; `tests/test_rest.py` runs mbse-rest's service
  serving the store. A journal is not kept in a store: what the store's follow gives, a one-transaction
  `Changes.Journal`, is held to mbse-schemas' BLW, and positions and slices of it to mbse-journals, which the store
  uses.

## Commands

```sh
python3 ../mbse-basis/scripts/polyrepo.py pin   # the siblings beside this repository, pinned in siblings.json
cd python3 && uv sync && uv run coverage run -m pytest && uv run coverage report   # use uv, never pip; DynamoDB mocked by moto; 100% gate
MBSE_DYNAMODB_LOCAL=http://localhost:8000 uv run pytest tests/test_local.py   # against DynamoDB Local
```
