# Guide for AI agents

mbse-schemas stores kept in DynamoDB: a store kept in DynamoDB, below a lazy `Builders.Store`, its journal the record
and an index from which it reads one object, no snapshot (mbse-basis [FEA-006: persistence](https://github.com/pitaman71/mbse-basis/blob/main/docs/features/FEA-006.md), [CAP-289: journals kept in DynamoDB](https://github.com/pitaman71/mbse-basis/blob/main/docs/capabilities/README.md#CAP-289)), which
mbse-rest's service serves a store over ([CAP-290: a service's store stacked, with no snapshot](https://github.com/pitaman71/mbse-basis/blob/main/docs/capabilities/README.md#CAP-290)). Python, in `python3/mbse/Persistence/DynamoDB`
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

- `mbse/Persistence/DynamoDB/Store.py` is the store kept in DynamoDB, a store below (mbse-schemas' `Stores.Below`),
  which interprets each transaction in a `Builders.Store` of its own over its index: how the table holds it is its
  module docstring. It uses mbse-schemas' public interface alone: no private name of it, nor a subclass overriding one
  (mbse-basis [CAP-306: a public store-below interface](https://github.com/pitaman71/mbse-basis/blob/main/docs/capabilities/README.md#CAP-306)).
- `mbse/Persistence/DynamoDB/Table.py` holds the settings (`MBSE_DYNAMODB_*`) and the table.
- `tests/test_below.py` and `tests/test_journals.py` run siblings' notebook suites, mbse-schemas' BLW and
  mbse-journals' JRN, from their checkouts, case by case (`tests/notebooks.py`); `tests/test_rest.py` runs mbse-rest's
  service over the store.

## Commands

```sh
python3 ../mbse-basis/scripts/polyrepo.py pin   # the siblings beside this repository, pinned in siblings.json
cd python3 && uv sync && uv run coverage run -m pytest && uv run coverage report   # use uv, never pip; DynamoDB mocked by moto; 100% gate
MBSE_DYNAMODB_LOCAL=http://localhost:8000 uv run pytest tests/test_local.py   # against DynamoDB Local
```
