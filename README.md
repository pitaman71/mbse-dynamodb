# mbse-dynamodb

Persistence in DynamoDB for any mbse-schemas store, so that a service's data outlives its process with no app-specific storage code.

An in-memory store journaled to DynamoDB, each committed transaction a journal entry, with occasional snapshots (VISUALIZATION.md, FEA-006, CAP-013).

It depends on [mbse-schemas](https://github.com/pitaman71/mbse-schemas) as a checkout beside this repository, pinned in
[siblings.json](siblings.json).

```sh
cd python3 && uv sync && uv run pytest
```
