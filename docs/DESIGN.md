# Design: the DynamoDB persistence plugin

`mbse.Persistence.DynamoDB.Plugin.Plugin` implements mbse-rest's `Persistence.Plugin` (mbse-rest [`docs/CONTRACT.md`](../../mbse-rest/docs/CONTRACT.md),
Persistence) for mbse-basis [CAP-013: an in-memory store journaled to DynamoDB](../../mbse-basis/docs/capabilities/CAP-013.md) ([FEA-006: persistence](../../mbse-basis/docs/features/FEA-006.md)). The contract fixes what a plugin is given and gives back; this
page fixes how DynamoDB holds it.

## The table

One table, with on-demand capacity, holds any number of stores.

| Attribute | Key | Holds |
|---|---|---|
| `store` (S) | partition | the store's name, `store` by default |
| `key` (S) | sort | `J#<number>#<part>` for the journal, `S#<number>#<part>` for snapshots; numbers zero-padded (20 and 6 digits), so that keys sort as numbers |
| `data` (B) | | one part of the record: its plain form as compact UTF-8 JSON, at most `part_bytes` (350 000) bytes |
| `parts` (N) | | on part 0 only: how many parts the record has |
| `since` (N) | | on a snapshot's part 0 only: the journal's length when it was taken |

- **Two append-only logs.** The journal numbers each committed transaction that writes, from 0, in the order `journal`
  is called (commit order, as the contract requires); snapshots are numbered in the order taken. A partition's keys
  sort the journal in commit order, so `load` is two strongly consistent `Query`s: the latest snapshot (read
  backwards, keys only, to find the highest number with a part 0, then its parts), and the journal from its `since`
  on. Neither ever scans.
- **Parts, for the 400 KB item limit.** A record of any size is cut into parts of `part_bytes`; a WorkTracker
  snapshot of the seed is about 80 KB (one part), and the tests write records of hundreds of parts.
- **A record is there exactly when its part 0 is.** Parts 1 and on are written first, part 0 last, with the condition
  `attribute_not_exists(key)`. A write that fails partway leaves parts without a part 0, which `load` passes over and
  the next record of that number writes over (parts beyond the count its part 0 gives are passed over too). So a
  failed write leaves nothing `load` reads, without a transaction, which DynamoDB bounds at 100 items and 4 MB.
- **One writer.** `load` sets the numbers to write next; a second service writing the same store since then finds
  part 0 taken, and its write is refused (`RuntimeError`) rather than interleaved.
- **The plain form is kept as JSON, not as DynamoDB maps.** The plugin is storage only (CONTRACT.md, Decided 12): it
  never interprets mbse-schemas' plain form, and JSON keeps every value as mbse-schemas wrote it (DynamoDB's own
  number type would lose the difference between `1` and `1.0`, which mbse-schemas keeps strictly).

## Configuration

`Plugin()`, with no arguments, takes every setting from the environment, so that a service can make it by name; an
argument given overrides the environment (tests give them all).

| Variable | Argument | Default |
|---|---|---|
| `MBSE_DYNAMODB_TABLE` | `table` | `mbse-store` |
| `MBSE_DYNAMODB_STORE` | `store` (the partition key) | `store` |
| `MBSE_DYNAMODB_REGION` | `region` | boto3's own: `AWS_REGION`, `AWS_DEFAULT_REGION`, its config |
| `MBSE_DYNAMODB_ENDPOINT_URL` | `endpoint_url` | AWS's; `http://localhost:8000` for DynamoDB Local |
| `MBSE_DYNAMODB_CREATE` | `create` | `1`: make the table if absent (on-demand capacity), and wait until it is active; `0`, `false` or `no` not to |

Credentials are boto3's own (`AWS_ACCESS_KEY_ID`, `AWS_PROFILE`, ...). `client=` takes a boto3 client in place of
one the plugin makes, and `part_bytes=` (350 000) bounds each part. `Plugin.from_environment(environ, **options)` reads
a mapping given in place of the process's environment.

## Tests

- `tests/test_plugin.py`: the plugin's own cases over the WorkTracker in moto (no AWS account, no network).
- `tests/test_rest.py`: mbse-rest's persistence cases (`mbse.Services.Rest.PersistenceCases`), each given a maker of
  fresh plugins over tables of their own; and mbse-rest's service as the environment configures it, selecting this
  plugin by name, over one table twice, a first start and a restart.
- `tests/test_local.py`: against DynamoDB Local, run where `MBSE_DYNAMODB_LOCAL` gives its endpoint, skipped
  otherwise.

## How mbse-rest's service selects it

mbse-rest's service selects a plugin with
`MBSE_REST_PLUGIN=module:attribute`, calling the attribute with no arguments (`Service.plugin_of`). For this plugin:

```sh
MBSE_REST_PLUGIN=mbse.Persistence.DynamoDB.Plugin:Plugin
```

`mbse.Persistence.DynamoDB:Plugin`, the package's attribute, is the module `Plugin.py`, not the class, and is not
callable: the module path above names the class. `tests/test_plugin.py` makes the plugin by that name, as the service
would. Beyond that, the plugin needs nothing from the service: its settings are its own (`MBSE_DYNAMODB_*`, the AWS
environment). Since a snapshot writes the whole store, `MBSE_REST_SNAPSHOT_EVERY` (100 by default) sets how many journaled
transactions come between snapshots.

## Decided by the agent, for review

Where mbse-basis' capabilities and CONTRACT.md are silent, the simplest default consistent with them:

1. **One table, keyed by store and record**, rather than a table each for the journal and snapshots: one thing to
   make and configure, and several stores (demos, tests) may share it.
2. **Nothing is deleted.** A snapshot does not remove the journal before it or older snapshots: the history stays
   whole, a snapshot that fails partway falls back to the one before (with the journal from its `since`), and no
   write the plugin makes is destructive. Pruning, or a DynamoDB TTL on records older than the latest snapshot, is
   left for later.
3. **Parts of 350 000 bytes** of UTF-8 JSON, uncompressed, so that a record reads back as text in the console;
   compression (zlib) would cut cost and parts roughly tenfold, and can be added under a part 0 attribute.
4. **No `TransactWriteItems`**: atomicity comes from writing part 0 last, conditionally, so a record has no limit of
   100 items or 4 MB.
5. **Strongly consistent reads** for `load` and the numbers, so that a restart sees every write made before it.
6. **A second writer is refused**, not merged: CONTRACT.md's Decided 13 has one service, one lock, one store.
7. **The table is made when absent by default** (`create=True`, on-demand capacity), for demos; production sets
   `MBSE_DYNAMODB_CREATE=0` and makes the table with its own tooling.
8. *(Withdrawn in M4: `Demo.work_tracker` went once mbse-rest's service selected its plugin by name.)*
9. **mbse-demo-apps is a development dependency** (the WorkTracker, for tests and the demo), pinned in
   `siblings.json` like the others.
10. **Settings from `MBSE_DYNAMODB_*`**, read by `Plugin()` with no arguments (as M1's `MBSE_REST_PLUGIN` calls it),
    each overridden by an argument; the region falls back to boto3's own.
