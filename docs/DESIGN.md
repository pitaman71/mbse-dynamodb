# Design: the DynamoDB persistence plugin

`mbse.Persistence.DynamoDB.Plugin.Plugin` implements mbse-rest's `Persistence.Plugin` (mbse-rest `docs/CONTRACT.md`,
Persistence) for VISUALIZATION.md's CAP-013 (FEA-006). The contract fixes what a plugin is given and gives back; this
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

`Plugin(table="mbse-store", *, store="store", region=None, endpoint_url=None, client=None, create=True,
part_bytes=350_000)`: `region` and `endpoint_url` go to boto3 (credentials are boto3's own); `create` makes the table
if absent and waits until it is active. `Plugin.from_environment()` reads `MBSE_DYNAMODB_TABLE`,
`MBSE_DYNAMODB_STORE`, `MBSE_DYNAMODB_ENDPOINT_URL` and `MBSE_DYNAMODB_CREATE` (`0` not to make the table), with the
region from `AWS_REGION`.

`Demo.work_tracker()` is mbse-rest's `Service.work_tracker` given `Plugin.from_environment()`: an ASGI factory that
serves the WorkTracker over DynamoDB until mbse-rest's service selects its plugin by configuration of its own (see
"What mbse-rest's service needs to select it", below).

## Tests

- `tests/test_plugin.py`: the plugin's own cases over the WorkTracker in moto (no AWS account, no network).
- `tests/test_rest.py`: mbse-rest's persistence cases (its `tests/test_service.py`), run unchanged from the sibling
  checkout, with this plugin in place of `Persistence.Memory`; see "Reusing mbse-rest's cases", below. It also runs
  `Demo.work_tracker` twice over one table, a first start and a restart.
- `tests/test_local.py`: against DynamoDB Local, run where `MBSE_DYNAMODB_LOCAL` gives its endpoint, skipped
  otherwise.

### Reusing mbse-rest's cases

Those cases make `Persistence.Memory()` themselves and look at its `snapshots` and `entries`, which are `Memory`'s
own, not the protocol's. `test_rest.py` loads mbse-rest's test module from the sibling and replaces, in that module
only, `Persistence` with mbse-rest's members and `Memory` set to `Recording`, this plugin over a mocked table whose
`snapshots` and `entries` are read back from the table. It also fails when mbse-rest adds a persistence case (one
that makes a `Memory`) not yet listed here.

That works, but it leans on two things that are not contract: the cases' names and the attributes of `Memory`. They
would be reusable as they are meant to be if mbse-rest's cases took the plugin from a fixture (`plugin`, defaulting to
`Memory()`, overridable by a dependent's `conftest.py`) and asserted through the protocol alone: `load()` for what is
held, and a count of snapshots taken by a wrapper the case makes, rather than `plugin.snapshots` and `plugin.entries`.

## What mbse-rest's service needs to select it

mbse-rest's service is given its plugin (`Service.app(installs, plugin, snapshot_every)`); it has no configuration
yet that chooses one. What this plugin needs from that configuration, and nothing more:

1. A way to name the plugin without mbse-rest importing mbse-dynamodb: for example an environment variable
   `MBSE_PERSISTENCE=mbse.Persistence.DynamoDB.Plugin:Plugin.from_environment`, a callable the service imports and
   calls with no arguments, `Memory` where it is unset.
2. The plugin's own settings stay the plugin's (`MBSE_DYNAMODB_*`, the AWS environment); the service passes nothing
   through.
3. `snapshot_every` configurable (for example `MBSE_SNAPSHOT_EVERY`), since with DynamoDB a snapshot costs a write
   of the whole store.

## Decided by the agent, for review

Where VISUALIZATION.md and CONTRACT.md are silent, the simplest default consistent with them:

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
8. **`Demo.work_tracker`** lives here, so the demo can run over DynamoDB before mbse-rest's service has a
   configuration that selects a plugin.
9. **mbse-demo-apps is a development dependency** (the WorkTracker, for tests and the demo), pinned in
   `siblings.json` like the others.
