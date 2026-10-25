# mbse-dynamodb

Persistence in DynamoDB for any mbse-schemas store, so that a service's data outlives its process with no app-specific
storage code.

## Why

mbse-rest's service holds its store in memory and executes transactions over it, which is fast and simple, but a
store in memory is lost when the process ends. This package makes it last: each committed transaction that writes is
journaled to DynamoDB, with occasional snapshots of the whole store, and a service starting up reads the latest
snapshot and replays the journal after it (VISUALIZATION.md, FEA-006, CAP-013). It stores only mbse-schemas' plain
form, so it knows nothing of any app: the same table holds the WorkTracker or any other store.

It implements mbse-rest's persistence plugin, `mbse.Services.Rest.Persistence.Plugin` (mbse-rest `docs/CONTRACT.md`,
Persistence), as `mbse.Persistence.DynamoDB.Plugin.Plugin`. How the table holds a journal and snapshots, and the
decisions for review, are in [docs/DESIGN.md](docs/DESIGN.md).

It depends on [mbse-schemas](https://github.com/pitaman71/mbse-schemas) and
[mbse-rest](https://github.com/pitaman71/mbse-rest), and for tests and the demo on
[mbse-demo-apps](https://github.com/pitaman71/mbse-demo-apps)' WorkTracker, each a checkout beside this repository
pinned in [siblings.json](siblings.json).

## Using it

```python
from mbse.Persistence.DynamoDB import Plugin
from mbse.Services.Rest import Service

plugin = Plugin.Plugin("mbse-store", region="us-east-1")          # on AWS, with boto3's credentials
plugin = Plugin.Plugin("mbse-store", endpoint_url="http://localhost:8000", region="us-east-1")  # DynamoDB Local
plugin = Plugin.Plugin()                                           # every setting from MBSE_DYNAMODB_* (docs/DESIGN.md)
app = Service.app(installs, plugin)                                # mbse-rest's service, persisted
```

The table is made if absent (`create=False`, or `MBSE_DYNAMODB_CREATE=0`, not to). Several stores may share one table,
each under its own `store` name.

## Running the demo against DynamoDB Local

```sh
docker run -d --rm --name dynamodb-local -p 8000:8000 amazon/dynamodb-local    # DynamoDB on localhost:8000
cd python3 && uv sync
MBSE_DYNAMODB_ENDPOINT_URL=http://localhost:8000 AWS_REGION=us-east-1 \
AWS_ACCESS_KEY_ID=local AWS_SECRET_ACCESS_KEY=local \
  uv run --with uvicorn uvicorn --factory mbse.Persistence.DynamoDB.Demo:work_tracker
```

The first start runs the WorkTracker's installs and snapshots them into the table `mbse-store`; every write after is
journaled, and a restart reads it all back. DynamoDB Local accepts any credentials. `docker stop dynamodb-local`
stops it, and, as run here without a volume, forgets the table.

`Demo.work_tracker` serves mbse-rest's WorkTracker service with this plugin. With an mbse-rest whose service selects
its plugin by name (M1's `MBSE_REST_PLUGIN`), serve mbse-rest's own factory instead, with the same environment and
`MBSE_REST_PLUGIN=mbse.Persistence.DynamoDB.Plugin:Plugin` (docs/DESIGN.md, "How mbse-rest's service selects it").

## Testing it

```sh
cd python3 && uv sync && uv run pytest
```

The tests need no AWS account: DynamoDB is mocked by moto. They run the plugin's own cases and mbse-rest's
persistence cases with this plugin in place of `Memory`. With DynamoDB Local running, also run the test against it:

```sh
MBSE_DYNAMODB_LOCAL=http://localhost:8000 uv run pytest tests/test_local.py
```
