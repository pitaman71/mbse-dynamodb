# Guide for AI agents

An in-memory store journaled to DynamoDB, each committed transaction a journal entry, with occasional snapshots (mbse-basis [FEA-006: persistence](../mbse-basis/docs/features/FEA-006.md), [CAP-013: an in-memory store journaled to DynamoDB](../mbse-basis/docs/capabilities/CAP-013.md)). Python, in `python3/mbse/Persistence/DynamoDB` (`mbse.Persistence.DynamoDB`).

## Invariants

- **Use mbse-schemas as it stands.** A gap found is reported to the user, not worked around here.
- **Siblings are checkouts beside this repository**, pinned by commit in `siblings.json`. Other agents may be working
  in them: never edit a sibling from here.
- **What this repository builds is specified in mbse-basis' features and capabilities**
  ([`mbse-basis/docs/features/`](../mbse-basis/docs/features), [`mbse-basis/docs/capabilities/`](../mbse-basis/docs/capabilities)), by ID; cite those IDs, each introduced
  by bridge language (mbse-basis [`docs/README.md`](../mbse-basis/docs/README.md), Rules for writing items, "Bridge, don't duplicate").
- **Tests run with pytest** from `python3/`: `uv sync && uv run pytest`. Use uv, never pip. `tests/test_plugin.py`
  runs the plugin's own cases over the WorkTracker in moto (no AWS account, no network); `tests/test_rest.py`, mbse-rest's
  persistence cases (`mbse.Services.Rest.PersistenceCases`), each given a maker of fresh plugins over tables of their
  own, and mbse-rest's service as the environment configures it, selecting this plugin by name, over one table twice,
  a first start and a restart; `tests/test_local.py`, against DynamoDB Local, run where `MBSE_DYNAMODB_LOCAL` gives
  its endpoint, skipped otherwise.
- **It implements mbse-rest's persistence plugin**, `mbse.Services.Rest.Persistence.Plugin` (`load`, `journal`,
  `snapshot`), as `mbse/Persistence/DynamoDB/Plugin.py`; [CAP-013: an in-memory store journaled to DynamoDB](../mbse-basis/docs/capabilities/CAP-013.md#the-persistence-plugin), as built, is the contract
  and `Persistence.Memory` the reference it must agree with. mbse-rest is a sibling checkout, pinned in `siblings.json`.

## Working agreement (M0, for every agent on mbse-rest, mbse-dynamodb, mbse-react and mbse-demo-apps)

- Work on a branch in each repository you change (M0's is `m0-contract`). Commit and push after every step whose
  tests pass (`git push -u origin <branch>`, or `python3 ../mbse-basis/scripts/polyrepo.py push` where it supports
  every sibling). Never commit to `main`. When done, open one pull request per repository with `gh pr create`,
  listing the decisions for review.
- Where the capabilities are silent, take the simplest default consistent with them and with mbse-schemas, record it
  in the detail file of the capability it serves, in mbse-basis' [`docs/capabilities/`](../mbse-basis/docs/capabilities), under "Decided by
  an agent, for review", labeled with this repository's name and the next number, and keep going.
- Stop and report (do not work around) for: a gap or bug in mbse-schemas; an outward-facing action beyond pushing
  branches and opening pull requests in these repositories; anything destructive (force-push, deleting branches or
  files outside your work, rewriting history); a contradiction in a capability that changes what gets built.
- Never edit a sibling from another repository's work; never edit mbse-schemas; in mbse-basis,
  edit only a capability's or feature's As built, Blocked and Decided sections, in a pull request beside this
  repository's.
- Never write to `~/.claude/projects/*/memory/` or any hidden memory: what is worth keeping goes in the repository.
- Conventions: Python with uv (`uv sync`, `uv run pytest`), never pip; package capitalization as mbse-schemas has it
  (`mbse.Services.Rest`); "property", never "field", and "schema", never "type", for schema-governed things. Models
  as data are written in the builders' fluent syntax, never wire format, never helpers that wrap builder syntax.
- End every commit message with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`, and every pull request's
  description with `🤖 Generated with [Claude Code](https://claude.com/claude-code)`.
