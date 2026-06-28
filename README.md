# open-automations

Deterministic automation control plane and daemon for Hasna open-source apps.

`open-automations` is the real automation product surface. It owns automation
specs, trigger materialization, durable run/action queue state, replay requests,
daemon leases, and release-grade audit boundaries. It uses `@hasna/actions` as
the action contract layer and can hand execution to runtime providers such as
OpenLoops without turning those runtimes into the automation product.

## Package

```sh
bun add @hasna/automations @hasna/actions
```

```ts
import { AutomationsStore, exampleAutomationSpec } from "@hasna/automations";

const store = new AutomationsStore();
store.createAutomation(exampleAutomationSpec());
console.log(store.status());
store.close();
```

## CLI

```sh
automations --help
automations --json status
automations --json spec example
automations --json validate automation.json
automations --json create automation.json
automations --json list
automations --json simulate automation.json --persist --event-json '{"id":"evt_1","source":"open-events","type":"ticket.created","data":{"priority":"critical"}}'
automations --json queue claim --runner worker-1
automations --json queue fail <action-id> --code UPSTREAM_500 --message "upstream failed"
automations --json dlq list
automations --json dlq replay <action-id>
automations --json runtimes
automations-daemon --json status
automations-daemon --json run
automations-daemon --json run --once
```

The default data root is `~/.hasna/automations`. Override it with
`HASNA_AUTOMATIONS_DIR` or `AUTOMATIONS_DATA_DIR`.

`automations-daemon run` stays alive and maintains the local daemon lease until
it receives `SIGINT` or `SIGTERM`. Use `--once` for smoke checks and tests.

## Boundaries

- `open-actions` defines portable action manifests and invocation contracts.
- `open-events` is trigger ingress.
- `open-automations` materializes triggers into durable automation runs and
  queued action work.
- `open-loops` can be a runtime binding, but it is not the automation product.

## Runtime Model

The local store enforces idempotent event-to-run materialization and idempotent
run-step queue rows. Queue workers claim available actions with a lease, mark
them succeeded, retryable, or dead, and can replay dead actions through the DLQ
surface. Event ingestion accepts OpenEvents-compatible envelopes structurally,
so the OpenEvents package remains the trigger ingress boundary.

## Integration Contracts

OpenEvents deliveries are input, not durable automation state. OpenAutomations
uses `event.dedupeKey` first and falls back to `event.id` when building
event-to-run and event-to-action idempotency keys. Replaying the same event
through OpenEvents therefore returns the existing run/action rows unless the
operator creates an explicit replay request through OpenAutomations.

OpenLoops is an execution runtime binding, not the scheduler or control plane
for automations. A runtime worker claims queued actions with:

```sh
automations queue claim --runner open-loops:<worker-id>
```

It must complete or fail the same action with the same runner id before the
lease expires:

```sh
automations queue complete <action-id> --runner open-loops:<worker-id>
automations queue fail <action-id> --runner open-loops:<worker-id> --code <code> --message <message>
```

The queue enforces runner ownership and live leases for completion/failure, so
stale workers cannot finalize reclaimed actions.
