# Design notes (M1)

## Network model
Discrete-event channel applying partition windows, stochastic loss, latency/jitter, duplicate delivery, and reorder buffers. Partition drops do not burn retry budget.

## Clients
- **cloud_only:** single-shot send; failures are user-visible losses.
- **naive_retry_queue:** in-memory queue; retries without idempotency; crash clears queue.
- **durable_idempotent_queue:** durable log; idempotency keys; resume after crash; version conflict detection when server concurrent edits or reorder skew apply.

## Scenarios
Each scenario holds initial inspection records, local mutations (with idempotency keys + base versions), optional server concurrent mutations, and a fault profile.
