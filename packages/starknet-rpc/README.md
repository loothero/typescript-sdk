# @apibara/starknet-rpc

Event-focused Starknet JSON-RPC helpers for indexers.

This package is the phase 1 RPC path for Starknet indexing. It uses:

- `starknet_getEvents` over HTTP for historical backfills.
- `starknet_subscribeEvents` over WebSocket for recent live indexing.
- Explicit event cursors.
- Explicit reorg notifications.

It does not use the Apibara DNA runtime as a block cache.

## Requirements

Use Starknet JSON-RPC v0.10 or newer endpoints, for example URLs ending in
`/rpc/v0_10` and `/ws/rpc/v0_10`.

The package targets modern Node.js runtimes with global `fetch` support. It
ships a default WebSocket client for Node environments, and callers can still
provide `webSocketFactory` when they need a custom transport.

This package relies on `block_number`, `transaction_index`, and `event_index`
from emitted events for duplicate-safe cursoring. Older RPC versions do not
include all of these fields. Pre-confirmed live indexing also requires a node
that includes those cursor fields in `starknet_subscribeEvents` notifications.
Pathfinder v0.10 provides them.

## Backfill

```ts
import { backfillEvents } from "@apibara/starknet-rpc";

const cursor = await loadCursor();

for await (const message of backfillEvents({
  url: process.env.STARKNET_RPC_URL!,
  fromBlock: { block_number: cursor?.blockNumber ?? 0 },
  toBlock: "latest",
  addresses: ["0x1234"],
  keys: [["0xabcdef"]],
  cursor,
  chunkSize: 1024,
})) {
  await db.transaction(async (tx) => {
    await insertEvent(tx, message.event);
    await saveCursor(tx, message.cursor);
  });
}
```

`cursor` means "last successfully persisted event". When a cursor is supplied,
the backfill starts from that block and skips events at or before the cursor.
Reorg handling should persist `message.rollbackCursor`; that cursor sorts before
the first event in the rollback block.

## Live Subscription

```ts
import { subscribeEvents } from "@apibara/starknet-rpc";

const subscription = subscribeEvents({
  url: process.env.STARKNET_WS_URL!,
  blockId: "latest",
  addresses: ["0x1234"],
  keys: [["0xabcdef"]],
  idleTimeoutMs: 60_000,
  maxQueueSize: 10_000,
  reconnect: {
    minDelayMs: 500,
    maxDelayMs: 10_000,
  },
});

try {
  for await (const message of subscription) {
    if (message.type === "reorg") {
      await db.transaction(async (tx) => {
        await rollbackFrom(tx, message.reorg.startingBlockNumber);
        await saveCursor(tx, message.rollbackCursor);
      });
      continue;
    }

    await persistEvent(message.event, message.cursor);
  }
} finally {
  await subscription.unsubscribe();
}
```

Subscriptions are for recent live indexing. If a node rejects the requested
`blockId` with `TooManyBlocksBack`, use HTTP backfill first and then subscribe
from a recent accepted head.

The WebSocket helpers close and reconnect when the connection is idle for
`idleTimeoutMs` milliseconds. Set `idleTimeoutMs: 0` to disable this watchdog.
Incoming messages are also bounded by `maxQueueSize`; if the consumer falls too
far behind, the subscription closes instead of buffering without limit. Use
`reconnect.maxAttempts` when a misconfigured endpoint should fail permanently
instead of retrying forever.

## Combined Stream

```ts
import { streamEvents } from "@apibara/starknet-rpc";

for await (const message of streamEvents({
  url: process.env.STARKNET_RPC_URL!,
  wsUrl: process.env.STARKNET_WS_URL!,
  fromBlock: { block_number: 0 },
  cursor: await loadCursor(),
  cursorFinalityStatus: await loadCursorFinalityStatus(),
  addresses: ["0x1234"],
  keys: [["0xabcdef"]],
})) {
  if (message.type === "reorg") {
    await db.transaction(async (tx) => {
      await rollbackFrom(tx, message.reorg.startingBlockNumber);
      await saveCursor(tx, message.rollbackCursor);
    });
    continue;
  }

  await db.transaction(async (tx) => {
    await insertEvent(tx, message.event);
    await saveCursor(tx, message.cursor);
  });
}
```

`streamEvents` performs an inclusive HTTP-to-WS handoff:

1. Reads the current accepted head.
2. Backfills with `starknet_getEvents` through that head.
3. Subscribes with `starknet_subscribeEvents` from the same head.
4. Deduplicates replayed events using the persisted cursor identity.

This avoids a missing-event window between HTTP and WebSocket indexing.

If the WebSocket handoff block has fallen outside the node's subscription
history window, `streamEvents` catches the `TooManyBlocksBack` response, runs
another HTTP catch-up pass to the latest accepted head, and retries the live
subscription from the newer block.

`streamEvents` retries transport-level HTTP failures with exponential backoff.
JSON-RPC errors are surfaced to the caller. Direct `getEvents` and
`backfillEvents` calls do not add a global rate limiter; callers indexing large
histories should choose a node and `chunkSize` appropriate for their rate limits.

## Apibara RPC Stream

`StarknetRpcStream` implements the same `RpcStreamConfig` shape used by
`@apibara/evm-rpc`:

```ts
import { createRpcClient } from "@apibara/protocol/rpc";
import { StarknetRpcStream } from "@apibara/starknet-rpc";

const client = createRpcClient(
  new StarknetRpcStream({
    url: process.env.STARKNET_RPC_URL!,
    getEventsRangeSize: 1_000n,
    headRefreshIntervalMs: 1_000,
  }),
);

for await (const message of client.streamData({
  filter: [
    {
      addresses: ["0x1234"],
      keys: [["0xabcdef"]],
    },
  ],
  startingCursor: { orderKey: 0n },
})) {
  // message.data.data contains event-focused StarknetRpcBlock values.
}
```

This adapter is for accepted event blocks fetched over HTTP. Use `streamEvents`
when the indexer needs the low-latency pre-confirmed WebSocket path.

Apibara CLI indexers can use the adapter without a DNA `streamUrl`:

```ts
import { defineIndexer } from "apibara/indexer";
import { StarknetRpcStream } from "@apibara/starknet-rpc";

export default defineIndexer(
  new StarknetRpcStream({
    url: process.env.STARKNET_RPC_URL!,
  }),
)({
  filter: {
    addresses: ["0x1234"],
    keys: [["0xabcdef"]],
  },
  async transform({ block }) {
    for (const event of block.events) {
      // Persist accepted events.
    }
  },
});
```

## Cursor Contract

Persist the cursor in the same transaction as the indexed rows derived from the
event:

```sql
create table indexer_cursor (
  id text primary key,
  block_number integer not null,
  transaction_index integer not null,
  transaction_hash text not null,
  event_index integer not null,
  finality_status text
);
```

Cursor ordering and resume semantics use:

```text
block_number + transaction_index + event_index
```

The stable event identity is:

```text
block_number + transaction_hash + event_index
```

`event_index` is scoped to the transaction, not the whole block. Do not use
`block_number + event_index` as a unique key.

When using the default pre-confirmed live stream, persist the event finality
next to the cursor and pass it back as `cursorFinalityStatus` on restart. If the
cursor finality is unknown and the live stream uses `PRE_CONFIRMED`,
`streamEvents` conservatively emits a synthetic rollback for the cursor block
and replays that block over HTTP before returning to WebSocket live indexing.
This prevents a missed reorg from skipping replacement events that share the
same `block_number + transaction_index + event_index` ordering.

If cursor finality is not persisted, this conservative replay happens on every
restart of a pre-confirmed stream. Rollback and transform handlers should be
idempotent, especially when they perform external side effects. Persisting
`finality_status` and passing `cursorFinalityStatus` avoids unnecessary
cursor-block rollbacks once the cursor is known to be accepted.

Pre-confirmed subscriptions may deliver the same stable event identity again
when finality or `block_hash` changes. The subscription helpers dedupe exact
replays, but they emit these finality updates so callers can update the
persisted row and cursor finality.

## Schema Recommendations

For event-derived tables, store:

- `block_number`
- `block_hash`
- `transaction_index`
- `transaction_hash`
- `event_index`
- contract address
- normalized selector or first key

Use a unique constraint on `(block_number, transaction_hash, event_index)` for
raw event rows. Domain tables can use their own keys, but they should also store
the event cursor fields that created or last updated each row, including
`transaction_index` for ordering.

## Reorg Rollback

On a reorg message, roll back all chain-derived rows where:

```sql
block_number >= starting_block_number
```

Update the persisted cursor to `message.rollbackCursor` in the same transaction
as the chain-row rollback:

```ts
await db.transaction(async (tx) => {
  await tx.sql`
    delete from indexed_events
    where block_number >= ${message.reorg.startingBlockNumber}
  `;

  await saveCursor(tx, message.rollbackCursor);
});
```

After the caller handles the rollback, `streamEvents` resumes from the reorg
starting block with HTTP backfill and then returns to WebSocket live indexing.
If a process restarts with a persisted pre-confirmed cursor beyond the current
accepted head, `streamEvents` emits a synthetic reorg message and resets its
dedupe state to `message.rollbackCursor` before subscribing live. This prevents
replacement events at the same cursor ordering from being skipped.
If the accepted head has already caught up to the cursor block, the synthetic
rollback starts at the cursor block and the HTTP backfill leg replays that block.

## Finality

Phase 1 is designed for low-latency Starknet event indexing:

- `backfillEvents` and the HTTP backfill leg of `streamEvents` use
  `starknet_getEvents` for accepted historical events.
- `subscribeEvents` and the live WebSocket leg of `streamEvents` subscribe with
  `finalityStatus: "PRE_CONFIRMED"` by default.

Callers can override `finalityStatus` when they need accepted-only live
indexing, but the default live path is pre-confirmed so applications can observe
new events as soon as the Starknet node publishes them over
`starknet_subscribeEvents`.

Pre-confirmed events can be reorged. Callers must persist cursors atomically
with indexed rows and honor reorg messages by rolling back all chain-derived rows
where `block_number >= starting_block_number`.

## Live Integration Tests

Optional live integration tests should read URLs from:

```sh
TEST_STARKNET_RPC_URL=...
TEST_STARKNET_WS_URL=...
```

Do not commit full-node URLs into tests or examples.
