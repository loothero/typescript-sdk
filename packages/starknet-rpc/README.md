# @apibara/starknet-rpc

Event-focused Starknet JSON-RPC helpers for indexers.

This package is the phase 1 RPC path for Starknet indexing. It uses:

- `starknet_getEvents` over HTTP for historical backfills.
- `starknet_subscribeEvents` over WebSocket for recent live indexing.
- Explicit event cursors.
- Explicit reorg notifications.

It does not use the Apibara DNA runtime as a block cache.

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

## Live Subscription

```ts
import { subscribeEvents } from "@apibara/starknet-rpc";

const subscription = subscribeEvents({
  url: process.env.STARKNET_WS_URL!,
  blockId: "latest",
  addresses: ["0x1234"],
  keys: [["0xabcdef"]],
});

try {
  for await (const message of subscription) {
    if (message.type === "reorg") {
      await rollbackFrom(message.reorg.startingBlockNumber);
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

## Combined Stream

```ts
import { streamEvents } from "@apibara/starknet-rpc";

for await (const message of streamEvents({
  url: process.env.STARKNET_RPC_URL!,
  wsUrl: process.env.STARKNET_WS_URL!,
  fromBlock: { block_number: 0 },
  cursor: await loadCursor(),
  addresses: ["0x1234"],
  keys: [["0xabcdef"]],
})) {
  if (message.type === "reorg") {
    await rollbackFrom(message.reorg.startingBlockNumber);
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

## Cursor Contract

Persist the cursor in the same transaction as the indexed rows derived from the
event:

```sql
create table indexer_cursor (
  id text primary key,
  block_number integer not null,
  transaction_index integer not null,
  transaction_hash text not null,
  event_index integer not null
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

After the caller handles the rollback, `streamEvents` resumes from the reorg
starting block with HTTP backfill and then returns to WebSocket live indexing.

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
