import { backfillEvents } from "./backfill";
import { getLatestBlock } from "./block-cache";
import { compareEventCursor } from "./cursor";
import { subscribeEvents } from "./subscribe";
import type {
  BlockId,
  EventCursor,
  EventSubscription,
  ReorgMessage,
  StreamEventsOptions,
  StreamMessage,
} from "./types";

export async function* streamEvents(
  options: StreamEventsOptions,
): AsyncGenerator<StreamMessage> {
  let lastYieldedCursor = options.cursor;
  let lastRealCursor = options.cursor;
  let backfillFromBlock = options.fromBlock;
  let backfillCursor = options.cursor;
  let subscription: EventSubscription | undefined;

  try {
    while (true) {
      throwIfAborted(options.signal);

      const head = await getLatestBlock({
        url: options.url,
        signal: options.signal,
      });
      const headBlockId: BlockId = { block_number: head.blockNumber };

      for await (const message of backfillEvents({
        url: options.url,
        fromBlock: backfillFromBlock,
        toBlock: headBlockId,
        addresses: options.addresses,
        keys: options.keys,
        chunkSize: options.chunkSize,
        cursor: backfillCursor,
        signal: options.signal,
      })) {
        if (message.type === "event") {
          if (!shouldYieldEvent(message.cursor, lastYieldedCursor)) {
            continue;
          }

          lastYieldedCursor = message.cursor;
          lastRealCursor = message.cursor;
        }

        yield message;
      }

      throwIfAborted(options.signal);

      backfillFromBlock = undefined;
      backfillCursor = lastRealCursor;

      subscription = subscribeEvents({
        url: options.wsUrl,
        blockId: headBlockId,
        addresses: options.addresses,
        keys: options.keys,
        finalityStatus: options.finalityStatus,
        cursor: lastRealCursor,
        signal: options.signal,
        webSocketFactory: options.webSocketFactory,
      });

      const reorg = yield* yieldFromSubscription(
        subscription,
        lastYieldedCursor,
      );
      subscription = undefined;

      if (!reorg) {
        return;
      }

      const startingBlockNumber = reorg.reorg.startingBlockNumber;
      lastYieldedCursor = cursorBeforeBlock(startingBlockNumber);
      lastRealCursor = undefined;
      backfillFromBlock = { block_number: startingBlockNumber };
      backfillCursor = undefined;

      yield reorg;
    }
  } finally {
    if (subscription) {
      await subscription.unsubscribe();
    }
  }
}

async function* yieldFromSubscription(
  subscription: EventSubscription,
  initialCursor: EventCursor | undefined,
): AsyncGenerator<StreamMessage, ReorgMessage | undefined> {
  let lastYieldedCursor = initialCursor;

  for await (const message of subscription) {
    if (message.type === "reorg") {
      await subscription.unsubscribe();
      return message;
    }

    if (!shouldYieldEvent(message.cursor, lastYieldedCursor)) {
      continue;
    }

    lastYieldedCursor = message.cursor;
    yield message;
  }

  return undefined;
}

function shouldYieldEvent(
  cursor: EventCursor,
  lastYieldedCursor: EventCursor | undefined,
): boolean {
  return (
    lastYieldedCursor === undefined ||
    compareEventCursor(cursor, lastYieldedCursor) > 0
  );
}

function cursorBeforeBlock(blockNumber: number): EventCursor {
  // Synthetic in-memory cursor used only for local dedupe after a rollback.
  return {
    blockNumber,
    transactionIndex: -1,
    transactionHash: "0x0",
    eventIndex: -1,
  };
}

function throwIfAborted(signal: AbortSignal | undefined): void {
  if (signal?.aborted) {
    throw signal.reason ?? new Error("The operation was aborted.");
  }
}
