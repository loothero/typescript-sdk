import { backfillEvents } from "./backfill";
import { StarknetBlockCache } from "./block-cache";
import { compareEventCursor } from "./cursor";
import { StarknetTransportError } from "./http";
import { subscribeEvents } from "./subscribe";
import type {
  BlockId,
  EventCursor,
  EventSubscription,
  ReorgMessage,
  StreamEventsOptions,
  StreamMessage,
} from "./types";
import { TooManyBlocksBackError } from "./ws";

const DEFAULT_HTTP_RETRY_DELAY_MS = 500;
const MAX_HTTP_RETRY_DELAY_MS = 10_000;

export async function* streamEvents(
  options: StreamEventsOptions,
): AsyncGenerator<StreamMessage> {
  rejectUnsupportedOptions(options);

  const blockCache = new StarknetBlockCache({
    url: options.url,
    signal: options.signal,
  });

  let lastYieldedCursor = options.cursor;
  let lastRealCursor = options.cursor;
  let backfillFromBlock = options.fromBlock;
  let backfillCursor = options.cursor;
  let lastBackfilledBlockNumber = options.cursor?.blockNumber;
  let httpRetryAttempt = 0;
  let subscription: EventSubscription | undefined;

  try {
    while (true) {
      throwIfAborted(options.signal);

      let headBlockId: BlockId;

      try {
        const head = await blockCache.getLatestBlock();
        headBlockId = { block_number: head.blockNumber };

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

        lastBackfilledBlockNumber = head.blockNumber;
        httpRetryAttempt = 0;
      } catch (error) {
        if (!isRetryableHttpError(error, options.signal)) {
          throw error;
        }

        if (lastRealCursor) {
          backfillCursor = lastRealCursor;
        }

        httpRetryAttempt += 1;
        await waitForStreamRetry(
          retryDelayMs(httpRetryAttempt),
          options.signal,
        );
        continue;
      }

      throwIfAborted(options.signal);

      backfillFromBlock = headBlockId;
      backfillCursor = undefined;

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

      const subscriptionState = {
        lastYieldedCursor,
        lastRealCursor,
      };
      let reorg: ReorgMessage | undefined;

      try {
        reorg = yield* yieldFromSubscription(subscription, subscriptionState);
        lastYieldedCursor = subscriptionState.lastYieldedCursor;
        lastRealCursor = subscriptionState.lastRealCursor;
        subscription = undefined;
      } catch (error) {
        lastYieldedCursor = subscriptionState.lastYieldedCursor;
        lastRealCursor = subscriptionState.lastRealCursor;

        if (!(error instanceof TooManyBlocksBackError)) {
          throw error;
        }

        if (subscription) {
          await subscription.unsubscribe();
        }
        subscription = undefined;

        const resumeBlockNumber = Math.max(
          lastBackfilledBlockNumber ?? 0,
          lastRealCursor?.blockNumber ?? 0,
        );
        backfillFromBlock = { block_number: resumeBlockNumber };
        backfillCursor =
          lastRealCursor && lastRealCursor.blockNumber >= resumeBlockNumber
            ? lastRealCursor
            : undefined;
        continue;
      }

      if (!reorg) {
        return;
      }

      const startingBlockNumber = reorg.reorg.startingBlockNumber;
      blockCache.invalidateFrom(startingBlockNumber);
      lastYieldedCursor = cursorBeforeBlock(startingBlockNumber);
      lastRealCursor = undefined;
      lastBackfilledBlockNumber = undefined;
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
  state: {
    lastYieldedCursor: EventCursor | undefined;
    lastRealCursor: EventCursor | undefined;
  },
): AsyncGenerator<StreamMessage, ReorgMessage | undefined> {
  for await (const message of subscription) {
    if (message.type === "reorg") {
      await subscription.unsubscribe();
      return message;
    }

    if (!shouldYieldEvent(message.cursor, state.lastYieldedCursor)) {
      continue;
    }

    state.lastYieldedCursor = message.cursor;
    state.lastRealCursor = message.cursor;
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

function rejectUnsupportedOptions(options: StreamEventsOptions): void {
  if ((options as { toBlock?: unknown }).toBlock !== undefined) {
    throw new Error(
      "streamEvents does not support toBlock. Use backfillEvents for bounded historical indexing.",
    );
  }
}

function isRetryableHttpError(
  error: unknown,
  signal: AbortSignal | undefined,
): boolean {
  return !signal?.aborted && error instanceof StarknetTransportError;
}

function retryDelayMs(attempt: number): number {
  const exponentialDelay =
    DEFAULT_HTTP_RETRY_DELAY_MS * 2 ** Math.max(0, attempt - 1);
  const boundedDelay = Math.min(exponentialDelay, MAX_HTTP_RETRY_DELAY_MS);
  const jitter = Math.random() * boundedDelay * 0.2;

  return Math.min(MAX_HTTP_RETRY_DELAY_MS, Math.round(boundedDelay + jitter));
}

async function waitForStreamRetry(
  delayMs: number,
  signal: AbortSignal | undefined,
): Promise<void> {
  if (signal?.aborted || delayMs <= 0) {
    return;
  }

  await new Promise<void>((resolve) => {
    const onAbort = () => {
      clearTimeout(timeout);
      signal?.removeEventListener?.("abort", onAbort);
      resolve();
    };

    const timeout = setTimeout(() => {
      signal?.removeEventListener?.("abort", onAbort);
      resolve();
    }, delayMs);

    signal?.addEventListener("abort", onAbort, { once: true });
  });
}
