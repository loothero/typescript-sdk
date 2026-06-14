import { compareEventCursor, cursorEquals, eventCursorKey } from "./cursor";
import { normalizeFelt } from "./normalize";
import type {
  BlockId,
  EventCursor,
  EventSubscription,
  Felt,
  FinalityStatus,
  StreamMessage,
  SubscribeEventsOptions,
} from "./types";
import { TooManyBlocksBackError, connectSubscribeEvents } from "./ws";

const DEFAULT_FINALITY_STATUS: FinalityStatus = "ACCEPTED_ON_L2";
const DEFAULT_MIN_RECONNECT_DELAY_MS = 500;
const DEFAULT_MAX_RECONNECT_DELAY_MS = 10_000;
const MAX_REMEMBERED_CURSOR_KEYS = 2_048;

type ReconnectConfig = {
  enabled: boolean;
  minDelayMs: number;
  maxDelayMs: number;
};

export function subscribeEvents(
  options: SubscribeEventsOptions,
): EventSubscription {
  const controller = new AbortController();
  let unsubscribed = false;

  const abortFromParent = () => {
    unsubscribed = true;
    controller.abort(options.signal?.reason);
  };

  if (options.signal?.aborted) {
    abortFromParent();
  } else {
    options.signal?.addEventListener("abort", abortFromParent, {
      once: true,
    });
  }

  const subscription: EventSubscription = {
    async unsubscribe() {
      if (unsubscribed) {
        return;
      }

      unsubscribed = true;
      options.signal?.removeEventListener?.("abort", abortFromParent);
      controller.abort(new Error("Subscription unsubscribed"));
    },

    async *[Symbol.asyncIterator]() {
      try {
        yield* subscribeWithReconnect({
          options: normalizeSubscribeOptions(options),
          signal: controller.signal,
          isUnsubscribed: () => unsubscribed,
        });
      } finally {
        unsubscribed = true;
        options.signal?.removeEventListener?.("abort", abortFromParent);
        controller.abort(new Error("Subscription iterator closed"));
      }
    },
  };

  return subscription;
}

async function* subscribeWithReconnect({
  options,
  signal,
  isUnsubscribed,
}: {
  options: SubscribeEventsOptions;
  signal: AbortSignal;
  isUnsubscribed: () => boolean;
}): AsyncGenerator<StreamMessage> {
  const reconnect = normalizeReconnectConfig(options.reconnect);
  const seenCursorKeys = new RememberedCursorKeys(MAX_REMEMBERED_CURSOR_KEYS);
  let lastCursor = options.cursor;
  let blockId = initialBlockId(options);
  let attempt = 0;

  if (lastCursor) {
    seenCursorKeys.remember(eventCursorKey(lastCursor));
  }

  while (!signal.aborted && !isUnsubscribed()) {
    let receivedMessage = false;

    try {
      for await (const message of connectSubscribeEvents({
        ...options,
        blockId,
        cursor: undefined,
        reconnect: undefined,
        signal,
      })) {
        receivedMessage = true;
        attempt = 0;

        if (message.type === "reorg") {
          if (
            lastCursor &&
            lastCursor.blockNumber >= message.reorg.startingBlockNumber
          ) {
            lastCursor = undefined;
            seenCursorKeys.clear();
          }

          blockId = { block_number: message.reorg.startingBlockNumber };
          yield message;
          continue;
        }

        const key = eventCursorKey(message.cursor);
        if (shouldSkipEvent(message.cursor, key, lastCursor, seenCursorKeys)) {
          continue;
        }

        lastCursor = message.cursor;
        blockId = { block_number: message.cursor.blockNumber };
        seenCursorKeys.remember(key);
        yield message;
      }
    } catch (error) {
      if (signal.aborted || isUnsubscribed()) {
        return;
      }

      if (error instanceof TooManyBlocksBackError) {
        throw error;
      }

      if (!reconnect.enabled) {
        throw error;
      }
    }

    if (signal.aborted || isUnsubscribed()) {
      return;
    }

    if (!reconnect.enabled) {
      return;
    }

    attempt = receivedMessage ? 1 : attempt + 1;
    await waitForReconnect(reconnectDelayMs(reconnect, attempt), signal);
  }
}

function normalizeSubscribeOptions(
  options: SubscribeEventsOptions,
): SubscribeEventsOptions {
  return {
    ...options,
    blockId: options.blockId ? normalizeBlockId(options.blockId) : undefined,
    addresses: normalizeFelts(options.addresses),
    keys: options.keys?.map((keys) => normalizeFelts(keys) ?? []),
    finalityStatus: options.finalityStatus ?? DEFAULT_FINALITY_STATUS,
  };
}

function initialBlockId(options: SubscribeEventsOptions): BlockId {
  if (options.blockId) {
    return options.blockId;
  }

  if (options.cursor) {
    return { block_number: options.cursor.blockNumber };
  }

  return "latest";
}

function normalizeBlockId(blockId: BlockId): BlockId {
  if (typeof blockId === "string") {
    return blockId;
  }

  if ("block_hash" in blockId) {
    return { block_hash: normalizeFelt(blockId.block_hash) };
  }

  return blockId;
}

function normalizeFelts(values?: Felt[]): Felt[] | undefined {
  if (!values) {
    return undefined;
  }

  return Array.from(
    new Set(values.map((value) => normalizeFelt(value, "subscription.filter"))),
  );
}

function shouldSkipEvent(
  cursor: EventCursor,
  key: string,
  lastCursor: EventCursor | undefined,
  seenCursorKeys: RememberedCursorKeys,
): boolean {
  if (seenCursorKeys.has(key)) {
    return true;
  }

  if (!lastCursor) {
    return false;
  }

  if (cursorEquals(cursor, lastCursor)) {
    return true;
  }

  return compareEventCursor(cursor, lastCursor) < 0;
}

function normalizeReconnectConfig(
  reconnect: SubscribeEventsOptions["reconnect"],
): ReconnectConfig {
  if (reconnect === false) {
    return {
      enabled: false,
      minDelayMs: DEFAULT_MIN_RECONNECT_DELAY_MS,
      maxDelayMs: DEFAULT_MAX_RECONNECT_DELAY_MS,
    };
  }

  if (typeof reconnect === "object" && reconnect.enabled === false) {
    return {
      enabled: false,
      minDelayMs: DEFAULT_MIN_RECONNECT_DELAY_MS,
      maxDelayMs: DEFAULT_MAX_RECONNECT_DELAY_MS,
    };
  }

  const minDelayMs =
    typeof reconnect === "object" && reconnect.minDelayMs !== undefined
      ? Math.max(0, reconnect.minDelayMs)
      : DEFAULT_MIN_RECONNECT_DELAY_MS;
  const maxDelayMs =
    typeof reconnect === "object" && reconnect.maxDelayMs !== undefined
      ? Math.max(minDelayMs, reconnect.maxDelayMs)
      : DEFAULT_MAX_RECONNECT_DELAY_MS;

  return {
    enabled: true,
    minDelayMs,
    maxDelayMs: Math.max(minDelayMs, maxDelayMs),
  };
}

function reconnectDelayMs(
  { minDelayMs, maxDelayMs }: ReconnectConfig,
  attempt: number,
): number {
  const exponentialDelay = minDelayMs * 2 ** Math.max(0, attempt - 1);
  const boundedDelay = Math.min(exponentialDelay, maxDelayMs);
  const jitter = Math.random() * boundedDelay * 0.2;

  return Math.min(maxDelayMs, Math.round(boundedDelay + jitter));
}

async function waitForReconnect(
  delayMs: number,
  signal: AbortSignal,
): Promise<void> {
  if (signal.aborted || delayMs <= 0) {
    return;
  }

  await new Promise<void>((resolve) => {
    const onAbort = () => {
      clearTimeout(timeout);
      signal.removeEventListener("abort", onAbort);
      resolve();
    };

    const timeout = setTimeout(() => {
      signal.removeEventListener("abort", onAbort);
      resolve();
    }, delayMs);

    signal.addEventListener("abort", onAbort, { once: true });
  });
}

class RememberedCursorKeys {
  private readonly keys = new Set<string>();
  private readonly order: string[] = [];

  constructor(private readonly maxSize: number) {}

  has(key: string): boolean {
    return this.keys.has(key);
  }

  remember(key: string): void {
    if (this.keys.has(key)) {
      return;
    }

    this.keys.add(key);
    this.order.push(key);

    while (this.order.length > this.maxSize) {
      const oldest = this.order.shift();
      if (oldest) {
        this.keys.delete(oldest);
      }
    }
  }

  clear(): void {
    this.keys.clear();
    this.order.length = 0;
  }
}
