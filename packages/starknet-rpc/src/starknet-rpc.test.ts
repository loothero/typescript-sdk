import { afterEach, describe, expect, it, vi } from "vitest";
import { backfillEvents } from "./backfill";
import { getBlockWithTxHashes } from "./block-cache";
import { compareEventCursor, eventCursorKey } from "./cursor";
import { getEvents } from "./http";
import { normalizeEvent, normalizeFelt } from "./normalize";
import { streamEvents } from "./stream";
import { subscribeEvents } from "./subscribe";
import type { EventCursor, RpcEvent } from "./types";
import { TooManyBlocksBackError, connectSubscribeEvents } from "./ws";

const RPC_URL = "http://example.test/rpc";
const WS_URL = "ws://example.test/rpc";

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

describe("felt normalization", () => {
  it("pads felts to canonical Starknet hex", () => {
    expect(normalizeFelt("0xAbC")).toBe(`0x${"0".repeat(61)}abc`);
    expect(() => normalizeFelt("abc")).toThrow(/0x-prefixed/);
  });
});

describe("cursor comparison", () => {
  it("orders by block number, transaction index, and per-transaction event index", () => {
    expect(
      compareEventCursor(cursor(1, "0x2", 0, 0), cursor(2, "0x1", 0, 0)),
    ).toBeLessThan(0);
    expect(
      compareEventCursor(cursor(2, "0xff", 0, 0), cursor(2, "0x1", 1, 0)),
    ).toBeLessThan(0);
    expect(
      compareEventCursor(cursor(2, "0x2", 1, 1), cursor(2, "0x2", 1, 0)),
    ).toBeGreaterThan(0);
    expect(
      compareEventCursor(cursor(2, "0x2", 1, 0), cursor(2, "0x1", 1, 0)),
    ).toBeGreaterThan(0);
  });

  it("keeps the cursor identity keyed by transaction hash and event index", () => {
    expect(eventCursorKey(cursor(5, "0xabc", 1, 2))).toBe(
      eventCursorKey(cursor(5, "0x0abc", 99, 2)),
    );
  });
});

describe("event normalization", () => {
  it("requires transaction_index and includes it in the cursor", () => {
    expect(normalizeEvent(rawEvent({ transactionIndex: 7 })).cursor).toEqual({
      blockNumber: 1,
      transactionIndex: 7,
      transactionHash: normalizeFelt("0x1"),
      eventIndex: 0,
    });

    const { transaction_index: _transactionIndex, ...missingTransactionIndex } =
      rawEvent();

    expect(() => normalizeEvent(missingTransactionIndex)).toThrow(
      /event\.transaction_index/,
    );
  });
});

describe("HTTP backfill", () => {
  it("follows continuation tokens without skipping pages", async () => {
    const requests: unknown[] = [];
    mockRpcFetch((request) => {
      requests.push(request);

      if (request.method !== "starknet_getEvents") {
        throw new Error(`unexpected method ${request.method}`);
      }

      if (!singleParam(request).continuation_token) {
        return {
          events: [
            rawEvent({ blockNumber: 1, transactionHash: "0x1", eventIndex: 0 }),
            rawEvent({ blockNumber: 1, transactionHash: "0x2", eventIndex: 0 }),
          ],
          continuation_token: "page-2",
        };
      }

      return {
        events: [
          rawEvent({ blockNumber: 2, transactionHash: "0x1", eventIndex: 0 }),
        ],
      };
    });

    const messages = await collect(
      backfillEvents({
        url: RPC_URL,
        fromBlock: { block_number: 1 },
        toBlock: { block_number: 2 },
        chunkSize: 2,
      }),
    );

    expect(messages.map((message) => message.cursor.blockNumber)).toEqual([
      1, 1, 2,
    ]);
    expect(requests).toHaveLength(2);
    expect((requests[0] as JsonRpcRequest).params).toEqual([
      {
        from_block: { block_number: 1 },
        to_block: { block_number: 2 },
        chunk_size: 2,
      },
    ]);
    expect((requests[1] as JsonRpcRequest).params).toEqual([
      {
        from_block: { block_number: 1 },
        to_block: { block_number: 2 },
        chunk_size: 2,
        continuation_token: "page-2",
      },
    ]);
  });

  it("sends getEvents filters as positional JSON-RPC params", async () => {
    const requests: unknown[] = [];
    mockRpcFetch((request) => {
      requests.push(request);
      return { events: [] };
    });

    await getEvents({
      url: RPC_URL,
      fromBlock: { block_number: 1 },
      toBlock: { block_number: 2 },
      addresses: ["0xaaa"],
      keys: [["0x111"]],
      chunkSize: 25,
      continuationToken: "page-2",
    });

    expect(requests).toHaveLength(1);
    expect(requests[0]).toMatchObject({
      method: "starknet_getEvents",
    });
    expect((requests[0] as JsonRpcRequest).params).toEqual([
      {
        from_block: { block_number: 1 },
        to_block: { block_number: 2 },
        address: normalizeFelt("0xaaa"),
        keys: [[normalizeFelt("0x111")]],
        chunk_size: 25,
        continuation_token: "page-2",
      },
    ]);
  });

  it("sends getBlockWithTxHashes block ids as positional JSON-RPC params", async () => {
    const requests: unknown[] = [];
    mockRpcFetch((request) => {
      requests.push(request);
      return {
        block_hash: "0x123",
        block_number: 10,
        timestamp: 100,
        transactions: [],
      };
    });

    await getBlockWithTxHashes({
      url: RPC_URL,
      blockId: { block_number: 10 },
    });

    expect(requests).toHaveLength(1);
    expect(requests[0]).toMatchObject({
      method: "starknet_getBlockWithTxHashes",
    });
    expect((requests[0] as JsonRpcRequest).params).toEqual([
      { block_number: 10 },
    ]);
  });

  it("resumes from a persisted cursor and skips replayed events", async () => {
    const requests: unknown[] = [];
    const resumeCursor = cursor(5, "0x2", 1, 1);

    mockRpcFetch((request) => {
      requests.push(request);
      return {
        events: [
          rawEvent({
            blockNumber: 5,
            transactionHash: "0x1",
            transactionIndex: 0,
            eventIndex: 0,
          }),
          rawEvent({
            blockNumber: 5,
            transactionHash: "0x2",
            transactionIndex: 1,
            eventIndex: 1,
          }),
          rawEvent({
            blockNumber: 5,
            transactionHash: "0x3",
            transactionIndex: 2,
            eventIndex: 0,
          }),
        ],
      };
    });

    const messages = await collect(
      backfillEvents({
        url: RPC_URL,
        fromBlock: { block_number: 0 },
        continuationToken: "old-token",
        cursor: resumeCursor,
      }),
    );

    expect(messages).toHaveLength(1);
    expect(messages[0].cursor).toMatchObject({
      blockNumber: 5,
      transactionIndex: 2,
      transactionHash: normalizeFelt("0x3"),
      eventIndex: 0,
    });
    expect((requests[0] as JsonRpcRequest).params).toEqual([
      {
        from_block: { block_number: 5 },
        chunk_size: 100,
      },
    ]);
    expect(
      singleParam(requests[0] as JsonRpcRequest).continuation_token,
    ).toBeUndefined();
  });
});

describe("WebSocket subscriptions", () => {
  it("emits Pathfinder reorg notifications", async () => {
    const sockets: MockWebSocket[] = [];
    const iterator = connectSubscribeEvents({
      url: WS_URL,
      webSocketFactory: mockWebSocketFactory(sockets),
    });

    const next = iterator.next();
    const socket = await waitForSocket(sockets, 0);
    socket.open();
    await waitForSent(socket, "starknet_subscribeEvents");
    socket.message({ jsonrpc: "2.0", id: 1, result: "sub-1" });
    socket.message({
      jsonrpc: "2.0",
      method: "starknet_subscriptionReorg",
      params: {
        subscription_id: "sub-1",
        result: {
          starting_block_number: 12,
          starting_block_hash: "0xabc",
          ending_block_number: 13,
          ending_block_hash: "0xdef",
        },
      },
    });

    await expect(next).resolves.toMatchObject({
      done: false,
      value: {
        type: "reorg",
        reorg: {
          startingBlockNumber: 12,
          startingBlockHash: normalizeFelt("0xabc"),
          endingBlockNumber: 13,
          endingBlockHash: normalizeFelt("0xdef"),
        },
      },
    });

    await iterator.return?.(undefined);
  });

  it("dedupes replayed events after reconnect", async () => {
    const sockets: MockWebSocket[] = [];
    const subscription = subscribeEvents({
      url: WS_URL,
      blockId: { block_number: 1 },
      reconnect: { minDelayMs: 0, maxDelayMs: 0 },
      webSocketFactory: mockWebSocketFactory(sockets),
    });
    const iterator = subscription[Symbol.asyncIterator]();

    const first = iterator.next();
    const socket1 = await waitForSocket(sockets, 0);
    socket1.open();
    await waitForSent(socket1, "starknet_subscribeEvents");
    socket1.message({ jsonrpc: "2.0", id: 1, result: "sub-1" });
    socket1.message(eventNotification("sub-1", rawEvent({ blockNumber: 1 })));

    await expect(first).resolves.toMatchObject({
      done: false,
      value: {
        type: "event",
        cursor: {
          blockNumber: 1,
        },
      },
    });

    const second = iterator.next();
    socket1.close();
    const socket2 = await waitForSocket(sockets, 1);
    socket2.open();
    await waitForSent(socket2, "starknet_subscribeEvents");
    socket2.message({ jsonrpc: "2.0", id: 1, result: "sub-2" });
    socket2.message(eventNotification("sub-2", rawEvent({ blockNumber: 1 })));
    socket2.message(
      eventNotification(
        "sub-2",
        rawEvent({ blockNumber: 2, transactionHash: "0x2" }),
      ),
    );

    await expect(second).resolves.toMatchObject({
      done: false,
      value: {
        type: "event",
        cursor: {
          blockNumber: 2,
        },
      },
    });

    await subscription.unsubscribe();
  });

  it("surfaces TooManyBlocksBack without trying historical WS backfill", async () => {
    const sockets: MockWebSocket[] = [];
    const iterator = connectSubscribeEvents({
      url: WS_URL,
      blockId: { block_number: 1 },
      webSocketFactory: mockWebSocketFactory(sockets),
    });

    const next = iterator.next();
    const socket = await waitForSocket(sockets, 0);
    socket.open();
    await waitForSent(socket, "starknet_subscribeEvents");
    socket.message({
      jsonrpc: "2.0",
      id: 1,
      error: {
        code: 68,
        message: "Cannot go back more than 1024 blocks",
        data: { limit: 1024, requested: 1025 },
      },
    });

    await expect(next).rejects.toBeInstanceOf(TooManyBlocksBackError);
    expect(socket.sent).toHaveLength(1);
  });
});

describe("combined stream", () => {
  it("dedupes the inclusive HTTP-to-WS handoff", async () => {
    const sockets: MockWebSocket[] = [];
    mockRpcFetch((request) => {
      if (request.method === "starknet_getBlockWithTxHashes") {
        return {
          block_hash: "0x123",
          block_number: 10,
          timestamp: 100,
          transactions: [],
        };
      }

      if (request.method === "starknet_getEvents") {
        return {
          events: [rawEvent({ blockNumber: 10, transactionHash: "0x1" })],
        };
      }

      throw new Error(`unexpected method ${request.method}`);
    });

    const iterator = streamEvents({
      url: RPC_URL,
      wsUrl: WS_URL,
      fromBlock: { block_number: 0 },
      webSocketFactory: mockWebSocketFactory(sockets),
    });

    await expect(iterator.next()).resolves.toMatchObject({
      done: false,
      value: {
        type: "event",
        cursor: {
          blockNumber: 10,
        },
      },
    });

    const live = iterator.next();
    const socket = await waitForSocket(sockets, 0);
    socket.open();
    const subscribe = await waitForSent(socket, "starknet_subscribeEvents");
    expect((subscribe.params as { block_id: unknown }).block_id).toEqual({
      block_number: 10,
    });
    socket.message({ jsonrpc: "2.0", id: 1, result: "sub-1" });
    socket.message(
      eventNotification(
        "sub-1",
        rawEvent({ blockNumber: 10, transactionHash: "0x1" }),
      ),
    );
    socket.message(
      eventNotification(
        "sub-1",
        rawEvent({ blockNumber: 11, transactionHash: "0x2" }),
      ),
    );

    await expect(live).resolves.toMatchObject({
      done: false,
      value: {
        type: "event",
        cursor: {
          blockNumber: 11,
        },
      },
    });

    await iterator.return?.(undefined);
  });
});

type JsonRpcRequest = {
  method: string;
  params: unknown;
};

function singleParam(request: JsonRpcRequest): Record<string, unknown> {
  const params = request.params;
  if (!Array.isArray(params) || params.length !== 1) {
    throw new Error("expected one positional JSON-RPC param");
  }

  const [param] = params;
  if (typeof param !== "object" || param === null || Array.isArray(param)) {
    throw new Error("expected positional JSON-RPC param object");
  }

  return param as Record<string, unknown>;
}

function cursor(
  blockNumber: number,
  transactionHash = "0x1",
  transactionIndex = 0,
  eventIndex = 0,
): EventCursor {
  return { blockNumber, transactionIndex, transactionHash, eventIndex };
}

function rawEvent({
  blockNumber = 1,
  blockHash = "0xabc",
  transactionHash = "0x1",
  transactionIndex = 0,
  eventIndex = 0,
  fromAddress = "0xaaa",
  keys = ["0x111"],
  data = ["0x222"],
}: {
  blockNumber?: number;
  blockHash?: string;
  transactionHash?: string;
  transactionIndex?: number;
  eventIndex?: number;
  fromAddress?: string;
  keys?: string[];
  data?: string[];
} = {}): RpcEvent {
  return {
    block_number: blockNumber,
    block_hash: blockHash,
    transaction_hash: transactionHash,
    transaction_index: transactionIndex,
    event_index: eventIndex,
    from_address: fromAddress,
    keys,
    data,
    finality_status: "ACCEPTED_ON_L2",
  };
}

function eventNotification(subscriptionId: string, event: RpcEvent) {
  return {
    jsonrpc: "2.0",
    method: "starknet_subscriptionEvents",
    params: {
      subscription_id: subscriptionId,
      result: event,
    },
  };
}

function mockRpcFetch(handler: (request: JsonRpcRequest) => unknown): void {
  vi.stubGlobal(
    "fetch",
    vi.fn(async (_url: string, init?: RequestInit) => {
      const request = JSON.parse(String(init?.body)) as JsonRpcRequest & {
        id?: number | string;
      };
      const result = handler(request);

      return new Response(
        JSON.stringify({
          jsonrpc: "2.0",
          id: request.id ?? 1,
          result,
        }),
        {
          headers: {
            "content-type": "application/json",
          },
        },
      );
    }),
  );
}

async function collect<T>(iterable: AsyncIterable<T>): Promise<T[]> {
  const values: T[] = [];
  for await (const value of iterable) {
    values.push(value);
  }
  return values;
}

function mockWebSocketFactory(sockets: MockWebSocket[]) {
  return (url: string) => {
    const socket = new MockWebSocket(url);
    sockets.push(socket);
    return socket;
  };
}

class MockWebSocket {
  readyState = 0;
  sent: Array<Record<string, unknown>> = [];
  readonly #listeners = new Map<string, Set<(event: unknown) => void>>();

  constructor(readonly url: string) {}

  send(data: string): void {
    this.sent.push(JSON.parse(data) as Record<string, unknown>);
  }

  close(): void {
    if (this.readyState === 3) {
      return;
    }

    this.readyState = 3;
    this.dispatch("close", {});
  }

  addEventListener(
    type: "open" | "message" | "error" | "close",
    listener: (event: unknown) => void,
  ): void {
    const listeners = this.#listeners.get(type) ?? new Set();
    listeners.add(listener);
    this.#listeners.set(type, listeners);
  }

  removeEventListener(
    type: "open" | "message" | "error" | "close",
    listener: (event: unknown) => void,
  ): void {
    this.#listeners.get(type)?.delete(listener);
  }

  open(): void {
    this.readyState = 1;
    this.dispatch("open", {});
  }

  message(value: unknown): void {
    this.dispatch("message", { data: JSON.stringify(value) });
  }

  private dispatch(type: string, event: unknown): void {
    for (const listener of this.#listeners.get(type) ?? []) {
      listener(event);
    }
  }
}

async function waitForSocket(
  sockets: MockWebSocket[],
  index: number,
): Promise<MockWebSocket> {
  await waitFor(() => sockets[index] !== undefined);
  return sockets[index];
}

async function waitForSent(
  socket: MockWebSocket,
  method: string,
): Promise<Record<string, unknown>> {
  await waitFor(() => socket.sent.some((message) => message.method === method));
  const message = socket.sent.find((message) => message.method === method);
  if (!message) {
    throw new Error(`missing sent message ${method}`);
  }

  return message;
}

async function waitFor(predicate: () => boolean): Promise<void> {
  const startedAt = Date.now();

  while (!predicate()) {
    if (Date.now() - startedAt > 1_000) {
      throw new Error("timed out waiting for condition");
    }

    await new Promise((resolve) => setTimeout(resolve, 0));
  }
}
