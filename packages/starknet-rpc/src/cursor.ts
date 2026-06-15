import { normalizeFelt } from "./normalize";
import type { EventCursor } from "./types";

export function compareEventCursor(a: EventCursor, b: EventCursor): number {
  if (a.blockNumber !== b.blockNumber) {
    return a.blockNumber < b.blockNumber ? -1 : 1;
  }

  if (a.transactionIndex !== b.transactionIndex) {
    return a.transactionIndex < b.transactionIndex ? -1 : 1;
  }

  if (a.eventIndex !== b.eventIndex) {
    return a.eventIndex < b.eventIndex ? -1 : 1;
  }

  const aTransactionHash = normalizeFelt(
    a.transactionHash,
    "cursor.transactionHash",
  );
  const bTransactionHash = normalizeFelt(
    b.transactionHash,
    "cursor.transactionHash",
  );

  if (aTransactionHash !== bTransactionHash) {
    return aTransactionHash < bTransactionHash ? -1 : 1;
  }

  return 0;
}

export function cursorEquals(a: EventCursor, b: EventCursor): boolean {
  return compareEventCursor(a, b) === 0;
}

export function eventCursorKey(cursor: EventCursor): string {
  return [
    cursor.blockNumber,
    normalizeFelt(cursor.transactionHash, "cursor.transactionHash"),
    cursor.eventIndex,
  ].join(":");
}
