import type { EventEnvelope } from "./types.js";

export class InvalidEventError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "InvalidEventError";
  }
}

export function parseEvent(value: Buffer, maxMessageBytes: number): EventEnvelope {
  if (value.length > maxMessageBytes) {
    throw new InvalidEventError(`message exceeds ${maxMessageBytes} bytes`);
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(value.toString("utf8"));
  } catch {
    throw new InvalidEventError("message is not valid JSON");
  }
  if (!isRecord(parsed)) {
    throw new InvalidEventError("message root must be an object");
  }

  const eventId = requiredString(parsed, "eventId");
  const eventType = requiredString(parsed, "eventType");
  const occurredAt = requiredString(parsed, "occurredAt");
  if (Number.isNaN(Date.parse(occurredAt))) {
    throw new InvalidEventError("occurredAt must be a valid date-time string");
  }
  if (!isRecord(parsed.payload)) {
    throw new InvalidEventError("payload must be an object");
  }

  return {
    eventId,
    eventType,
    occurredAt,
    payload: parsed.payload,
  };
}

function requiredString(data: Record<string, unknown>, key: string): string {
  const value = data[key];
  if (typeof value !== "string" || value.trim() === "") {
    throw new InvalidEventError(`${key} must be a non-empty string`);
  }
  return value.trim();
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
