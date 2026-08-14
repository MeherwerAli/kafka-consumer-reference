export type EventEnvelope = Readonly<{
  eventId: string;
  eventType: string;
  occurredAt: string;
  payload: Readonly<Record<string, unknown>>;
}>;

export type MessageCoordinates = Readonly<{
  topic: string;
  partition: number;
  offset: string;
  key: string | null;
}>;

export type ProcessingOutcome = "processed" | "duplicate" | "dead-lettered";

export interface EventSink {
  /** Must be an idempotent upsert keyed by event.eventId. */
  upsert(event: EventEnvelope): Promise<void>;
}

export interface IdempotencyStore {
  has(eventId: string): Promise<boolean>;
  mark(eventId: string): Promise<void>;
}

export type DeadLetterRecord = Readonly<{
  coordinates: MessageCoordinates;
  reason: string;
  payloadPreview: string;
  truncated: boolean;
}>;

export interface DeadLetterPublisher {
  publish(record: DeadLetterRecord): Promise<void>;
}
