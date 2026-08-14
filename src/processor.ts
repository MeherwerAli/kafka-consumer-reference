import {
  type DeadLetterPublisher,
  type DeadLetterRecord,
  type EventSink,
  type IdempotencyStore,
  type MessageCoordinates,
  type ProcessingOutcome,
} from "./types.js";
import { InvalidEventError, parseEvent } from "./validation.js";

const DEAD_LETTER_PREVIEW_BYTES = 4_096;

export class MessageProcessor {
  constructor(
    private readonly sink: EventSink,
    private readonly idempotencyStore: IdempotencyStore,
    private readonly deadLetterPublisher: DeadLetterPublisher,
    private readonly maxMessageBytes: number,
  ) {
    if (!Number.isSafeInteger(maxMessageBytes) || maxMessageBytes < 1) {
      throw new RangeError("maxMessageBytes must be a positive safe integer");
    }
  }

  async process(
    value: Buffer | null,
    coordinates: MessageCoordinates,
  ): Promise<ProcessingOutcome> {
    const messageValue = value ?? Buffer.alloc(0);
    let event;
    try {
      event = parseEvent(messageValue, this.maxMessageBytes);
    } catch (error) {
      if (!(error instanceof InvalidEventError)) {
        throw error;
      }
      await this.deadLetterPublisher.publish(
        createDeadLetterRecord(messageValue, coordinates, error.message),
      );
      return "dead-lettered";
    }

    if (await this.idempotencyStore.has(event.eventId)) {
      return "duplicate";
    }

    await this.sink.upsert(event);
    await this.idempotencyStore.mark(event.eventId);
    return "processed";
  }
}

function createDeadLetterRecord(
  value: Buffer,
  coordinates: MessageCoordinates,
  reason: string,
): DeadLetterRecord {
  const preview = value.subarray(0, DEAD_LETTER_PREVIEW_BYTES);
  return {
    coordinates,
    reason,
    payloadPreview: preview.toString("utf8"),
    truncated: value.length > preview.length,
  };
}
