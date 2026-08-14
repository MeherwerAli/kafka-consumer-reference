import type { Producer } from "kafkajs";

import type {
  DeadLetterPublisher,
  DeadLetterRecord,
  EventEnvelope,
  EventSink,
  IdempotencyStore,
} from "./types.js";

export class ConsoleUpsertSink implements EventSink {
  async upsert(event: EventEnvelope): Promise<void> {
    console.log(JSON.stringify({
      level: "info",
      action: "event-upsert",
      eventId: event.eventId,
      eventType: event.eventType,
      occurredAt: event.occurredAt,
    }));
  }
}

export class InMemoryIdempotencyStore implements IdempotencyStore {
  private readonly processedEventIds = new Set<string>();

  async has(eventId: string): Promise<boolean> {
    return this.processedEventIds.has(eventId);
  }

  async mark(eventId: string): Promise<void> {
    this.processedEventIds.add(eventId);
  }
}

export class KafkaDeadLetterPublisher implements DeadLetterPublisher {
  constructor(
    private readonly producer: Producer,
    private readonly topic: string,
  ) {}

  async publish(record: DeadLetterRecord): Promise<void> {
    await this.producer.send({
      topic: this.topic,
      acks: -1,
      messages: [{
        key: coordinateKey(record),
        value: JSON.stringify(record),
      }],
    });
  }
}

function coordinateKey(record: DeadLetterRecord): string {
  const { topic, partition, offset } = record.coordinates;
  return `${topic}:${partition}:${offset}`;
}
