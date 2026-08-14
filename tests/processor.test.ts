import assert from "node:assert/strict";
import test from "node:test";

import { MessageProcessor } from "../src/processor.js";
import type {
  DeadLetterPublisher,
  DeadLetterRecord,
  EventEnvelope,
  EventSink,
  IdempotencyStore,
  MessageCoordinates,
} from "../src/types.js";


const coordinates: MessageCoordinates = {
  topic: "events",
  partition: 0,
  offset: "12",
  key: "event-key",
};

const validEvent = Buffer.from(JSON.stringify({
  eventId: "evt-1",
  eventType: "portfolio.page.crawled",
  occurredAt: "2026-08-14T08:00:00Z",
  payload: { title: "Guide" },
}));

class FakeSink implements EventSink {
  readonly events: EventEnvelope[] = [];
  error: Error | null = null;

  async upsert(event: EventEnvelope): Promise<void> {
    if (this.error) {
      throw this.error;
    }
    this.events.push(event);
  }
}

class FakeIdempotencyStore implements IdempotencyStore {
  readonly eventIds = new Set<string>();

  async has(eventId: string): Promise<boolean> {
    return this.eventIds.has(eventId);
  }

  async mark(eventId: string): Promise<void> {
    this.eventIds.add(eventId);
  }
}

class FakeDeadLetterPublisher implements DeadLetterPublisher {
  readonly records: DeadLetterRecord[] = [];
  error: Error | null = null;

  async publish(record: DeadLetterRecord): Promise<void> {
    if (this.error) {
      throw this.error;
    }
    this.records.push(record);
  }
}

function fixture(maxMessageBytes = 1_024): {
  processor: MessageProcessor;
  sink: FakeSink;
  store: FakeIdempotencyStore;
  deadLetters: FakeDeadLetterPublisher;
} {
  const sink = new FakeSink();
  const store = new FakeIdempotencyStore();
  const deadLetters = new FakeDeadLetterPublisher();
  return {
    processor: new MessageProcessor(sink, store, deadLetters, maxMessageBytes),
    sink,
    store,
    deadLetters,
  };
}

test("processes a valid event and records idempotency after the sink", async () => {
  const { processor, sink, store, deadLetters } = fixture();

  assert.equal(await processor.process(validEvent, coordinates), "processed");
  assert.equal(sink.events.length, 1);
  assert.equal(await store.has("evt-1"), true);
  assert.equal(deadLetters.records.length, 0);
});

test("skips an event that was already processed", async () => {
  const { processor, sink, store } = fixture();
  await store.mark("evt-1");

  assert.equal(await processor.process(validEvent, coordinates), "duplicate");
  assert.equal(sink.events.length, 0);
});

test("dead-letters invalid and oversized messages", async () => {
  const invalidFixture = fixture();
  assert.equal(
    await invalidFixture.processor.process(Buffer.from("not-json"), coordinates),
    "dead-lettered",
  );
  assert.match(invalidFixture.deadLetters.records[0]?.reason ?? "", /valid JSON/);

  const oversizedFixture = fixture(4);
  assert.equal(
    await oversizedFixture.processor.process(validEvent, coordinates),
    "dead-lettered",
  );
  assert.match(oversizedFixture.deadLetters.records[0]?.reason ?? "", /exceeds 4 bytes/);
});

test("does not mark an event when the sink fails", async () => {
  const { processor, sink, store } = fixture();
  sink.error = new Error("temporary sink failure");

  await assert.rejects(processor.process(validEvent, coordinates), /temporary sink failure/);
  assert.equal(await store.has("evt-1"), false);
});

test("propagates dead-letter publication failure", async () => {
  const { processor, deadLetters } = fixture();
  deadLetters.error = new Error("DLQ unavailable");

  await assert.rejects(
    processor.process(Buffer.from("not-json"), coordinates),
    /DLQ unavailable/,
  );
});
