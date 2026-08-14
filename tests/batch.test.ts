import assert from "node:assert/strict";
import test from "node:test";

import type { KafkaMessage } from "kafkajs";

import { processPartitionBatch } from "../src/batch.js";
import { MessageProcessor } from "../src/processor.js";
import type {
  DeadLetterPublisher,
  DeadLetterRecord,
  EventEnvelope,
  EventSink,
  IdempotencyStore,
} from "../src/types.js";


class BatchSink implements EventSink {
  readonly events: EventEnvelope[] = [];
  failEventId: string | null = null;

  async upsert(event: EventEnvelope): Promise<void> {
    if (event.eventId === this.failEventId) {
      throw new Error("sink unavailable");
    }
    this.events.push(event);
  }
}

class BatchStore implements IdempotencyStore {
  readonly ids = new Set<string>();

  async has(eventId: string): Promise<boolean> {
    return this.ids.has(eventId);
  }

  async mark(eventId: string): Promise<void> {
    this.ids.add(eventId);
  }
}

class BatchDeadLetters implements DeadLetterPublisher {
  readonly records: DeadLetterRecord[] = [];

  async publish(record: DeadLetterRecord): Promise<void> {
    this.records.push(record);
  }
}

function message(offset: string, eventId: string): KafkaMessage {
  return {
    key: Buffer.from(eventId),
    value: Buffer.from(JSON.stringify({
      eventId,
      eventType: "example.event",
      occurredAt: "2026-08-14T08:00:00Z",
      payload: {},
    })),
    timestamp: "0",
    attributes: 0,
    offset,
    headers: {},
  };
}

test("commits the next offset only after every message succeeds", async () => {
  const commits: string[] = [];
  let heartbeats = 0;
  const processor = new MessageProcessor(
    new BatchSink(),
    new BatchStore(),
    new BatchDeadLetters(),
    1_024,
  );

  const processed = await processPartitionBatch(
    "events",
    2,
    [message("20", "evt-20"), message("21", "evt-21")],
    processor,
    {
      isRunning: () => true,
      isStale: () => false,
      heartbeat: async () => { heartbeats += 1; },
      commit: async (offset) => { commits.push(offset); },
    },
  );

  assert.equal(processed, 2);
  assert.equal(heartbeats, 2);
  assert.deepEqual(commits, ["22"]);
});

test("does not commit when a later message fails", async () => {
  const commits: string[] = [];
  const sink = new BatchSink();
  sink.failEventId = "evt-21";
  const processor = new MessageProcessor(
    sink,
    new BatchStore(),
    new BatchDeadLetters(),
    1_024,
  );

  await assert.rejects(
    processPartitionBatch(
      "events",
      2,
      [message("20", "evt-20"), message("21", "evt-21")],
      processor,
      {
        isRunning: () => true,
        isStale: () => false,
        heartbeat: async () => undefined,
        commit: async (offset) => { commits.push(offset); },
      },
    ),
    /sink unavailable/,
  );
  assert.deepEqual(commits, []);
});

test("does not process or commit a stopped or stale batch", async () => {
  const commits: string[] = [];
  const sink = new BatchSink();
  const processor = new MessageProcessor(
    sink,
    new BatchStore(),
    new BatchDeadLetters(),
    1_024,
  );

  const processed = await processPartitionBatch(
    "events",
    2,
    [message("20", "evt-20")],
    processor,
    {
      isRunning: () => false,
      isStale: () => false,
      heartbeat: async () => undefined,
      commit: async (offset) => { commits.push(offset); },
    },
  );
  assert.equal(processed, 0);
  assert.equal(sink.events.length, 0);
  assert.deepEqual(commits, []);
});

test("does not commit partial progress when a batch becomes stale", async () => {
  const commits: string[] = [];
  const sink = new BatchSink();
  const processor = new MessageProcessor(
    sink,
    new BatchStore(),
    new BatchDeadLetters(),
    1_024,
  );
  let heartbeatCount = 0;

  const processed = await processPartitionBatch(
    "events",
    2,
    [message("20", "evt-20"), message("21", "evt-21")],
    processor,
    {
      isRunning: () => true,
      isStale: () => heartbeatCount > 0,
      heartbeat: async () => { heartbeatCount += 1; },
      commit: async (offset) => { commits.push(offset); },
    },
  );

  assert.equal(processed, 1);
  assert.equal(sink.events.length, 1);
  assert.deepEqual(commits, []);
});
