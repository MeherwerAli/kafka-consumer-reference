import assert from "node:assert/strict";
import test from "node:test";

import { loadConfig } from "../src/config.js";


test("loads and trims explicit environment configuration", () => {
  const config = loadConfig({
    KAFKA_BROKERS: "broker-a:9092, broker-b:9092 ",
    KAFKA_CLIENT_ID: "reference-client",
    KAFKA_GROUP_ID: "reference-group",
    KAFKA_TOPIC: "events",
    KAFKA_DEAD_LETTER_TOPIC: "events.dlq",
    MAX_MESSAGE_BYTES: "2048",
  });

  assert.deepEqual(config.brokers, ["broker-a:9092", "broker-b:9092"]);
  assert.equal(config.maxMessageBytes, 2048);
});

test("rejects missing or invalid environment configuration", () => {
  assert.throws(() => loadConfig({}), /KAFKA_BROKERS/);
  assert.throws(
    () => loadConfig({
      KAFKA_BROKERS: "localhost:9092",
      KAFKA_CLIENT_ID: "client",
      KAFKA_GROUP_ID: "group",
      KAFKA_TOPIC: "events",
      KAFKA_DEAD_LETTER_TOPIC: "events.dlq",
      MAX_MESSAGE_BYTES: "zero",
    }),
    /MAX_MESSAGE_BYTES/,
  );
});
