import assert from "node:assert/strict";
import test from "node:test";

import * as packageEntry from "../src/index.js";

test("exports the reusable API without starting a consumer", () => {
  assert.equal(typeof packageEntry.MessageProcessor, "function");
  assert.equal(typeof packageEntry.processPartitionBatch, "function");
  assert.equal(typeof packageEntry.startConsumer, "function");
  assert.equal(process.exitCode, undefined);
});
