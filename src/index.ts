export {
  ConsoleUpsertSink,
  InMemoryIdempotencyStore,
  KafkaDeadLetterPublisher,
} from "./adapters.js";
export { processPartitionBatch } from "./batch.js";
export type { BatchHooks } from "./batch.js";
export { loadConfig } from "./config.js";
export type { ConsumerConfig } from "./config.js";
export { MessageProcessor } from "./processor.js";
export { startConsumer } from "./runner.js";
export type { RunningConsumer } from "./runner.js";
export type {
  DeadLetterPublisher,
  DeadLetterRecord,
  EventEnvelope,
  EventSink,
  IdempotencyStore,
  MessageCoordinates,
  ProcessingOutcome,
} from "./types.js";
export { InvalidEventError, parseEvent } from "./validation.js";
