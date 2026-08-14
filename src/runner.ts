import { Kafka, logLevel, type Consumer, type Producer } from "kafkajs";

import { KafkaDeadLetterPublisher } from "./adapters.js";
import { processPartitionBatch } from "./batch.js";
import type { ConsumerConfig } from "./config.js";
import { MessageProcessor } from "./processor.js";
import type { EventSink, IdempotencyStore } from "./types.js";

export type RunningConsumer = Readonly<{
  consumer: Consumer;
  producer: Producer;
  stop: () => Promise<void>;
}>;

export async function startConsumer(
  config: ConsumerConfig,
  sink: EventSink,
  idempotencyStore: IdempotencyStore,
): Promise<RunningConsumer> {
  const kafka = new Kafka({
    clientId: config.clientId,
    brokers: config.brokers,
    logLevel: logLevel.INFO,
  });
  const consumer = kafka.consumer({
    groupId: config.groupId,
    allowAutoTopicCreation: false,
  });
  const producer = kafka.producer({ allowAutoTopicCreation: false });
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: config.topic, fromBeginning: false });

  const processor = new MessageProcessor(
    sink,
    idempotencyStore,
    new KafkaDeadLetterPublisher(producer, config.deadLetterTopic),
    config.maxMessageBytes,
  );

  await consumer.run({
    autoCommit: false,
    eachBatchAutoResolve: false,
    eachBatch: async ({ batch, heartbeat, isRunning, isStale }) => {
      await processPartitionBatch(
        batch.topic,
        batch.partition,
        batch.messages,
        processor,
        {
          heartbeat,
          isRunning,
          isStale,
          commit: async (nextOffset) => {
            await consumer.commitOffsets([{
              topic: batch.topic,
              partition: batch.partition,
              offset: nextOffset,
            }]);
          },
        },
      );
    },
  });

  let stopPromise: Promise<void> | null = null;
  return {
    consumer,
    producer,
    stop: () => {
      stopPromise ??= Promise.all([
        consumer.disconnect(),
        producer.disconnect(),
      ]).then(() => undefined);
      return stopPromise;
    },
  };
}
