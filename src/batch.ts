import type { KafkaMessage } from "kafkajs";

import type { MessageProcessor } from "./processor.js";

export type BatchHooks = Readonly<{
  isRunning: () => boolean;
  isStale: () => boolean;
  heartbeat: () => Promise<void>;
  commit: (nextOffset: string) => Promise<void>;
}>;

export async function processPartitionBatch(
  topic: string,
  partition: number,
  messages: readonly KafkaMessage[],
  processor: MessageProcessor,
  hooks: BatchHooks,
): Promise<number> {
  let processedCount = 0;
  let lastProcessedOffset: string | null = null;
  let interrupted = false;

  for (const message of messages) {
    if (!hooks.isRunning() || hooks.isStale()) {
      interrupted = true;
      break;
    }
    await processor.process(message.value, {
      topic,
      partition,
      offset: message.offset,
      key: message.key?.toString("utf8") ?? null,
    });
    processedCount += 1;
    lastProcessedOffset = message.offset;
    await hooks.heartbeat();
  }

  if (lastProcessedOffset !== null && !interrupted) {
    await hooks.commit(nextOffset(lastProcessedOffset));
  }
  return processedCount;
}

function nextOffset(offset: string): string {
  try {
    return (BigInt(offset) + 1n).toString();
  } catch {
    throw new Error(`Kafka offset is not an integer: ${offset}`);
  }
}
