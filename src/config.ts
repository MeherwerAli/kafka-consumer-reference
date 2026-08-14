export type ConsumerConfig = Readonly<{
  brokers: string[];
  clientId: string;
  groupId: string;
  topic: string;
  deadLetterTopic: string;
  maxMessageBytes: number;
}>;

export function loadConfig(environment: NodeJS.ProcessEnv = process.env): ConsumerConfig {
  const brokers = required(environment, "KAFKA_BROKERS")
    .split(",")
    .map((value) => value.trim())
    .filter(Boolean);
  if (brokers.length === 0) {
    throw new Error("KAFKA_BROKERS must contain at least one broker");
  }

  const rawMaxMessageBytes = environment.MAX_MESSAGE_BYTES ?? "1048576";
  const maxMessageBytes = Number(rawMaxMessageBytes);
  if (!Number.isSafeInteger(maxMessageBytes) || maxMessageBytes < 1) {
    throw new Error("MAX_MESSAGE_BYTES must be a positive safe integer");
  }

  return {
    brokers,
    clientId: required(environment, "KAFKA_CLIENT_ID"),
    groupId: required(environment, "KAFKA_GROUP_ID"),
    topic: required(environment, "KAFKA_TOPIC"),
    deadLetterTopic: required(environment, "KAFKA_DEAD_LETTER_TOPIC"),
    maxMessageBytes,
  };
}

function required(environment: NodeJS.ProcessEnv, name: string): string {
  const value = environment[name]?.trim();
  if (!value) {
    throw new Error(`${name} is required`);
  }
  return value;
}
