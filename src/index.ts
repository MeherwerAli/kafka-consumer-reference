import { ConsoleUpsertSink, InMemoryIdempotencyStore } from "./adapters.js";
import { loadConfig } from "./config.js";
import { startConsumer } from "./runner.js";


async function main(): Promise<void> {
  const runningConsumer = await startConsumer(
    loadConfig(),
    new ConsoleUpsertSink(),
    new InMemoryIdempotencyStore(),
  );
  let stopping = false;
  const stop = async (signal: NodeJS.Signals): Promise<void> => {
    if (stopping) {
      return;
    }
    stopping = true;
    console.log(JSON.stringify({ level: "info", action: "shutdown", signal }));
    await runningConsumer.stop();
  };

  process.once("SIGINT", () => void stop("SIGINT"));
  process.once("SIGTERM", () => void stop("SIGTERM"));
}

main().catch((error: unknown) => {
  console.error(JSON.stringify({
    level: "error",
    action: "startup-failed",
    error: error instanceof Error ? error.message : "unknown error",
  }));
  process.exitCode = 1;
});
