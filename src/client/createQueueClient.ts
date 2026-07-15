import type { QueueClientConfig } from "../config/QueueClientConfig.js";

import { QueueClient } from "./QueueClient.js";

export function createQueueClient(
  config: QueueClientConfig,
): QueueClient {
  return new QueueClient(config);
}