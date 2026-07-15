import { QueueClient } from "./queueClient.js";
import { ConnectionManager } from "../connection/connectionManager.js";
import type { QueueClientConfig } from "../config/QueueClientConfig.js";

export function createQueueClient(
  config: QueueClientConfig,
): QueueClient {
  const connectionManager = new ConnectionManager(config);

  return new QueueClient(connectionManager);
}