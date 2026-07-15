import type { Options, Replies } from "amqplib";

import type { QueueClientConfig } from "../config/QueueClientConfig.js";
import { ConnectionManager } from "../connection/ConnectionManager.js";
import { ManagedQueue } from "../queue/ManagedQueue.js";

export class QueueClient {
  private readonly connectionManager: ConnectionManager;
  private readonly queues = new Map<string, ManagedQueue>();

  constructor(config: QueueClientConfig) {
    this.connectionManager = new ConnectionManager(config);
  }

  public get connected(): boolean {
    return this.connectionManager.connected;
  }

  public async connect(): Promise<void> {
    await this.connectionManager.connect();
  }

  public async publish<T>(
    queue: string,
    message: T,
    options?: Options.Publish,
  ): Promise<boolean> {
    return this.getQueue(queue).publish(message, options);
  }

  public async subscribe<T>(
    queue: string,
    handler: (message: T) => Promise<void> | void,
    options?: Options.Consume,
  ): Promise<Replies.Consume> {
    return this.getQueue(queue).subscribe(handler, options);
  }

  public async close(): Promise<void> {
    await this.connectionManager.close();
    this.queues.clear();
  }

  private getQueue(name: string): ManagedQueue {
    let queue = this.queues.get(name);

    if (!queue) {
      queue = new ManagedQueue(name, this.connectionManager);
      this.queues.set(name, queue);
    }

    return queue;
  }
}