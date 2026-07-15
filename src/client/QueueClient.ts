import type { Options, Replies } from "amqplib";

import type { QueueClientConfig } from "../config/QueueClientConfig.js";
import { ConnectionManager } from "../connection/ConnectionManager.js";
import { ManagedQueue } from "../queue/ManagedQueue.js";
import { QueueClientState } from "./QueueClientState.js";

export class QueueClient {
  private connectionManager: ConnectionManager;

  private readonly queues = new Map<string, ManagedQueue>();

  private state = QueueClientState.DISCONNECTED;

  constructor(private readonly config: QueueClientConfig) {
    this.connectionManager = new ConnectionManager(config);
  }

  public get connected(): boolean {
    return this.state === QueueClientState.CONNECTED;
  }

  public get currentState(): QueueClientState {
    return this.state;
  }

  public async connect(): Promise<void> {
    if (this.state !== QueueClientState.DISCONNECTED) {
      return;
    }

    this.state = QueueClientState.CONNECTING;

    try {
      await this.connectionManager.connect();

      this.state = QueueClientState.CONNECTED;

      this.registerConnectionHandlers();
    } catch (error) {
      this.state = QueueClientState.DISCONNECTED;

      throw error;
    }
  }

  public async publish<T>(
    queueName: string,
    message: T,
    options?: Options.Publish,
  ): Promise<boolean> {
    if (this.state === QueueClientState.CLOSED) {
      throw new Error("Queue client is closed.");
    }

    return this.getQueue(queueName).publish(message, options);
  }

  public async subscribe<T>(
    queueName: string,
    handler: (message: T) => Promise<void> | void,
    options?: Options.Consume,
  ): Promise<Replies.Consume> {
    if (this.state === QueueClientState.CLOSED) {
      throw new Error("Queue client is closed.");
    }

    return this.getQueue(queueName).subscribe(handler, options);
  }

  public async close(): Promise<void> {
    if (this.state === QueueClientState.CLOSED) {
      return;
    }

    this.state = QueueClientState.CLOSED;

    await Promise.all(
      [...this.queues.values()].map((queue) => queue.close()),
    );

    this.queues.clear();

    await this.connectionManager.close();
  }

  private getQueue(name: string): ManagedQueue {
    let queue = this.queues.get(name);

    if (!queue) {
      queue = new ManagedQueue(
        name,
        () => this.connectionManager.createConfirmChannel(),
      );

      this.queues.set(name, queue);
    }

    return queue;
  }

  private registerConnectionHandlers(): void {
    this.connectionManager.onClose(() => {
      if (this.state === QueueClientState.CLOSED) {
        return;
      }

      this.state = QueueClientState.RECONNECTING;

      void this.reconnect();
    });

    this.connectionManager.onError(() => {
      if (this.state === QueueClientState.CLOSED) {
        return;
      }

      this.state = QueueClientState.RECONNECTING;
    });
  }

  private async reconnect(): Promise<void> {
    let delay = this.config.reconnectDelay ?? 1000;
    const maxDelay = this.config.maxReconnectDelay ?? 30000;
    const maxAttempts = this.config.maxReconnectAttempts;

    let attempts = 0;

    while (this.state !== QueueClientState.CLOSED) {
      if (
        maxAttempts !== undefined &&
        attempts >= maxAttempts
      ) {
        this.state = QueueClientState.DISCONNECTED;
        return;
      }

      try {
        const connectionManager = new ConnectionManager(this.config);

        await connectionManager.connect();

        this.connectionManager = connectionManager;

        this.registerConnectionHandlers();

        for (const queue of this.queues.values()) {
          await queue.recover();
        }

        this.state = QueueClientState.CONNECTED;

        return;
      } catch {
        attempts++;

        await new Promise((resolve) =>
          setTimeout(resolve, delay),
        );

        delay = Math.min(delay * 2, maxDelay);
      }
    }
  }
}