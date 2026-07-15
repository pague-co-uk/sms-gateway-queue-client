import {
  connect,
  type ChannelModel,
  type ConfirmChannel,
} from "amqplib";

import type { QueueClientConfig } from "../config/QueueClientConfig.js";

export class ConnectionManager {
  private connection: ChannelModel | null = null;

  constructor(
    private readonly config: QueueClientConfig,
  ) { }

  public get connected(): boolean {
    return this.connection !== null;
  }

  public async connect(): Promise<void> {
    if (this.connection) {
      return;
    }

    this.connection = await connect(this.config.url, {
      heartbeat: this.config.heartbeat,
      clientProperties: {
        connection_name: this.config.connectionName,
      },
    });
  }

  public async createConfirmChannel(): Promise<ConfirmChannel> {
    if (!this.connection) {
      throw new Error("RabbitMQ connection has not been established.");
    }

    return this.connection.createConfirmChannel();
  }

  public async close(): Promise<void> {
    if (!this.connection) {
      return;
    }

    await this.connection.close();

    this.connection = null;
  }

  public onClose(listener: () => void): void {
    if (!this.connection) {
      throw new Error("RabbitMQ connection has not been established.");
    }

    this.connection.on("close", listener);
  }

  public onError(listener: (error: Error) => void): void {
    if (!this.connection) {
      throw new Error("RabbitMQ connection has not been established.");
    }

    this.connection.on("error", listener);
  }
}