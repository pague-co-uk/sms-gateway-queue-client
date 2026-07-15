import { connect, type ChannelModel } from "amqplib";

import type { QueueClientConfig } from "../config/QueueClientConfig.js";

export class ConnectionManager {
  private connection: ChannelModel | null = null;

  constructor(
    private readonly config: QueueClientConfig,
  ) {}

  public getConnection(): ChannelModel {
    if (!this.connection) {
      throw new Error("RabbitMQ is not connected.");
    }

    return this.connection;
  }

  public get connected(): boolean {
    return this.connection !== null;
  }

  public async connect(): Promise<void> {
    if (this.connection) {
      return;
    }

    this.connection = await connect(this.config.url, {
      clientProperties: {
        connection_name: this.config.connectionName,
      },
      heartbeat: this.config.heartbeat,
    });
  }

  public async close(): Promise<void> {
    if (!this.connection) {
      return;
    }

    await this.connection.close();
    this.connection = null;
  }
}