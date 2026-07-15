import type { ConfirmChannel, Options } from "amqplib";

import { ConnectionManager } from "../connection/connectionManager.js";

export class Topology {
  private channel: ConfirmChannel | null = null;

  constructor(
    private readonly connectionManager: ConnectionManager,
  ) {}

  public async assertExchange(
    exchange: string,
    type: "direct" | "fanout" | "topic" | "headers",
    options?: Options.AssertExchange,
  ): Promise<void> {
    const channel = await this.getChannel();

    await channel.assertExchange(exchange, type, options);
  }

  public async assertQueue(
    queue: string,
    options?: Options.AssertQueue,
  ): Promise<void> {
    const channel = await this.getChannel();

    await channel.assertQueue(queue, options);
  }

  public async bindQueue(
    queue: string,
    exchange: string,
    routingKey: string,
    args?: Record<string, unknown>,
  ): Promise<void> {
    const channel = await this.getChannel();

    await channel.bindQueue(queue, exchange, routingKey, args);
  }

  private async getChannel(): Promise<ConfirmChannel> {
    if (!this.channel) {
      this.channel = await this.connectionManager.createConfirmChannel();
    }

    return this.channel;
  }
}