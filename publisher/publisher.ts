import type { ConfirmChannel, Options } from "amqplib";

import { ConnectionManager } from "../connection/connectionManager.js";

export class Publisher {
  private channel: ConfirmChannel | null = null;

  constructor(
    private readonly connectionManager: ConnectionManager,
  ) {}

  public async publish(
    exchange: string,
    routingKey: string,
    message: Buffer,
    options?: Options.Publish,
  ): Promise<boolean> {
    const channel = await this.getChannel();

    const published = channel.publish(
      exchange,
      routingKey,
      message,
      options,
    );

    await channel.waitForConfirms();

    return published;
  }

  private async getChannel(): Promise<ConfirmChannel> {
    if (!this.channel) {
      this.channel = await this.connectionManager.createConfirmChannel();
    }

    return this.channel;
  }
}