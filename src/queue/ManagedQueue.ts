import type {
  ConfirmChannel,
  ConsumeMessage,
  Options,
  Replies,
} from "amqplib";

import { ConnectionManager } from "../connection/ConnectionManager.js";

export class ManagedQueue {
  private channel: ConfirmChannel | null = null;

  constructor(
    private readonly name: string,
    private readonly connectionManager: ConnectionManager,
  ) {}

  public get queue(): string {
    return this.name;
  }

  public async publish<T>(
    message: T,
    options?: Options.Publish,
  ): Promise<boolean> {
    const channel = await this.getChannel();

    return channel.publish(
      "",
      this.name,
      Buffer.from(JSON.stringify(message)),
      {
        persistent: true,
        contentType: "application/json",
        ...options,
      },
    );
  }

  public async subscribe<T>(
    handler: (message: T) => Promise<void> | void,
    options?: Options.Consume,
  ): Promise<Replies.Consume> {
    const channel = await this.getChannel();

    return channel.consume(
      this.name,
      async (message: ConsumeMessage | null) => {
        if (!message) {
          return;
        }

        const payload = JSON.parse(
          message.content.toString(),
        ) as T;

        await handler(payload);

        channel.ack(message);
      },
      options,
    );
  }

  private async getChannel(): Promise<ConfirmChannel> {
    if (this.channel) {
      return this.channel;
    }

    this.channel = await this.connectionManager
      .getConnection()
      .createConfirmChannel();

    await this.channel.assertQueue(this.name, {
      durable: true,
    });

    return this.channel;
  }
}