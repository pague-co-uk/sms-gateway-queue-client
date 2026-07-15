import type {
  ConfirmChannel,
  ConsumeMessage,
  Options,
  Replies,
} from "amqplib";

import { ChannelManager } from "../channel/ChannelManager.js";

export class ManagedQueue {
  private readonly channelManager: ChannelManager;

  private readonly queueOptions: Options.AssertQueue;

  private consumer:
    | {
        tag?: string;
        handler: (message: unknown) => Promise<void> | void;
        options?: Options.Consume;
      }
    | undefined;

  private closing = false;

  constructor(
    private readonly name: string,
    createChannel: () => Promise<ConfirmChannel>,
    options: Options.AssertQueue = {
      durable: true,
    },
  ) {
    this.channelManager = new ChannelManager(createChannel);
    this.queueOptions = options;
  }

  public async publish<T>(
    message: T,
    options?: Options.Publish,
  ): Promise<boolean> {
    if (this.closing) {
      throw new Error(`Queue '${this.name}' is shutting down.`);
    }

    const channel = await this.channelManager.getChannel();

    const published = channel.publish(
      "",
      this.name,
      Buffer.from(JSON.stringify(message)),
      {
        persistent: true,
        contentType: "application/json",
        ...options,
      },
    );

    await channel.waitForConfirms();

    return published;
  }

  public async subscribe<T>(
    handler: (message: T) => Promise<void> | void,
    options?: Options.Consume,
  ): Promise<Replies.Consume> {
    this.consumer = {
      handler: handler as (message: unknown) => Promise<void> | void,
    };

    if (options) {
      this.consumer.options = options;
    }

    return this.startConsumer();
  }

  public invalidate(): void {
    this.channelManager.invalidate();
  }

  public async recover(): Promise<void> {
    this.channelManager.invalidate();

    const channel = await this.channelManager.getChannel();

    await channel.assertQueue(
      this.name,
      this.queueOptions,
    );

    if (this.consumer) {
      await this.startConsumer();
    }
  }

  public async close(): Promise<void> {
    if (this.closing) {
      return;
    }

    this.closing = true;

    const channel = await this.channelManager.getChannel();

    if (this.consumer?.tag) {
      await channel.cancel(this.consumer.tag);
    }

    await this.channelManager.close();

    this.closing = false;
  }

  private async startConsumer(): Promise<Replies.Consume> {
    if (!this.consumer) {
      throw new Error("No consumer has been registered.");
    }

    const channel = await this.channelManager.getChannel();

    await channel.assertQueue(
      this.name,
      this.queueOptions,
    );

    const reply = await channel.consume(
      this.name,
      async (message: ConsumeMessage | null) => {
        if (!message) {
          return;
        }

        const payload = JSON.parse(message.content.toString());

        await this.consumer!.handler(payload);

        channel.ack(message);
      },
      this.consumer.options,
    );

    this.consumer.tag = reply.consumerTag;

    return reply;
  }
}