import type {
  ConfirmChannel,
  ConsumeMessage,
  Options,
  Replies,
} from "amqplib";

import { ConnectionManager } from "../connection/connectionManager.js";
import { MessageContext } from "./messageContext.js";

export class Consumer {
  constructor(
    private readonly connectionManager: ConnectionManager,
  ) {}

  public async consume(
    queue: string,
    onMessage: (
      context: MessageContext,
    ) => Promise<void> | void,
    options?: Options.Consume,
  ): Promise<Replies.Consume> {
    const channel = await this.getChannel();

    return channel.consume(
      queue,
      async (message: ConsumeMessage | null) => {
        if (!message) {
          return;
        }

        await onMessage(
          new MessageContext(message, channel),
        );
      },
      options,
    );
  }

  private async getChannel(): Promise<ConfirmChannel> {
    return this.connectionManager.createConfirmChannel();
  }
}