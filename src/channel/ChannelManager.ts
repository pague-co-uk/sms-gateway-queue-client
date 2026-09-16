import type { ConfirmChannel } from "amqplib";

export class ChannelManager {
  private channel: ConfirmChannel | null = null;

  private creating:
    | Promise<ConfirmChannel>
    | null = null;

  private closing = false;

  constructor(
    private readonly createChannel: () => Promise<ConfirmChannel>,
  ) { }

  public async getChannel(): Promise<ConfirmChannel> {
    if (this.closing) {
      throw new Error(
        "RabbitMQ channel manager is closing.",
      );
    }

    if (this.channel) {
      return this.channel;
    }

    if (this.creating) {
      return this.creating;
    }

    this.creating = this.create();

    try {
      return await this.creating;
    } finally {
      this.creating = null;
    }
  }

  public invalidate(): void {
    this.channel = null;
  }

  public async close(): Promise<void> {
    this.closing = true;

    try {
      const channel = this.channel;

      this.channel = null;

      if (!channel) {
        return;
      }

      try {
        await channel.close();
      } catch {
        /*
         * The channel may already have been closed by
         * RabbitMQ. In that case there is nothing left
         * for the manager to close.
         */
      }
    } finally {
      this.closing = false;
    }
  }

  private async create(): Promise<ConfirmChannel> {
    const channel = await this.createChannel();

    if (this.closing) {
      try {
        await channel.close();
      } catch {
        // Channel may already have been closed.
      }

      throw new Error(
        "RabbitMQ channel manager was closed while creating a channel.",
      );
    }

    this.channel = channel;

    channel.once("close", () => {
      this.handleChannelClosed(channel);
    });

    channel.once("error", () => {
      this.handleChannelClosed(channel);
    });

    return channel;
  }

  private handleChannelClosed(
    channel: ConfirmChannel,
  ): void {
    /*
     * Only invalidate the channel if this is still
     * the channel currently owned by this manager.
     *
     * A stale channel must never be allowed to
     * invalidate a newer replacement channel.
     */
    if (this.channel === channel) {
      this.channel = null;
    }
  }
}