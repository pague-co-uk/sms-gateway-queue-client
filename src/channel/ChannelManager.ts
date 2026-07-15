import type { ConfirmChannel } from "amqplib";

export class ChannelManager {
  private channel: ConfirmChannel | null = null;

  constructor(
    private readonly createChannel: () => Promise<ConfirmChannel>,
  ) {}

  public async getChannel(): Promise<ConfirmChannel> {
    if (!this.channel) {
      await this.create();
    }

    return this.channel!;
  }

  public invalidate(): void {
    this.channel = null;
  }

  public async close(): Promise<void> {
    if (!this.channel) {
      return;
    }

    try {
      await this.channel.close();
    } finally {
      this.channel = null;
    }
  }

  private async create(): Promise<void> {
    this.channel = await this.createChannel();

    this.channel.once("close", () => {
      this.channel = null;
    });

    this.channel.once("error", () => {
      this.channel = null;
    });
  }
}