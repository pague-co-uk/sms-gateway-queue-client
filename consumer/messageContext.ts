import type { ConfirmChannel, ConsumeMessage } from "amqplib";

export class MessageContext {
  constructor(
    public readonly message: ConsumeMessage,
    private readonly channel: ConfirmChannel,
  ) {}

  public ack(allUpTo = false): void {
    this.channel.ack(this.message, allUpTo);
  }

  public nack(allUpTo = false, requeue = true): void {
    this.channel.nack(this.message, allUpTo, requeue);
  }

  public reject(requeue = false): void {
    this.channel.reject(this.message, requeue);
  }
}