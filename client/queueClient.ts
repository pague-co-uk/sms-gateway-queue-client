import type { MessageContext } from "../consumer/messageContext.js";
import type { Options, Replies } from "amqplib";

import { ConnectionManager } from "../connection/connectionManager.js";
import { Consumer } from "../consumer/consumer.js";
import { Publisher } from "../publisher/publisher.js";
import { Topology } from "../topology/topology.js";

export class QueueClient {
  private readonly consumer: Consumer;
  private readonly publisher: Publisher;
  private readonly topology: Topology;

  constructor(
    private readonly connectionManager: ConnectionManager,
  ) {
    this.consumer = new Consumer(connectionManager);
    this.publisher = new Publisher(connectionManager);
    this.topology = new Topology(connectionManager);
  }

  public get connected(): boolean {
    return this.connectionManager.connected;
  }

  public async connect(): Promise<void> {
    await this.connectionManager.connect();
  }

  public async assertExchange(
    exchange: string,
    type: "direct" | "fanout" | "topic" | "headers",
    options?: Options.AssertExchange,
  ): Promise<void> {
    return this.topology.assertExchange(exchange, type, options);
  }

  public async assertQueue(
    queue: string,
    options?: Options.AssertQueue,
  ): Promise<void> {
    return this.topology.assertQueue(queue, options);
  }

  public async bindQueue(
    queue: string,
    exchange: string,
    routingKey: string,
    args?: Record<string, unknown>,
  ): Promise<void> {
    return this.topology.bindQueue(queue, exchange, routingKey, args);
  }

  public async publish(
    exchange: string,
    routingKey: string,
    message: Buffer,
    options?: Options.Publish,
  ): Promise<boolean> {
    return this.publisher.publish(
      exchange,
      routingKey,
      message,
      options,
    );
  }

  public async consume(
    queue: string,
    onMessage: (
      context: MessageContext,
    ) => Promise<void> | void,
    options?: Options.Consume,
  ): Promise<Replies.Consume> {
    return this.consumer.consume(
      queue,
      onMessage,
      options,
    );
  }

  public async close(): Promise<void> {
    await this.connectionManager.close();
  }
}