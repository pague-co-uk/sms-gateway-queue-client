import type {
  ConfirmChannel,
  ConsumeMessage,
  Options,
  Replies,
} from "amqplib";

import {
  QueueProcessingError,
} from "../errors/queue-processing.error.js";

import {
  ChannelManager,
} from "../channel/ChannelManager.js";

export type QueueRetryBackoffType =
  | "FIXED"
  | "EXPONENTIAL";

export interface QueueBackoffPolicy {
  readonly type: QueueRetryBackoffType;
  readonly delayMs: number;
  readonly maxDelayMs?: number;
}

export interface QueueRetryPolicy {
  /**
   * Number of retries after the initial
   * message delivery.
   *
   * For example, maxRetries: 10 means
   * the message may be delivered a total
   * of 11 times:
   *
   * initial delivery + 10 retries.
   */
  readonly maxRetries: number;

  readonly backoff: QueueBackoffPolicy;
}

export interface QueueSubscribeOptions
  extends Options.Consume {
  /**
   * Retry policy for failed message
   * processing.
   *
   * When omitted, the default policy is used.
   */
  readonly retry?: QueueRetryPolicy;
}

export const DEFAULT_QUEUE_RETRY_POLICY:
  QueueRetryPolicy = {
  maxRetries: 10,
  backoff: {
    type: "EXPONENTIAL",
    delayMs: 1_000,
    maxDelayMs: 30_000,
  },
};

interface ResolvedQueueRetryPolicy {
  readonly maxRetries: number;
  readonly backoff: QueueBackoffPolicy;
}

const RETRY_ATTEMPT_HEADER =
  "x-pague-retry-attempt";

const ORIGINAL_QUEUE_HEADER =
  "x-pague-original-queue";

const LAST_ERROR_HEADER =
  "x-pague-last-error";

const DEAD_LETTER_REASON_HEADER =
  "x-pague-dead-letter-reason";

const RETRY_QUEUE_SUFFIX =
  ".retry";

const DEAD_LETTER_QUEUE_SUFFIX =
  ".dlq";

export class ManagedQueue {
  private readonly channelManager: ChannelManager;

  private readonly queueOptions: Options.AssertQueue;

  private consumer:
    | {
      tag?: string;
      handler: (
        message: unknown,
      ) => Promise<void> | void;
      options?: QueueSubscribeOptions;
      retryPolicy: ResolvedQueueRetryPolicy;
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
    this.channelManager =
      new ChannelManager(
        createChannel,
      );

    this.queueOptions =
      options;
  }

  public async publish<T>(
    message: T,
    options?: Options.Publish,
  ): Promise<boolean> {
    if (this.closing) {
      throw new Error(
        `Queue '${this.name}' is shutting down.`,
      );
    }

    const channel =
      await this.channelManager.getChannel();

    await channel.assertQueue(
      this.name,
      this.queueOptions,
    );

    const published =
      channel.publish(
        "",
        this.name,
        Buffer.from(
          JSON.stringify(message),
        ),
        {
          persistent: true,
          contentType:
            "application/json",
          ...options,
        },
      );

    await channel.waitForConfirms();

    return published;
  }

  public async subscribe<T>(
    handler: (
      message: T,
    ) => Promise<void> | void,
    options?: QueueSubscribeOptions,
  ): Promise<Replies.Consume> {
    const retryPolicy =
      this.resolveRetryPolicy(
        options?.retry,
      );

    this.consumer = {
      handler:
        handler as (
          message: unknown,
        ) =>
          | Promise<void>
          | void,
      retryPolicy,
    };

    if (options !== undefined) {
      this.consumer.options =
        options;
    }

    return this.startConsumer();
  }

  public invalidate(): void {
    this.channelManager.invalidate();
  }

  public async recover(): Promise<void> {
    this.channelManager.invalidate();

    const channel =
      await this.channelManager.getChannel();

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

    const channel =
      await this.channelManager.getChannel();

    if (this.consumer?.tag) {
      await channel.cancel(
        this.consumer.tag,
      );
    }

    await this.channelManager.close();

    this.closing = false;
  }

  private async startConsumer(): Promise<Replies.Consume> {
    if (!this.consumer) {
      throw new Error(
        "No consumer has been registered.",
      );
    }

    const channel =
      await this.channelManager.getChannel();

    await channel.assertQueue(
      this.name,
      this.queueOptions,
    );

    const reply =
      await channel.consume(
        this.name,
        async (
          message: ConsumeMessage | null,
        ) => {
          if (!message) {
            return;
          }

          try {
            const payload =
              JSON.parse(
                message.content.toString(),
              );

            await this.consumer!.handler(
              payload,
            );

            /*
             * ACK only after the consumer
             * handler has completed successfully.
             */
            channel.ack(message);
          } catch (error) {
            /*
             * Processing failures are handled
             * internally by the queue client.
             *
             * The consumer callback never allows
             * the exception to escape into RabbitMQ.
             */
            try {
              await this.handleProcessingFailure(
                channel,
                message,
                error,
              );
            } catch (handlingError) {
              /*
               * If retry/DLQ publication itself
               * fails, do not ACK the original
               * message.
               *
               * NACK without requeue allows RabbitMQ
               * to apply its normal broker-level
               * handling rather than creating an
               * immediate retry loop here.
               */
              channel.nack(
                message,
                false,
                false,
              );

              void handlingError;
            }
          }
        },
        this.consumer.options,
      );

    this.consumer.tag =
      reply.consumerTag;

    return reply;
  }

  private async handleProcessingFailure(
    channel: ConfirmChannel,
    message: ConsumeMessage,
    error: unknown,
  ): Promise<void> {
    const retryPolicy =
      this.consumer?.retryPolicy ??
      this.resolveRetryPolicy();

    const currentAttempt =
      this.getRetryAttempt(message);

    /*
     * QueueProcessingError explicitly controls
     * whether the message is retryable.
     *
     * requeue: false means the message is
     * permanently rejected and goes directly
     * to the dead-letter queue.
     */
    if (
      error instanceof QueueProcessingError &&
      !error.requeue
    ) {
      await this.deadLetter(
        channel,
        message,
        error,
        currentAttempt,
        "PROCESSING_REJECTED",
      );

      return;
    }

    /*
     * maxRetries represents retries AFTER
     * the initial delivery.
     *
     * attempt 0 = initial delivery
     * attempt 1 = first retry
     * ...
     * attempt 10 = tenth retry
     *
     * Once attempt 10 fails, the message
     * is dead-lettered.
     */
    if (
      currentAttempt >=
      retryPolicy.maxRetries
    ) {
      await this.deadLetter(
        channel,
        message,
        error,
        currentAttempt,
        "RETRY_EXHAUSTED",
      );

      return;
    }

    const nextAttempt =
      currentAttempt + 1;

    const delayMs =
      this.calculateBackoff(
        nextAttempt,
        retryPolicy,
      );

    await this.retry(
      channel,
      message,
      error,
      nextAttempt,
      delayMs,
    );
  }

  private async retry(
    channel: ConfirmChannel,
    message: ConsumeMessage,
    error: unknown,
    attempt: number,
    delayMs: number,
  ): Promise<void> {
    const retryQueue =
      `${this.name}${RETRY_QUEUE_SUFFIX}.${attempt}`;

    await channel.assertQueue(
      retryQueue,
      {
        durable: true,
        arguments: {
          "x-message-ttl":
            delayMs,
          "x-dead-letter-exchange":
            "",
          "x-dead-letter-routing-key":
            this.name,
        },
      },
    );

    const headers =
      this.buildRetryHeaders(
        message,
        attempt,
        error,
      );

    const published =
      channel.publish(
        "",
        retryQueue,
        message.content,
        {
          persistent: true,
          contentType:
            message.properties
              .contentType ??
            "application/json",
          ...(message.properties
            .contentEncoding !==
            undefined
            ? {
              contentEncoding:
                message.properties
                  .contentEncoding,
            }
            : {}),
          headers,
        },
      );

    await channel.waitForConfirms();

    if (!published) {
      throw new Error(
        `Failed to publish message from queue '${this.name}' to retry queue '${retryQueue}'.`,
      );
    }

    /*
     * ACK the original message only after
     * the retry message has been successfully
     * published and confirmed.
     */
    channel.ack(message);
  }

  private async deadLetter(
    channel: ConfirmChannel,
    message: ConsumeMessage,
    error: unknown,
    attempt: number,
    reason: string,
  ): Promise<void> {
    const deadLetterQueue =
      `${this.name}${DEAD_LETTER_QUEUE_SUFFIX}`;

    await channel.assertQueue(
      deadLetterQueue,
      {
        durable: true,
      },
    );

    const headers =
      this.buildDeadLetterHeaders(
        message,
        attempt,
        error,
        reason,
      );

    const published =
      channel.publish(
        "",
        deadLetterQueue,
        message.content,
        {
          persistent: true,
          contentType:
            message.properties
              .contentType ??
            "application/json",
          ...(message.properties
            .contentEncoding !==
            undefined
            ? {
              contentEncoding:
                message.properties
                  .contentEncoding,
            }
            : {}),
          headers,
        },
      );

    await channel.waitForConfirms();

    if (!published) {
      throw new Error(
        `Failed to publish message from queue '${this.name}' to dead-letter queue '${deadLetterQueue}'.`,
      );
    }

    /*
     * ACK only after the DLQ publication
     * has been confirmed.
     */
    channel.ack(message);
  }

  private buildRetryHeaders(
    message: ConsumeMessage,
    attempt: number,
    error: unknown,
  ): Record<string, unknown> {
    const headers: Record<string, unknown> = {
      ...message.properties.headers,

      [RETRY_ATTEMPT_HEADER]:
        attempt,

      [ORIGINAL_QUEUE_HEADER]:
        message.properties.headers?.[
        ORIGINAL_QUEUE_HEADER
        ] ?? this.name,

      [LAST_ERROR_HEADER]:
        this.getErrorMessage(error),
    };

    return headers;
  }

  private buildDeadLetterHeaders(
    message: ConsumeMessage,
    attempt: number,
    error: unknown,
    reason: string,
  ): Record<string, unknown> {
    const headers: Record<string, unknown> = {
      ...message.properties.headers,

      [RETRY_ATTEMPT_HEADER]:
        attempt,

      [ORIGINAL_QUEUE_HEADER]:
        message.properties.headers?.[
        ORIGINAL_QUEUE_HEADER
        ] ?? this.name,

      [LAST_ERROR_HEADER]:
        this.getErrorMessage(error),

      [DEAD_LETTER_REASON_HEADER]:
        reason,
    };

    return headers;
  }

  private getRetryAttempt(
    message: ConsumeMessage,
  ): number {
    const value =
      message.properties.headers?.[
      RETRY_ATTEMPT_HEADER
      ];

    if (
      typeof value === "number" &&
      Number.isInteger(value) &&
      value >= 0
    ) {
      return value;
    }

    if (typeof value === "string") {
      const parsed =
        Number.parseInt(
          value,
          10,
        );

      if (
        Number.isInteger(parsed) &&
        parsed >= 0
      ) {
        return parsed;
      }
    }

    return 0;
  }

  private calculateBackoff(
    attempt: number,
    policy: ResolvedQueueRetryPolicy,
  ): number {
    const {
      type,
      delayMs,
      maxDelayMs,
    } = policy.backoff;

    if (type === "FIXED") {
      return delayMs;
    }

    const exponentialDelay =
      delayMs *
      Math.pow(
        2,
        attempt - 1,
      );

    if (
      maxDelayMs !== undefined
    ) {
      return Math.min(
        exponentialDelay,
        maxDelayMs,
      );
    }

    return exponentialDelay;
  }

  private resolveRetryPolicy(
    policy?: QueueRetryPolicy,
  ): ResolvedQueueRetryPolicy {
    const resolved =
      policy ??
      DEFAULT_QUEUE_RETRY_POLICY;

    this.validateRetryPolicy(
      resolved,
    );

    if (
      resolved.backoff.maxDelayMs ===
      undefined
    ) {
      return {
        maxRetries:
          resolved.maxRetries,
        backoff: {
          type:
            resolved.backoff.type,
          delayMs:
            resolved.backoff.delayMs,
        },
      };
    }

    return {
      maxRetries:
        resolved.maxRetries,
      backoff: {
        type:
          resolved.backoff.type,
        delayMs:
          resolved.backoff.delayMs,
        maxDelayMs:
          resolved.backoff.maxDelayMs,
      },
    };
  }

  private validateRetryPolicy(
    policy: QueueRetryPolicy,
  ): void {
    if (
      !Number.isInteger(
        policy.maxRetries,
      ) ||
      policy.maxRetries < 0
    ) {
      throw new Error(
        "Queue retry maxRetries must be a non-negative integer.",
      );
    }

    if (
      !Number.isFinite(
        policy.backoff.delayMs,
      ) ||
      policy.backoff.delayMs <= 0
    ) {
      throw new Error(
        "Queue retry delayMs must be greater than zero.",
      );
    }

    if (
      policy.backoff.maxDelayMs !==
      undefined &&
      (
        !Number.isFinite(
          policy.backoff.maxDelayMs,
        ) ||
        policy.backoff.maxDelayMs <= 0
      )
    ) {
      throw new Error(
        "Queue retry maxDelayMs must be greater than zero.",
      );
    }

    if (
      policy.backoff.maxDelayMs !==
      undefined &&
      policy.backoff.maxDelayMs <
      policy.backoff.delayMs
    ) {
      throw new Error(
        "Queue retry maxDelayMs must be greater than or equal to delayMs.",
      );
    }

    if (
      policy.backoff.type !==
      "FIXED" &&
      policy.backoff.type !==
      "EXPONENTIAL"
    ) {
      throw new Error(
        `Unsupported queue retry backoff type: ${policy.backoff.type}`,
      );
    }
  }

  private getErrorMessage(
    error: unknown,
  ): string {
    if (error instanceof Error) {
      return error.message;
    }

    if (typeof error === "string") {
      return error;
    }

    return "Unknown queue processing error";
  }
}