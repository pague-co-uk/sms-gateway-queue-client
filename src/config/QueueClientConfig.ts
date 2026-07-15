export interface QueueClientConfig {
  /**
   * RabbitMQ connection URL.
   *
   * Example:
   * amqp://guest:guest@localhost:5672
   */
  readonly url: string;

  /**
   * Name displayed in the RabbitMQ Management UI.
   */
  readonly connectionName?: string;

  /**
   * Heartbeat interval in seconds.
   */
  readonly heartbeat?: number;

  /**
   * Maximum reconnect attempts.
   *
   * Undefined means retry forever.
   */
  readonly maxReconnectAttempts?: number;

  /**
   * Initial reconnect delay in milliseconds.
   *
   * @default 1000
   */
  readonly reconnectDelay?: number;

  /**
   * Maximum reconnect delay in milliseconds.
   *
   * @default 30000
   */
  readonly maxReconnectDelay?: number;

  /**
   * Automatically create queues that don't exist.
   *
   * @default true
   */
  readonly autoCreateQueues?: boolean;

  /**
   * Automatically recreate queues after reconnect.
   *
   * @default true
   */
  readonly autoRecover?: boolean;
}