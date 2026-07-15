export interface QueueClientConfig {
  /**
   * RabbitMQ connection URL.
   *
   * Example:
   * amqp://guest:guest@localhost:5672
   */
  readonly url: string;

  /**
   * Connection name shown in the RabbitMQ management UI.
   */
  readonly connectionName?: string;

  /**
   * Heartbeat interval in seconds.
   *
   * Defaults to RabbitMQ's negotiated value when omitted.
   */
  readonly heartbeat?: number;

  /**
   * Maximum number of reconnect attempts.
   *
   * Undefined means retry indefinitely.
   */
  readonly maxReconnectAttempts?: number;

  /**
   * Initial reconnect delay in milliseconds.
   */
  readonly reconnectDelay?: number;

  /**
   * Maximum reconnect delay in milliseconds.
   */
  readonly maxReconnectDelay?: number;

  /**
   * Enable publisher confirms.
   *
   * Defaults to true.
   */
  readonly confirmPublish?: boolean;
}