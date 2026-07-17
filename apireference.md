# SMS Gateway Queue Client API Reference

This reference documents version `0.1.1` of `@pague-co-uk/sms-gateway-queue-client`. The package publishes one root entry point and three public symbols: `createQueueClient`, `QueueClient`, and `QueueClientConfig`.

# Queue Client

## createQueueClient

### Purpose

Creates a framework-agnostic RabbitMQ client that can publish JSON messages and subscribe to queues using confirm channels.

### Signature

```ts
function createQueueClient(config: QueueClientConfig): QueueClient;
```

### Parameters

| Name | Type | Required | Description |
| --- | --- | --- | --- |
| `config` | `QueueClientConfig` | Yes | RabbitMQ connection and reconnection configuration. |

### Returns

A new, disconnected `QueueClient`. Call `connect()` before publishing or subscribing.

### Throws

No exceptions are thrown directly by the factory. Connection and operation errors are raised by methods on the returned client.

### Example

```ts
import { createQueueClient } from '@pague-co-uk/sms-gateway-queue-client';

const queueClient = createQueueClient({
  url: process.env.RABBITMQ_URL!,
  connectionName: 'billing-worker',
});

await queueClient.connect();
await queueClient.publish('billing.jobs', { invoiceId: 'inv_123' });
await queueClient.close();
```

### Best Practices

Create one client per application process and manage it through application startup and graceful shutdown. Do not create a client for each message or request.

### Related APIs

`QueueClient`, `QueueClientConfig`.

## QueueClient

### Purpose

Provides RabbitMQ connection management, JSON publishing with publisher confirms, queue subscription, and reconnect/recovery behavior.

### Signature

```ts
class QueueClient {
  constructor(config: QueueClientConfig);
  get connected(): boolean;
  get currentState(): QueueClientState;
  connect(): Promise<void>;
  publish<T>(
    queueName: string,
    message: T,
    options?: import('amqplib').Options.Publish,
  ): Promise<boolean>;
  subscribe<T>(
    queueName: string,
    handler: (message: T) => Promise<void> | void,
    options?: import('amqplib').Options.Consume,
  ): Promise<import('amqplib').Replies.Consume>;
  close(): Promise<void>;
}
```

`QueueClientState` is used by the `currentState` return type but is not exported by this package; consumers can inspect `connected` without relying on that internal enum.

### Parameters

| Name | Type | Required | Description |
| --- | --- | --- | --- |
| `config` | `QueueClientConfig` | Yes, constructor | Connection and reconnect configuration. |
| `queueName` | `string` | Yes, `publish`/`subscribe` | Name of the target RabbitMQ queue. |
| `message` | `T` | Yes, `publish` | JSON-serializable payload. It is serialized with `JSON.stringify`. |
| `options` | `amqplib.Options.Publish` | No, `publish` | RabbitMQ publish options merged over the client's JSON/persistent defaults. |
| `handler` | `(message: T) => Promise<void> \| void` | Yes, `subscribe` | Function invoked with each parsed JSON payload. |
| `options` | `amqplib.Options.Consume` | No, `subscribe` | RabbitMQ consume options. |

### Returns

- `connected` is `true` only while the client state is connected.
- `currentState` returns the internal connection-state enum value.
- `connect()` resolves when the RabbitMQ connection is established; repeated calls outside the disconnected state are no-ops.
- `publish()` resolves to the boolean returned by `amqplib` after publisher confirms complete.
- `subscribe()` resolves to the `amqplib` consumer reply, including its consumer tag.
- `close()` resolves after managed queues and the RabbitMQ connection are closed.

### Throws

- `publish()` and `subscribe()` throw `Error("Queue client is closed.")` after `close()`.
- RabbitMQ connection, channel, queue assertion, publish-confirm, consumer, JSON serialization, JSON parsing, and handler errors can reject their respective promises.

### Constructor

```ts
new QueueClient(config: QueueClientConfig)
```

Creates a disconnected client. It does not establish a network connection until `connect()` is called.

### Properties

| Name | Type | Description |
| --- | --- | --- |
| `connected` | `boolean` | Whether the client state is currently `CONNECTED`. |
| `currentState` | internal `QueueClientState` | Current state: `DISCONNECTED`, `CONNECTING`, `CONNECTED`, `RECONNECTING`, or `CLOSED`. The enum is not publicly exported. |

### Methods

| Method | Description |
| --- | --- |
| `connect()` | Establishes the initial RabbitMQ connection and registers reconnect handlers. |
| `publish()` | Publishes one persistent JSON message through a managed confirm channel. |
| `subscribe()` | Asserts the queue and starts one consumer whose successful handler calls are acknowledged. |
| `close()` | Closes managed queues and the underlying connection; it is idempotent after closure. |

### Lifecycle

1. Construct with `createQueueClient()` or `new QueueClient()`.
2. Call `connect()` during application startup.
3. Call `publish()` and/or `subscribe()` while the application runs.
4. Call `close()` during graceful shutdown. A closed client cannot be reused.

On a connection close, the client enters reconnecting state and retries with exponential backoff. After a successful reconnect, it recovers all managed queues and restarts registered consumers.

### Thread Safety

The client owns mutable connection, channel, queue, consumer, and state data. Use an instance from one Node.js event-loop context. Do not share it across worker threads; create and connect a client in each worker that needs RabbitMQ access.

### Example

```ts
import { QueueClient } from '@pague-co-uk/sms-gateway-queue-client';

const queueClient = new QueueClient({
  url: 'amqp://guest:guest@localhost:5672',
  connectionName: 'status-update-consumer',
  maxReconnectAttempts: 10,
  reconnectDelay: 1_000,
  maxReconnectDelay: 30_000,
});

await queueClient.connect();

await queueClient.subscribe<{ messageId: string }>(
  'status.updates',
  async ({ messageId }) => {
    console.log(`Processing ${messageId}`);
  },
);
```

### Best Practices

Connect before use and always close during shutdown. Keep handlers idempotent because delivery can be retried after failures or reconnects. Ensure message payloads are JSON-serializable; `undefined`, cyclic structures, and non-JSON values are unsuitable. Avoid calling `close()` while application code is still publishing or registering consumers.

### Related APIs

`createQueueClient`, `QueueClientConfig`.

# Common Types

## QueueClientConfig

### Purpose

Configures the RabbitMQ connection and the queue client's reconnect policy.

### Signature

```ts
interface QueueClientConfig {
  readonly url: string;
  readonly connectionName?: string;
  readonly heartbeat?: number;
  readonly maxReconnectAttempts?: number;
  readonly reconnectDelay?: number;
  readonly maxReconnectDelay?: number;
  readonly autoCreateQueues?: boolean;
  readonly autoRecover?: boolean;
}
```

### Parameters

Not applicable; this is an interface.

### Returns

Not applicable.

### Throws

No exceptions are thrown directly.

### Properties

| Name | Type | Required | Description |
| --- | --- | --- | --- |
| `url` | `string` | Yes | RabbitMQ connection URL, for example `amqp://guest:guest@localhost:5672`. |
| `connectionName` | `string` | No | Name sent as RabbitMQ `connection_name` client property. |
| `heartbeat` | `number` | No | Heartbeat interval in seconds. |
| `maxReconnectAttempts` | `number` | No | Maximum reconnect attempts. Omit for unlimited retries. |
| `reconnectDelay` | `number` | No | Initial reconnect delay in milliseconds. Defaults to `1000` in the implementation. |
| `maxReconnectDelay` | `number` | No | Maximum reconnect delay in milliseconds. Defaults to `30000` in the implementation. |
| `autoCreateQueues` | `boolean` | No | Declared option for automatically creating queues; the current implementation does not read it. |
| `autoRecover` | `boolean` | No | Declared option for automatic recovery; the current implementation reconnects/recover queues regardless of this value. |

### Example

```ts
import type { QueueClientConfig } from '@pague-co-uk/sms-gateway-queue-client';

const config: QueueClientConfig = {
  url: process.env.RABBITMQ_URL!,
  connectionName: 'notification-api',
  heartbeat: 30,
  maxReconnectAttempts: 20,
  reconnectDelay: 500,
  maxReconnectDelay: 15_000,
};
```

### Best Practices

Provide the URL through secure environment configuration and give each process a clear connection name. Set a finite reconnect limit only when the application can safely degrade after RabbitMQ is unavailable. Do not rely on `autoCreateQueues` or `autoRecover` to change current behavior until the implementation consumes those options.

### Related APIs

`createQueueClient`, `QueueClient`.

# Errors

The package exports no custom error classes. Methods may throw ordinary `Error` values for invalid lifecycle usage and reject with errors from `amqplib`, JSON processing, or user-provided handlers.

# Constants

The package exports no constants or enums. `QueueClientState` is intentionally internal even though it appears in the generated return type of `QueueClient.currentState`.

# Documentation Review

- Undocumented public exports: none. The root declaration exports `createQueueClient`, `QueueClient`, and `QueueClientConfig`.
- API exposure issue: `QueueClient.currentState` returns `QueueClientState`, but that enum is not exported by the package. Consumers can receive the value but cannot import the named enum type.
- Configuration issue: `QueueClientConfig.autoCreateQueues` and `autoRecover` are documented configuration properties but are not read by the current implementation.
- Missing JSDoc: `QueueClient`, `createQueueClient`, and their public methods have no source JSDoc. `QueueClientConfig` is documented well.
- Reliability consideration: the consumer acknowledges a message only after the handler resolves. A handler failure rejects the consume callback without an explicit nack/requeue policy, so delivery/error behavior should be made explicit in a future API revision.

# Public API Summary

```text
Queue Client
  createQueueClient()
  QueueClient
    connected
    currentState
    connect()
    publish()
    subscribe()
    close()

Common Types
  QueueClientConfig

Errors
  (no custom error exports)

Constants
  (no public constants or enums)
```
