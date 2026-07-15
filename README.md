# @pague-co-uk/sms-gateway-queue-client

A lightweight, production-ready, framework-agnostic RabbitMQ client for Node.js.

The package provides a simple queue-based API for publishing and consuming messages while transparently handling RabbitMQ connections, channels, queue creation and automatic recovery.

---

# Features

- Framework agnostic
- Node.js 22+
- TypeScript first
- ESM only
- Strongly typed API
- Automatic queue declaration
- Automatic connection recovery
- Automatic queue recovery
- Exponential reconnect backoff
- One managed channel per queue
- Publisher confirms
- Persistent messages by default
- JSON serialization
- JSON deserialization
- Graceful shutdown
- Minimal public API

---

# Philosophy

Applications should think in terms of queues—not RabbitMQ.

Instead of managing:

- Connections
- Channels
- Confirm channels
- Queue declarations
- Recovery
- Reconnection
- Serialization

Applications simply publish and subscribe.

```ts
await client.publish("sms.submit", message);

await client.subscribe(
    "sms.submit",
    async (message) => {
        // process message
    },
);
```

The package owns the RabbitMQ infrastructure.

---

# Installation

```bash
npm install @pague-co-uk/sms-gateway-queue-client
```

---

# Quick Start

```ts
import {
    createQueueClient,
} from "@pague-co-uk/sms-gateway-queue-client";

const client = createQueueClient({
    url: process.env.RABBITMQ_URL!,
});

await client.connect();

await client.publish(
    "sms.submit",
    {
        id: "123",
        recipient: "+265991234567",
        text: "Hello World",
    },
);

await client.close();
```

---

# Consuming Messages

```ts
interface SmsMessage {
    id: string;
    recipient: string;
    text: string;
}

const client = createQueueClient({
    url: process.env.RABBITMQ_URL!,
});

await client.connect();

await client.subscribe<SmsMessage>(
    "sms.submit",
    async (message) => {
        console.log(message.recipient);
        console.log(message.text);
    },
);
```

---

# Public API

## createQueueClient()

Creates a new queue client.

```ts
const client = createQueueClient(config);
```

---

# QueueClient

## connect()

Establishes the RabbitMQ connection.

```ts
await client.connect();
```

If RabbitMQ later becomes unavailable, the client automatically attempts to reconnect in the background.

---

## publish()

Publishes a strongly typed message.

```ts
await client.publish<T>(
    queue,
    message,
    options?,
);
```

### Parameters

| Name | Type | Description |
|------|------|-------------|
| queue | string | Queue name |
| message | T | Message payload |
| options | Options.Publish | RabbitMQ publish options |

Example

```ts
await client.publish<OrderCreated>(
    "orders.created",
    order,
);
```

---

## subscribe()

Registers a consumer.

```ts
await client.subscribe<T>(
    queue,
    async (message) => {
    },
    options?,
);
```

### Parameters

| Name | Type | Description |
|------|------|-------------|
| queue | string | Queue name |
| handler | Function | Message handler |
| options | Options.Consume | RabbitMQ consume options |

Example

```ts
await client.subscribe<OrderCreated>(
    "orders.created",
    async (order) => {
        console.log(order.id);
    },
);
```

---

## connected

Returns whether the client currently has an active RabbitMQ connection.

```ts
if (client.connected) {

}
```

---

## currentState

Returns the current connection state.

```ts
console.log(client.currentState);
```

Possible values:

- DISCONNECTED
- CONNECTING
- CONNECTED
- RECONNECTING
- CLOSED

---

## close()

Gracefully shuts down the client.

Shutdown sequence:

1. Stop all consumers.
2. Close all managed channels.
3. Close the RabbitMQ connection.
4. Mark the client as closed.

```ts
await client.close();
```

---

# Configuration

```ts
interface QueueClientConfig {
    url: string;

    connectionName?: string;

    heartbeat?: number;

    reconnectDelay?: number;

    maxReconnectDelay?: number;

    maxReconnectAttempts?: number;
}
```

## url

RabbitMQ connection string.

Example

```text
amqp://guest:guest@localhost:5672
```

---

## connectionName

Optional name shown in the RabbitMQ Management UI.

Example

```ts
connectionName: "sms-gateway-api"
```

---

## heartbeat

Heartbeat interval in seconds.

Example

```ts
heartbeat: 60
```

---

## reconnectDelay

Initial reconnect delay in milliseconds.

Default

```text
1000
```

---

## maxReconnectDelay

Maximum reconnect delay.

Default

```text
30000
```

---

## maxReconnectAttempts

Maximum reconnect attempts.

If omitted, the client retries indefinitely.

---

# Automatic Queue Declaration

Queues are created automatically when first used.

No manual setup is required.

```ts
await client.publish(
    "sms.submit",
    message,
);
```

internally performs

```text
assertQueue()

↓

publish()
```

The queue is declared only once and then reused.

---

# Automatic Recovery

The client automatically detects RabbitMQ connection failures.

Recovery process:

```text
Connection Lost
        │
        ▼
Reconnect
        │
        ▼
Replace Connection
        │
        ▼
Recover Managed Queues
        │
        ▼
Resume Consumers
```

No application code is required.

---

# Queue Recovery

Each managed queue remembers:

- Queue name
- Queue configuration
- Consumer
- Consumer options

After reconnect, every queue automatically rebuilds itself.

Applications do not need to resubscribe.

---

# Serialization

Messages are automatically serialized before publishing.

```ts
await client.publish(
    "notifications",
    {
        id: "1",
        text: "Hello",
    },
);
```

Internally

```ts
Buffer.from(JSON.stringify(message))
```

Incoming messages are automatically deserialized.

---

# Queue Lifecycle

Queues are created lazily.

The first call to either

```ts
publish()
```

or

```ts
subscribe()
```

creates the managed queue.

The queue then owns:

- Queue configuration
- Consumer
- Managed channel

and survives reconnects automatically.

---

# Design

Internally the package is composed of four primary components.

```text
QueueClient
    │
    ├── ConnectionManager
    │
    └── ManagedQueue
            │
            └── ChannelManager
```

## QueueClient

Responsible for:

- Client lifecycle
- Connection supervision
- Queue registry
- Recovery orchestration
- Graceful shutdown

---

## ConnectionManager

Responsible for:

- RabbitMQ connection
- Creating confirm channels

---

## ManagedQueue

Responsible for:

- Queue declaration
- Publishing
- Consuming
- Queue recovery
- Queue configuration

---

## ChannelManager

Responsible for:

- Channel lifecycle
- Lazy channel creation
- Channel invalidation
- Channel shutdown

---

# Connection States

```text
DISCONNECTED

↓

CONNECTING

↓

CONNECTED

↓

RECONNECTING

↓

CONNECTED
```

Closing the client transitions to

```text
CLOSED
```

after which no further operations are permitted.

---

# Error Handling

Publishing or subscribing on a closed client throws an error.

```ts
Queue client is closed.
```

Connection failures are automatically handled through background reconnect attempts.

---

# Current Capabilities

- Queue publishing
- Queue subscription
- Automatic queue declaration
- Automatic reconnect
- Queue recovery
- Exponential reconnect backoff
- Publisher confirms
- JSON serialization
- JSON deserialization
- Graceful shutdown

---

# Future Enhancements

Potential future additions include:

- Exchange support
- Dead-letter queues
- Retry infrastructure
- RPC
- Message scheduling
- Queue administration
- Metrics
- Health checks

These will be added without changing the existing public API.

---

# Complete Example

```ts
import {
    createQueueClient,
} from "@pague-co-uk/sms-gateway-queue-client";

interface SmsMessage {
    id: string;
    recipient: string;
    text: string;
}

const client = createQueueClient({
    url: process.env.RABBITMQ_URL!,
    connectionName: "sms-gateway-api",
});

await client.connect();

await client.subscribe<SmsMessage>(
    "sms.submit",
    async (message) => {
        console.log(message);
    },
);

await client.publish<SmsMessage>(
    "sms.submit",
    {
        id: "1",
        recipient: "+265991234567",
        text: "Hello World",
    },
);

// ...

await client.close();
```

---

# License

MIT