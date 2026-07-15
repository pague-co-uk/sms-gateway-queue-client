# @pague-co-uk/sms-gateway-queue-client

A lightweight, production-ready, framework-agnostic RabbitMQ client for Node.js.

This package provides a simple, opinionated API for publishing and consuming messages without exposing the complexity of RabbitMQ's connection, channel, and topology management.

---

## Features

- Framework agnostic
- Node.js 22+
- TypeScript first
- ESM only
- Automatic queue declaration
- Persistent messages by default
- JSON serialization/deserialization
- Queue-based API
- One managed channel per queue
- Strong typing
- Minimal public API

---

## Philosophy

Applications should think in terms of queues—not RabbitMQ.

Instead of managing:

- Connections
- Channels
- Exchanges
- Queue declarations
- Bindings
- Confirm channels

Applications simply publish to and subscribe from queues.

Example:

```ts
await client.publish("sms.submit", message);

await client.subscribe("sms.submit", async (message) => {
    // process message
});
```

The package owns the RabbitMQ implementation details.

---

# Installation

```bash
npm install @pague-co-uk/sms-gateway-queue-client
```

---

# Quick Start

```ts
import { createQueueClient } from "@pague-co-uk/sms-gateway-queue-client";

const client = createQueueClient({
    url: process.env.RABBITMQ_URL!,
});

await client.connect();

await client.publish("sms.submit", {
    id: "123",
    recipient: "+265991234567",
    text: "Hello World",
});

await client.close();
```

---

# Consuming Messages

```ts
import { createQueueClient } from "@pague-co-uk/sms-gateway-queue-client";

const client = createQueueClient({
    url: process.env.RABBITMQ_URL!,
});

await client.connect();

await client.subscribe<SmsMessage>(
    "sms.submit",
    async (message) => {
        console.log(message);
    },
);
```

---

# API

## createQueueClient()

Creates a new QueueClient instance.

```ts
const client = createQueueClient(config);
```

---

# QueueClient

## connect()

Establishes a connection to RabbitMQ.

```ts
await client.connect();
```

---

## publish()

Publishes a strongly typed message to a queue.

```ts
await client.publish<T>(
    queue,
    message,
    options?,
);
```

### Parameters

| Name | Type | Description |
|-------|------|-------------|
| queue | string | Queue name |
| message | T | Message payload |
| options | PublishOptions | Optional RabbitMQ publish options |

### Example

```ts
await client.publish<OrderCreated>(
    "orders.created",
    order,
);
```

---

## subscribe()

Subscribes to a queue.

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
|-------|------|-------------|
| queue | string | Queue name |
| handler | Function | Message handler |
| options | ConsumeOptions | Optional RabbitMQ consume options |

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

## close()

Gracefully closes the RabbitMQ connection.

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

    maxReconnectAttempts?: number;

    reconnectDelay?: number;

    maxReconnectDelay?: number;

    autoCreateQueues?: boolean;

    autoRecover?: boolean;
}
```

---

# Queue Management

The package internally manages queues.

Each logical queue owns:

- Confirm channel
- Queue declaration
- JSON serialization
- JSON deserialization
- Consumer
- Publisher

Applications never interact with these objects directly.

---

# Serialization

Messages are automatically serialized as JSON before publishing.

```ts
await client.publish(
    "notifications",
    {
        id: "1",
        message: "Hello"
    }
);
```

Internally this becomes

```ts
Buffer.from(JSON.stringify(message))
```

Likewise, consumed messages are automatically deserialized before being passed to the handler.

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

creates and initializes the queue.

Subsequent operations reuse the existing managed queue and channel.

---

# Design Principles

The package intentionally hides RabbitMQ implementation details.

Applications should never need to know about:

- Connections
- Channels
- Confirm channels
- Queue declaration
- Exchange declaration
- Queue bindings
- Serialization
- Channel lifecycle

These are considered infrastructure concerns.

---

# Current Implementation

The current implementation provides:

- Queue publishing
- Queue subscription
- Automatic queue creation
- JSON serialization
- JSON deserialization
- One managed channel per queue

---

# Planned Features

Future versions will add:

- Automatic reconnect
- Automatic topology recovery
- Publisher confirms
- Dead-letter queues
- Retry support
- RPC
- Prefetch configuration
- Consumer acknowledgements
- Message headers
- Delayed delivery
- Queue metrics

These features will be added without changing the public API.

---

# Example

Publisher

```ts
const client = createQueueClient({
    url: process.env.RABBITMQ_URL!,
});

await client.connect();

await client.publish("sms.submit", {
    id: "1",
    destination: "+265991234567",
    text: "Hello"
});
```

Consumer

```ts
const client = createQueueClient({
    url: process.env.RABBITMQ_URL!,
});

await client.connect();

await client.subscribe<SmsMessage>(
    "sms.submit",
    async (message) => {
        console.log(message.destination);
    },
);
```

---

# License

MIT