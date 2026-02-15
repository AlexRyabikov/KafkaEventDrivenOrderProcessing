# Kafka Event-Driven Order Processing (Go + React + Postgres + Docker)

A portfolio-ready demo where Kafka is the backbone of a microservices workflow.

## Why this architecture

In distributed systems, direct synchronous calls between services create tight coupling and failure cascades.  
With Kafka, each service communicates through events:

- reliable delivery (messages persist in broker)
- independent scaling of producers/consumers
- ordered processing per entity key (`order_id`)
- replay for debugging and recovery

This is conceptually similar to using RabbitMQ to protect SQL from race-heavy concurrent writes: event flow serializes access patterns and reduces direct contention on the database.

## What this demo shows

- event-driven communication between services
- CQRS-style split (write path through API/events, read path through `GET /orders`)
- idempotent orchestration via `processed_events` table
- dead-letter topics for malformed payloads
- correlation IDs for traceability
- Kafka in KRaft mode (no ZooKeeper)

## Architecture

`React UI -> order-api (producer) -> Kafka -> payment-service + inventory-service + analytics-service`

`payment/inventory results -> order-orchestrator -> Postgres status update -> order.status.changed`

`notification-service <- order.status.changed`

## Event flow

1. UI creates order via `POST /orders`.
2. `order-api` saves order (`NEW`) in Postgres and emits `order.created`.
3. `payment-service` emits `payment.result`.
4. `inventory-service` emits `inventory.result`.
5. `order-orchestrator` consumes both and updates final status:
   - `CONFIRMED` if payment + inventory succeeded
   - `CANCELLED` if any failed
6. `notification-service` consumes `order.status.changed`.
7. `analytics-service` consumes `order.created`.

## Stack

- Go + chi (API and microservices)
- Kafka (KRaft)
- Postgres
- React + Vite
- Docker Compose

## Configuration

All ports and topic/group names are configured via `.env`.

```bash
cp .env.example .env
```

Change ports in `.env` if your host has collisions.

## Quick start

```bash
docker compose up --build
```

Open:

- UI: `http://localhost:${UI_PORT}` from your `.env`
- API health: `http://localhost:${API_PORT}/health`

## API

- `POST /orders`
- `GET /orders`
- `GET /orders/{id}`

Example payload:

```json
{
  "customer_email": "demo@example.com",
  "item_sku": "SKU-CHAIR-01",
  "quantity": 1,
  "amount_cents": 14990
}
```

## Repository layout

- `services/order-api` - REST API + producer
- `services/payment-service` - payment consumer/producer
- `services/inventory-service` - inventory consumer/producer
- `services/order-orchestrator` - state orchestration and final status updates
- `services/notification-service` - final status notification consumer
- `services/analytics-service` - analytics consumer
- `pkg/events` - shared event contracts
- `pkg/kafkaio` - Kafka helpers
- `infra/postgres/init.sql` - DB schema
- `frontend` - React UI

## Notes

- Demo uses deterministic business rules:
  - payment fails if `amount_cents > 100000`
  - inventory fails if `quantity > 10`
- Auto topic creation is enabled for local demo convenience.
