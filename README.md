# analytics-core

`analytics-core` is a business-neutral analytics data-plane core.

It owns event intake contracts, event bus adapters, ingestion boundaries,
storage adapters, metadata, and analysis primitives. Product concerns such as
pricing, billing, onboarding, account management, and dashboards belong to
upstream applications.

## P1 Scope

- Standard event envelope.
- EventBus abstraction.
- Direct in-process bus for tests and local demos.
- Redis Stream bus for the first deployable queue path.
- Kafka adapter boundary reserved for high-throughput deployments.

