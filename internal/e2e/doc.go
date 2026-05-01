// Package e2e verifies the deployable P1 data pipeline against local services.
//
// Tests in this package are opt-in because they require Redis, MySQL, and
// ClickHouse from docker-compose rather than in-memory fakes.
package e2e
