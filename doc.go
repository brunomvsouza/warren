// Package amqp is a modern, ergonomic Go client for AMQP 0-9-1 (RabbitMQ).
//
// It wraps github.com/rabbitmq/amqp091-go with a generics-based, type-safe
// API that handles the production-grade concerns every team rebuilds on top
// of the low-level driver: supervised reconnect with publisher confirms,
// pluggable codecs over typed messages, channel pooling, centralized
// topology declaration, built-in observability (metrics, logging,
// OpenTelemetry), and common patterns (RPC, delayed messages, batch
// consume and publish, dead-letter routing).
//
// See SPEC.md in the repository root for the complete public API surface.
package amqp
