//go:build integration

package warren_test

// Multi-connection integration tests for T07d.
//
// Run with: make test-integration
//
// These tests require a live RabbitMQ broker (started by the testcontainers
// helper in amqptest/).  They assert:
//   - WithPublisherConnections(3)+WithConsumerConnections(2) opens 5 sockets
//     visible via rabbitmqctl list_connections name.
//   - Connection names follow the "<base>-pub-<n>" / "<base>-con-<n>" format.
//   - Consumer pin stability: killing the consumer connection that hosts a
//     known consumer causes it to re-subscribe on the same logical pin-index
//     (verified via the named connection suffix).
//
// TODO(T07d): implement once amqptest/ container helper is available (T37).
