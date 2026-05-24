//go:build integration

package warren_test

import (
	"testing"

	amqp091 "github.com/rabbitmq/amqp091-go"
)

// purgeQueues removes any leftover messages from the named queues using a
// raw AMQP connection. Non-existent queues are silently ignored.
func purgeQueues(t *testing.T, url string, queues ...string) {
	t.Helper()
	rc, err := amqp091.Dial(url)
	if err != nil {
		return
	}
	defer rc.Close()
	ch, err := rc.Channel()
	if err != nil {
		return
	}
	defer ch.Close()
	for _, q := range queues {
		ch.QueuePurge(q, false) //nolint:errcheck
	}
}

// deleteQueues deletes the named queues using a fresh raw AMQP connection.
// Non-existent queues are silently ignored.
func deleteQueues(url string, queues ...string) {
	rc, err := amqp091.Dial(url)
	if err != nil {
		return
	}
	defer rc.Close()
	ch, err := rc.Channel()
	if err != nil {
		return
	}
	defer ch.Close()
	for _, q := range queues {
		ch.QueueDelete(q, false, false, false) //nolint:errcheck
	}
}
