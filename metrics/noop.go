package metrics

import "time"

// NoOpClientMetrics is a ClientMetrics that silently discards all observations.
// Every method is a no-op and performs zero allocations.
type NoOpClientMetrics struct{}

// RecordReconnect discards the reconnect event.
func (NoOpClientMetrics) RecordReconnect(_ string) {}

// RecordBlocked discards the blocked-duration observation.
func (NoOpClientMetrics) RecordBlocked(_ string, _ time.Duration) {}

// RecordTopologyRedeclare discards the redeclare-duration observation.
func (NoOpClientMetrics) RecordTopologyRedeclare(_ string, _ time.Duration) {}

// RecordDegraded discards the degraded-state counter increment.
func (NoOpClientMetrics) RecordDegraded(_, _ string) {}

// NoOpPublisherMetrics is a PublisherMetrics that silently discards all observations.
// Every method is a no-op and performs zero allocations.
type NoOpPublisherMetrics struct{}

// InFlightAdd discards the in-flight gauge adjustment.
func (NoOpPublisherMetrics) InFlightAdd(_ string, _ int64) {}

// RecordRateLimited discards the rate-limited counter increment.
func (NoOpPublisherMetrics) RecordRateLimited(_ string) {}

// RecordPublish discards the publish observation.
func (NoOpPublisherMetrics) RecordPublish(_, _, _, _ string, _ time.Duration) {}

// RecordRetry discards the retry counter increment.
func (NoOpPublisherMetrics) RecordRetry(_, _ string) {}

// NoOpConsumerMetrics is a ConsumerMetrics that silently discards all observations.
// Every method is a no-op and performs zero allocations.
type NoOpConsumerMetrics struct{}

// RecordResubscribed discards the resubscription counter increment.
func (NoOpConsumerMetrics) RecordResubscribed(_ string) {}

// RecordHandlerAbortedChannelClosed discards the aborted-handler counter increment.
func (NoOpConsumerMetrics) RecordHandlerAbortedChannelClosed(_ string) {}

// RecordHandlerTimeout discards the handler-timeout counter increment.
func (NoOpConsumerMetrics) RecordHandlerTimeout(_ string) {}

// RecordHandler discards the handler observation.
func (NoOpConsumerMetrics) RecordHandler(_, _, _ string, _ time.Duration) {}

// RecordCancelled discards the broker-initiated cancel counter increment.
func (NoOpConsumerMetrics) RecordCancelled(_, _ string) {}

// RecordMaxRedeliveries discards the max-redeliveries counter increment.
func (NoOpConsumerMetrics) RecordMaxRedeliveries(_, _ string) {}

// RecordReplierDropNoDLX discards the Replier drop-no-DLX counter increment.
func (NoOpConsumerMetrics) RecordReplierDropNoDLX(_ string) {}

// InFlightBytesAdd discards the in-flight-bytes gauge adjustment.
func (NoOpConsumerMetrics) InFlightBytesAdd(_ string, _ int64) {}
