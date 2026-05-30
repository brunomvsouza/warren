// Package amqpmock provides gomock mocks for warren's public interfaces plus
// hand-written constructors for fabricating concrete warren.Delivery[M] /
// warren.Batch[M] values in tests.
//
// Mocked interfaces (one generated file each):
//
//   - codec.Codec, codec.HeaderCodec            → codec.go
//   - log.Logger                                → logger.go
//   - metrics.ClientMetrics, .PublisherMetrics,
//     .ConsumerMetrics                          → metrics.go
//   - otel.Tracer, otel.Span, otel.LinkingTracer → tracer.go
//
// The mocks are generated with go.uber.org/mock (gomock). Regenerate them with:
//
//	go install go.uber.org/mock/mockgen@latest
//	go generate ./...
//
// Importing amqpmock pulls in go.uber.org/mock. Tests that only need delivery
// fixtures and want to stay gomock-free can call the root-package constructors
// warren.NewDeliveryFixture / warren.NewBatchFixture directly — NewDelivery and
// NewBatch here re-export them (SPEC §10 decision 9, GA-09). The root warren
// package itself has no dependency on gomock.
package amqpmock
