package amqpmock

// Mock generation directives. Run `go generate ./...` from the module root with
// mockgen installed (`go install go.uber.org/mock/mockgen@latest`). Each line
// regenerates one destination file in this package, in gomock "package mode".

//go:generate mockgen -destination codec.go -package amqpmock github.com/brunomvsouza/warren/codec Codec,HeaderCodec
//go:generate mockgen -destination logger.go -package amqpmock github.com/brunomvsouza/warren/log Logger
//go:generate mockgen -destination metrics.go -package amqpmock github.com/brunomvsouza/warren/metrics ClientMetrics,PublisherMetrics,ConsumerMetrics
//go:generate mockgen -destination tracer.go -package amqpmock github.com/brunomvsouza/warren/otel Tracer,Span,LinkingTracer
