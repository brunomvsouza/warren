package warren

import (
	"crypto/tls"
	"maps"
	"net"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"

	"github.com/brunomvsouza/warren/log"
	"github.com/brunomvsouza/warren/metrics"
	"github.com/brunomvsouza/warren/otel"
)

// connOptions holds the resolved settings for a Connection after all Option
// functions have been applied. Dial applies defaults for any field left at
// its zero value.
type connOptions struct {
	// Network targets. addr is the primary URI; addrs overrides it with a
	// cluster-failover list. If both are empty, "amqp://guest:guest@localhost/" is used.
	addr  string
	addrs []string

	// TLS configuration. Required when the URI scheme is amqps://.
	tlsConfig *tls.Config

	// PLAIN credentials. Used unless saslMechanism == SASLExternal.
	username string
	password string

	// saslMechanism selects PLAIN (default) or EXTERNAL. EXTERNAL requires
	// tlsConfig with a client certificate and an amqps:// URI.
	saslMechanism SASLMechanism

	// vhost is the AMQP virtual-host namespace. Defaults to "/" when empty.
	vhost string

	// heartbeat configures the AMQP heartbeat interval negotiated with the
	// broker. Zero uses the server's default (~10 s). Setting it to a
	// negative value disables heartbeats (strongly discouraged; Dial logs a
	// warning).
	heartbeat time.Duration

	// channelMax is the maximum number of AMQP channels the client will
	// negotiate. Zero lets the server choose (recommended default).
	channelMax uint16

	// frameMax is the maximum frame size in bytes. Must be ≥ 4096
	// (AMQP-spec minimum) or zero (server-driven). Dial rejects values
	// between 1 and 4095 with ErrInvalidOptions.
	frameMax uint32

	// dialer is an optional custom net.Conn factory. If nil, the amqp091-go
	// default (net.DialTimeout with 30 s) is used.
	dialer func(network, addr string) (net.Conn, error)

	// clientProperties is the AMQP client-properties table sent to the broker
	// during connection.open. Merged over the amqp091-go defaults.
	clientProperties amqp091.Table

	// connectionName is the human-readable name shown in the broker's
	// management UI. Defaults to "<binary>-<hostname>-<pid>".
	connectionName string

	// connectDelay is an optional fixed pause before the first connection
	// attempt. Useful when starting alongside a broker container.
	connectDelay time.Duration

	// reconnectBackoff controls the exponential-backoff policy used between
	// reconnect attempts. Zero values use RetryPolicy defaults (1 s / 30 s / ×2).
	reconnectBackoff RetryPolicy

	// channelPoolSize is the number of pre-opened channels in the per-connection
	// pool used by publishers. Validated against the requested channel-max
	// (fail-fast) and the broker-negotiated channel-max (post-handshake) in Dial.
	// Default 8.
	channelPoolSize int

	// pubConns / conConns are used by T07d (multi-conn fan-out). They are
	// accepted here so that Dial can emit the single-socket warning when
	// either is explicitly set to 1.
	pubConns int
	conConns int

	// callbacks
	onBlocked      func(reason string)
	onTopoDegraded func(error)
	onReconnect    func()
	onResubscribe  func(queue string)

	// observability
	logger  log.Logger
	metrics metrics.ClientMetrics
	tracer  otel.Tracer
}

// Option configures a Connection during Dial.
type Option func(*connOptions)

// WithAddr sets the primary AMQP URI (e.g. "amqp://user:pass@host:5672/vhost").
// If not provided, defaults to "amqp://guest:guest@localhost/".
func WithAddr(addr string) Option {
	return func(o *connOptions) { o.addr = addr }
}

// WithAddrs sets a cluster-failover list of AMQP URIs. When set, this overrides
// WithAddr.
//
// The list is tried in order on the initial connect: the first reachable node
// wins and sticks for the life of that socket. On a disconnect, reconnect
// rotates round-robin to the next URI in the list (wrapping at the end), so a
// downed node is skipped on the following attempt instead of being retried in
// place. Each TCP socket in the pool keeps its own cursor.
//
// All URIs should share the same scheme (amqp:// or amqps://); the TLS, SASL,
// and credential settings configured on the Connection apply to whichever node
// is dialled.
func WithAddrs(addrs []string) Option {
	return func(o *connOptions) { o.addrs = addrs }
}

// WithTLSConfig sets the TLS configuration for amqps:// connections.
// Required when using WithSASLMechanism(SASLExternal).
func WithTLSConfig(cfg *tls.Config) Option {
	return func(o *connOptions) { o.tlsConfig = cfg }
}

// WithAuth sets PLAIN credentials. Ignored when
// WithSASLMechanism(SASLExternal) is in effect (a Dial-time warning is emitted).
func WithAuth(username, password string) Option {
	return func(o *connOptions) {
		o.username = username
		o.password = password
	}
}

// WithSASLMechanism selects the SASL mechanism. The default is SASLPlain.
// SASLExternal delegates authentication to the TLS client certificate and
// requires WithTLSConfig with at least one certificate and an amqps:// URI.
func WithSASLMechanism(m SASLMechanism) Option {
	return func(o *connOptions) { o.saslMechanism = m }
}

// WithVHost sets the AMQP virtual-host. Defaults to "/" when empty.
func WithVHost(vhost string) Option {
	return func(o *connOptions) { o.vhost = vhost }
}

// WithHeartbeat sets the AMQP heartbeat interval negotiated with the broker.
//
// Partition detection: the broker (and amqp091-go) drops the connection after
// missing two heartbeats, so the time to notice a half-open TCP link is roughly
// 2× the interval. Size the interval to the detection latency you can tolerate:
//
//   - 5 s  — high-throughput / low-latency services (≈10 s detection)
//   - 30 s — batch / low-priority workloads (≈60 s detection)
//   - 60 s — battery-constrained clients or behind an idle-timeout load balancer
//     (≈120 s detection)
//
// WithHeartbeat(0) — the zero value, also the default when the option is not
// set — uses the server's negotiated default (~10 s on RabbitMQ); it is not a
// request to disable heartbeats. A negative value disables heartbeats entirely;
// Dial emits a warning because disabled heartbeats prevent detection of
// half-open TCP connections.
func WithHeartbeat(d time.Duration) Option {
	return func(o *connOptions) { o.heartbeat = d }
}

// WithChannelMax sets the maximum number of AMQP channels to negotiate with
// the broker. Zero (the default) lets the server choose the limit.
func WithChannelMax(n uint16) Option {
	return func(o *connOptions) { o.channelMax = n }
}

// WithFrameMax sets the maximum AMQP frame size in bytes.
//
// Recommended sizing tiers:
//   - 4 096 B  — AMQP-spec minimum; never go below this.
//   - 131 072 B (128 KiB) — default amqp091-go value; good for most workloads.
//   - 1 048 576 B (1 MiB) — high-throughput bulk transfers.
//   - 104 857 600 B (100 MiB) — hard ceiling; values above this risk OOM on
//     broker and client. Use chunked publishing instead of large frames.
//
// Zero lets the server choose. Values in [1, 4095] are rejected at Dial time
// with ErrInvalidOptions (AMQP spec §2.3.5 — frame minimum is 4 096 bytes).
func WithFrameMax(n uint32) Option {
	return func(o *connOptions) { o.frameMax = n }
}

// WithDialer sets a custom net.Conn factory used instead of net.DialTimeout.
// Useful for testing, proxies, or Unix-socket transports.
func WithDialer(fn func(network, addr string) (net.Conn, error)) Option {
	return func(o *connOptions) { o.dialer = fn }
}

// WithClientProperties merges additional key-value pairs into the AMQP
// client-properties table sent during connection.open.
func WithClientProperties(props map[string]any) Option {
	return func(o *connOptions) {
		if o.clientProperties == nil {
			o.clientProperties = make(amqp091.Table, len(props))
		}
		maps.Copy(o.clientProperties, props)
	}
}

// WithConnectionName sets a human-readable name shown in the broker's
// management UI and in log lines. Defaults to "<binary>-<hostname>-<pid>".
func WithConnectionName(name string) Option {
	return func(o *connOptions) { o.connectionName = name }
}

// WithConnectDelay introduces a fixed pause before the first connection
// attempt. Useful when co-starting alongside a broker container.
func WithConnectDelay(d time.Duration) Option {
	return func(o *connOptions) { o.connectDelay = d }
}

// WithReconnectBackoff configures the exponential-backoff policy for reconnect
// attempts. Zero-value fields in p use RetryPolicy defaults (Min=1 s, Max=30 s,
// Factor=2.0, unlimited retries).
func WithReconnectBackoff(p RetryPolicy) Option {
	return func(o *connOptions) { o.reconnectBackoff = p }
}

// WithChannelPoolSize sets the number of pre-opened channels per publisher
// TCP connection. Default is 8.
//
// The value must be ≥ 1 and must not exceed the channel-max ceiling, otherwise
// Dial returns ErrInvalidOptions. When WithChannelMax is set explicitly this is
// checked synchronously before any socket is opened; when WithChannelMax is 0
// (server-driven, RabbitMQ defaults to 2047) it is checked against the
// broker-negotiated value once the handshake completes.
func WithChannelPoolSize(n int) Option {
	return func(o *connOptions) { o.channelPoolSize = n }
}

// WithPublisherConnections sets the number of dedicated publisher TCP
// connections. Default is 2 (T07d). Setting n=1 causes Dial to log a
// warning: a single socket is a full-availability gap during reconnect.
func WithPublisherConnections(n int) Option {
	return func(o *connOptions) { o.pubConns = n }
}

// WithConsumerConnections sets the number of dedicated consumer TCP
// connections. Default is 2 (T07d). Setting n=1 causes Dial to log a
// warning.
func WithConsumerConnections(n int) Option {
	return func(o *connOptions) { o.conConns = n }
}

// WithOnBlocked registers a callback fired when the broker sends a
// connection.blocked notification (e.g. due to a memory or disk alarm).
// The reason string is the human-readable explanation from the broker.
func WithOnBlocked(fn func(reason string)) Option {
	return func(o *connOptions) { o.onBlocked = fn }
}

// WithOnTopologyDegraded registers a callback fired exactly once each time
// the connection enters the degraded state (topology redeclare failed after
// reconnect). The callback receives the redeclare error. It is re-armed on
// successful recovery so it fires again on the next degraded transition.
func WithOnTopologyDegraded(fn func(error)) Option {
	return func(o *connOptions) { o.onTopoDegraded = fn }
}

// WithOnReconnect registers a callback fired after each successful reconnect
// barrier (topology redeclared, consumers re-subscribed). The callback runs
// synchronously inside the reconnect barrier before traffic resumes.
func WithOnReconnect(fn func()) Option {
	return func(o *connOptions) { o.onReconnect = fn }
}

// WithOnResubscribe registers a callback fired once per consumer re-subscribe
// after a reconnect, with the queue name that was re-subscribed. It runs inside
// the reconnect barrier alongside the consumer_resubscribed_total metric, after
// the replacement subscription is installed and before delivery resumes.
//
// Each consumer pinned to a reconnecting socket fires the callback once per
// reconnect; keep it fast and non-blocking — a slow callback delays delivery
// resumption for that consumer. Use it to refresh per-subscription state (e.g.
// reset an in-process dedupe window) on reconnect.
func WithOnResubscribe(fn func(queue string)) Option {
	return func(o *connOptions) { o.onResubscribe = fn }
}

// WithLogger sets the logger used for connection lifecycle events.
// Defaults to log.NoOpLogger when not provided.
func WithLogger(l log.Logger) Option {
	return func(o *connOptions) { o.logger = l }
}

// WithMetrics sets the ClientMetrics implementation.
// Defaults to NoOpClientMetrics when not provided.
func WithMetrics(m metrics.ClientMetrics) Option {
	return func(o *connOptions) { o.metrics = m }
}

// WithoutMetrics disables all metric emission for this connection
// (equivalent to WithMetrics(metrics.NoOpClientMetrics{})).
func WithoutMetrics() Option {
	return func(o *connOptions) { o.metrics = metrics.NoOpClientMetrics{} }
}

// WithTracer stores a connection-level OTel tracer. It is reserved for future
// connection-level spans and currently drives none; publish/consume spans are
// enabled per builder via PublisherBuilder.Tracer / ConsumerBuilder.Tracer.
// Defaults to otel.NoOpTracer when not provided.
func WithTracer(t otel.Tracer) Option {
	return func(o *connOptions) { o.tracer = t }
}
