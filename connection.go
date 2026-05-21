package amqp

import (
	"context"
	"crypto/x509"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"

	"github.com/brunomvsouza/amqp/internal/reconnect"
	"github.com/brunomvsouza/amqp/internal/redact"
	"github.com/brunomvsouza/amqp/log"
	"github.com/brunomvsouza/amqp/metrics"
	"github.com/brunomvsouza/amqp/otel"
)

// connRole is the metric role label for the single TCP connection used in T07.
// T07d replaces this with per-supervised-connection roles ("publisher" | "consumer").
const connRole = "publisher"

// Connection manages a single supervised AMQP TCP connection.
//
// It wraps amqp091-go with automatic reconnect (exponential backoff), a
// synchronous reconnect barrier that guarantees topology + consumer state is
// fully restored before traffic resumes, and a degraded-state machine for
// persistent topology failures.
//
// Multi-connection fan-out by role (publisher / consumer) is added in T07d.
type Connection struct {
	mu  sync.RWMutex
	raw *amqp091.Connection // current live socket; nil while reconnecting

	opts     connOptions
	authUser string // set before Dial returns; immutable after

	// reconnect barrier — callers block here while reconnecting or broker-blocked
	barrierMu    sync.Mutex
	barrierCond  *sync.Cond
	reconnecting bool // true while barrier is active
	blocked      bool // true while broker has sent connection.blocked (cleared on reconnect)

	// degraded state — set when topology redeclare fails persistently
	degraded    bool
	degradedErr error

	// reconnect hooks registered by Topology.AttachTo (T16) and Consumer (T18)
	hooksMu sync.Mutex
	hooks   []func(ctx context.Context) error

	// lifecycle
	cancel context.CancelFunc
	done   chan struct{}
	closed bool

	// tracks in-flight onTopoDegraded callback goroutines; drained in Close
	wg sync.WaitGroup
}

// Dial establishes a supervised AMQP connection and returns it.
//
// Dial applies all provided options, validates them, and attempts to connect
// with the configured backoff policy. It returns when the first connection
// succeeds. Subsequent failures are handled automatically by the internal
// supervisor; callers observe reconnects via metric increments and the
// WithOnReconnect callback.
//
// Validation errors (ErrInvalidOptions) are returned synchronously. Network
// errors cause Dial to retry up to the configured Retries limit; when exhausted
// (or when ctx is cancelled), the last network error is returned.
func Dial(ctx context.Context, options ...Option) (*Connection, error) {
	opts := connOptions{
		saslMechanism:   SASLPlain,
		channelPoolSize: 8,
		pubConns:        2,
		conConns:        2,
	}
	for _, o := range options {
		o(&opts)
	}
	applyConnDefaults(&opts)

	if err := validateConnOptions(&opts); err != nil {
		return nil, err
	}

	authUser := computeAuthUser(&opts)

	c := &Connection{
		opts:     opts,
		authUser: authUser,
		done:     make(chan struct{}),
	}
	c.barrierCond = sync.NewCond(&c.barrierMu)

	if opts.connectDelay > 0 {
		t := time.NewTimer(opts.connectDelay)
		select {
		case <-ctx.Done():
			t.Stop()
			return nil, ctx.Err()
		case <-t.C:
		}
	}

	// initial connect (with backoff)
	if err := c.connectOnce(ctx); err != nil {
		return nil, err
	}

	// start supervisor goroutine; inherit trace/log values from Dial ctx but
	// not its cancellation (supervisor must outlive the caller's ctx).
	// cancel is stored on c and called by Close — gosec G118 is a false positive here.
	supCtx, cancel := context.WithCancel(context.WithoutCancel(ctx)) //nolint:gosec // G118: cancel stored on struct, called by Close
	c.cancel = cancel
	go c.supervisor(supCtx)

	return c, nil
}

// AuthenticatedUser returns the identity that was authenticated during Dial.
//
// For SASLPlain this is the username supplied via WithAuth. For SASLExternal it
// is the Common Name of the first client certificate. The value is set before
// Dial returns and does not change, even if the connection enters a degraded
// state.
func (c *Connection) AuthenticatedUser() string { return c.authUser }

// Health opens a temporary AMQP channel and immediately closes it. Returns an
// error if the connection is closed, not yet established, or the channel
// open fails. Returns ErrReconnecting immediately (non-blocking) if a
// reconnect barrier is currently active.
func (c *Connection) Health(ctx context.Context) error {
	c.mu.RLock()
	raw, closed := c.raw, c.closed
	c.mu.RUnlock()
	if closed {
		return ErrAlreadyClosed
	}
	c.barrierMu.Lock()
	reconnecting := c.reconnecting
	c.barrierMu.Unlock()
	if reconnecting {
		return ErrReconnecting
	}
	if raw == nil {
		return ErrNotConnected
	}
	ch, err := raw.Channel()
	if err != nil {
		return err
	}
	return ch.Close()
}

// Close drains in-flight work and shuts down the connection. Returns
// ErrAlreadyClosed if called more than once.
func (c *Connection) Close(ctx context.Context) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrAlreadyClosed
	}
	c.closed = true
	c.mu.Unlock()

	c.cancel()

	select {
	case <-c.done:
	case <-ctx.Done():
		return fmt.Errorf("amqp: close timed out: %w", ctx.Err())
	}

	// drain in-flight onTopoDegraded callback goroutines
	callbacksDone := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(callbacksDone)
	}()
	select {
	case <-callbacksDone:
	case <-ctx.Done():
		return fmt.Errorf("amqp: close timed out waiting for callbacks: %w", ctx.Err())
	}
	return nil
}

// ForceReconnect triggers a manual reconnect cycle without restarting the
// process. Intended as an operator escape hatch for recovering from a
// degraded state. Returns ErrAlreadyClosed if the connection is shut down.
func (c *Connection) ForceReconnect() error {
	c.mu.RLock()
	closed := c.closed
	raw := c.raw
	c.mu.RUnlock()
	if closed {
		return ErrAlreadyClosed
	}
	if raw != nil {
		// closing the current socket triggers the supervisor's reconnect path
		_ = raw.Close()
	}
	return nil
}

// registerReconnectHook adds fn to the list of hooks invoked synchronously
// inside the reconnect barrier after each successful TCP reconnect. fn is
// called with a context that is cancelled when the barrier deadline expires.
// If fn returns an error, the connection transitions to the degraded state.
//
// This is an internal API consumed by Topology.AttachTo (T16) and Consumer
// re-subscribe (T18). It is not part of the public surface.
func (c *Connection) registerReconnectHook(fn func(ctx context.Context) error) {
	c.hooksMu.Lock()
	c.hooks = append(c.hooks, fn)
	c.hooksMu.Unlock()
}

// — internal helpers —————————————————————————————————————————————————————

// connectOnce dials the broker once (with backoff). It does NOT run the
// reconnect barrier — topology hooks are not yet registered at Dial time,
// and WithOnReconnect must not fire on the initial connection.
func (c *Connection) connectOnce(ctx context.Context) error {
	connected, lastErr := c.reconnectRaw(ctx)
	if !connected {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return redact.Error(lastErr)
	}
	return nil
}

// reconnectRaw dials a new raw AMQP connection using the configured backoff
// policy and sets c.raw on success.
func (c *Connection) reconnectRaw(ctx context.Context) (connected bool, lastErr error) {
	loop := reconnect.New(
		ctx,
		func(ctx context.Context) error {
			raw, err := dialAMQP(ctx, &c.opts)
			if err != nil {
				lastErr = err
				return err
			}
			c.mu.Lock()
			c.raw = raw
			c.mu.Unlock()
			connected = true
			return nil
		},
		c.opts.reconnectBackoff.NextBackoff,
		c.opts.reconnectBackoff.Retries,
	)
	<-loop.Done()
	return connected, lastErr
}

// supervisor is the lifecycle goroutine. It listens for blocked/close
// notifications from the live socket and re-enters the connect-with-backoff
// path when the socket drops.
func (c *Connection) supervisor(ctx context.Context) {
	defer close(c.done)

	for {
		c.mu.RLock()
		raw := c.raw
		c.mu.RUnlock()

		if raw == nil {
			return
		}

		// listen for blocked / close notifications from the live socket
		blockedCh := raw.NotifyBlocked(make(chan amqp091.Blocking, 2))
		closeCh := raw.NotifyClose(make(chan *amqp091.Error, 1))

		blockedStart := time.Time{}
		for {
			select {
			case <-ctx.Done():
				_ = raw.Close()
				return

			case b, ok := <-blockedCh:
				if !ok {
					goto reconnect
				}
				if b.Active {
					blockedStart = time.Now()
					c.barrierMu.Lock()
					c.blocked = true
					c.barrierMu.Unlock()
					if c.opts.onBlocked != nil {
						c.opts.onBlocked(b.Reason)
					}
				} else if !blockedStart.IsZero() {
					elapsed := time.Since(blockedStart)
					blockedStart = time.Time{}
					c.barrierMu.Lock()
					c.blocked = false
					c.barrierCond.Broadcast()
					c.barrierMu.Unlock()
					c.opts.metrics.RecordBlocked(connRole, elapsed)
				}

			case _, ok := <-closeCh:
				if !ok || ctx.Err() != nil {
					return
				}
				goto reconnect
			}
		}

	reconnect:
		if ctx.Err() != nil {
			return
		}

		// Clear blocked state and enter the reconnect barrier atomically.
		// The new TCP connection starts unblocked; any blocked notification
		// from the previous socket is no longer relevant.
		c.barrierMu.Lock()
		c.blocked = false
		c.reconnecting = true
		c.barrierMu.Unlock()

		c.opts.metrics.RecordReconnect(connRole)
		c.opts.logger.Infof("amqp: connection lost; reconnecting…")

		connected, lastErr := c.reconnectRaw(ctx)

		if !connected {
			c.opts.logger.Errorf("amqp: reconnect failed: %v", redact.Error(lastErr))
			c.barrierMu.Lock()
			c.reconnecting = false
			c.barrierCond.Broadcast()
			c.barrierMu.Unlock()
			return
		}

		c.runBarrier(ctx)
	}
}

// runBarrier executes the synchronous reconnect barrier:
//  1. Topology hooks (registered by Topology.AttachTo, T16)
//  2. Consumer re-subscribe hooks (registered by Consumer, T18)
//  3. User WithOnReconnect callback
//
// If any hook returns an error the connection enters the degraded state.
// On completion (success or degraded), the barrier cond is broadcast so that
// waiting Publish calls can unblock.
func (c *Connection) runBarrier(ctx context.Context) {
	start := time.Now()

	c.hooksMu.Lock()
	hooks := make([]func(context.Context) error, len(c.hooks))
	copy(hooks, c.hooks)
	c.hooksMu.Unlock()

	var hookErr error
	for _, fn := range hooks {
		if err := fn(ctx); err != nil {
			hookErr = err
			break
		}
	}

	elapsed := time.Since(start)
	c.opts.metrics.RecordTopologyRedeclare(connRole, elapsed)

	if hookErr != nil {
		reason := "topology_failed"
		c.opts.metrics.RecordDegraded(connRole, reason)
		c.opts.logger.Errorf("amqp: topology redeclare failed; entering degraded state: %v",
			redact.Error(hookErr))
		c.mu.Lock()
		wasAlreadyDegraded := c.degraded
		c.degraded = true
		c.degradedErr = fmt.Errorf("%w: %w", ErrTopologyRedeclareFailed, redact.Error(hookErr))
		if !wasAlreadyDegraded && c.opts.onTopoDegraded != nil {
			degradedErr := c.degradedErr
			c.wg.Add(1)
			go func() {
				defer c.wg.Done()
				c.opts.onTopoDegraded(degradedErr)
			}()
		}
		c.mu.Unlock()
	} else {
		c.mu.Lock()
		wasDegraded := c.degraded
		c.degraded = false
		c.degradedErr = nil
		c.mu.Unlock()
		if wasDegraded {
			c.opts.logger.Infof("amqp: recovered from degraded state")
		}

		if c.opts.onReconnect != nil {
			c.opts.onReconnect()
		}
	}

	// broadcast: Publish waiters can now re-check state
	c.barrierMu.Lock()
	c.reconnecting = false
	c.barrierCond.Broadcast()
	c.barrierMu.Unlock()
}

// waitBarrier blocks the caller while a reconnect barrier is active or the
// broker has blocked the connection (connection.blocked).
//
// On ctx cancellation:
//   - If reconnecting: returns ErrReconnecting wrapping ctx.Err().
//   - If broker-blocked: returns ErrConnectionBlocked wrapping ctx.Err().
//
// Returns ErrTopologyRedeclareFailed when the connection is in the degraded
// state (topology redeclare failed on last reconnect).
func (c *Connection) waitBarrier(ctx context.Context) error {
	c.barrierMu.Lock()
	defer c.barrierMu.Unlock()
	for c.reconnecting || c.blocked {
		// Spawn a helper goroutine so ctx cancellation unblocks the cond.
		done := make(chan struct{})
		go func() {
			select {
			case <-ctx.Done():
				c.barrierCond.Broadcast()
			case <-done:
			}
		}()
		c.barrierCond.Wait()
		close(done)
		if ctx.Err() != nil {
			if c.reconnecting {
				return fmt.Errorf("%w: %w", ErrReconnecting, ctx.Err())
			}
			return fmt.Errorf("%w: %w", ErrConnectionBlocked, ctx.Err())
		}
	}
	// Release barrierMu before acquiring mu to avoid AB/BA deadlock:
	// runBarrier holds mu.Lock then acquires barrierMu; we must never hold
	// barrierMu while waiting for mu.
	c.barrierMu.Unlock()
	c.mu.RLock()
	err := c.degradedErr
	c.mu.RUnlock()
	c.barrierMu.Lock() // reacquire for deferred Unlock
	if err != nil {
		return err
	}
	return nil
}

// — option defaults and validation ————————————————————————————————————————

func applyConnDefaults(opts *connOptions) {
	if opts.connectionName == "" {
		opts.connectionName = DefaultConnectionName()
	}
	if opts.addr == "" && len(opts.addrs) == 0 {
		opts.addr = "amqp://guest:guest@localhost/"
	}
	if opts.logger == nil {
		opts.logger = log.NewNoOp()
	}
	if opts.metrics == nil {
		opts.metrics = metrics.NoOpClientMetrics{}
	}
	if opts.tracer == nil {
		opts.tracer = otel.NoOpTracer{}
	}
}

// frameMaxHardCeiling is the upper bound for WithFrameMax. Values above this
// risk OOM on broker and client; use chunked publishing for large payloads.
const frameMaxHardCeiling = 104_857_600 // 100 MiB

func validateConnOptions(opts *connOptions) error {
	// FrameMax: zero means server-driven (ok). [1,4095] violates the spec minimum.
	if opts.frameMax > 0 && opts.frameMax < 4096 {
		return fmt.Errorf("%w: WithFrameMax must be 0 or ≥ 4096 (AMQP spec minimum); got %d",
			ErrInvalidOptions, opts.frameMax)
	}
	if opts.frameMax > frameMaxHardCeiling {
		return fmt.Errorf("%w: WithFrameMax must be ≤ %d (100 MiB); got %d",
			ErrInvalidOptions, frameMaxHardCeiling, opts.frameMax)
	}

	// Connection pool sizes must be positive when explicitly set.
	if opts.pubConns <= 0 {
		return fmt.Errorf("%w: WithPublisherConnections must be ≥ 1; got %d",
			ErrInvalidOptions, opts.pubConns)
	}
	if opts.conConns <= 0 {
		return fmt.Errorf("%w: WithConsumerConnections must be ≥ 1; got %d",
			ErrInvalidOptions, opts.conConns)
	}
	if opts.channelPoolSize <= 0 {
		return fmt.Errorf("%w: WithChannelPoolSize must be ≥ 1; got %d",
			ErrInvalidOptions, opts.channelPoolSize)
	}

	// SASL EXTERNAL: fail-closed
	if opts.saslMechanism == SASLExternal {
		addr := opts.addr
		if len(opts.addrs) > 0 {
			addr = opts.addrs[0]
		}
		if opts.tlsConfig == nil {
			return fmt.Errorf("%w: SASLExternal requires WithTLSConfig", ErrInvalidOptions)
		}
		hasCert := len(opts.tlsConfig.Certificates) > 0 || opts.tlsConfig.GetClientCertificate != nil
		if !hasCert {
			return fmt.Errorf("%w: SASLExternal requires a client certificate in TLSConfig "+
				"(Certificates or GetClientCertificate)", ErrInvalidOptions)
		}
		if !strings.HasPrefix(addr, "amqps://") {
			return fmt.Errorf("%w: SASLExternal requires amqps:// scheme; got %q",
				ErrInvalidOptions, redact.URI(addr))
		}
		// WithAuth under EXTERNAL is valid but ignored — warn at log level only
		if opts.username != "" {
			opts.logger.Warningf("amqp: WithAuth is ignored when SASLExternal is active")
		}
	}

	// Heartbeat: negative disables heartbeats entirely — log warning but allow
	if opts.heartbeat < 0 {
		opts.logger.Warningf("amqp: heartbeats disabled (WithHeartbeat(%v)) — strongly discouraged; "+
			"half-open TCP connections may go undetected", opts.heartbeat)
	}

	// Single-socket availability warning
	if opts.pubConns == 1 {
		opts.logger.Warningf("amqp: WithPublisherConnections(1): a single publisher socket is a " +
			"full-availability gap during reconnect")
	}
	if opts.conConns == 1 {
		opts.logger.Warningf("amqp: WithConsumerConnections(1): a single consumer socket is a " +
			"full-availability gap during reconnect")
	}

	return nil
}

// computeAuthUser derives the authenticated identity from connection options
// without making a network call.
//
// For SASLPlain: returns opts.username (the broker accepts/rejects this; we
// store it optimistically since the connection is only returned on success).
// For SASLExternal: returns the Common Name of the first client certificate
// (this is the identity RabbitMQ typically resolves; the broker confirms it
// by accepting the TLS handshake).
func computeAuthUser(opts *connOptions) string {
	if opts.saslMechanism == SASLExternal {
		if opts.tlsConfig != nil && len(opts.tlsConfig.Certificates) > 0 {
			cert := opts.tlsConfig.Certificates[0]
			if len(cert.Certificate) > 0 {
				if x509cert, err := x509.ParseCertificate(cert.Certificate[0]); err == nil {
					return x509cert.Subject.CommonName
				}
			}
		}
		return ""
	}
	return opts.username
}

// dialAMQP creates a single amqp091-go connection from the resolved options.
//
// The context is accepted but not forwarded to amqp091.DialConfig, which has
// no context-aware dial API. Context cancellation is honoured only between
// retry attempts inside reconnectRaw, not during the TCP handshake itself.
// Provide a context-aware WithDialer to make individual dial attempts
// interruptible.
func dialAMQP(_ context.Context, opts *connOptions) (*amqp091.Connection, error) {
	cfg := amqp091.Config{
		Vhost:      opts.vhost,
		ChannelMax: opts.channelMax,
		FrameSize:  int(opts.frameMax),
		Heartbeat:  opts.heartbeat,
		Dial:       opts.dialer,
	}
	if opts.heartbeat < 0 {
		cfg.Heartbeat = 0 // amqp091 treats 0 as "disabled"
	}

	// SASL
	switch opts.saslMechanism {
	case SASLExternal:
		cfg.SASL = []amqp091.Authentication{&externalAuth{}}
		cfg.TLSClientConfig = opts.tlsConfig
	default:
		if opts.username != "" || opts.password != "" {
			cfg.SASL = []amqp091.Authentication{
				&amqp091.PlainAuth{Username: opts.username, Password: opts.password},
			}
		}
		if opts.tlsConfig != nil {
			cfg.TLSClientConfig = opts.tlsConfig
		}
	}

	// client-properties
	props := amqp091.Table{
		"connection_name": opts.connectionName,
		"product":         "amqp.go",
	}
	for k, v := range opts.clientProperties {
		props[k] = v
	}
	cfg.Properties = props

	addr := opts.addr
	if len(opts.addrs) > 0 {
		addr = opts.addrs[0]
	}

	return amqp091.DialConfig(addr, cfg)
}

// externalAuth implements amqp091.Authentication for SASL EXTERNAL.
type externalAuth struct{}

func (*externalAuth) Mechanism() string { return "EXTERNAL" }
func (*externalAuth) Response() string  { return "\x00" }

// — public helpers ————————————————————————————————————————————————————————

// DefaultConnectionName returns the default connection name in the format
// "<binary>-<hostname>-<pid>". It is called by Dial when WithConnectionName is
// not provided. Exported so tests can verify the format without dialling.
func DefaultConnectionName() string {
	exe := filepath.Base(os.Args[0])
	hostname, _ := os.Hostname()
	return fmt.Sprintf("%s-%s-%d", exe, hostname, os.Getpid())
}
