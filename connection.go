package warren

import (
	"context"
	"crypto/x509"
	"fmt"
	"maps"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"

	"github.com/brunomvsouza/warren/internal/connpool"
	"github.com/brunomvsouza/warren/internal/reconnect"
	"github.com/brunomvsouza/warren/internal/redact"
	"github.com/brunomvsouza/warren/log"
	"github.com/brunomvsouza/warren/metrics"
	"github.com/brunomvsouza/warren/otel"
)

// — per-socket supervision ————————————————————————————————————————————————

// managedConn is a single supervised AMQP TCP connection with reconnect
// barrier and topology-hook runner.  Connection wraps two slices of these:
// pubConns (publisher role) and conConns (consumer role).
type managedConn struct {
	name string // e.g. "myapp-host-42-pub-0"
	role string // "publisher" or "consumer"
	idx  int    // position within its pool

	mu  sync.RWMutex
	raw *amqp091.Connection // current live socket; nil while reconnecting

	// reconnect barrier — callers block while reconnecting or broker-blocked
	barrierMu    sync.Mutex
	barrierCond  *sync.Cond
	reconnecting bool
	blocked      bool

	// degraded state — set when topology redeclare fails persistently
	degraded    bool
	degradedErr error

	// reconnect hooks registered by Topology.AttachTo (T16) and Consumer (T18)
	hooksMu sync.Mutex
	hooks   []func(ctx context.Context) error

	// lifecycle
	cancel           context.CancelFunc
	done             chan struct{}
	forceReconnectCh chan struct{} // signals supervisor to reconnect without returning

	// tracks in-flight onTopoDegraded callback goroutines; drained in close
	wg sync.WaitGroup

	opts *connOptions // shared with parent Connection; read-only after Dial

	// chanFactory, if non-nil, replaces the real amqp091 channel open in openChannel.
	// Used only in unit tests that inject a fake channel to avoid a live broker.
	chanFactory func() (topologyChannel, error)
}

// — Connection ————————————————————————————————————————————————————————————

// Connection manages a pool of supervised AMQP TCP connections split by role:
// publisher connections and consumer connections.
//
// Each TCP socket has its own reconnect supervisor (exponential backoff),
// synchronous reconnect barrier (topology + consumer state fully restored
// before traffic resumes), and degraded-state machine (persistent topology
// failures).
//
// Default pool sizes: 2 publisher connections, 2 consumer connections.
// Configure with WithPublisherConnections / WithConsumerConnections.
type Connection struct {
	opts     connOptions
	authUser string // set before Dial returns; immutable after

	closedMu sync.Mutex
	closed   bool

	pubConns []*managedConn // publisher-role TCP connections
	conConns []*managedConn // consumer-role TCP connections

	// topology snapshot registry — keyed by *Topology pointer identity (see AttachTo)
	topoMu     sync.RWMutex
	topoKeys   []*Topology             // registration order (for deterministic redeclare)
	topoSnaps  map[*Topology]*Topology // original ptr → deep-expanded snapshot
	topoHooked bool                    // true once the redeclare hook has been registered
}

// Dial establishes a supervised pool of AMQP connections and returns the
// Connection.  It opens WithPublisherConnections + WithConsumerConnections TCP
// sockets (default 2+2), validates options, and attempts each connection with
// the configured backoff policy.  Dial returns when all initial connections
// succeed.
//
// Validation errors (ErrInvalidOptions) are returned synchronously.  Network
// errors cause Dial to retry up to the configured Retries limit per socket;
// when exhausted (or ctx cancelled), the last network error is returned.
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
	}

	if opts.connectDelay > 0 {
		t := time.NewTimer(opts.connectDelay)
		select {
		case <-ctx.Done():
			t.Stop()
			return nil, ctx.Err()
		case <-t.C:
		}
	}

	// open publisher connections
	var err error
	c.pubConns, err = openPool(ctx, "publisher", opts.pubConns, &c.opts)
	if err != nil {
		closeManagedConns(c.pubConns)
		return nil, err
	}

	// open consumer connections
	c.conConns, err = openPool(ctx, "consumer", opts.conConns, &c.opts)
	if err != nil {
		closeManagedConns(c.pubConns)
		closeManagedConns(c.conConns)
		return nil, err
	}

	return c, nil
}

// openPool dials count connections of the given role and starts their supervisors.
func openPool(ctx context.Context, role string, count int, opts *connOptions) ([]*managedConn, error) {
	pool := make([]*managedConn, count)
	for i := range count {
		name := connpool.ConnName(opts.connectionName, role, i)
		mc := &managedConn{
			name:             name,
			role:             role,
			idx:              i,
			done:             make(chan struct{}),
			forceReconnectCh: make(chan struct{}, 1),
			opts:             opts,
		}
		mc.barrierCond = sync.NewCond(&mc.barrierMu)

		if err := mc.connectOnce(ctx); err != nil {
			// shut down already-opened conns in this pool
			for j := range i {
				pool[j].cancel()
				<-pool[j].done
			}
			return nil, err
		}

		// start supervisor; inherit trace/log values from Dial ctx but not its
		// cancellation (supervisor must outlive the caller's context).
		// cancel is stored on mc and called by Connection.Close.
		supCtx, cancel := context.WithCancel(context.WithoutCancel(ctx)) //nolint:gosec // G118: cancel stored on struct, called by Close
		mc.cancel = cancel
		go mc.supervisor(supCtx)

		pool[i] = mc
	}
	return pool, nil
}

// closeManagedConns cancels all conns in the slice and waits for them to exit.
func closeManagedConns(conns []*managedConn) {
	for _, mc := range conns {
		if mc.cancel != nil {
			mc.cancel()
		}
	}
	for _, mc := range conns {
		if mc.done != nil {
			<-mc.done
		}
		mc.wg.Wait()
	}
}

// AuthenticatedUser returns the identity authenticated during Dial.
//
// For SASLPlain this is the username from WithAuth.  For SASLExternal it is
// the Common Name of the first client certificate.  The value is set before
// Dial returns and does not change.
func (c *Connection) AuthenticatedUser() string { return c.authUser }

// Health opens a temporary AMQP channel on the first publisher connection and
// immediately closes it.  Returns ErrAlreadyClosed if the Connection is shut
// down, ErrReconnecting if a reconnect barrier is active, ErrNotConnected if
// the socket is not yet established.
func (c *Connection) Health(ctx context.Context) error {
	c.closedMu.Lock()
	closed := c.closed
	c.closedMu.Unlock()
	if closed {
		return ErrAlreadyClosed
	}
	if len(c.pubConns) == 0 {
		return ErrNotConnected
	}
	return c.pubConns[0].health(ctx)
}

// Close drains in-flight work and shuts down all TCP connections in the pool.
// Returns ErrAlreadyClosed if called more than once.
func (c *Connection) Close(ctx context.Context) error {
	c.closedMu.Lock()
	if c.closed {
		c.closedMu.Unlock()
		return ErrAlreadyClosed
	}
	c.closed = true
	c.closedMu.Unlock()

	// cancel all supervisor goroutines
	for _, mc := range c.pubConns {
		mc.cancel()
	}
	for _, mc := range c.conConns {
		mc.cancel()
	}

	// wait for all supervisors to exit
	for _, mc := range append(c.pubConns, c.conConns...) {
		select {
		case <-mc.done:
		case <-ctx.Done():
			return fmt.Errorf("warren: close timed out: %w", ctx.Err())
		}
	}

	// drain in-flight onTopoDegraded callback goroutines.
	// The goroutine below runs to completion regardless of ctx; if ctx expires
	// before all callbacks finish we return a timeout error but the goroutine
	// continues until the callbacks return naturally. onTopoDegraded has no
	// context parameter so it cannot be signalled to stop early — callers that
	// need bounded shutdown should ensure their onTopoDegraded implementation
	// respects an externally managed deadline.
	doneCh := make(chan struct{})
	go func() {
		for _, mc := range append(c.pubConns, c.conConns...) {
			mc.wg.Wait()
		}
		close(doneCh)
	}()
	select {
	case <-doneCh:
	case <-ctx.Done():
		return fmt.Errorf("warren: close timed out waiting for callbacks: %w", ctx.Err())
	}
	return nil
}

// ForceReconnect triggers a manual reconnect cycle on every socket without
// restarting the process.  Intended as an operator escape hatch for recovering
// from a degraded state.  Returns ErrAlreadyClosed if the connection is shut
// down.
func (c *Connection) ForceReconnect() error {
	c.closedMu.Lock()
	closed := c.closed
	c.closedMu.Unlock()
	if closed {
		return ErrAlreadyClosed
	}
	for _, mc := range append(c.pubConns, c.conConns...) {
		mc.forceClose()
	}
	return nil
}

// PubConnAt returns the publisher-role managed connection at the given index.
// Used by Publisher (T12) to acquire channels.
func (c *Connection) PubConnAt(idx int) *managedConn { return c.pubConns[idx] }

// ConConnAt returns the consumer-role managed connection at the given index.
// Used by Consumer (T18) to pin subscriptions.
func (c *Connection) ConConnAt(idx int) *managedConn { return c.conConns[idx] }

// NumPubConns returns the number of publisher TCP connections in the pool.
func (c *Connection) NumPubConns() int { return len(c.pubConns) }

// NumConConns returns the number of consumer TCP connections in the pool.
func (c *Connection) NumConConns() int { return len(c.conConns) }

// registerReconnectHook adds fn to every managed connection's hook list.
// Called by Topology.AttachTo (T16) and Consumer re-subscribe (T18).
// This is an internal API; it is not part of the public surface.
func (c *Connection) registerReconnectHook(fn func(ctx context.Context) error) {
	for _, mc := range append(c.pubConns, c.conConns...) {
		mc.registerHook(fn)
	}
}

// — managedConn methods ——————————————————————————————————————————————————

func (mc *managedConn) registerHook(fn func(ctx context.Context) error) {
	mc.hooksMu.Lock()
	mc.hooks = append(mc.hooks, fn)
	mc.hooksMu.Unlock()
}

// openChannel opens a temporary AMQP channel on this managed connection.
// The caller is responsible for closing the returned channel.
func (mc *managedConn) openChannel() (topologyChannel, error) {
	if mc.chanFactory != nil {
		return mc.chanFactory()
	}
	mc.mu.RLock()
	raw := mc.raw
	mc.mu.RUnlock()
	if raw == nil {
		return nil, ErrNotConnected
	}
	return raw.Channel()
}

// openDeclareChannel opens a temporary AMQP channel on the first publisher
// connection for use by Topology.Declare. The caller is responsible for
// closing the returned channel.
func (c *Connection) openDeclareChannel(_ context.Context) (topologyChannel, error) {
	if len(c.pubConns) == 0 {
		return nil, ErrNotConnected
	}
	return c.pubConns[0].openChannel()
}

// health opens a temporary AMQP channel and closes it to verify liveness.
func (mc *managedConn) health(_ context.Context) error {
	mc.barrierMu.Lock()
	reconnecting := mc.reconnecting
	mc.barrierMu.Unlock()
	if reconnecting {
		return ErrReconnecting
	}
	mc.mu.RLock()
	raw := mc.raw
	mc.mu.RUnlock()
	if raw == nil {
		return ErrNotConnected
	}
	ch, err := raw.Channel()
	if err != nil {
		return err
	}
	return ch.Close()
}

// forceClose signals the supervisor to drop the current connection and
// reconnect.  Using a dedicated channel avoids calling raw.Close() directly —
// a graceful AMQP close sends ok=false on NotifyClose, which the supervisor
// interprets as a normal shutdown rather than a reconnect trigger.
func (mc *managedConn) forceClose() {
	select {
	case mc.forceReconnectCh <- struct{}{}:
	default: // already a pending force-reconnect; skip
	}
}

// connectOnce dials the broker once (with backoff).  It does NOT run the
// reconnect barrier — topology hooks are not yet registered at Dial time,
// and WithOnReconnect must not fire on the initial connection.
func (mc *managedConn) connectOnce(ctx context.Context) error {
	connected, lastErr := mc.reconnectRaw(ctx)
	if !connected {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return redact.Error(lastErr)
	}
	return nil
}

// reconnectRaw dials a new raw AMQP connection using the configured backoff
// policy and sets mc.raw on success.
func (mc *managedConn) reconnectRaw(ctx context.Context) (connected bool, lastErr error) {
	loop := reconnect.New(
		ctx,
		func(ctx context.Context) error {
			raw, err := dialAMQP(ctx, mc.opts, mc.name)
			if err != nil {
				lastErr = err
				return err
			}
			mc.mu.Lock()
			mc.raw = raw
			mc.mu.Unlock()
			connected = true
			return nil
		},
		mc.opts.reconnectBackoff.NextBackoff,
		mc.opts.reconnectBackoff.Retries,
	)
	<-loop.Done()
	return connected, lastErr
}

// supervisor is the per-socket lifecycle goroutine.  It listens for
// blocked/close notifications from the live socket and re-enters the
// connect-with-backoff path when the socket drops.
func (mc *managedConn) supervisor(ctx context.Context) {
	defer close(mc.done)

	for {
		mc.mu.RLock()
		raw := mc.raw
		mc.mu.RUnlock()

		if raw == nil {
			return
		}

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
					mc.barrierMu.Lock()
					mc.blocked = true
					mc.barrierMu.Unlock()
					if mc.opts.onBlocked != nil {
						mc.opts.onBlocked(b.Reason)
					}
				} else if !blockedStart.IsZero() {
					elapsed := time.Since(blockedStart)
					blockedStart = time.Time{}
					mc.barrierMu.Lock()
					mc.blocked = false
					mc.barrierCond.Broadcast()
					mc.barrierMu.Unlock()
					mc.opts.metrics.RecordBlocked(mc.role, elapsed)
				}

			case _, ok := <-closeCh:
				if !ok || ctx.Err() != nil {
					return
				}
				goto reconnect

			case <-mc.forceReconnectCh:
				// Operator-initiated reconnect: close the raw connection (which
				// may produce a graceful ok=false on closeCh) and reconnect.
				_ = raw.Close()
				goto reconnect
			}
		}

	reconnect:
		if ctx.Err() != nil {
			return
		}

		// Clear blocked state and enter the reconnect barrier atomically.
		mc.barrierMu.Lock()
		mc.blocked = false
		mc.reconnecting = true
		mc.barrierMu.Unlock()

		mc.opts.metrics.RecordReconnect(mc.role)
		mc.opts.logger.Infof("warren: %s connection[%d] lost; reconnecting…", mc.role, mc.idx)

		connected, lastErr := mc.reconnectRaw(ctx)

		if !connected {
			mc.opts.logger.Errorf("warren: %s connection[%d] reconnect failed: %v",
				mc.role, mc.idx, redact.Error(lastErr))
			mc.barrierMu.Lock()
			mc.reconnecting = false
			mc.barrierCond.Broadcast()
			mc.barrierMu.Unlock()
			return
		}

		mc.runBarrier(ctx)
	}
}

// runBarrier executes the synchronous reconnect barrier:
//  1. Topology hooks (registered by Topology.AttachTo, T16)
//  2. Consumer re-subscribe hooks (registered by Consumer, T18)
//  3. User WithOnReconnect callback
//
// If any hook returns an error the connection enters the degraded state.
// On completion (success or degraded), the barrier cond is broadcast.
func (mc *managedConn) runBarrier(ctx context.Context) {
	start := time.Now()

	mc.hooksMu.Lock()
	hooks := make([]func(context.Context) error, len(mc.hooks))
	copy(hooks, mc.hooks)
	mc.hooksMu.Unlock()

	var hookErr error
	for _, fn := range hooks {
		if err := fn(ctx); err != nil {
			hookErr = err
			break
		}
	}

	elapsed := time.Since(start)
	mc.opts.metrics.RecordTopologyRedeclare(mc.role, elapsed)

	if hookErr != nil {
		reason := "topology_failed"
		mc.opts.metrics.RecordDegraded(mc.role, reason)
		mc.opts.logger.Errorf("warren: %s connection[%d] topology redeclare failed; entering degraded state: %v",
			mc.role, mc.idx, redact.Error(hookErr))
		mc.mu.Lock()
		wasAlreadyDegraded := mc.degraded
		mc.degraded = true
		mc.degradedErr = fmt.Errorf("%w: %w", ErrTopologyRedeclareFailed, redact.Error(hookErr))
		if !wasAlreadyDegraded && mc.opts.onTopoDegraded != nil {
			degradedErr := mc.degradedErr
			mc.wg.Add(1)
			go func() {
				defer mc.wg.Done()
				mc.opts.onTopoDegraded(degradedErr)
			}()
		}
		mc.mu.Unlock()
	} else {
		mc.mu.Lock()
		wasDegraded := mc.degraded
		mc.degraded = false
		mc.degradedErr = nil
		mc.mu.Unlock()
		if wasDegraded {
			mc.opts.logger.Infof("warren: %s connection[%d] recovered from degraded state",
				mc.role, mc.idx)
		}

		if mc.opts.onReconnect != nil {
			mc.opts.onReconnect()
		}
	}

	// broadcast: waiters can now re-check state
	mc.barrierMu.Lock()
	mc.reconnecting = false
	mc.barrierCond.Broadcast()
	mc.barrierMu.Unlock()
}

// waitBarrier blocks the caller while a reconnect barrier is active or the
// broker has blocked the connection.
//
// On ctx cancellation:
//   - If reconnecting: returns ErrReconnecting wrapping ctx.Err().
//   - If broker-blocked: returns ErrConnectionBlocked wrapping ctx.Err().
//
// Returns ErrTopologyRedeclareFailed when the connection is in the degraded
// state (topology redeclare failed on last reconnect).
func (mc *managedConn) waitBarrier(ctx context.Context) error {
	mc.barrierMu.Lock()
	defer mc.barrierMu.Unlock()
	for mc.reconnecting || mc.blocked {
		done := make(chan struct{})
		go func() {
			select {
			case <-ctx.Done():
				mc.barrierCond.Broadcast()
			case <-done:
			}
		}()
		mc.barrierCond.Wait()
		close(done)
		if ctx.Err() != nil {
			if mc.reconnecting {
				return fmt.Errorf("%w: %w", ErrReconnecting, ctx.Err())
			}
			return fmt.Errorf("%w: %w", ErrConnectionBlocked, ctx.Err())
		}
	}
	// Release barrierMu before acquiring mu to avoid AB/BA deadlock:
	// runBarrier holds mu.Lock then acquires barrierMu; we must never hold
	// barrierMu while waiting for mu.
	mc.barrierMu.Unlock()
	mc.mu.RLock()
	err := mc.degradedErr
	mc.mu.RUnlock()
	mc.barrierMu.Lock() // reacquire for deferred Unlock
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
	// Populate username/password from URL userinfo when WithAuth was not called,
	// so AuthenticatedUser() reflects credentials embedded in WithAddr and
	// dialAMQP does not send PlainAuth with an empty password.
	if opts.username == "" && opts.saslMechanism == SASLPlain {
		addr := opts.addr
		if len(opts.addrs) > 0 {
			addr = opts.addrs[0]
		}
		if u, err := url.Parse(addr); err == nil && u.User != nil {
			opts.username = u.User.Username()
			opts.password, _ = u.User.Password()
		}
	}
}

// frameMaxHardCeiling is the upper bound for WithFrameMax. Values above this
// risk OOM on broker and client; use chunked publishing for large payloads.
const frameMaxHardCeiling = 104_857_600 // 100 MiB

// channelPoolSizeMax is the upper bound for WithChannelPoolSize. Values above
// this cause a large channel-confirm buffer allocation in openPublisherEntry
// (one buffered channel of that size per pool entry) and risk OOM under
// misconfiguration. 4096 channels per TCP connection is already well above any
// realistic workload; values beyond this indicate a configuration error.
const channelPoolSizeMax = 4096

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
	if opts.channelPoolSize > channelPoolSizeMax {
		return fmt.Errorf("%w: WithChannelPoolSize must be ≤ %d; got %d",
			ErrInvalidOptions, channelPoolSizeMax, opts.channelPoolSize)
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
			opts.logger.Warningf("warren: WithAuth is ignored when SASLExternal is active")
		}
	}

	// Heartbeat: negative disables heartbeats entirely — log warning but allow
	if opts.heartbeat < 0 {
		opts.logger.Warningf("warren: heartbeats disabled (WithHeartbeat(%v)) — strongly discouraged; "+
			"half-open TCP connections may go undetected", opts.heartbeat)
	}

	// Single-socket availability warning
	if opts.pubConns == 1 {
		opts.logger.Warningf("warren: WithPublisherConnections(1): a single publisher socket is a " +
			"full-availability gap during reconnect")
	}
	if opts.conConns == 1 {
		opts.logger.Warningf("warren: WithConsumerConnections(1): a single consumer socket is a " +
			"full-availability gap during reconnect")
	}

	return nil
}

// computeAuthUser derives the authenticated identity from connection options
// without making a network call.
//
// For SASLPlain: returns opts.username. For SASLExternal: returns the Common
// Name of the first client certificate.
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
// The name parameter sets the AMQP connection_name client-property, allowing
// each socket in the pool to carry a distinct name visible in the broker UI.
func dialAMQP(_ context.Context, opts *connOptions, name string) (*amqp091.Connection, error) {
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
	// Clone tlsConfig so that concurrent dial calls from different supervisors
	// do not race on the ServerName field that amqp091 writes during handshake.
	switch opts.saslMechanism {
	case SASLExternal:
		cfg.SASL = []amqp091.Authentication{&externalAuth{}}
		if opts.tlsConfig != nil {
			cfg.TLSClientConfig = opts.tlsConfig.Clone()
		}
	default:
		if opts.username != "" || opts.password != "" {
			cfg.SASL = []amqp091.Authentication{
				&amqp091.PlainAuth{Username: opts.username, Password: opts.password},
			}
		}
		if opts.tlsConfig != nil {
			cfg.TLSClientConfig = opts.tlsConfig.Clone()
		}
	}

	// client-properties: use the per-socket name, not opts.connectionName
	props := amqp091.Table{
		"connection_name": name,
		"product":         "Warren",
	}
	maps.Copy(props, opts.clientProperties)
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
// "<binary>-<hostname>-<pid>".  It is called by Dial when WithConnectionName
// is not provided.  Exported so tests can verify the format without dialling.
func DefaultConnectionName() string {
	exe := filepath.Base(os.Args[0])
	hostname, _ := os.Hostname()
	return fmt.Sprintf("%s-%s-%d", exe, hostname, os.Getpid())
}
