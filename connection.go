package warren

import (
	"context"
	"crypto/x509"
	"fmt"
	"maps"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
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

	// dialCursor is the round-robin index into the WithAddrs failover list.
	// It advances once per dial attempt (see nextDialAddr), so the initial
	// connect walks the list in order and each subsequent reconnect resumes at
	// the next address. Touched only from the per-socket reconnect path, which
	// runs serially for a given socket, so it needs no synchronisation.
	dialCursor int

	// lifecycle
	cancel           context.CancelFunc
	done             chan struct{}
	forceReconnectCh chan struct{} // signals supervisor to reconnect without returning

	// tracks in-flight onTopoDegraded and onBlocked callback goroutines; drained in close
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

	// Validate the channel pool against the actual broker-negotiated channel-max,
	// regardless of whether WithChannelMax was set (the server may negotiate a
	// lower ceiling than requested; with WithChannelMax(0) RabbitMQ drives it,
	// defaulting to 2047). A pool larger than the ceiling can never be fully
	// populated, so fail-closed here rather than surfacing channel-open errors lazily.
	if negMax := c.pubConns[0].negotiatedChannelMax(); negMax > 0 && opts.channelPoolSize > int(negMax) {
		closeManagedConns(c.pubConns)
		return nil, fmt.Errorf("%w: WithChannelPoolSize (%d) exceeds the broker-negotiated channel-max (%d)",
			ErrInvalidOptions, opts.channelPoolSize, negMax)
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

// Health verifies broker responsiveness by opening and immediately closing a
// temporary AMQP channel on the first publisher connection (SPEC §6.1).
//
// It returns, in precedence order: ErrAlreadyClosed if the Connection is shut
// down; ErrNotConnected if the connection pool is empty; ctx.Err() if ctx is
// already done; ErrReconnecting if a reconnect barrier is active;
// ErrTopologyRedeclareFailed if the connection is in a degraded topology state;
// ErrNotConnected if the socket is not yet established; otherwise any error from
// the channel open/close round-trip.
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

	// drain in-flight onTopoDegraded / onBlocked callback goroutines.
	// The goroutine below runs to completion regardless of ctx; if ctx expires
	// before all callbacks finish we return a timeout error but the goroutine
	// continues until the callbacks return naturally. Neither onTopoDegraded nor
	// onBlocked has a context parameter so it cannot be signalled to stop early —
	// callers that need bounded shutdown should ensure their callback
	// implementation respects an externally managed deadline.
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

// negotiatedChannelMax returns the channel-max the broker negotiated during the
// AMQP handshake (amqp091 resolves 0 to its 2047 default), or 0 if the socket is
// not yet established.
func (mc *managedConn) negotiatedChannelMax() uint16 {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	if mc.raw == nil {
		return 0
	}
	return mc.raw.Config.ChannelMax
}

// health probes broker responsiveness on this socket: it honors a done ctx and
// the reconnect/degraded state, then opens and immediately closes a temporary
// AMQP channel. See Connection.Health for the full error precedence.
func (mc *managedConn) health(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	mc.barrierMu.Lock()
	reconnecting := mc.reconnecting
	mc.barrierMu.Unlock()
	if reconnecting {
		return ErrReconnecting
	}
	mc.mu.RLock()
	raw := mc.raw
	degradedErr := mc.degradedErr
	mc.mu.RUnlock()
	// SPEC §6.1: Health reports a degraded topology state rather than masking it
	// behind a successful channel open.
	if degradedErr != nil {
		return degradedErr
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

// dialAddrs returns the ordered list of candidate broker addresses. WithAddrs
// takes precedence over WithAddr; a lone WithAddr collapses to a one-element
// list so the dial path is uniform.
func (opts *connOptions) dialAddrs() []string {
	if len(opts.addrs) > 0 {
		return opts.addrs
	}
	return []string{opts.addr}
}

// nextDialAddr returns the broker address for the next dial attempt and advances
// the round-robin cursor (T33). With a single address it always returns that
// address. With WithAddrs it walks the list in order — so the initial connect
// tries addrs[0], addrs[1], … — and each later reconnect resumes at the next
// entry, wrapping at the end. The previously-connected address therefore sticks
// until a disconnect forces a fresh attempt, which then rotates to the next node.
func (mc *managedConn) nextDialAddr() string {
	addrs := mc.opts.dialAddrs()
	addr := addrs[mc.dialCursor]
	// Wrap within [0, len) on advance rather than incrementing without bound, so
	// the cursor can never overflow int into a negative index (opts.addrs is
	// read-only after Dial, so len is stable).
	mc.dialCursor = (mc.dialCursor + 1) % len(addrs)
	return addr
}

// reconnectRaw dials a new raw AMQP connection using the configured backoff
// policy and sets mc.raw on success.
func (mc *managedConn) reconnectRaw(ctx context.Context) (connected bool, lastErr error) {
	loop := reconnect.New(
		ctx,
		func(ctx context.Context) error {
			raw, err := dialAMQP(ctx, mc.opts, mc.name, mc.nextDialAddr())
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
					mc.safeOnBlocked(b.Reason)
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
				// recover runs before wg.Done (LIFO): a panic degrades to a
				// logged error instead of crashing the process, and Close's
				// wg.Wait still returns.
				defer mc.recoverCallback("WithOnTopologyDegraded")
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
			// Inline (the callback runs inside the barrier, before delivery
			// resumes — SPEC §6.1), but recover-guarded so a panic cannot skip
			// the barrierCond.Broadcast() below and deadlock every Publisher.
			func() {
				defer mc.recoverCallback("WithOnReconnect")
				mc.opts.onReconnect()
			}()
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

// recoverCallback recovers a panic from a user-supplied callback, logging it with
// a stack trace at error level. Call it deferred so a panicking callback degrades
// to a logged error instead of crashing the process or deadlocking an internal
// loop (T34c). The panic value is rendered with %v and a full stack is attached;
// no message content flows through this path, so credential/payload leakage is not
// a concern here.
func (mc *managedConn) recoverCallback(name string) {
	if r := recover(); r != nil {
		mc.opts.logger.Errorf("warren: %s connection[%d] %s callback panicked: %v\n%s",
			mc.role, mc.idx, name, r, debug.Stack())
	}
}

// safeOnBlocked runs the user WithOnBlocked callback in a dedicated, recover-guarded
// goroutine. Running it off the supervisor select loop keeps that loop responsive to
// unblock / close / force-reconnect events even if the callback is slow, and the
// recover ensures a panicking callback degrades to a logged error instead of
// crashing the process. The goroutine is tracked by mc.wg so Close drains it,
// subject to the same "the callback must return" caveat as WithOnTopologyDegraded.
func (mc *managedConn) safeOnBlocked(reason string) {
	fn := mc.opts.onBlocked
	if fn == nil {
		return
	}
	mc.wg.Add(1)
	go func() {
		defer mc.wg.Done()
		// recover runs before wg.Done (LIFO) so the log is emitted before Close's
		// wg.Wait returns.
		defer mc.recoverCallback("WithOnBlocked")
		fn(reason)
	}()
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
	// A channel pool larger than the requested channel-max can never be
	// satisfied. When WithChannelMax is explicitly set (>0) this is caught
	// fail-fast here, before any socket is opened; when left at 0 the server
	// negotiates the ceiling, validated post-handshake in Dial.
	if opts.channelMax > 0 && opts.channelPoolSize > int(opts.channelMax) {
		return fmt.Errorf("%w: WithChannelPoolSize (%d) exceeds WithChannelMax (%d)",
			ErrInvalidOptions, opts.channelPoolSize, opts.channelMax)
	}

	// WithAddrs entries must all share one URI scheme. The connection's TLS, SASL,
	// and credential settings apply to whichever node is dialled, and the
	// round-robin failover path (nextDialAddr) now dials every entry — a list
	// mixing amqp:// and amqps:// would silently dial some nodes in plaintext and
	// others over TLS. Reject the mix fail-fast rather than at an unpredictable
	// later reconnect.
	if err := validateAddrsScheme(opts.addrs); err != nil {
		return err
	}

	// SASL EXTERNAL: fail-closed
	if opts.saslMechanism == SASLExternal {
		if opts.tlsConfig == nil {
			return fmt.Errorf("%w: SASLExternal requires WithTLSConfig", ErrInvalidOptions)
		}
		hasCert := len(opts.tlsConfig.Certificates) > 0 || opts.tlsConfig.GetClientCertificate != nil
		if !hasCert {
			return fmt.Errorf("%w: SASLExternal requires a client certificate in TLSConfig "+
				"(Certificates or GetClientCertificate)", ErrInvalidOptions)
		}
		// Every dial target must be amqps:// — the client certificate is presented
		// during the TLS handshake, so a plaintext node would silently authenticate
		// with no client cert. The scheme guard above already rejects a mixed list, so
		// in practice all entries agree; iterating every dialAddrs() entry here is
		// belt-and-suspenders against a future relaxation of that guard, not a second
		// behaviour. The round-robin failover path dials each of these in turn.
		for _, addr := range opts.dialAddrs() {
			if !strings.HasPrefix(addr, "amqps://") {
				return fmt.Errorf("%w: SASLExternal requires amqps:// scheme; got %q",
					ErrInvalidOptions, redact.URI(addr))
			}
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

// validateAddrsScheme rejects a WithAddrs list whose entries do not all share the
// same URI scheme. Unparseable entries (or entries without a scheme) are left for
// the dialer to surface; only a genuine scheme disagreement among parseable
// entries is an options error. The error names the two schemes — never the URIs —
// so no credentials can leak.
func validateAddrsScheme(addrs []string) error {
	var scheme string
	for _, a := range addrs {
		u, err := url.Parse(a)
		if err != nil || u.Scheme == "" {
			continue
		}
		if scheme == "" {
			scheme = u.Scheme
			continue
		}
		if u.Scheme != scheme {
			return fmt.Errorf("%w: WithAddrs entries must all use the same scheme; found both %q and %q",
				ErrInvalidOptions, scheme, u.Scheme)
		}
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
// The addr parameter is the concrete broker URI to dial, selected by the
// caller's round-robin failover cursor (see nextDialAddr).
func dialAMQP(_ context.Context, opts *connOptions, name, addr string) (*amqp091.Connection, error) {
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
	cfg.Properties = buildClientProperties(opts, name)

	return amqp091.DialConfig(addr, cfg)
}

// buildClientProperties assembles the AMQP client-properties table advertised to
// the broker during connection.open. It seeds the library defaults — the
// per-socket connection_name, product, the warren module version, and the Go
// runtime platform — then overlays any user-supplied WithClientProperties keys
// last, so an explicit caller value wins over a default.
func buildClientProperties(opts *connOptions, name string) amqp091.Table {
	props := amqp091.Table{
		"connection_name": name,
		"product":         "Warren",
		"version":         warrenVersion(),
		"platform":        "Go " + runtime.Version(),
	}
	maps.Copy(props, opts.clientProperties)
	return props
}

// warrenVersion reports the module version of warren as recorded in the build
// info, so the broker UI shows which client release opened the connection. It
// returns the main-module version when warren is built directly (its own
// tests/binaries), the dependency version when imported downstream, and a
// "(devel)"/"unknown" fallback when build info is unavailable.
func warrenVersion() string {
	const modulePath = "github.com/brunomvsouza/warren"
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	if info.Main.Path == modulePath && info.Main.Version != "" {
		return info.Main.Version
	}
	for _, dep := range info.Deps {
		if dep.Path == modulePath && dep.Version != "" {
			return dep.Version
		}
	}
	return "(devel)"
}

// notifyResubscribed records the resubscription metric and fires the optional
// WithOnResubscribe callback after a consumer re-installs its subscription on
// reconnect. Consumer and BatchConsumer funnel through this single seam so the
// metric and the user callback stay paired.
//
// The callback is recover-guarded (T34c parity): it runs inside the reconnect
// barrier on the supervisor goroutine, where an unrecovered panic would crash
// the process. The replacement subscription is already installed by the time
// this fires, so a panicking callback degrades to a logged error and delivery
// still resumes — identical handling to WithOnReconnect.
func notifyResubscribed(mc *managedConn, cm metrics.ConsumerMetrics, queue string) {
	cm.RecordResubscribed(queue)
	if mc.opts.onResubscribe != nil {
		defer mc.recoverCallback("WithOnResubscribe")
		mc.opts.onResubscribe(queue)
	}
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

// peerAddress returns the broker host and port parsed from the configured
// address (the first entry when WithAddrs is used). It is best-effort and
// feeds the network.peer.* OTel span attributes (SPEC §6.9); ok is false when
// no host can be parsed. The userinfo is never included, so there is nothing
// to redact.
func (c *Connection) peerAddress() (host string, port int, ok bool) {
	addr := c.opts.addr
	if len(c.opts.addrs) > 0 {
		addr = c.opts.addrs[0]
	}
	u, err := url.Parse(addr)
	if err != nil || u.Hostname() == "" {
		return "", 0, false
	}
	host = u.Hostname()
	switch {
	case u.Port() != "":
		port, _ = strconv.Atoi(u.Port())
	case u.Scheme == "amqps":
		port = 5671
	default:
		port = 5672
	}
	return host, port, true
}
