package warren

import (
	"context"
	"crypto/x509"
	"fmt"
	"maps"
	"math/rand/v2"
	"net"
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

	// bootDegraded is set when this socket's initial connect failed at Dial but
	// the pool still booted because ≥1 connection in its role succeeded (T67 /
	// SRE-08). Its supervisor enters the reconnect path immediately to bring the
	// socket up under supervision instead of giving up at boot.
	bootDegraded bool

	// reconnect hooks registered by Topology.AttachTo (T16) and Consumer (T18)
	hooksMu sync.Mutex
	hooks   []func(ctx context.Context) error

	// dialCursor is the round-robin index into this socket's per-connection
	// shuffled address order (dialOrder, not the raw WithAddrs list). It advances
	// once per dial attempt (see nextDialAddr), so the initial connect walks the
	// shuffled order and each subsequent reconnect resumes at the next address.
	// Touched only from the per-socket reconnect path, which runs serially for a
	// given socket, so it needs no synchronisation.
	dialCursor int

	// shuffleSeed seeds this socket's private permutation of the WithAddrs list
	// (T66). openPool derives it from the process-level base XOR the (role, idx)
	// so each socket — and, via the random base, each client process — walks a
	// DIFFERENT order. That prevents every socket from stampeding addrs[0] on a
	// full-cluster restart. Set directly in unit tests for determinism.
	shuffleSeed int64

	// addrPerm caches dialOrder()'s shuffled view of dialAddrs(), built lazily on
	// the first dial. Touched only from the per-socket reconnect path (serial,
	// same as dialCursor), so it needs no synchronisation.
	addrPerm []string

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

	// dialFactory, if non-nil, replaces the real dialAMQP in reconnectRaw. Used
	// only in unit tests that need to drive the supervisor's dial path without a
	// live broker (e.g. asserting the barrier-cap force-reconnect re-dials).
	dialFactory func(context.Context) (*amqp091.Connection, error)
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

	// Topology-redeclare de-amplification (T62 / DS-09 / SRE-06). The redeclare
	// hook is registered on every managed connection (preserving the per-consumer
	// ordering guarantee: topology is ensured before each consumer's basic.consume),
	// but the actual broker declares are coalesced to once per recovery wave so a
	// pool-wide reconnect (broker restart) does not fire N×pool queue.declares at a
	// just-recovered, fragile broker.
	topoRedeclareMu   sync.Mutex
	topoRedeclareWait chan struct{} // non-nil while a redeclare is in flight; closed on completion
	topoLastDeclare   time.Time     // last successful redeclare (coalesce-window anchor)
}

// topoRedeclareCoalesceWindow coalesces the redeclare frames from a pool-wide
// reconnect wave (a broker restart closes every socket at once, and each socket's
// barrier fires the topology hook) into a single set of declares: a barrier whose
// redeclare falls within this window of a prior success skips and relies on it
// (topology is broker-global and idempotent). Independent reconnects spaced wider
// than the window each redeclare, so a genuinely new recovery is never skipped.
const topoRedeclareCoalesceWindow = 10 * time.Second

// Dial establishes a supervised pool of AMQP connections and returns the
// Connection.  It opens WithPublisherConnections + WithConsumerConnections TCP
// sockets (default 2+2), validates options, and attempts each connection with
// the configured backoff policy.
//
// Partial-pool-connect policy (T67): Dial succeeds once at least ONE connection
// per role connects; sockets that fail their initial connect are brought up by
// their supervisor under the reconnect backoff, and the reduced-capacity boot is
// surfaced via connection_degraded_total{reason="boot_reduced_capacity"} plus a
// warning log. Dial fails only when an entire role gets zero connections, or
// when ctx is cancelled.
//
// Validation errors (ErrInvalidOptions) are returned synchronously.  Network
// errors cause Dial to retry up to the configured Retries limit per socket;
// when an entire role is exhausted (or ctx cancelled), the last network error
// is returned.
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
	connected := 0
	var lastErr error
	for i := range count {
		name := connpool.ConnName(opts.connectionName, role, i)
		mc := &managedConn{
			name:             name,
			role:             role,
			idx:              i,
			shuffleSeed:      perConnSeed(opts.addrShuffleSeed, role, i),
			done:             make(chan struct{}),
			forceReconnectCh: make(chan struct{}, 1),
			opts:             opts,
		}
		mc.barrierCond = sync.NewCond(&mc.barrierMu)

		if err := mc.connectOnce(ctx); err != nil {
			// Partial-pool-connect policy (T67 / SRE-08): a single socket failing
			// at boot does NOT fail Dial as long as ≥1 connection in this role
			// connects. Mark it boot-degraded so its supervisor brings it up under
			// supervision, and pre-set reconnecting so callers block on the barrier
			// (and observe the T63 cap) rather than racing a nil socket. Supervisors
			// are NOT started here — only after we know the pool boots — so a
			// fail-fast (connected==0) path spawns no background reconnect goroutines.
			mc.bootDegraded = true
			mc.barrierMu.Lock()
			mc.reconnecting = true
			mc.barrierMu.Unlock()
			lastErr = err
		} else {
			connected++
		}

		pool[i] = mc
	}

	// Decide whether the pool boots BEFORE spawning any supervisor.
	// A cancelled Dial ctx must fail even if some sockets connected before
	// cancellation — the caller asked to stop.
	if ctx.Err() != nil {
		closeRawConns(pool)
		return nil, ctx.Err()
	}
	// Fail-fast only when the ENTIRE role has no connectivity: a Connection with
	// zero live sockets for a role cannot serve that role.
	if connected == 0 {
		closeRawConns(pool)
		return nil, fmt.Errorf("warren: %s pool: no connection could be established: %w", role, lastErr)
	}

	// The pool boots. Start a supervisor for every connection — live sockets are
	// watched for close/block, boot-degraded sockets are brought up via the
	// reconnect path. The supervisor inherits the Dial ctx's trace/log values but
	// not its cancellation (it must outlive the caller's context); mc.cancel is
	// stored and called by Connection.Close.
	for _, mc := range pool {
		supCtx, cancel := context.WithCancel(context.WithoutCancel(ctx)) //nolint:gosec // G118: cancel stored on struct, called by Close
		mc.cancel = cancel
		go mc.supervisor(supCtx)
	}

	// Booted with reduced capacity: make the silent capacity loss observed
	// (SRE-08) — the missing sockets reconnect under supervision.
	if connected < count {
		opts.metrics.RecordDegraded(role, "boot_reduced_capacity")
		opts.logger.Warningf(
			"warren: %s pool booted at reduced capacity: %d/%d connections live, %d reconnecting under supervision (last error: %v)",
			role, connected, count, count-connected, redact.Error(lastErr),
		)
	}
	return pool, nil
}

// closeRawConns closes the live raw socket of every connection in the pool that
// managed to connect. Used on the openPool fail-fast paths (cancelled ctx or
// zero connections in a role) where no supervisors have been started yet, so
// there is nothing to cancel — only raw sockets to release.
func closeRawConns(conns []*managedConn) {
	for _, mc := range conns {
		if mc == nil {
			continue
		}
		mc.mu.RLock()
		raw := mc.raw
		mc.mu.RUnlock()
		if raw != nil {
			_ = raw.Close()
		}
	}
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
// the channel open/close round-trip, classified through the AMQP reply-code
// sentinels (errors.Is against ErrAccessRefused, ErrNotFound, etc. — SPEC §6.3).
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

// brokerVersion returns the broker's reported version string (from the
// connection.start server-properties), or "" if no live socket is available or
// the property is absent. Used by Topology.Declare to emit version-aware
// warnings (T58). Best-effort: it reads from the first live connection found.
func (c *Connection) brokerVersion() string {
	for _, mc := range append(append([]*managedConn(nil), c.pubConns...), c.conConns...) {
		if v := mc.serverVersion(); v != "" {
			return v
		}
	}
	return ""
}

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

// isReconnecting reports whether this socket is mid-reconnect (the supervisor set
// the barrier and has not yet finished re-open + redeclare + re-subscribe). It is
// a non-blocking snapshot — unlike waitBarrier, it never blocks on the barrier.
// Consumer channel-level self-heal (T61) consults it to avoid racing the reconnect
// hook with a duplicate basic.consume on the same consumer tag.
func (mc *managedConn) isReconnecting() bool {
	mc.barrierMu.Lock()
	defer mc.barrierMu.Unlock()
	return mc.reconnecting
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

// serverVersion returns the broker version string from the connection.start
// server-properties ("version"), or "" if the socket is not established or the
// property is missing/non-string.
func (mc *managedConn) serverVersion() string {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	if mc.raw == nil {
		return ""
	}
	v, ok := mc.raw.Properties["version"]
	if !ok {
		return ""
	}
	s, _ := v.(string)
	return s
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
// AMQP channel, classifying any broker error through wrapAMQPError. See
// Connection.Health for the full error precedence.
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
	// Route the probe through openChannel (honoring the chanFactory test seam) and
	// classify both the open and close errors through wrapAMQPError, so a broker
	// reply code surfaces as a sentinel callers can errors.Is — consistent with
	// every other broker-touching path. SPEC §6.3.
	ch, err := mc.openChannel()
	if err != nil {
		return fmt.Errorf("warren: health: %w", wrapAMQPError(err))
	}
	if err := ch.Close(); err != nil {
		return fmt.Errorf("warren: health: %w", wrapAMQPError(err))
	}
	return nil
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

// dial opens one raw AMQP connection, honoring the dialFactory test seam when
// set and otherwise dialing the next address in the connection's rotation.
func (mc *managedConn) dial(ctx context.Context) (*amqp091.Connection, error) {
	if mc.dialFactory != nil {
		return mc.dialFactory(ctx)
	}
	return dialAMQP(ctx, mc.opts, mc.name, mc.nextDialAddr())
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

// addrShuffleStream selects the PCG stream that shuffledAddrs draws its
// permutation from. Set to the 64-bit golden-ratio constant for good bit
// diffusion.
const addrShuffleStream uint64 = 0x9E3779B97F4A7C15

// seedMixMultiplier is the multiplier perConnSeed folds the socket index through
// so adjacent indices map to distant seeds. Numerically the same golden-ratio
// constant as addrShuffleStream, but named separately because the use is
// unrelated: here it is an avalanche multiplier, there a PCG stream selector.
const seedMixMultiplier uint64 = 0x9E3779B97F4A7C15

// FNV-1a 64-bit basis/prime, used to fold a socket's role string into its seed.
const (
	fnvOffsetBasis uint64 = 1469598103934665603
	fnvPrime       uint64 = 1099511628211
)

// shuffledAddrs returns a copy of addrs permuted by a deterministic
// Fisher–Yates shuffle seeded by seed (T66). The input slice (the shared,
// read-only opts.addrs) is never mutated. With fewer than two addresses the
// copy is returned unchanged.
func shuffledAddrs(addrs []string, seed int64) []string {
	out := make([]string, len(addrs))
	copy(out, addrs)
	if len(out) < 2 {
		return out
	}
	r := rand.New(rand.NewPCG(uint64(seed), addrShuffleStream)) //nolint:gosec // address spread, not cryptographic
	r.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	return out
}

// perConnSeed derives a per-socket shuffle seed from the process-level base and
// the socket's (role, idx), so publisher-0 and consumer-0 — and socket 0 vs 1 —
// walk different address permutations. The role is folded in via FNV-1a and the
// index via the golden-ratio multiply (good bit spread); all mixing is done in
// uint64 to avoid int64 overflow on the constants.
func perConnSeed(base int64, role string, idx int) int64 {
	h := fnvOffsetBasis
	for i := range len(role) {
		h = (h ^ uint64(role[i])) * fnvPrime
	}
	return base ^ int64(h^(uint64(idx)*seedMixMultiplier)) //nolint:gosec // seed mix, not cryptographic
}

// dialOrder returns this socket's shuffled permutation of dialAddrs(), built
// lazily on first use and cached for the life of the socket so the round-robin
// cursor walks a stable order.
func (mc *managedConn) dialOrder() []string {
	if mc.addrPerm == nil {
		mc.addrPerm = shuffledAddrs(mc.opts.dialAddrs(), mc.shuffleSeed)
	}
	return mc.addrPerm
}

// nextDialAddr returns the broker address for the next dial attempt and advances
// the round-robin cursor. With a single address it always returns that address.
// With WithAddrs it walks this socket's per-connection *shuffled* order (T66) —
// so the initial connect starts at a per-socket-distinct node rather than every
// socket stampeding addrs[0] — and each later reconnect resumes at the next
// entry, wrapping at the end. The previously-connected address therefore sticks
// until a disconnect forces a fresh attempt, which then rotates to the next node.
func (mc *managedConn) nextDialAddr() string {
	addrs := mc.dialOrder()
	addr := addrs[mc.dialCursor]
	// Wrap within [0, len) on advance rather than incrementing without bound, so
	// the cursor can never overflow int into a negative index (the shuffled order
	// is fixed after the first dial, so len is stable).
	mc.dialCursor = (mc.dialCursor + 1) % len(addrs)
	return addr
}

// reconnectRaw dials a new raw AMQP connection using the configured backoff
// policy and sets mc.raw on success.
func (mc *managedConn) reconnectRaw(ctx context.Context) (connected bool, lastErr error) {
	loop := reconnect.New(
		ctx,
		func(ctx context.Context) error {
			raw, err := mc.dial(ctx)
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
			// raw is nil at supervisor entry in three cases: a normal shutdown
			// (Close cancelled ctx — exit); a boot-degraded socket whose initial
			// connect failed at Dial (T67 — bring it up under supervision via the
			// reconnect path below); or a barrier-cap force-reconnect that nil'd
			// raw while reconnecting (T63 — re-dial rather than exit). The
			// notify-watch declarations are scoped to an inner block so this
			// forward goto is legal.
			mc.barrierMu.Lock()
			forcedRedial := mc.reconnecting
			mc.barrierMu.Unlock()
			if (mc.bootDegraded || forcedRedial) && ctx.Err() == nil {
				goto reconnect
			}
			return
		}

		{
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

	// Run the redeclare/resubscribe hooks under the barrier cap (T63 / R10-8 /
	// DS-02). A half-alive broker (accepts the socket, stalls on queue.declare —
	// e.g. Khepri Raft-quorum recovery, RMQ-17) would otherwise hang the barrier
	// indefinitely, and ConfirmTimeout does NOT cover the barrier (the frame is
	// still unwritten). On cap we force-reconnect: close the socket (its rotated
	// re-dial with WithAddrs hits a different node) and let the supervisor re-run
	// the barrier.
	//
	// The hooks run under hookCtx, cancelled on the cap/shutdown paths so a hook
	// that loops over many declares bails between operations instead of issuing
	// the full set against a doomed socket (bounding goroutine accumulation across
	// repeated caps). A hook blocked inside a single non-ctx-aware amqp091 wire
	// call (e.g. one queue.declare) still unblocks only when the socket closes —
	// which the cap path does — and mc.wg tracks it so Close drains it.
	hookCtx, hookCancel := context.WithCancel(ctx)
	defer hookCancel()
	hookErrCh := make(chan error, 1)
	mc.wg.Add(1)
	go func() {
		defer mc.wg.Done()
		var hookErr error
		for _, fn := range hooks {
			if err := fn(hookCtx); err != nil {
				hookErr = err
				break
			}
		}
		hookErrCh <- hookErr
	}()

	capTimer := time.NewTimer(mc.opts.reconnectBarrierTimeout)
	var hookErr error
	select {
	case hookErr = <-hookErrCh:
		capTimer.Stop()
	case <-ctx.Done():
		capTimer.Stop()
		// Shutdown: cancel the hook ctx and let the orphaned hook goroutine exit
		// when Close tears down the socket (drained by mc.wg). Clear the barrier
		// so no publisher hangs.
		hookCancel()
		mc.barrierMu.Lock()
		mc.reconnecting = false
		mc.barrierCond.Broadcast()
		mc.barrierMu.Unlock()
		return
	case <-capTimer.C:
		// Barrier exceeded its cap. Force-reconnect the half-alive socket: cancel
		// the hooks, close the socket, and — critically — nil mc.raw so the
		// supervisor loop takes its nil-raw-while-reconnecting branch and re-dials.
		// (Re-reading the just-closed *amqp091.Connection would register fresh
		// NotifyClose/NotifyBlocked channels that come back pre-closed, and the
		// supervisor's closeCh !ok arm would then exit the goroutine ~half the time,
		// permanently wedging the socket.) reconnecting stays true (a fresh
		// reconnect is imminent) and blocked publishers already return
		// ErrReconnecting via the wait-side cap. The forced re-dial records its own
		// RecordReconnect on the supervisor reconnect path, so we do not count here.
		hookCancel()
		mc.opts.logger.Errorf("warren: %s connection[%d] reconnect barrier exceeded %s cap; forcing reconnect",
			mc.role, mc.idx, mc.opts.reconnectBarrierTimeout)
		mc.mu.Lock()
		raw := mc.raw
		mc.raw = nil
		mc.mu.Unlock()
		if raw != nil {
			_ = raw.Close()
		}
		mc.barrierMu.Lock()
		mc.barrierCond.Broadcast()
		mc.barrierMu.Unlock()
		return
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
			mc.wg.Go(func() {
				// recover runs before wg.Done (LIFO): a panic degrades to a
				// logged error instead of crashing the process, and Close's
				// wg.Wait still returns.
				defer mc.recoverCallback("WithOnTopologyDegraded")
				mc.opts.onTopoDegraded(degradedErr)
			})
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
	// Bound the reconnect-barrier wait (T63 / R10-8 / DS-02): with no ctx
	// deadline (PublishTimeout=0 + context.Background()), a publisher would
	// otherwise stall forever behind a half-alive broker whose redeclare never
	// clears. The cap applies only to the reconnecting state, not to broker
	// flow-control (blocked) backpressure, which is legitimate and unbounded.
	// capDeadline is fixed lazily on the first reconnecting wait (and only then
	// is mc.opts read, so a bare managedConn that is merely degraded never
	// dereferences opts).
	var capDeadline time.Time
	mc.barrierMu.Lock()
	defer mc.barrierMu.Unlock()
	for mc.reconnecting || mc.blocked {
		if !mc.reconnecting {
			// Not (yet) reconnecting — e.g. a legitimate flow-control blocked
			// interval. Drop any prior wave's deadline so a later reconnecting
			// edge anchors its own full cap rather than reusing a stale, already-
			// elapsed deadline (which would return ErrReconnecting spuriously).
			capDeadline = time.Time{}
		}
		done := make(chan struct{})
		var timer *time.Timer
		var timerC <-chan time.Time
		if mc.reconnecting {
			if capDeadline.IsZero() {
				capDeadline = time.Now().Add(mc.opts.reconnectBarrierTimeout)
			}
			timer = time.NewTimer(time.Until(capDeadline))
			timerC = timer.C
		}
		go func() {
			select {
			case <-ctx.Done():
				mc.barrierCond.Broadcast()
			case <-timerC:
				mc.barrierCond.Broadcast()
			case <-done:
			}
		}()
		mc.barrierCond.Wait()
		close(done)
		if timer != nil {
			timer.Stop()
		}
		if ctx.Err() != nil {
			if mc.reconnecting {
				return fmt.Errorf("%w: %w", ErrReconnecting, ctx.Err())
			}
			return fmt.Errorf("%w: %w", ErrConnectionBlocked, ctx.Err())
		}
		// Barrier cap exceeded: surface ErrReconnecting rather than stall past it.
		if mc.reconnecting && !time.Now().Before(capDeadline) {
			return fmt.Errorf("%w: reconnect barrier exceeded %s cap", ErrReconnecting, mc.opts.reconnectBarrierTimeout)
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
	mc.wg.Go(func() {
		// recover runs before wg.Done (LIFO) so the log is emitted before Close's
		// wg.Wait returns.
		defer mc.recoverCallback("WithOnBlocked")
		fn(reason)
	})
}

// — option defaults and validation ————————————————————————————————————————

func applyConnDefaults(opts *connOptions) {
	if opts.connectionName == "" {
		opts.connectionName = DefaultConnectionName()
	}
	if opts.addr == "" && len(opts.addrs) == 0 {
		opts.addr = "amqp://guest:guest@localhost/"
	}
	if opts.addrShuffleSeed == 0 {
		// Per-process random base so each client spreads its sockets across the
		// cluster differently (T66). Force non-zero so the "0 means unset"
		// sentinel stays meaningful and a unit-test-set seed is preserved.
		opts.addrShuffleSeed = int64(rand.Uint64() | 1) //nolint:gosec // address spread, not cryptographic
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
	if opts.reconnectBarrierTimeout <= 0 {
		opts.reconnectBarrierTimeout = defaultReconnectBarrierTimeout
	}
	if opts.dialer == nil {
		opts.dialer = defaultDialer()
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

// Default dialer timing (T72 / R10-17 / SRE-09). AMQP heartbeats cover only
// read-side partition detection (~2×heartbeat); a WRITE to a half-open socket
// can block far longer with ConfirmTimeout (30s) as the only backstop. The
// default dialer enables TCP keepalive so the OS probes a silent peer and fails
// a pending write promptly. For even faster write-side failure on Linux, set
// TCP_USER_TIMEOUT via WithDialer + net.Dialer.Control (see WithDialer godoc).
const (
	defaultDialTimeout   = 30 * time.Second
	defaultDialKeepAlive = 15 * time.Second
)

// defaultNetDialer returns the net.Dialer used when WithDialer is not set. It
// carries an explicit connect timeout and TCP keepalive (T72).
func defaultNetDialer() *net.Dialer {
	return &net.Dialer{
		Timeout:   defaultDialTimeout,
		KeepAlive: defaultDialKeepAlive,
	}
}

// defaultDialer adapts defaultNetDialer to the amqp091 Config.Dial signature.
// amqp091 layers TLS over the returned raw connection for amqps:// URLs, so the
// same keepalive dialer serves plaintext and TLS connections.
func defaultDialer() func(network, addr string) (net.Conn, error) {
	d := defaultNetDialer()
	return func(network, addr string) (net.Conn, error) {
		return d.Dial(network, addr)
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
