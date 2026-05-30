//go:build integration

package warren_test

// Security regression scan (T45b / SPEC §8, §9): drive a credentialed workload
// against a real broker, capture every observable surface — log lines (through
// the real redacting Slog adapter), returned error messages, OpenTelemetry span
// attributes, and Prometheus metric labels — and assert that no clear-text
// credential survives into any of them.
//
// Lens-10 TV-14 — exercise, not just scan: a grep only catches output that was
// actually produced, so the run must FORCE the highest-risk leak path before
// scanning. Per Lens-07 the likeliest leak is a wrapped underlying amqp091-go
// error whose message embeds the dial URL, not a warren-own string. This test
// therefore dials with a distinctive sentinel credential that the broker
// rejects, producing a real wrapped-auth-failure error, and scans that error's
// full chain alongside the steady-state surfaces.
//
// Anti-vacuity (VG-6 analogue): scanForSecrets is self-tested against a planted
// leak so a passing run means "redacted", never "the scanner is broken"; and the
// combined surface is asserted non-empty so the scan is never run over nothing.

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.uber.org/goleak"

	"github.com/brunomvsouza/warren"
	warrenlog "github.com/brunomvsouza/warren/log"
	warrenmetrics "github.com/brunomvsouza/warren/metrics"
	warrenotel "github.com/brunomvsouza/warren/otel"
)

// Distinctive sentinel credentials: deliberately wrong so the broker rejects the
// dial, and distinctive so a leak is unambiguous in the scan (no false match
// against unrelated output). The password must never appear in any surface.
const (
	sentinelUser = "warren-leak-probe"
	sentinelPass = "s3cret-pass-do-not-log" //nolint:gosec // G101: fake credential used to prove redaction
)

// secScanDuration is how long the credentialed workload runs. SPEC §9 nominates a
// 60s run; CI uses a short default to keep the lane fast, overridable via the env
// for the nightly/release-candidate cadence. The leak surfaces are exercised by a
// single publish/consume pass plus the forced error path, so a longer run adds
// sustained volume, not new surface coverage.
func secScanDuration(t *testing.T) time.Duration {
	t.Helper()
	if v := os.Getenv("WARREN_SECSCAN_DURATION"); v != "" {
		d, err := time.ParseDuration(v)
		require.NoErrorf(t, err, "WARREN_SECSCAN_DURATION=%q must be a Go duration", v)
		return d
	}
	return 3 * time.Second
}

type secEvent struct {
	Seq int `json:"seq"`
}

func TestSecurityRedaction_NoCredentialLeak_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	workingURL := amqpTestURL(t)

	// Derive the working userinfo (e.g. "guest:guest") so the scan can flag it
	// verbatim — a leak even when the password itself is generic.
	wu, err := url.Parse(workingURL)
	require.NoError(t, err, "AMQP_TEST_URL must be a valid URL")
	require.NotNil(t, wu.User, "AMQP_TEST_URL must carry credentials for this test to mean anything")
	workingPass, _ := wu.User.Password()
	workingUserinfo := wu.User.Username()
	if workingPass != "" {
		workingUserinfo += ":" + workingPass
	}

	// The exact clear-text strings that must never appear in any surface.
	secrets := []string{
		sentinelPass,
		sentinelUser + ":" + sentinelPass,
		workingUserinfo,
	}

	// — Scanner self-test (VG-6 analogue) ————————————————————————————————————
	// A planted leak must be detected, both by exact-substring match and by the
	// unredacted-userinfo regex. If this finds nothing, the scanner is broken and
	// every later "no leak" assertion would be vacuous.
	planted := "warren: connecting to amqp://" + sentinelUser + ":" + sentinelPass + "@broker:5672/prod"
	require.NotEmpty(t, scanForSecrets(planted, secrets),
		"scanner self-test failed: scanForSecrets must detect a planted credential leak")

	// — Capture surfaces ——————————————————————————————————————————————————————
	// Logs flow through the REAL redacting Slog adapter (redaction is the adapter's
	// job, so capturing through it exercises the production path exactly).
	logSink := &syncBuffer{}
	logger := warrenlog.NewSlog(slog.New(slog.NewTextHandler(logSink, &slog.HandlerOptions{Level: slog.LevelDebug})))

	reg := prometheus.NewRegistry()
	clientMetrics, err := warrenmetrics.NewPrometheusClientMetrics(reg, nil)
	require.NoError(t, err)

	tracer := &capturingTracer{}

	var capturedErrs []string
	recordErr := func(context string, err error) {
		if err != nil {
			capturedErrs = append(capturedErrs, context+": "+err.Error())
		}
	}

	// — Credentialed workload ————————————————————————————————————————————————
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	conn, err := warren.Dial(ctx,
		warren.WithAddr(workingURL),
		warren.WithLogger(logger),
		warren.WithMetrics(clientMetrics),
		warren.WithTracer(tracer),
	)
	require.NoError(t, err, "dial with working credentials")

	const (
		exchange = "warren.sectest.ex"
		queue    = "warren.sectest.q"
	)
	topo := &warren.Topology{
		Exchanges: []warren.Exchange{{Name: exchange, Kind: warren.ExchangeTopic, AutoDelete: true}},
		Queues:    []warren.Queue{{Name: queue, AutoDelete: true}},
		Bindings:  []warren.Binding{{Exchange: exchange, Queue: queue, RoutingKey: "sec.#"}},
	}
	require.NoError(t, topo.Declare(ctx, conn))
	t.Cleanup(func() { cleanSecTopology(workingURL, exchange, queue) })

	pub, err := warren.PublisherFor[secEvent](conn).
		Exchange(exchange).
		RoutingKey("sec.created").
		ConfirmTimeout(10 * time.Second).
		Build()
	require.NoError(t, err)

	consumer, err := warren.ConsumerFor[secEvent](conn).
		Queue(queue).
		Concurrency(2).
		Prefetch(16).
		Build()
	require.NoError(t, err)

	var handled int
	var handledMu sync.Mutex
	consumeCtx, cancelConsume := context.WithCancel(ctx)
	consumeErr := make(chan error, 1)
	go func() {
		consumeErr <- consumer.Consume(consumeCtx, func(_ context.Context, _ secEvent) error {
			handledMu.Lock()
			handled++
			handledMu.Unlock()
			return nil
		})
	}()

	// Publish a paced, sustained stream for the configured duration so the publish
	// and process span/metric surfaces are exercised across many messages. The
	// ticker paces the rate; it is a load pacer, not a synchronization sleep.
	published := 0
	deadline := time.NewTimer(secScanDuration(t))
	ticker := time.NewTicker(20 * time.Millisecond)
publishLoop:
	for {
		select {
		case <-deadline.C:
			break publishLoop
		case <-ticker.C:
			err := pub.Publish(ctx, warren.Message[secEvent]{Body: &secEvent{Seq: published}})
			recordErr("publish", err)
			if err == nil {
				published++
			}
		}
	}
	ticker.Stop()
	deadline.Stop()
	require.Positive(t, published, "the credentialed workload must publish at least one message")

	// Let the consumer drain what was published before we tear it down.
	drain := time.NewTimer(10 * time.Second)
	defer drain.Stop()
drainLoop:
	for {
		handledMu.Lock()
		done := handled >= published
		handledMu.Unlock()
		if done {
			break
		}
		select {
		case <-drain.C:
			t.Logf("drain timeout: handled %d/%d (non-fatal; surfaces already exercised)", handled, published)
			break drainLoop
		case <-time.After(25 * time.Millisecond):
		}
	}

	// — Force the wrapped amqp091 error path (TV-14) ——————————————————————————
	// A real auth failure: same broker host, sentinel credentials the broker
	// rejects. warren must redact the URL in whatever it returns/logs; if it wraps
	// the raw amqp091 error with the un-redacted dial URL, the sentinel password
	// leaks here and the scan below catches it.
	badURL := withUserinfo(wu, sentinelUser, sentinelPass)
	badCtx, badCancel := context.WithTimeout(ctx, 15*time.Second)
	badConn, badErr := warren.Dial(badCtx,
		warren.WithAddr(badURL),
		warren.WithLogger(logger), // any connect-failure logs route through the redactor too
	)
	badCancel()
	require.Error(t, badErr, "dial with sentinel credentials must fail (broker rejects them)")
	recordErr("forced-auth-failure", badErr)
	if badConn != nil {
		closeCtx, c := context.WithTimeout(context.Background(), 5*time.Second)
		_ = badConn.Close(closeCtx)
		c()
	}

	// — Tear down the workload before gathering ————————————————————————————————
	cancelConsume()
	recordErr("consume-return", filterCanceled(<-consumeErr))
	{
		closeCtx, c := context.WithTimeout(context.Background(), 5*time.Second)
		recordErr("publisher-close", pub.Close(closeCtx))
		recordErr("conn-close", conn.Close(closeCtx))
		c()
	}

	// — Assemble the combined surface ——————————————————————————————————————————
	combined := strings.Join([]string{
		logSink.String(),
		strings.Join(capturedErrs, "\n"),
		tracer.dump(),
		gatherMetricText(t, reg),
	}, "\n")

	require.NotEmpty(t, strings.TrimSpace(combined),
		"no observable output captured — the scan would be vacuous")

	// — The assertion ——————————————————————————————————————————————————————————
	leaks := scanForSecrets(combined, secrets)
	assert.Empty(t, leaks,
		"clear-text credential leaked into an observable surface: %v\n--- combined surface ---\n%s",
		leaks, combined)
}

// scanForSecrets and amqpUserinfoRe live in security_redaction_scan_test.go (no
// build tag) so the scanner logic is unit-tested on the fast lane and reused here.

// withUserinfo returns base with its userinfo replaced by user:pass, scheme
// forced to amqp:// (plaintext, so the broker reaches the auth check rather than
// failing a TLS handshake first).
func withUserinfo(base *url.URL, user, pass string) string {
	u := *base
	u.Scheme = "amqp"
	u.User = url.UserPassword(user, pass)
	return u.String()
}

// filterCanceled drops the benign context.Canceled returned by a consumer that
// was stopped on purpose, so it is not recorded as a surface error.
func filterCanceled(err error) error {
	if err == nil || strings.Contains(err.Error(), context.Canceled.Error()) {
		return nil
	}
	return err
}

// gatherMetricText renders every gathered metric's name plus its label name=value
// pairs into one string, so a credential that ever reached a metric label would
// appear in the scan. (No metric label carries a URI today; this guards against a
// future regression.)
func gatherMetricText(t *testing.T, reg *prometheus.Registry) string {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	var b strings.Builder
	for _, mf := range mfs {
		b.WriteString(mf.GetName())
		b.WriteByte('\n')
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				fmt.Fprintf(&b, "  %s=%s\n", lp.GetName(), lp.GetValue())
			}
		}
	}
	return b.String()
}

// cleanSecTopology removes the test's auto-delete topology via a throwaway
// connection; it is defensive against a prior crashed run and a best-effort
// no-op on any error.
func cleanSecTopology(url, exchange, queue string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := warren.Dial(ctx, warren.WithAddr(url))
	if err != nil {
		return
	}
	defer func() {
		c, cc := context.WithTimeout(context.Background(), 5*time.Second)
		_ = conn.Close(c)
		cc()
	}()
	topo := &warren.Topology{
		Exchanges: []warren.Exchange{{Name: exchange, Kind: warren.ExchangeTopic, AutoDelete: true}},
		Queues:    []warren.Queue{{Name: queue, AutoDelete: true}},
	}
	_ = topo.Declare(ctx, conn)
}

// — syncBuffer: concurrency-safe io.Writer for the slog handler ——————————————

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// — capturingTracer: records span names, attribute values, and recorded errors —

type capturingTracer struct {
	mu    sync.Mutex
	lines []string
}

func (c *capturingTracer) Start(ctx context.Context, spanName string, attrs ...attribute.KeyValue) (context.Context, warrenotel.Span) {
	c.add("span " + spanName)
	c.addAttrs(attrs)
	return ctx, &capturingSpan{t: c}
}

func (c *capturingTracer) add(s string) {
	c.mu.Lock()
	c.lines = append(c.lines, s)
	c.mu.Unlock()
}

func (c *capturingTracer) addAttrs(attrs []attribute.KeyValue) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, a := range attrs {
		c.lines = append(c.lines, "  attr "+string(a.Key)+"="+a.Value.Emit())
	}
}

func (c *capturingTracer) dump() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return strings.Join(c.lines, "\n")
}

type capturingSpan struct{ t *capturingTracer }

func (s *capturingSpan) End() {}
func (s *capturingSpan) SetAttributes(attrs ...attribute.KeyValue) {
	s.t.addAttrs(attrs)
}
func (s *capturingSpan) SetStatus(code codes.Code, description string) {
	s.t.add("  status " + code.String() + " " + description)
}
func (s *capturingSpan) RecordError(err error) {
	if err != nil {
		s.t.add("  error " + err.Error())
	}
}
