//go:build integration

package warren_test

// T74 — Verification gates G1–G6 (Lens-01 protocol re-review).
//
// This file is the *instrument* for the Phase-12 gate task: it drives a real
// broker and captures the ground truth that gates the downstream protocol
// fixes (T75/T76/T58/T78/T80). It must run against BOTH RabbitMQ 3.13 and 4.x
// — the lane image is overridable via WARREN_RMQ_IMAGE (see
// test/docker-compose.integration.yml). Every captured value is emitted on a
// `GATE-RESULT` log line so the committed results table
// (docs/spec-validation/01-rabbitmq-gate-results.md) can be reproduced by
// grepping the verbose test output on each version:
//
//	go test -race -v -tags=integration -run '^TestGate_VerificationGates_integration$' . | grep GATE-
//
// Where a behaviour is stable across versions the gate also *asserts* it (a
// gate that only logged would silently rot); where the whole point is to
// discover a version difference, the per-version branch is logged and only the
// version we are confident about is asserted.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

// gateBrokerVersion reads the running broker's version from the management API
// /api/overview (AMQP_TEST_MANAGEMENT_URL). The gate assertions are
// version-differential, so a missing management URL is a misconfiguration that
// fails — never a silent skip (same rule as queueArgsViaManagement).
func gateBrokerVersion(t *testing.T) (full string, major, minor int) {
	t.Helper()

	mgmt := os.Getenv("AMQP_TEST_MANAGEMENT_URL")
	if mgmt == "" {
		t.Fatal("AMQP_TEST_MANAGEMENT_URL must be set to read the broker version for the differential gate assertions " +
			"(e.g. http://guest:guest@localhost:15672)")
	}
	base, err := url.Parse(mgmt)
	require.NoError(t, err, "AMQP_TEST_MANAGEMENT_URL must be a valid URL")
	require.NotEmpty(t, base.Host, "AMQP_TEST_MANAGEMENT_URL must include host:port")

	apiURL := fmt.Sprintf("%s://%s/api/overview", base.Scheme, base.Host)
	req, err := http.NewRequest(http.MethodGet, apiURL, nil)
	require.NoError(t, err)
	if base.User != nil {
		pass, _ := base.User.Password()
		req.SetBasicAuth(base.User.Username(), pass)
	}

	client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
	defer client.CloseIdleConnections()

	resp, err := client.Do(req)
	require.NoErrorf(t, err, "management API GET %s://%s failed", base.Scheme, base.Host)
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("management API GET %s returned %d: %s", apiURL, resp.StatusCode, string(body))
	}

	var payload struct {
		RabbitMQVersion string `json:"rabbitmq_version"`
		ProductVersion  string `json:"product_version"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))

	full = payload.RabbitMQVersion
	if full == "" {
		full = payload.ProductVersion
	}
	require.NotEmpty(t, full, "broker version must be reported by /api/overview")

	parts := strings.SplitN(full, ".", 3)
	major, err = strconv.Atoi(parts[0])
	require.NoErrorf(t, err, "parse major from %q", full)
	if len(parts) > 1 {
		minor, _ = strconv.Atoi(parts[1])
	}
	return full, major, minor
}

// gateDialRaw opens a raw amqp091 connection + channel registered for cleanup.
// Raw (not warren) is deliberate: the gates poke protocol corners (specific
// queue arguments, oversize bodies, non-zero prefetch_size) that the public API
// intentionally does not expose.
func gateDialRaw(t *testing.T, url string) (*amqp091.Connection, *amqp091.Channel) {
	t.Helper()
	conn, err := amqp091.Dial(url)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	ch, err := conn.Channel()
	require.NoError(t, err)
	return conn, ch
}

// xDeathEntries normalises the x-death header (an array of tables) into a slice
// of (queue, reason, count) for assertion + logging.
type xDeathEntry struct {
	Queue  string
	Reason string
	Count  int64
}

func parseXDeathRaw(t *testing.T, h amqp091.Table) []xDeathEntry {
	t.Helper()
	raw, ok := h["x-death"]
	if !ok {
		return nil
	}
	arr, ok := raw.([]interface{})
	require.Truef(t, ok, "x-death must be an array, got %T", raw)
	out := make([]xDeathEntry, 0, len(arr))
	for _, e := range arr {
		tbl, ok := e.(amqp091.Table)
		require.Truef(t, ok, "x-death entry must be a table, got %T", e)
		var ent xDeathEntry
		if q, ok := tbl["queue"].(string); ok {
			ent.Queue = q
		}
		if r, ok := tbl["reason"].(string); ok {
			ent.Reason = r
		}
		switch c := tbl["count"].(type) {
		case int64:
			ent.Count = c
		case int32:
			ent.Count = int64(c)
		case int:
			ent.Count = int64(c)
		}
		out = append(out, ent)
	}
	return out
}

func TestGate_VerificationGates_integration(t *testing.T) {
	defer goleak.VerifyNone(t)

	url := amqpTestURL(t)
	full, major, minor := gateBrokerVersion(t)
	is4x := major >= 4
	t.Logf("GATE-BROKER version=%s major=%d minor=%d is4x=%t", full, major, minor, is4x)

	// G1 + G2 — quorum x-delivery-limit dead-letter reason atom + count shape.
	t.Run("G1_G2_xdeath_deliverylimit", func(t *testing.T) {
		const (
			dlx = "gate.g1.dlx"
			dlq = "gate.g1.dlq"
			src = "gate.g1.src"
		)
		purgeQueues(t, url, src, dlq)
		t.Cleanup(func() {
			deleteQueues(url, src, dlq)
			deleteExchanges(url, dlx)
		})

		ctx := context.Background()
		_, ch := gateDialRaw(t, url)

		require.NoError(t, ch.ExchangeDeclare(dlx, "fanout", true, false, false, false, nil))
		_, err := ch.QueueDeclare(dlq, true, false, false, false, nil)
		require.NoError(t, err)
		require.NoError(t, ch.QueueBind(dlq, "", dlx, false, nil))

		// Quorum source with a delivery-limit of 1 so two delivery attempts
		// (initial + one redelivery) trip the limit and dead-letter the message.
		_, err = ch.QueueDeclare(src, true, false, false, false, amqp091.Table{
			"x-queue-type":           "quorum",
			"x-delivery-limit":       int32(1),
			"x-dead-letter-exchange": dlx,
		})
		require.NoError(t, err)

		require.NoError(t, ch.PublishWithContext(ctx, "", src, false, false,
			amqp091.Publishing{Body: []byte("poison")}))

		// Nack-requeue every delivery; once the delivery-limit is exceeded the
		// broker stops redelivering and dead-letters instead.
		deliveries, err := ch.Consume(src, "", false, false, false, false, nil)
		require.NoError(t, err)
		redeliveries := 0
	loop:
		for {
			select {
			case d, ok := <-deliveries:
				if !ok {
					break loop
				}
				redeliveries++
				_ = d.Nack(false, true)
				if redeliveries >= 5 { // safety bound — should dead-letter well before this
					break loop
				}
			case <-time.After(3 * time.Second):
				break loop
			}
		}
		t.Logf("GATE-RESULT G1/G2 src-delivery-attempts=%d", redeliveries)

		// The dead-lettered message must now be in the DLQ with an x-death header.
		ddCh, err := ch.Consume(dlq, "", true, false, false, false, nil)
		require.NoError(t, err)
		var dead amqp091.Delivery
		select {
		case dead = <-ddCh:
		case <-time.After(5 * time.Second):
			t.Fatal("message did not dead-letter to the DLQ after exceeding x-delivery-limit")
		}

		entries := parseXDeathRaw(t, dead.Headers)
		require.NotEmpty(t, entries, "dead-lettered message must carry an x-death header")
		// Select the source-queue entry explicitly rather than trusting array
		// order: parseXDeathRaw preserves broker order, so entries[0] is not
		// guaranteed to be the delivery-limit eviction (a future broker or
		// redeliver path could prepend another (queue, reason) entry). The
		// delivery-limit eviction records its x-death against the source queue.
		var srcEntry *xDeathEntry
		for i := range entries {
			if entries[i].Queue == src {
				srcEntry = &entries[i]
				break
			}
		}
		require.NotNilf(t, srcEntry,
			"x-death must carry an entry for the source queue %q (raw entries=%+v)", src, entries)
		reason := srcEntry.Reason
		count := srcEntry.Count
		t.Logf("GATE-RESULT G1 xdeath-reason=%q (raw entries=%+v)", reason, entries)
		t.Logf("GATE-RESULT G2 xdeath-count=%d entries=%d", count, len(entries))

		// G1 ground truth: the reason atom for a delivery-limit eviction. The
		// open question (RMQ-01) is the exact spelling — `delivery-limit` vs
		// `delivery_limit`. Normalise separators and assert the *concept* so the
		// gate is a real check on either version while still recording the atom.
		norm := strings.ReplaceAll(reason, "_", "-")
		assert.Equalf(t, "delivery-limit", norm,
			"G1: a quorum delivery-limit eviction must report the delivery-limit reason (raw=%q)", reason)

		// G2 ground truth: a single dead-letter event yields count==1 on the
		// (queue, reason) entry — the shape DeathCount() sums over.
		assert.Equalf(t, int64(1), count, "G2: a single dead-letter event must have count==1 (raw=%q)", reason)
	})

	// G3 — how does a *classic* queue treat x-delivery-limit? Two distinct facts:
	// (a) is the declare even accepted, and (b) if so, is the limit honoured (the
	// message dead-letters). On 3.13 the declare is rejected outright (406
	// PRECONDITION_FAILED — x-delivery-limit is a quorum-only arg); on 4.x the
	// gate discovers whether classic queues started accepting/honouring it.
	t.Run("G3_classic_xdeliverylimit_honoring", func(t *testing.T) {
		const (
			dlx     = "gate.g3.dlx"
			dlq     = "gate.g3.dlq"
			classic = "gate.g3.classic"
		)
		purgeQueues(t, url, classic, dlq)
		deleteQueues(url, classic) // drop any stale queue with conflicting args
		t.Cleanup(func() {
			deleteQueues(url, classic, dlq)
			deleteExchanges(url, dlx)
		})

		ctx := context.Background()
		conn, ch := gateDialRaw(t, url)

		require.NoError(t, ch.ExchangeDeclare(dlx, "fanout", true, false, false, false, nil))
		_, err := ch.QueueDeclare(dlq, true, false, false, false, nil)
		require.NoError(t, err)
		require.NoError(t, ch.QueueBind(dlq, "", dlx, false, nil))

		// Attempt the classic + x-delivery-limit declare on its own channel: a
		// rejected declare closes the channel, so we must not reuse `ch`.
		declCh, err := conn.Channel()
		require.NoError(t, err)
		_, derr := declCh.QueueDeclare(classic, true, false, false, false, amqp091.Table{
			"x-queue-type":           "classic",
			"x-delivery-limit":       int32(1),
			"x-dead-letter-exchange": dlx,
		})
		declareAccepted := derr == nil
		t.Logf("GATE-RESULT G3 classic-xdeliverylimit-declare-accepted=%t err=%v", declareAccepted, derr)

		if !declareAccepted {
			// 3.13: the broker rejects x-delivery-limit on a classic queue, so
			// honouring is moot. Record it and assert the documented 3.13 reject.
			t.Logf("GATE-RESULT G3 classic-xdeliverylimit-honored=false (declare rejected)")
			if !is4x {
				var ae *amqp091.Error
				require.Truef(t, errors.As(derr, &ae), "G3: expected an AMQP error, got %v", derr)
				assert.Equalf(t, 406, ae.Code,
					"G3: RabbitMQ 3.13 rejects x-delivery-limit on a classic queue with PRECONDITION_FAILED (version=%s)", full)
			}
			return
		}

		// Declare accepted (e.g. 4.x): probe whether the limit is actually honoured.
		require.NoError(t, ch.PublishWithContext(ctx, "", classic, false, false,
			amqp091.Publishing{Body: []byte("poison")}))

		deliveries, err := ch.Consume(classic, "", false, false, false, false, nil)
		require.NoError(t, err)
		// Bounded nack-requeue: if classic ignores the limit the message requeues
		// forever, so we cap attempts and then ack-drain to stop the loop.
		attempts := 0
	g3loop:
		for attempts < 8 {
			select {
			case d, ok := <-deliveries:
				if !ok {
					break g3loop
				}
				attempts++
				if attempts >= 8 {
					_ = d.Ack(false) // drain so we don't requeue endlessly
				} else {
					_ = d.Nack(false, true)
				}
			case <-time.After(3 * time.Second):
				break g3loop
			}
		}

		// Did anything dead-letter to the DLQ?
		ddCh, err := ch.Consume(dlq, "", true, false, false, false, nil)
		require.NoError(t, err)
		honored := false
		select {
		case <-ddCh:
			honored = true
		case <-time.After(3 * time.Second):
		}
		t.Logf("GATE-RESULT G3 classic-xdeliverylimit-honored=%t attempts=%d", honored, attempts)
	})

	// G4 — which {quorum, x-overflow, x-dead-letter-strategy} declare
	// permutations the broker accepts. This gates T76, which forces/validates
	// `reject-publish` for at-least-once. Ground truth on 3.13: the broker is
	// *permissive* — it accepts every combination, including the semantically
	// invalid at-least-once+drop-head and at-least-once+no-DLX — so the coupling
	// MUST be enforced client-side (exactly what T76 does). This gate therefore
	// records acceptance per permutation and only asserts the one invariant we
	// can rely on across versions: the canonical valid combo is accepted. Each
	// declare runs on its own channel because a rejected declare closes it.
	t.Run("G4_quorum_overflow_atleastonce_permutations", func(t *testing.T) {
		const dlx = "gate.g4.dlx"
		conn, setupCh := gateDialRaw(t, url)
		require.NoError(t, setupCh.ExchangeDeclare(dlx, "fanout", true, false, false, false, nil))

		type perm struct {
			name      string
			args      amqp091.Table
			canonical bool // the one combination T76 emits; must be accepted everywhere
		}
		perms := []perm{
			{
				name: "quorum+reject-publish+at-least-once+dlx",
				args: amqp091.Table{
					"x-queue-type":           "quorum",
					"x-overflow":             "reject-publish",
					"x-dead-letter-strategy": "at-least-once",
					"x-dead-letter-exchange": dlx,
				},
				canonical: true,
			},
			{
				name: "quorum+drop-head+at-least-once+dlx",
				args: amqp091.Table{
					"x-queue-type":           "quorum",
					"x-overflow":             "drop-head",
					"x-dead-letter-strategy": "at-least-once",
					"x-dead-letter-exchange": dlx,
				},
			},
			{
				name: "quorum+at-least-once+no-dlx",
				args: amqp091.Table{
					"x-queue-type":           "quorum",
					"x-overflow":             "reject-publish",
					"x-dead-letter-strategy": "at-least-once",
				},
			},
			{
				name: "quorum+reject-publish+at-most-once+dlx",
				args: amqp091.Table{
					"x-queue-type":           "quorum",
					"x-overflow":             "reject-publish",
					"x-dead-letter-strategy": "at-most-once",
					"x-dead-letter-exchange": dlx,
				},
			},
		}

		for i, p := range perms {
			qname := fmt.Sprintf("gate.g4.%d", i)
			purgeQueues(t, url, qname)
			deleteQueues(url, qname) // ensure no stale queue with conflicting args
			// Fresh channel per permutation: a rejected declare closes it.
			ch, err := conn.Channel()
			require.NoError(t, err)
			_, derr := ch.QueueDeclare(qname, true, false, false, false, p.args)
			ok := derr == nil
			t.Logf("GATE-RESULT G4 perm=%q accepted=%t err=%v", p.name, ok, derr)
			if p.canonical {
				assert.Truef(t, ok, "G4: the canonical at-least-once combo must be accepted (err=%v)", derr)
			}
			_ = ch.Close()
			deleteQueues(url, qname)
		}
		deleteExchanges(url, dlx)
	})

	// G5 — broker max_message_size default (128 MiB on 3.13, 16 MiB on 4.0+).
	// Behavioural probe: publish ~17 MiB (over the 4.x default, under the 3.13
	// default) with confirms and record whether the broker accepts it.
	t.Run("G5_max_message_size_default", func(t *testing.T) {
		const q = "gate.g5.q"
		purgeQueues(t, url, q)
		t.Cleanup(func() { deleteQueues(url, q) })

		ctx := context.Background()
		conn, ch := gateDialRaw(t, url)
		require.NoError(t, ch.Confirm(false))
		_, err := ch.QueueDeclare(q, false, true, false, false, nil)
		require.NoError(t, err)

		const size = 17 * 1024 * 1024
		body := make([]byte, size)

		// A connection-level NotifyClose fires if the broker rejects the oversize
		// frame by closing the connection (the 4.x enforcement path).
		closeCh := conn.NotifyClose(make(chan *amqp091.Error, 1))

		accepted := false
		conf, perr := ch.PublishWithDeferredConfirmWithContext(ctx, "", q, false, false,
			amqp091.Publishing{Body: body})
		if perr == nil {
			done := make(chan bool, 1)
			go func() { done <- conf.Wait() }()
			select {
			case ack := <-done:
				accepted = ack
			case <-closeCh:
				accepted = false
			case <-time.After(10 * time.Second):
				accepted = false
			}
		}
		t.Logf("GATE-RESULT G5 size-bytes=%d accepted=%t publish-err=%v", size, accepted, perr)

		if !is4x {
			assert.Truef(t, accepted,
				"G5: a 17 MiB message must be accepted on 3.13 (128 MiB default) (version=%s)", full)
		} else {
			assert.Falsef(t, accepted,
				"G5: a 17 MiB message must be rejected on 4.x (16 MiB default) (version=%s)", full)
		}
	})

	// G6 — does the broker reject a non-zero prefetch_size on per-consumer qos,
	// or silently ignore it? (Informs T78: warren always sends prefetch_size=0.)
	t.Run("G6_prefetch_size_nonzero", func(t *testing.T) {
		// Dedicated connection: a rejected qos closes the channel/connection.
		conn, err := amqp091.Dial(url)
		require.NoError(t, err)
		t.Cleanup(func() { _ = conn.Close() })
		ch, err := conn.Channel()
		require.NoError(t, err)

		// prefetchCount=10, prefetchSize=1024 (non-zero), global=false.
		qerr := ch.Qos(10, 1024, false)
		rejected := qerr != nil
		t.Logf("GATE-RESULT G6 prefetch_size-nonzero-rejected=%t err=%v", rejected, qerr)

		var ae *amqp091.Error
		require.ErrorAsf(t, qerr, &ae,
			"G6: a non-zero per-consumer prefetch_size must be rejected with an AMQP error (version=%s, rejected=%t)", full, rejected)
		t.Logf("GATE-RESULT G6 reply-code=%d reply-text=%q rejected=%t", ae.Code, ae.Reason, rejected)
		// Ground truth (both versions): the rejection is 540 NOT_IMPLEMENTED, not
		// merely "some error" — assert the documented reply code so a broker that
		// rejected with a different code would not silently pass G6.
		assert.Equalf(t, 540, ae.Code,
			"G6: prefetch_size!=0 must be rejected with 540 NOT_IMPLEMENTED (version=%s, got %d %q)",
			full, ae.Code, ae.Reason)
	})
}
