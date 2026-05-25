package warren

// LATER-33: batchCounterBKey MessageId length bound tests.

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
)

func TestBatchCounterBKey_longMessageId_isTruncated(t *testing.T) {
	longID := strings.Repeat("x", maxMsgIDKeyLen+100)
	key := batchCounterBKey("ctag-abc", longID, 1)
	maxExpected := len("mid:") + maxMsgIDKeyLen
	assert.LessOrEqual(t, len(key), maxExpected,
		"batchCounterBKey must truncate MessageId to maxMsgIDKeyLen bytes")
}

func TestBatchCounterBKey_shortMessageId_isNotTruncated(t *testing.T) {
	shortID := strings.Repeat("a", maxMsgIDKeyLen)
	key := batchCounterBKey("ctag-abc", shortID, 1)
	assert.Equal(t, "mid:"+shortID, key,
		"batchCounterBKey must not truncate MessageId within limit")
}

func TestBatchCounterBKey_emptyMessageId_usesFallback(t *testing.T) {
	key := batchCounterBKey("ctag-abc", "", 42)
	assert.True(t, strings.HasPrefix(key, "dlv:"),
		"empty MessageId must use dlv: key family; got %q", key)
	assert.Contains(t, key, "ctag-abc",
		"fallback key must embed the consumerTag; got %q", key)
	assert.Contains(t, key, "42",
		"fallback key must embed the deliveryTag; got %q", key)
}

// TestBatchCounterBKey_multibyteMessageId_truncatesAtRuneBoundary verifies
// that batchCounterBKey delegates to counterBKeyForMsgID which truncates at a
// rune boundary, so the resulting key is always valid UTF-8.
func TestBatchCounterBKey_multibyteMessageId_truncatesAtRuneBoundary(t *testing.T) {
	// "中" is U+4E2D, 3 bytes in UTF-8. Build a string that will cross the limit.
	longID := strings.Repeat("中", maxMsgIDKeyLen)
	key := batchCounterBKey("ctag-abc", longID, 1)
	assert.True(t, utf8.ValidString(key),
		"truncated key must be valid UTF-8; got %q", key)
	assert.LessOrEqual(t, len(key), len("mid:")+maxMsgIDKeyLen,
		"truncated key must not exceed the length bound")
}

// TestBatchCounterBKey_continuationBytesMessageId_doesNotCollide verifies that
// a MessageId composed entirely of UTF-8 continuation bytes does not produce
// the degenerate "mid:" key, which would collapse all such messages onto a
// single sync.Map slot and corrupt the redelivery counter.
func TestBatchCounterBKey_continuationBytesMessageId_doesNotCollide(t *testing.T) {
	invalidUTF8 := strings.Repeat("\x80", maxMsgIDKeyLen+1)
	key := batchCounterBKey("ctag-abc", invalidUTF8, 42)
	assert.NotEqual(t, "mid:", key,
		"pure continuation bytes must not collapse to the bare 'mid:' key")
	// Must fall back to a dlv: key instead.
	assert.True(t, strings.HasPrefix(key, "dlv:"),
		"must fall back to dlv: family when truncation produces empty MessageId; got %q", key)
}
