package warren

// LATER-33: batchCounterBKey MessageId length bound tests.

import (
	"strings"
	"testing"

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
}
