//go:build !functional

package sarama

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConsumerGroupSessionOwnsPartition(t *testing.T) {
	sess := &consumerGroupSession{}
	memberID := "member"
	sess.memberID.Store(&memberID)
	claims := map[string][]int32{"my_topic": {0, 1}}
	sess.claims.Store(&claims)

	require.True(t, sess.OwnsPartition("my_topic", 0))
	require.True(t, sess.OwnsPartition("my_topic", 1))
	require.False(t, sess.OwnsPartition("my_topic", 2))
	require.False(t, sess.OwnsPartition("other_topic", 0))

	// a cooperative rebalance revoking partition 1 is reflected immediately
	claims = map[string][]int32{"my_topic": {0}}
	sess.claims.Store(&claims)
	require.False(t, sess.OwnsPartition("my_topic", 1))

	// marks for partitions no longer owned are dropped, not committed
	// (no offset manager is attached; reaching it would panic)
	sess.MarkOffset("my_topic", 1, 10, "")
	sess.ResetOffset("my_topic", 1, 5, "")
}
