package sarama

import (
	"errors"
	"fmt"
)

// ErrOffsetCommitRevoked is the cause of an *OffsetCommitError when a commit
// is dropped or rejected because this member no longer owns the partition:
// the partition was revoked by a rebalance before the commit was built, or
// the coordinator fenced the member's generation. It signals a revocation,
// not a network failure; retrying the same commit cannot succeed.
var ErrOffsetCommitRevoked = errors.New("kafka: offset commit rejected: partition ownership lost")

// OffsetCommitError describes a single partition's offset commit that failed
// or was dropped. It records the partition and the group generation the
// commit was attempted in, so the caller can tell a revocation (the
// generation is stale, see IsCommitRevocation) from a network failure that
// may succeed when retried.
type OffsetCommitError struct {
	Topic      string
	Partition  int32
	Offset     int64
	Generation int32
	Err        error
}

func (e *OffsetCommitError) Error() string {
	return fmt.Sprintf("kafka: offset commit for %s/%d at offset %d in generation %d failed: %s",
		e.Topic, e.Partition, e.Offset, e.Generation, e.Err)
}

func (e *OffsetCommitError) Unwrap() error { return e.Err }

// IsCommitRevocation reports whether err means the commit was rejected
// because this member lost ownership of the partition — locally dropped
// after a revocation, or refused by the coordinator with ILLEGAL_GENERATION,
// UNKNOWN_MEMBER_ID or FENCED_INSTANCE_ID — as opposed to a network or
// coordinator failure that a retry may succeed on.
func IsCommitRevocation(err error) bool {
	return errors.Is(err, ErrOffsetCommitRevoked) ||
		errors.Is(err, ErrIllegalGeneration) ||
		errors.Is(err, ErrUnknownMemberId) ||
		errors.Is(err, ErrFencedInstancedId)
}
