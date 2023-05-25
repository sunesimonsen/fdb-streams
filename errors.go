package streams

import "errors"

// Error for when a partition doesn't have anymore message.
var endOfPartitionError = errors.New("end of partition")

// Error for when a partition changed owner
var partitionOwnerChangedError = errors.New("partition changed owner")
