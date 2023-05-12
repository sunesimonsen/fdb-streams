package streams

// Stream option type.
type StreamOption func(stream *Stream) error

// Provide a the number of partitions to a stream
func WithPartitions(partitionCount uint32) StreamOption {
	return func(stream *Stream) error {
		stream.partitionCount = partitionCount
		return nil
	}
}

func ApplyStreamOptions(stream *Stream, opts ...StreamOption) error {
	for _, opt := range opts {
		err := opt(stream)
		if err != nil {
			return err
		}
	}
	return nil
}
