package streams

import (
	"context"
	"hash/fnv"
	"strconv"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

// A message stream.
type Stream struct {
	db             fdb.Database
	dir            directory.DirectorySubspace
	partitionCount uint32
	systemTime     SystemTime
	idGenerator    IdGenerator
}

func hash(text string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(text))
	return h.Sum32()
}

func (stream *Stream) partitionKey(partition string) (directory.DirectorySubspace, error) {
	return createDirectory(stream.db, stream.dir, "partitions", partition)
}

func (stream *Stream) streamKey(partitionKey string) (fdb.Key, error) {
	partition := hash(partitionKey) % stream.partitionCount
	partitionDir, err := stream.partitionKey(strconv.FormatUint(uint64(partition), 10))

	if err != nil {
		return nil, err
	}

	return partitionDir.PackWithVersionstamp(
		tuple.Tuple{
			tuple.IncompleteVersionstamp(0),
		},
	)
}

func (stream *Stream) signalKey() fdb.Key {
	return stream.dir.Sub("signal").FDBKey()
}

// Emit a message on an open transaction in the partition calcualted based on the partitionKey.
//
// Notice the message needs to honour the FoundationDB value size limits.
func (stream *Stream) EmitOn(tr fdb.Transaction, partitionKey string, message []byte) error {
	key, err := stream.streamKey(partitionKey)
	if err != nil {
		return err
	}

	tr.SetVersionstampedKey(key, message)

	tr.Add(stream.signalKey(), encodeUInt64(1))

	return nil
}

// Emit a message on a new transaction in the partition calcualted based on the partitionKey.
//
// Notice the message needs to honour the FoundationDB value size limits.
func (stream *Stream) Emit(partitionKey string, message []byte) error {
	_, err := stream.db.Transact(func(tr fdb.Transaction) (any, error) {
		return nil, stream.EmitOn(tr, partitionKey, message)
	})

	return err
}

// Consumes the stream with the given message handler.
//
// The consumer participates in the given consumer group and continues from the
// position of the consumer group.
func (stream *Stream) Consume(ctx context.Context, consumerGroupId string, messageHandler MessageHandler) error {
	dir, err := createDirectory(stream.db, stream.dir, "consumer-groups", consumerGroupId)
	if err != nil {
		return err
	}

	cg := &consumerGroup{
		db:             stream.db,
		instanceId:     stream.idGenerator.NextId(),
		dir:            dir,
		stream:         stream,
		messageHandler: messageHandler,
	}

	return cg.consume(ctx)
}
