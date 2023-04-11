package streams

import (
	"hash/fnv"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

// A message stream.
type Stream struct {
	db         fdb.Database
	dir        directory.DirectorySubspace
	partitions uint32
}

func hash(text string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(text))
	return h.Sum32()
}

func (stream *Stream) streamKey(partitionKey string) (fdb.Key, error) {
	return stream.dir.PackWithVersionstamp(
		tuple.Tuple{
			hash(partitionKey) % stream.partitions,
			tuple.IncompleteVersionstamp(0),
		},
	)
}

func (stream *Stream) versionKey() fdb.Key {
	return stream.dir.Sub("version").FDBKey()
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

	tr.Add(stream.versionKey(), encodeUInt64(1))

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

// Returns a consumer for this stream with the given id. If the consumer already
// exist it will continue from where it left off.
func (stream *Stream) Consumer(id string) (*Consumer, error) {
	dir, err := stream.dir.CreateOrOpen(stream.db, []string{"consumers", id}, nil)
	if err != nil {
		return nil, err
	}

	return &Consumer{
		db:         stream.db,
		dir:        dir,
		streamDir:  stream.dir,
		versionKey: stream.versionKey(),
	}, nil
}
