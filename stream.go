package streams

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

// A message stream.
type Stream struct {
	db  fdb.Database
	dir directory.DirectorySubspace
}

func (stream *Stream) streamKey() (fdb.Key, error) {
	return stream.dir.PackWithVersionstamp(
		tuple.Tuple{tuple.IncompleteVersionstamp(0)},
	)
}

func (stream *Stream) versionKey() fdb.Key {
	return stream.dir.Sub("version").FDBKey()
}

// Emit a message on an open transaction.
//
// Notice the message needs to honour the FoundationDB value size limits.
func (stream *Stream) EmitOn(tr fdb.Transaction, message []byte) error {
	key, err := stream.streamKey()
	if err != nil {
		return err
	}

	tr.SetVersionstampedKey(key, message)

	tr.Add(stream.versionKey(), encodeUInt64(1))

	return nil
}

// Emit a message by opening a new transaction.
//
// Notice the message needs to honour the FoundationDB value size limits.
func (stream *Stream) Emit(message []byte) error {
	_, err := stream.db.Transact(func(tr fdb.Transaction) (any, error) {
		return nil, stream.EmitOn(tr, message)
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
		db:            stream.db,
		dir:           dir,
		initialCursor: stream.dir.FDBKey(),
		versionKey:    stream.versionKey(),
	}, nil
}
