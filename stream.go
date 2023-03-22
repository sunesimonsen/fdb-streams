package streams

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

type Stream struct {
	db  fdb.Database
	dir directory.DirectorySubspace
}

func (stream *Stream) streamKey() (fdb.Key, error) {
	return stream.dir.PackWithVersionstamp(
		tuple.Tuple{tuple.IncompleteVersionstamp(0)},
	)
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

	tr.Add(key, encodeUInt64(1))

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
