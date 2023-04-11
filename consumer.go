package streams

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
)

// A consumer of a message stream.
type Consumer struct {
	db         fdb.Database
	dir        directory.DirectorySubspace
	streamDir  directory.DirectorySubspace
	versionKey fdb.Key
}

func (consumer *Consumer) loadCursor(tr fdb.ReadTransaction) (fdb.Key, error) {
	cursor, err := tr.Get(consumer.dir.Sub("cursor")).Get()
	if err != nil {
		return nil, err
	}

	if len(cursor) == 0 {
		return consumer.initialCursor, nil
	}

	return fdb.Key(cursor), nil
}

// Returns the next message in the stream.
//
// If the stream is already fully consumed, then the method will wait for the
// next message to be emitted.
func (consumer *Consumer) Next() ([]byte, error) {
	result, err := consumer.db.Transact(func(tr fdb.Transaction) (any, error) {
		cursor, err := consumer.loadCursor(tr)
		if err != nil {
			return nil, err
		}

		nextKey, err := tr.GetKey(fdb.FirstGreaterThan(cursor)).Get()
		if err != nil {
			return nil, err
		}

		// TODO why 2?
		if len(nextKey) < 2 {
			// watch for next update
			return tr.Watch(consumer.versionKey), nil
		}

		message, err := tr.Get(nextKey).Get()
		if err != nil {
			return nil, err
		}

		tr.Set(consumer.dir.Sub("cursor"), nextKey)

		return message, nil
	})

	if err != nil {
		return nil, err
	}

	future, ok := result.(fdb.FutureNil)

	if ok {
		// Wait for update
		_ = future.Get()
		return consumer.Next()
	} else {
		return result.([]byte), nil
	}
}
