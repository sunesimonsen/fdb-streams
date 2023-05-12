package streams

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
)

// A consumer of a message stream.
type Consumer struct {
	db          fdb.Database
	dir         directory.DirectorySubspace
	stream      *Stream
	versionKey  fdb.Key
	systemTime  SystemTime
	idGenerator IdGenerator
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

type consumeHandler func([]byte) error

func (consumer *Consumer) nextCursor() (fdb.Key, error) {
	result, err := consumer.db.Transact(func(tr fdb.Transaction) (any, error) {
		cursor, err := consumer.loadCursor(tr)
		if err != nil {
			return nil, err
		}

		nextCursor, err := tr.GetKey(fdb.FirstGreaterThan(cursor)).Get()

		if err != nil {
			return nil, err
		}

		// TODO why 2?
		if len(cursor) < 2 {
			// watch for next update
			return tr.Watch(consumer.versionKey), nil
		}

		return nextCursor, nil
	})

	future, ok := result.(fdb.FutureNil)

	if ok {
		// Wait for update
		_ = future.Get()
		return consumer.nextCursor()
	} else {
		return result.(fdb.Key), err
	}
}

func (consumer *Consumer) readMessage(cursor fdb.Key) (fdb.Key, error) {
	message, err := consumer.db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) {
		return tr.Get(cursor).Get()
	})

	return message.([]byte), err
}

func (consumer *Consumer) setCursor(cursor fdb.Key) error {
	_, err := consumer.db.Transact(func(tr fdb.Transaction) (any, error) {
		tr.Set(consumer.dir.Sub("cursor"), cursor)
		return nil, nil
	})

	return err
}

// Consumes the next message on the stream.
//
// If the stream is already fully consumed, then the method will wait for the
// next message to be emitted.
//
// If an error occurs the consumer isn't advanced to the next message. The usual
// way to handle consumer errors is to log the error and retry with a gradual
// backoff.
func (consumer *Consumer) Consume(cb consumeHandler) error {
	nextCursor, err := consumer.nextCursor()

	if err != nil {
		return err
	}

	message, err := consumer.readMessage(nextCursor)

	if err != nil {
		return err
	}

	err = cb(message)

	if err != nil {
		return err
	}

	return consumer.setCursor(nextCursor)
}
