package streams

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
)

type Store struct {
	db          fdb.Database
	dir         directory.DirectorySubspace
	systemTime  SystemTime
	idGenerator IdGenerator
}

// NewStore constructs a new stream store with the given FoundationDB instance,
// a namespace ns the streams are stored under and a list of options.
func NewStore(db fdb.Database, ns string, opts ...StoreOption) (*Store, error) {
	dir, err := createDirectory(db, directory.Root(), "fdb-streams", ns, "streams")
	if err != nil {
		return nil, err
	}

	store := &Store{
		db:          db,
		dir:         dir,
		systemTime:  realClock{},
		idGenerator: UlidIdGenerator{},
	}

	ApplyStoreOptions(store, opts...)

	return store, nil
}

// Opens a stream for the given topic.
func (store *Store) Stream(topic string, opts ...StreamOption) (*Stream, error) {
	dir, err := createDirectory(store.db, store.dir, topic)
	if err != nil {
		return nil, err
	}

	stream := &Stream{
		db:             store.db,
		dir:            dir,
		partitionCount: 256,
		systemTime:     store.systemTime,
		idGenerator:    store.idGenerator,
	}

	ApplyStreamOptions(stream, opts...)

	return stream, nil
}
