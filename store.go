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
func NewStore(db fdb.Database, ns string, opts ...Option) (*Store, error) {
	dir, err := directory.CreateOrOpen(
		db,
		[]string{"fdb-streams", ns, "streams"},
		nil,
	)
	if err != nil {
		return nil, err
	}

	store := &Store{
		db:          db,
		dir:         dir,
		systemTime:  realClock{},
		idGenerator: UlidIdGenerator{},
	}

	for _, opt := range opts {
		err := opt(store)
		if err != nil {
			return store, err
		}
	}

	return store, nil
}

// Opens a stream for the given topic.
func (store *Store) Stream(topic string) (*Stream, error) {
	dir, err := store.dir.CreateOrOpen(
		store.db,
		[]string{topic},
		nil,
	)
	if err != nil {
		return nil, err
	}

	return &Stream{
		db:          store.db,
		dir:         dir,
		partitions:  256,
		systemTime:  store.systemTime,
		idGenerator: store.idGenerator,
	}, nil
}
