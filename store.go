package streams

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
)

type Store struct {
	db  fdb.Database
	dir directory.DirectorySubspace
}

// NewStore constructs a new stream store with the given FoundationDB instance,
// a namespace ns the streams are stored under.
func NewStore(db fdb.Database, ns string) (*Store, error) {
	dir, err := directory.CreateOrOpen(
		db,
		[]string{"fdb-streams", ns, "streams"},
		nil,
	)
	if err != nil {
		return nil, err
	}

	return &Store{
		db:  db,
		dir: dir,
	}, nil
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
		db:         store.db,
		dir:        dir,
		partitions: 256,
	}, nil
}
