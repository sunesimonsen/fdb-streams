package streams

import (
	"context"
	"fmt"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
)

type MessageHandler interface {
	OnMessage(message []byte) error
	OnError(err error)
}

// A consumer of a message stream.
type consumerGroup struct {
	db             fdb.Database
	id             Id
	dir            directory.DirectorySubspace
	stream         *Stream
	messageHandler MessageHandler
	systemTime     SystemTime
	idGenerator    IdGenerator
	consumers      []consumer
}

func (cg *consumerGroup) register() error {
	result, err := cg.db.Transact(func(tr fdb.Transaction) (any, error) {
		consumersDir, err := cg.dir.CreateOrOpen(tr, []string{"consumers"}, nil)
		if err != nil {
			return nil, err
		}
		instanceKey := consumersDir.Sub(cg.id)
		tr.Set(instanceKey, encodeUInt64(0))
		return tr.Watch(instanceKey), nil
	})

	if err != nil {
		return err
	}

	return result.(fdb.FutureNil).Get()
}

func (cg *consumerGroup) configurePartitions() error {
	cg.consumers = []consumer{}

	_, err := cg.db.Transact(func(tr fdb.Transaction) (any, error) {
		partitionsDir, err := cg.dir.CreateOrOpen(tr, []string{"partitions"}, nil)

		if err != nil {
			return nil, err
		}

		partitionIds, err := partitionsDir.List(tr, []string{})

		if err != nil {
			return nil, err
		}

		for _, id := range partitionIds {
			partitionDir, err := partitionsDir.CreateOrOpen(tr, []string{id}, nil)
			if err != nil {
				return nil, err
			}

			ownerData, err := tr.Get(partitionDir.Sub("owner")).Get()

			if cg.id != Id(ownerData) {
				continue
			}

			initialCursor, err := cg.stream.partitionKey(id)
			if err != nil {
				return nil, err
			}

			cg.consumers = append(cg.consumers, consumer{
				db:              cg.db,
				dir:             partitionDir,
				initialCursor:   initialCursor.FDBKey(),
				consumerGroupId: cg.id,
			})
		}

		if len(cg.consumers) == 0 {
			return nil, fmt.Errorf("No consumers configured")
		}

		return nil, nil
	})

	return err
}

func (cg *consumerGroup) waitForStreamSignal() {
	result, err := cg.db.Transact(func(tr fdb.Transaction) (any, error) {
		return tr.Watch(cg.stream.signalKey()), nil
	})

	if err != nil {
		// Something went sideways so sleep for a bit and continue
		time.Sleep(1 * time.Minute)
	}

	// TODO timeout
	result.(fdb.FutureNil).BlockUntilReady()
}

// Consumes the next message on the stream.
//
// If the stream is already fully consumed, then the method will wait for the
// next message to be emitted.
//
// If an error occurs the consumerGroup isn't advanced to the next message. The usual
// way to handle consumerGroup errors is to log the error and retry with a gradual
// backoff.
func (cg *consumerGroup) consume(ctx context.Context) error {
	// register consumerGroup by adding its id to the instances
	// consumerGroup-groups/<consumerGroup-group-id>/consumers/<consumerGroup-id>=false
	// wait for key to becomes true
	//
	// consumerGroup manager checks for unassigned instances and rebalances
	// partitions across consumerGroup instances
	//
	// Find assigned partitions
	//
	// Start consuming partitions round robin
	//
	// When non of the partitions has new messages wait for the stream signal
	// and continue consuming
	//
	// When trying to consume a partition that is owned by another consumerGroup,
	// reconfigure partitions and continue consuming

	err := cg.register()
	if err != nil {
		return err
	}

	// TODO start heartbeat

	for {
		if len(cg.consumers) == 0 {
			err = cg.configurePartitions()
			if err != nil {
				// Something went sideways so sleep for a bit and continue
				time.Sleep(1 * time.Minute)
			}
		}

		idleConsumers := 0
		for _, c := range cg.consumers {
			err := c.consume(func(message []byte) error {
				return cg.messageHandler.OnMessage(message)
			})

			if err == endOfPartitionError {
				idleConsumers++
				continue
			}

			if err == partitionOwnerChangedError {
				cg.consumers = []consumer{}
				break
			}

			if err != nil {
				cg.messageHandler.OnError(err)
			}
		}

		if ctx.Err() != nil {
			break
		}

		if len(cg.consumers) == idleConsumers {
			// all consumers are idle wait for stream updates
			cg.waitForStreamSignal()
		}
	}

	return ctx.Err()
}
