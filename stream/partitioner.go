package stream

import (
	"fmt"
	"hash"
	"hash/fnv"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/pkg/errors"
)

// HashPartitioner is a wrapper struct around the *kafka.Producer from
// confluent-kafka-go/kafka. It intercepts the Produce method and overrides the
// TopicPartition.Partition based on the hash of the message.Key.
type HashPartitioner struct {
	producer   *kafka.Producer
	partitions map[string]int32
	hasher     hash.Hash32
	mutex      *sync.Mutex
}

// WrapHashPartitioner the given *kafka.Producer with the HashPartitioner
func WrapHashPartitioner(p *kafka.Producer) Producer {
	return &HashPartitioner{
		producer:   p,
		partitions: make(map[string]int32),
		hasher:     fnv.New32a(),
		mutex:      &sync.Mutex{},
	}
}

// Close delegates the kafka.Producer.Close method.
func (p *HashPartitioner) Close() {
	p.producer.Close()
}

// Flush delegates the kafka.Producer.Flush method.
func (p *HashPartitioner) Flush(timeout int) int {
	return p.producer.Flush(timeout)
}

// Produce delegates the kafka.Producer.Produce method but intercepts to
// alter the TopicPartition in the given kafka.Message. It sets the partition to
// the on bucketed on the hash of the Key.
func (p *HashPartitioner) Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error {
	partition, err := p.partitionForKey(msg.Key, *msg.TopicPartition.Topic)
	if err != nil {
		return errors.Wrap(err, "failed to determine topic partition by key")
	}
	msg.TopicPartition.Partition = partition
	return p.producer.Produce(msg, deliveryChan)
}

func (p *HashPartitioner) partitionForKey(key []byte, topic string) (int32, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.hasher.Reset()
	_, err := p.hasher.Write(key)
	if err != nil {
		return -1, err
	}

	partitionCount, err := p.topicPartitionCount(topic)
	if err != nil {
		return -1, err
	}

	partition := int32(p.hasher.Sum32()) % partitionCount
	if partition < 0 {
		partition = -partition
	}
	return partition, nil
}

func (p *HashPartitioner) topicPartitionCount(topic string) (int32, error) {
	if partitionCount, found := p.partitions[topic]; found {
		return partitionCount, nil
	}

	meta, err := p.producer.GetMetadata(&topic, false, 30000)
	if err != nil {
		return -1, errors.Wrap(err, "failed to fetch topic metadata")
	}
	if _, found := meta.Topics[topic]; !found {
		return -1, fmt.Errorf("no such topic found: %v", topic)
	}
	p.partitions[topic] = int32(len(meta.Topics[topic].Partitions))

	return p.partitions[topic], nil
}
