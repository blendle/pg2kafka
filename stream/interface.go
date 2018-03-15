package stream

import "github.com/confluentinc/confluent-kafka-go/kafka"

// Producer is the minimal required interface pg2kafka requires to produce
// events to a kafka topic.
type Producer interface {
	Close()
	Flush(int) int

	Produce(*kafka.Message, chan kafka.Event) error
}
