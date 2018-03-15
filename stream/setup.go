package stream

import (
	"net/url"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/pkg/errors"
)

// NewProducer will create, connect and return a new kafka produce with config
// read from the environment var KAFKA_PRODUCER_URL.
func NewProducer() (Producer, error) {

	url, err := url.Parse(os.Getenv("KAFKA_PRODUCER_URL"))
	if err != nil {
		return nil, errors.Wrap(err, "failed to read KAFKA_PRODUCER_URL as an url")
	}

	hostname, err := os.Hostname()
	if err != nil {
		hostname = os.Getenv("HOSTNAME")
	}

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"client.id":         hostname,
		"bootstrap.servers": url.Host,
		"compression.codec": "snappy",
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to setup producer")
	}

	return WrapHashPartitioner(p), nil
}
