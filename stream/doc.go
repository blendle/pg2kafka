// Package stream is a small wrapper around the
// github.com/confluentinc/confluent-kafka-go Producer instance.
//
// It handles setup from environment and wraps the Produce to automatically
// set the Partition of a message by fnv hashing the Key.
package stream
