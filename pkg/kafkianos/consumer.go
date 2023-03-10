package kafkianos

import (
	"context"
	"encoding/json"
	"fmt"
	kaf "kafkapub/pkg"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

type consumer struct {
	reader *kafka.Reader
}

func (c *consumer) Read(ctx context.Context, chMsg chan kaf.Message, chErr chan error) {
	defer c.reader.Close()

	for {

		m, err := c.reader.ReadMessage(ctx)
		if err != nil {
			chErr <- fmt.Errorf(fmt.Sprintf("error while reading a message: %v", err))
			continue
		}

		var message kaf.Message
		err = json.Unmarshal(m.Value, &message)
		if err != nil {
			chErr <- err
		}

		chMsg <- message
	}
}

func NewConsumer(brokers []string, topic string) kaf.Consumer {

	c := kafka.ReaderConfig{
		Brokers:         brokers,
		Topic:           topic,
		Partition:       0,
		MinBytes:        10e3,            // 10KB
		MaxBytes:        10e6,            // 10MB
		MaxWait:         1 * time.Second, // Maximum amount of time to wait for new data to come when fetching batches of messages from kafka.
		ReadLagInterval: -1,
		GroupID:         "grupoprueba",
		StartOffset:     kafka.LastOffset,
	}

	return &consumer{kafka.NewReader(c)}
}
