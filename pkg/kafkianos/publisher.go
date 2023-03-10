package kafkianos

import (
	"context"
	"crypto/rand"
	"encoding/json"
	kaf "kafkapub/pkg"
	"time"

	"github.com/oklog/ulid/v2"
	kafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/snappy"
)

type publisher struct {
	writer *kafka.Writer
}

// Publish implements kafkapub.Publisher
func (p *publisher) Publish(ctx context.Context, payload interface{}) error {
	message, err := p.encodeMessage(payload)
	if err != nil {
		return err
	}
	return p.writer.WriteMessages(ctx, message)
}

func NewPublisher(brokers []string, topic string) kaf.Publisher {
	dialer := &kafka.Dialer{
		Timeout:  10 * time.Second,
		ClientID: "myclient",
	}
	c := kafka.WriterConfig{
		Brokers:          brokers,
		Topic:            topic,
		Balancer:         &kafka.LeastBytes{},
		Dialer:           dialer,
		WriteTimeout:     10 * time.Second,
		ReadTimeout:      10 * time.Second,
		CompressionCodec: snappy.NewCompressionCodec(),
	}
	return &publisher{
		kafka.NewWriter(c),
	}
}

func (p *publisher) encodeMessage(payload interface{}) (kafka.Message, error) {
	m, err := json.Marshal(payload)
	if err != nil {
		return kafka.Message{}, err
	}
	key := Ulid()
	return kafka.Message{
		Key:   []byte(key),
		Value: m,
	}, nil
}

// Ulid encapsulate the way to generate ulids
func Ulid() string {
	t := time.Now().UTC()
	id := ulid.MustNew(ulid.Timestamp(t), rand.Reader)

	return id.String()
}
