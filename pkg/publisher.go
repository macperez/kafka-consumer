//pkg/publisher.go

package kafkapub

import "context"

type Publisher interface {
	// Publish a message into a stream
	Publish(ctx context.Context, payload interface{}) error
}
