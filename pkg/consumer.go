//pkg/consumer.go

package kafkapub

import "context"

type Consumer interface {
	// Publish a message into a stream
	Read(ctx context.Context, chMsg chan Message, chErr chan error)
}
