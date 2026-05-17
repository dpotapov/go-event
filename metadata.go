package event

import (
	"context"
	"time"
)

type metadataKey struct{}

type MessageMetadata struct {
	NumDelivered uint64
	Timestamp    time.Time
}

func WithMessageMetadata(ctx context.Context, metadata MessageMetadata) context.Context {
	return context.WithValue(ctx, metadataKey{}, metadata)
}

func MessageMetadataFromContext(ctx context.Context) (MessageMetadata, bool) {
	metadata, ok := ctx.Value(metadataKey{}).(MessageMetadata)
	return metadata, ok
}
