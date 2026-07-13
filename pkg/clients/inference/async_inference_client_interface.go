package inference

import "context"

// AsyncInferenceClient defines the interface for non-blocking async dispatch.
type AsyncInferenceClient interface {
	Submit(ctx context.Context, req *GenerateRequest) *ClientError
	GetResult(ctx context.Context) (*GenerateResponse, error)
	Close() error
}
