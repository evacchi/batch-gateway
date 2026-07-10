package inference

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	asyncapi "github.com/llm-d-incubation/llm-d-async/api"

	"github.com/llm-d/llm-d-batch-gateway/internal/util/logging"
	httpclient "github.com/llm-d/llm-d-batch-gateway/pkg/clients/http"
)

// asyncSharedClient decouples submit from collect. All results from
// the pool are delivered to a single shared channel — any consumer
// can read them, not just the submitter.
type asyncSharedClient struct {
	pool    *asyncPool
	results chan *GenerateResponse
	cancel  context.CancelFunc
	logger  logr.Logger
}

var _ AsyncInferenceClient = (*asyncSharedClient)(nil)

// newAsyncSharedClient creates a client backed by pool.
// Results from the pool's producer are polled into a shared channel.
func newAsyncSharedClient(pool *asyncPool, bufferSize int, logger logr.Logger) *asyncSharedClient {
	ctx, cancel := context.WithCancel(context.Background())
	c := &asyncSharedClient{
		pool:    pool,
		results: make(chan *GenerateResponse, bufferSize),
		cancel:  cancel,
		logger:  logger,
	}
	go c.pollResults(ctx)
	return c
}

// pollResults reads all results from the producer and sends them
// to the shared channel. No per-request routing — every result
// is available to any consumer.
func (c *asyncSharedClient) pollResults(ctx context.Context) {
	for ctx.Err() == nil {
		pollCtx, pollCancel := context.WithTimeout(ctx, c.pool.dispatcher.pollTimeout)
		result, err := c.pool.producer.GetResult(pollCtx)
		pollCancel()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			continue
		}
		resp := &GenerateResponse{
			RequestID: result.ID,
			Response:  []byte(result.Payload),
		}
		select {
		case c.results <- resp:
		case <-ctx.Done():
			return
		}
	}
}

func (c *asyncSharedClient) Submit(ctx context.Context, req *GenerateRequest) *ClientError {
	now := time.Now()
	deadline := now.Add(defaultDeadline)
	if dl, ok := ctx.Deadline(); ok {
		deadline = dl
	}

	metadata := make(map[string]string)
	otel.GetTextMapPropagator().Inject(ctx, propagation.MapCarrier(metadata))

	reqMsg := &asyncapi.RequestMessage{
		ID:       req.RequestID,
		Created:  now.Unix(),
		Deadline: deadline.Unix(),
		Payload:  req.Params,
		Headers:  req.Headers,
		Endpoint: req.Endpoint,
		Metadata: metadata,
	}

	if err := c.pool.producer.SubmitRequest(ctx, reqMsg); err != nil {
		return &ClientError{
			Category: httpclient.ErrCategoryServer,
			Message:  fmt.Sprintf("submit async request: %v", err),
			RawError: err,
		}
	}

	c.logger.V(logging.TRACE).Info("Submitted async request", "requestID", req.RequestID)
	return nil
}

func (c *asyncSharedClient) GetResult(ctx context.Context) (*GenerateResponse, error) {
	select {
	case resp := <-c.results:
		return resp, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *asyncSharedClient) Close() error {
	c.cancel()
	return nil
}

// NewSharedClientResolver creates a resolver that returns shared clients
// (decoupled submit/collect) instead of per-job clients.
func NewSharedClientResolver(pools map[string]*asyncPool, logger logr.Logger) map[string]AsyncInferenceClient {
	clients := make(map[string]AsyncInferenceClient)
	for name, pool := range pools {
		clients[name] = newAsyncSharedClient(pool, defaultResultBufferSize, logger.WithValues("pool", name))
	}
	return clients
}
