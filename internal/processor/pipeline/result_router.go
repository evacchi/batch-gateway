package pipeline

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/go-logr/logr"

	batch_types "github.com/llm-d/llm-d-batch-gateway/internal/shared/types"
	"github.com/llm-d/llm-d-batch-gateway/pkg/clients/inference"
)

const defaultResultBuffer = 64

// ResultBroadcaster reads results from a shared async queue and
// broadcasts to all subscribed channels. Actor pattern — single
// goroutine owns the subscriber list, subscribe/unsubscribe via channels.
type ResultBroadcaster struct {
	client        inference.AsyncInferenceClient
	subscribeCh   chan chan<- ResultItem
	unsubscribeCh chan chan<- ResultItem
	logger        logr.Logger
}

func NewResultBroadcaster(client inference.AsyncInferenceClient, logger logr.Logger) *ResultBroadcaster {
	return &ResultBroadcaster{
		client:        client,
		subscribeCh:   make(chan chan<- ResultItem),
		unsubscribeCh: make(chan chan<- ResultItem),
		logger:        logger,
	}
}

// Subscribe registers dest to receive all results.
// dest should be buffered to avoid blocking the broadcaster.
func (b *ResultBroadcaster) Subscribe(dest chan<- ResultItem) {
	b.subscribeCh <- dest
}

// Unsubscribe removes dest from the broadcast list.
func (b *ResultBroadcaster) Unsubscribe(dest chan<- ResultItem) {
	b.unsubscribeCh <- dest
}

// Run reads results and broadcasts to all subscribers.
func (b *ResultBroadcaster) Run(ctx context.Context) {
	var subscribers []chan<- ResultItem

	type resultMsg struct {
		resp *inference.GenerateResponse
		err  error
	}
	incomingCh := make(chan resultMsg)

	go func() {
		defer close(incomingCh)
		for {
			resp, err := b.client.GetResult(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				incomingCh <- resultMsg{err: err}
				return
			}
			incomingCh <- resultMsg{resp: resp}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return

		case dest := <-b.subscribeCh:
			subscribers = append(subscribers, dest)

		case dest := <-b.unsubscribeCh:
			for i, ch := range subscribers {
				if ch == dest {
					subscribers = append(subscribers[:i], subscribers[i+1:]...)
					break
				}
			}

		case msg, ok := <-incomingCh:
			if !ok {
				return
			}
			if msg.err != nil {
				b.logger.Error(msg.err, "GetResult failed, stopping broadcaster")
				return
			}
			result := asyncResult(msg.resp, b.logger)
			for _, ch := range subscribers {
				ch <- result
			}
		}
	}
}

func asyncResult(resp *inference.GenerateResponse, logger logr.Logger) ResultItem {
	result := ResultItem{
		RequestID: resp.RequestID,
	}
	if resp.Response == nil {
		result.Error = &OutputError{Code: "server_error", Message: "async response has no body"}
		return result
	}
	var body map[string]any
	if err := json.Unmarshal(resp.Response, &body); err != nil {
		logger.Error(err, "Failed to unmarshal async response", "requestID", resp.RequestID)
		result.Error = &OutputError{
			Code:    "parse_error",
			Message: fmt.Sprintf("response body could not be parsed: %v", err),
		}
		return result
	}
	result.Response = &batch_types.ResponseData{
		StatusCode: 200,
		RequestID:  resp.RequestID,
		Body:       body,
	}
	return result
}
