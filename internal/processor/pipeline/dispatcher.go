package pipeline

import "context"

// RequestDispatcher handles a request — either by processing it directly
// (e.g. calling inference) or by delegating to another dispatcher
// (e.g. routing, microbatching). Composable as a chain.
//
// Receive is always asynchronous — it may return before the result
// is sent to out. Close waits for all in-flight work to complete.
type RequestDispatcher interface {
	Run(ctx context.Context, requestCh <-chan RequestItem, resultCh chan<- ResultItem) error
}
