package pipeline

import "context"

// RequestDispatcher handles a request — either by processing it directly
// (e.g. calling inference) or by delegating to another dispatcher
// (e.g. routing, microbatching). Composable as a chain.
type RequestDispatcher interface {
	Run(ctx context.Context, requestCh <-chan RequestItem, resultCh chan<- ResultItem) error
}
