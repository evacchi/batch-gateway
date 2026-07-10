package pipeline

import (
	"context"
	"net/http"
	"sync"

	"github.com/go-logr/logr"

	"github.com/llm-d/llm-d-batch-gateway/internal/processor/metrics"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/semaphore"
)

// EndpointAIMD pairs an adaptive semaphore with its AIMD controller
// for a single inference endpoint.
type EndpointAIMD struct {
	Sem   *semaphore.AdaptiveSemaphore
	AIMD  *semaphore.AIMDController
	Label string
}

// AIMDDispatcher wraps another dispatcher with per-endpoint adaptive
// semaphores and a global semaphore. Acquires endpoint then global
// (same order as the original processModel) to prevent starvation.
type AIMDDispatcher struct {
	next      RequestDispatcher
	models    map[string]*EndpointAIMD
	globalSem semaphore.Semaphore
	logger    logr.Logger
}

var _ RequestDispatcher = (*AIMDDispatcher)(nil)

func NewAIMDDispatcher(
	next RequestDispatcher,
	models map[string]*EndpointAIMD,
	globalLimit int,
	logger logr.Logger,
) *AIMDDispatcher {
	globalSem, _ := semaphore.New(globalLimit, nil)
	return &AIMDDispatcher{
		next:      next,
		models:    models,
		globalSem: globalSem,
		logger:    logger,
	}
}

func (d *AIMDDispatcher) Run(ctx context.Context, requestCh <-chan RequestItem, resultCh chan<- ResultItem) error {
	delegateRequestCh := make(chan RequestItem)
	delegateResultCh := make(chan ResultItem)

	// start the next dispatcher in the chain
	go func() {
		_ = d.next.Run(ctx, delegateRequestCh, delegateResultCh)
	}()

	// forward results concurrently: release semaphores, record AIMD, send to resultCh
	var resultWg sync.WaitGroup
	resultWg.Add(1)
	go func() {
		defer resultWg.Done()
		for result := range delegateResultCh {
			ep := d.models[result.ModelID]
			if ep != nil {
				ep.Sem.Release()
				recordAIMDSignal(ep, result)
			}
			d.globalSem.Release()
			resultCh <- result
		}
	}()

	// feed delegateRequestCh from requestCh with semaphore gating
	for msg := range requestCh {
		if err := d.acquireSlot(ctx, msg.ModelID); err != nil {
			resultCh <- *msg.Canceled()
			break
		}
		delegateRequestCh <- msg
	}

	// drain the remaining messages as canceled
	for msg := range requestCh {
		resultCh <- *msg.Canceled()
	}

	close(delegateRequestCh)

	// wait for all results to be forwarded
	resultWg.Wait()
	close(resultCh)
	return nil
}

func (d *AIMDDispatcher) Receive(_ context.Context, _ RequestItem, _ chan<- ResultItem) {
	panic("AIMDDispatcher.Receive should not be called directly; use Run")
}

func (d *AIMDDispatcher) Close() {}

func (d *AIMDDispatcher) acquireSlot(ctx context.Context, modelID string) error {
	ep := d.models[modelID]
	if ep != nil {
		if err := ep.Sem.Acquire(ctx); err != nil {
			return err
		}
	}
	if err := d.globalSem.Acquire(ctx); err != nil {
		if ep != nil {
			ep.Sem.Release()
		}
		return err
	}
	return nil
}

func recordAIMDSignal(ep *EndpointAIMD, result ResultItem) {
	if result.Response == nil {
		return
	}

	sc := result.Response.StatusCode

	switch {
	case sc == http.StatusTooManyRequests:
		ep.AIMD.RecordRateLimit(metrics.AIMDSignal429)
		metrics.RecordAIMDDecrease(ep.Label, metrics.AIMDSignal429)
	case sc >= http.StatusInternalServerError:
		ep.AIMD.RecordRateLimit(metrics.AIMDSignal5xx)
		metrics.RecordAIMDDecrease(ep.Label, metrics.AIMDSignal5xx)
	case result.HadCapacityRetry:
		ep.AIMD.RecordRateLimit(metrics.AIMDSignalCapacityRetry)
		metrics.RecordAIMDDecrease(ep.Label, metrics.AIMDSignalCapacityRetry)
	default:
		oldLimit := ep.AIMD.Limit()
		ep.AIMD.RecordSuccess()
		if ep.AIMD.Limit() != oldLimit {
			metrics.RecordAIMDIncrease(ep.Label)
		}
	}

	metrics.SetAIMDConcurrencyLimit(ep.Label, float64(ep.AIMD.Limit()))
}
