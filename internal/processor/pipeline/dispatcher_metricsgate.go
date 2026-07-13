package pipeline

import (
	"context"
	"sync"
	"time"

	"github.com/go-logr/logr"

	"github.com/llm-d/llm-d-batch-gateway/internal/util/logging"
)

// DispatchBudget represents available capacity as a value in [0, 1].
// 0 = fully saturated (gate closed), 1 = fully idle (gate open).
type DispatchBudget float64

// BudgetSource reads the current dispatch budget.
// Implementations may query Prometheus, scrape endpoints, read from Redis, etc.
type BudgetSource interface {
	Budget(ctx context.Context) (DispatchBudget, error)
}

// MetricsGateDispatcher gates requests based on a dispatch budget.
// When the budget is above the baseline, requests are forwarded to the
// next dispatcher. When at or below the baseline, the gate closes and
// polls periodically until capacity is available.
type MetricsGateDispatcher struct {
	next         RequestDispatcher
	source       BudgetSource
	baseline     float64
	pollInterval time.Duration
	logger       logr.Logger
}

var _ RequestDispatcher = (*MetricsGateDispatcher)(nil)

func NewMetricsGateDispatcher(
	next RequestDispatcher,
	source BudgetSource,
	baseline float64,
	pollInterval time.Duration,
	logger logr.Logger,
) *MetricsGateDispatcher {
	return &MetricsGateDispatcher{
		next:         next,
		source:       source,
		baseline:     baseline,
		pollInterval: pollInterval,
		logger:       logger,
	}
}

func MetricsGateDispatcherStage(
	source BudgetSource,
	baseline float64,
	pollInterval time.Duration,
	logger logr.Logger,
) ResultDispatcherStage {
	return func(next RequestDispatcher) RequestDispatcher {
		return NewMetricsGateDispatcher(next, source, baseline, pollInterval, logger)
	}
}

func (d *MetricsGateDispatcher) Run(ctx context.Context, requestCh <-chan RequestItem, resultCh chan<- ResultItem) error {
	delegateRequestCh := make(chan RequestItem)
	delegateResultCh := make(chan ResultItem)

	var innerWg sync.WaitGroup
	var innerErr error
	innerWg.Add(1)
	go func() {
		defer innerWg.Done()
		innerErr = d.next.Run(ctx, delegateRequestCh, delegateResultCh)
	}()

	var resultWg sync.WaitGroup
	resultWg.Add(1)
	go func() {
		defer resultWg.Done()
		for result := range delegateResultCh {
			resultCh <- result
		}
	}()

	// Forward requests while budget is available.
	for msg := range requestCh {
		if err := d.waitForBudget(ctx); err != nil {
			resultCh <- *msg.Error(cancelCode(ctx))
			break
		}
		delegateRequestCh <- msg
	}
	// If the loop broke early, drain remaining requests.
	// If it completed normally (channel closed), this is a no-op.
	for msg := range requestCh {
		resultCh <- *msg.Error(cancelCode(ctx))
	}
	close(delegateRequestCh)

	innerWg.Wait()
	resultWg.Wait()
	close(resultCh)
	return innerErr
}

// waitForBudget blocks until the budget exceeds the baseline or ctx is cancelled.
func (d *MetricsGateDispatcher) waitForBudget(ctx context.Context) error {
	for {
		budget, err := d.source.Budget(ctx)
		if err != nil {
			d.logger.Error(err, "Failed to read budget, allowing request through")
			return nil
		}
		if float64(budget) > d.baseline {
			return nil
		}

		d.logger.V(logging.INFO).Info("Gate closed, waiting for capacity",
			"budget", float64(budget), "baseline", d.baseline)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(d.pollInterval):
		}
	}
}
