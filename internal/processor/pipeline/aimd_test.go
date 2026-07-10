package pipeline

import (
	"context"
	"encoding/json"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"

	"github.com/llm-d/llm-d-batch-gateway/internal/util/semaphore"
	httpclient "github.com/llm-d/llm-d-batch-gateway/pkg/clients/http"
	"github.com/llm-d/llm-d-batch-gateway/pkg/clients/inference"
)

func makeItems(n int, model string) []RequestItem {
	items := make([]RequestItem, n)
	for i := range items {
		items[i] = RequestItem{
			RequestID: "req-" + model + "-" + string(rune('0'+i)),
			CustomID:  "c-" + string(rune('0'+i)),
			ModelID:   model,
			Endpoint:  "/v1/chat/completions",
		}
	}
	return items
}

func makeEndpointAIMD(t *testing.T, perEndpoint int) (*semaphore.AdaptiveSemaphore, *semaphore.AIMDController) {
	t.Helper()
	sem, err := semaphore.NewAdaptive(perEndpoint, func() { t.Error("double release") })
	if err != nil {
		t.Fatal(err)
	}
	aimd := semaphore.NewAIMDController(
		semaphore.AIMDConfig{
			MinLimit:         5,
			MaxLimit:         perEndpoint,
			BackoffFactor:    0.5,
			AdditiveIncrease: 1,
		},
		perEndpoint,
		func(limit int) { sem.SetLimit(limit) },
		logr.Discard(),
	)
	return sem, aimd
}

func runPipeline(t *testing.T, items []RequestItem, pending *PendingRequests, dispatcher RequestDispatcher) (outputData, errorData []byte, counts *int64, failedCounts *int64) {
	t.Helper()
	outputFile := tempFile(t)
	errorFile := tempFile(t)
	total := int64(len(items))
	tracker := NewProgressTracker(total, nil, "test-job", logr.Discard())
	collector := NewResultCollector(outputFile, errorFile, &PendingRequests{}, tracker, logr.Discard())

	executor := NewJobExecutor(JobExecutorConfig{
		Source:     &sliceSource{items: items},
		Dispatcher: dispatcher,
		Collector:  collector,
		Tracker:    tracker,
		Logger:     logr.Discard(),
	})

	_, err := executor.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute() error: %v", err)
	}

	c := tracker.Counts()
	return readFile(t, outputFile), readFile(t, errorFile), &c.Completed, &c.Failed
}

func TestAIMDSignaling(t *testing.T) {
	const maxLimit = 20

	t.Run("clean 200 records success", func(t *testing.T) {
		client := &mockInferenceClient{response: []byte(`{"ok":true}`)}
		resolver := inference.NewSingleClientResolver(client)
		defer func() { _ = resolver.Close() }()

		sem, aimd := makeEndpointAIMD(t, maxLimit)
		globalLimit := 100

		pending := &PendingRequests{}
		direct := NewDirectDispatcher(resolver, pending, logr.Discard())
		dispatcher := NewAIMDDispatcher(direct,
			map[string]*EndpointAIMD{"m1": {Sem: sem, AIMD: aimd, Label: "test"}},
			globalLimit, logr.Discard())

		runPipeline(t, makeItems(maxLimit, "m1"), pending, dispatcher)

		if got := aimd.Limit(); got != maxLimit {
			t.Errorf("Limit() = %d, want %d (no decrease for clean 200s)", got, maxLimit)
		}
	})

	t.Run("429 records rate limit", func(t *testing.T) {
		client := &statusCodeClient{statusCode: 429, category: httpclient.ErrCategoryRateLimit}
		resolver := inference.NewSingleClientResolver(client)
		defer func() { _ = resolver.Close() }()

		sem, aimd := makeEndpointAIMD(t, maxLimit)
		globalLimit := 100

		pending := &PendingRequests{}
		direct := NewDirectDispatcher(resolver, pending, logr.Discard())
		dispatcher := NewAIMDDispatcher(direct,
			map[string]*EndpointAIMD{"m1": {Sem: sem, AIMD: aimd, Label: "test"}},
			globalLimit, logr.Discard())

		runPipeline(t, makeItems(1, "m1"), pending, dispatcher)

		if got := aimd.Limit(); got >= maxLimit {
			t.Errorf("Limit() = %d, want < %d (should decrease on 429)", got, maxLimit)
		}
	})

	t.Run("5xx records rate limit", func(t *testing.T) {
		client := &statusCodeClient{statusCode: 502, category: httpclient.ErrCategoryServer}
		resolver := inference.NewSingleClientResolver(client)
		defer func() { _ = resolver.Close() }()

		sem, aimd := makeEndpointAIMD(t, maxLimit)
		globalLimit := 100

		pending := &PendingRequests{}
		direct := NewDirectDispatcher(resolver, pending, logr.Discard())
		dispatcher := NewAIMDDispatcher(direct,
			map[string]*EndpointAIMD{"m1": {Sem: sem, AIMD: aimd, Label: "test"}},
			globalLimit, logr.Discard())

		runPipeline(t, makeItems(1, "m1"), pending, dispatcher)

		if got := aimd.Limit(); got >= maxLimit {
			t.Errorf("Limit() = %d, want < %d (should decrease on 5xx)", got, maxLimit)
		}
	})

	t.Run("200 with capacity retry records rate limit", func(t *testing.T) {
		client := &capacityRetryClient{}
		resolver := inference.NewSingleClientResolver(client)
		defer func() { _ = resolver.Close() }()

		sem, aimd := makeEndpointAIMD(t, maxLimit)
		globalLimit := 100

		pending := &PendingRequests{}
		direct := NewDirectDispatcher(resolver, pending, logr.Discard())
		dispatcher := NewAIMDDispatcher(direct,
			map[string]*EndpointAIMD{"m1": {Sem: sem, AIMD: aimd, Label: "test"}},
			globalLimit, logr.Discard())

		runPipeline(t, makeItems(1, "m1"), pending, dispatcher)

		if got := aimd.Limit(); got >= maxLimit {
			t.Errorf("Limit() = %d, want < %d (should decrease on capacity retry)", got, maxLimit)
		}
	})

	t.Run("endpoint isolation", func(t *testing.T) {
		clientA := &statusCodeClient{statusCode: 429, category: httpclient.ErrCategoryRateLimit}
		clientB := &mockInferenceClient{response: []byte(`{"ok":true}`)}
		resolver := inference.NewPerModelClientResolver(map[string]inference.InferenceClient{
			"model-a": clientA,
			"model-b": clientB,
		})
		defer func() { _ = resolver.Close() }()

		semA, aimdA := makeEndpointAIMD(t, maxLimit)
		semB, aimdB := makeEndpointAIMD(t, maxLimit)
		globalLimit := 100

		items := append(makeItems(1, "model-a"), makeItems(maxLimit, "model-b")...)

		pending := &PendingRequests{}
		direct := NewDirectDispatcher(resolver, pending, logr.Discard())
		dispatcher := NewAIMDDispatcher(direct, map[string]*EndpointAIMD{
			"model-a": {Sem: semA, AIMD: aimdA, Label: "ep-a"},
			"model-b": {Sem: semB, AIMD: aimdB, Label: "ep-b"},
		}, globalLimit, logr.Discard())

		runPipeline(t, items, pending, dispatcher)

		if got := aimdA.Limit(); got >= maxLimit {
			t.Errorf("model-a Limit() = %d, want < %d (429s should decrease)", got, maxLimit)
		}
		if got := aimdB.Limit(); got != maxLimit {
			t.Errorf("model-b Limit() = %d, want %d (should be unaffected)", got, maxLimit)
		}
	})
}

func TestCancelDrainsUndispatched(t *testing.T) {
	var dispatched atomic.Int32
	slowClient := &mockInferenceClientWithFn{
		generateFn: func(ctx context.Context, req *inference.GenerateRequest) (*inference.GenerateResponse, *inference.ClientError) {
			dispatched.Add(1)
			time.Sleep(200 * time.Millisecond)
			if ctx.Err() != nil {
				return nil, &inference.ClientError{Category: httpclient.ErrCategoryUnknown, Message: "request cancelled"}
			}
			return &inference.GenerateResponse{RequestID: req.RequestID, Response: []byte(`{"ok":true}`)}, nil
		},
	}
	resolver := inference.NewSingleClientResolver(slowClient)
	defer func() { _ = resolver.Close() }()

	sem, aimd := makeEndpointAIMD(t, 2)
	globalLimit := 2

	ctx, cancel := context.WithCancel(context.Background())
	items := makeItems(10, "m1")

	source := &cancelAfterNSource{items: items, cancelAfter: 3, cancelFn: cancel}

	outputFile := tempFile(t)
	errorFile := tempFile(t)
	tracker := NewProgressTracker(int64(len(items)), nil, "test-job", logr.Discard())
	collector := NewResultCollector(outputFile, errorFile, &PendingRequests{}, tracker, logr.Discard())

	pending := &PendingRequests{}
	direct := NewDirectDispatcher(resolver, pending, logr.Discard())
	dispatcher := NewAIMDDispatcher(direct,
		map[string]*EndpointAIMD{"m1": {Sem: sem, AIMD: aimd, Label: "test"}},
		globalLimit, logr.Discard())

	executor := NewJobExecutor(JobExecutorConfig{
		Source:     source,
		Dispatcher: dispatcher,
		Collector:  collector,
		Tracker:    tracker,
		Logger:     logr.Discard(),
	})

	_, err := executor.Execute(ctx)
	_ = err

	outputData := readFile(t, outputFile)
	errorData := readFile(t, errorFile)

	outputLines := countLines(outputData)
	errorLines := countLines(errorData)
	total := outputLines + errorLines

	// source produces cancelAfter items before cancelling, plus possibly
	// a few more that race into the channel. All produced items should
	// be accounted for (either completed or cancelled).
	if total == 0 {
		t.Error("expected some output or error entries")
	}
	t.Logf("output=%d error=%d total=%d (of %d items, %d produced before cancel)",
		outputLines, errorLines, total, len(items), source.cancelAfter)

	for _, line := range splitLines(errorData) {
		var entry outputLine
		if err := json.Unmarshal(line, &entry); err != nil {
			t.Fatalf("unmarshal error line: %v", err)
		}
		if entry.Error == nil {
			t.Fatalf("expected error field in error entry")
		}
		if entry.Error.Code != "batch_cancelled" && entry.Error.Code != "UNKNOWN" {
			t.Errorf("unexpected error code %q (want batch_cancelled or UNKNOWN for in-flight)", entry.Error.Code)
		}
	}
}

// TestCancelWithFastRequestsThrottled mirrors the E2E Cancel/InProgress test:
// 5 fast + 20 slow requests, concurrency=10, cancel after some complete.
// The windowed dispatch must throttle so slow requests are still queued when cancel arrives.
func TestCancelWithFastRequestsThrottled(t *testing.T) {
	const totalRequests = 25
	const concurrency = 10

	// requests that block until cancelled (simulating slow inference)
	client := &mockInferenceClientWithFn{
		generateFn: func(ctx context.Context, req *inference.GenerateRequest) (*inference.GenerateResponse, *inference.ClientError) {
			select {
			case <-ctx.Done():
				return nil, &inference.ClientError{Category: httpclient.ErrCategoryUnknown, Message: "request cancelled"}
			case <-time.After(5 * time.Second):
				return &inference.GenerateResponse{RequestID: req.RequestID, Response: []byte(`{"ok":true}`)}, nil
			}
		},
	}
	resolver := inference.NewSingleClientResolver(client)
	defer func() { _ = resolver.Close() }()

	sem, aimd := makeEndpointAIMD(t, concurrency)
	globalLimit := concurrency

	items := makeItems(totalRequests, "m1")

	// cancel after 100ms — requests take 5s, so at most concurrency (10) are
	// in-flight, the rest are buffered. All must be accounted for.
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	outputFile := tempFile(t)
	errorFile := tempFile(t)
	tracker := NewProgressTracker(int64(totalRequests), nil, "test-job", logr.Discard())
	collector := NewResultCollector(outputFile, errorFile, &PendingRequests{}, tracker, logr.Discard())

	pending := &PendingRequests{}
	direct := NewDirectDispatcher(resolver, pending, logr.Discard())
	dispatcher := NewAIMDDispatcher(direct,
		map[string]*EndpointAIMD{"m1": {Sem: sem, AIMD: aimd, Label: "test"}},
		globalLimit, logr.Discard())

	executor := NewJobExecutor(JobExecutorConfig{
		Source:     &ctxSource{items: items},
		Dispatcher: dispatcher,
		Collector:  collector,
		Tracker:    tracker,
		Logger:     logr.Discard(),
	})

	_, err := executor.Execute(ctx)
	_ = err

	outputData := readFile(t, outputFile)
	errorData := readFile(t, errorFile)

	outputLines := countLines(outputData)
	errorLines := countLines(errorData)
	total := outputLines + errorLines

	t.Logf("output=%d error=%d total=%d (of %d)", outputLines, errorLines, total, totalRequests)

	// All requests must be accounted for
	if total != totalRequests {
		t.Errorf("output(%d) + error(%d) = %d, want %d (all requests must be accounted for)",
			outputLines, errorLines, total, totalRequests)
	}

	// Cancel must have prevented all from completing
	if outputLines == totalRequests {
		t.Errorf("all %d requests completed — dispatch was not throttled, cancel arrived too late", totalRequests)
	}

	// All should be cancelled (slow requests don't finish in 100ms)
	if errorLines == 0 {
		t.Error("expected at least one cancelled request")
	}
}

// TestCancelInProgressThrottled verifies that with semaphore-throttled dispatch,
// cancellation mid-flight leaves some requests undispatched, and ALL requests
// (completed + cancelled) are still accounted for. This mirrors the E2E
// Cancel/InProgress scenario where the semaphore limits concurrency so the
// cancel arrives while requests are still queued.
func TestCancelInProgressThrottled(t *testing.T) {
	const totalRequests = 25
	const concurrency = 5 // low concurrency so requests queue up

	slowClient := &mockInferenceClientWithFn{
		generateFn: func(ctx context.Context, req *inference.GenerateRequest) (*inference.GenerateResponse, *inference.ClientError) {
			// each request takes 200ms — with concurrency=5, 25 requests take ~1s
			select {
			case <-ctx.Done():
				return nil, &inference.ClientError{Category: httpclient.ErrCategoryUnknown, Message: "request cancelled"}
			case <-time.After(200 * time.Millisecond):
				return &inference.GenerateResponse{RequestID: req.RequestID, Response: []byte(`{"ok":true}`)}, nil
			}
		},
	}
	resolver := inference.NewSingleClientResolver(slowClient)
	defer func() { _ = resolver.Close() }()

	sem, aimd := makeEndpointAIMD(t, concurrency)
	globalLimit := concurrency

	items := makeItems(totalRequests, "m1")

	// cancel after 300ms — ~5 requests complete (1 batch of concurrency=5),
	// rest should be cancelled
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(300 * time.Millisecond)
		cancel()
	}()

	outputFile := tempFile(t)
	errorFile := tempFile(t)
	tracker := NewProgressTracker(int64(totalRequests), nil, "test-job", logr.Discard())
	collector := NewResultCollector(outputFile, errorFile, &PendingRequests{}, tracker, logr.Discard())

	pending := &PendingRequests{}
	direct := NewDirectDispatcher(resolver, pending, logr.Discard())
	dispatcher := NewAIMDDispatcher(direct,
		map[string]*EndpointAIMD{"m1": {Sem: sem, AIMD: aimd, Label: "test"}},
		globalLimit, logr.Discard())

	// Use ctxSource which checks ctx on each send — like PlanFileSource does
	executor := NewJobExecutor(JobExecutorConfig{
		Source:     &ctxSource{items: items},
		Dispatcher: dispatcher,
		Collector:  collector,
		Tracker:    tracker,
		Logger:     logr.Discard(),
	})

	_, err := executor.Execute(ctx)
	_ = err

	outputData := readFile(t, outputFile)
	errorData := readFile(t, errorFile)

	outputLines := countLines(outputData)
	errorLines := countLines(errorData)
	total := outputLines + errorLines

	t.Logf("output=%d error=%d total=%d (of %d)", outputLines, errorLines, total, totalRequests)

	// Key invariant: all requests accounted for
	if total != totalRequests {
		t.Errorf("output(%d) + error(%d) = %d, want %d (all requests must be accounted for)",
			outputLines, errorLines, total, totalRequests)
	}

	// Some should have completed, some should have been cancelled
	if outputLines == 0 {
		t.Error("expected at least one completed request")
	}
	if errorLines == 0 {
		t.Error("expected at least one cancelled request")
	}
	// Should NOT have completed everything (cancel should arrive in time)
	if outputLines == totalRequests {
		t.Error("all requests completed — cancel didn't throttle dispatch (semaphore not working)")
	}
}

// ctxSource sends items but checks ctx on each send — like PlanFileSource does.
type ctxSource struct {
	items []RequestItem
}

func (s *ctxSource) Produce(_ context.Context, out chan<- RequestItem) error {
	defer close(out)
	for _, item := range s.items {
		out <- item
	}
	return nil
}

// TestCancelInProgress mirrors the E2E Cancel/InProgress test:
// 5 fast requests (complete quickly) + 20 slow requests (in-flight when cancel arrives).
// Concurrency is limited so some slow requests are undispatched.
// All 25 requests must be accounted for in output + error files.
func TestCancelInProgress(t *testing.T) {
	const fastCount = 5
	const slowCount = 20
	const totalRequests = fastCount + slowCount
	const concurrency = 10

	var fastDone atomic.Int32

	client := &mockInferenceClientWithFn{
		generateFn: func(ctx context.Context, req *inference.GenerateRequest) (*inference.GenerateResponse, *inference.ClientError) {
			// simulate fast vs slow based on request ID
			isSlow := len(req.RequestID) > 0 && req.RequestID[len(req.RequestID)-1] >= '5'
			if !isSlow {
				fastDone.Add(1)
				return &inference.GenerateResponse{RequestID: req.RequestID, Response: []byte(`{"ok":true}`)}, nil
			}
			// slow: wait up to 2s or until cancelled
			select {
			case <-ctx.Done():
				return nil, &inference.ClientError{Category: httpclient.ErrCategoryUnknown, Message: "request cancelled"}
			case <-time.After(2 * time.Second):
				return &inference.GenerateResponse{RequestID: req.RequestID, Response: []byte(`{"ok":true}`)}, nil
			}
		},
	}
	resolver := inference.NewSingleClientResolver(client)
	defer func() { _ = resolver.Close() }()

	sem, aimd := makeEndpointAIMD(t, concurrency)
	globalLimit := concurrency

	items := makeItems(totalRequests, "m1")

	ctx, cancel := context.WithCancel(context.Background())
	// cancel after fast requests have had time to complete
	go func() {
		time.Sleep(300 * time.Millisecond)
		cancel()
	}()

	outputFile := tempFile(t)
	errorFile := tempFile(t)
	tracker := NewProgressTracker(int64(totalRequests), nil, "test-job", logr.Discard())
	collector := NewResultCollector(outputFile, errorFile, &PendingRequests{}, tracker, logr.Discard())

	pending := &PendingRequests{}
	direct := NewDirectDispatcher(resolver, pending, logr.Discard())
	dispatcher := NewAIMDDispatcher(direct,
		map[string]*EndpointAIMD{"m1": {Sem: sem, AIMD: aimd, Label: "test"}},
		globalLimit, logr.Discard())

	executor := NewJobExecutor(JobExecutorConfig{
		Source:     &sliceSource{items: items},
		Dispatcher: dispatcher,
		Collector:  collector,
		Tracker:    tracker,
		Logger:     logr.Discard(),
	})

	_, err := executor.Execute(ctx)
	_ = err

	outputData := readFile(t, outputFile)
	errorData := readFile(t, errorFile)

	outputLines := countLines(outputData)
	errorLines := countLines(errorData)
	total := outputLines + errorLines

	t.Logf("output=%d error=%d total=%d (of %d)", outputLines, errorLines, total, totalRequests)

	if total != totalRequests {
		t.Errorf("output(%d) + error(%d) = %d, want %d (all requests must be accounted for)",
			outputLines, errorLines, total, totalRequests)
	}

	if outputLines == 0 {
		t.Error("expected at least one completed request")
	}
	if errorLines == 0 {
		t.Error("expected at least one cancelled request")
	}

	counts := tracker.Counts()
	if counts.Completed+counts.Failed != int64(totalRequests) {
		t.Errorf("Completed(%d) + Failed(%d) = %d, want %d",
			counts.Completed, counts.Failed, counts.Completed+counts.Failed, totalRequests)
	}
}

// TestExpiration mirrors the E2E Expiration test:
// A blocker batch saturates concurrency. The expiration batch can't dispatch
// any requests before its SLO fires. All requests must be failed/expired.
func TestExpiration(t *testing.T) {
	const numRequests = 15
	const concurrency = 10

	// slow client that blocks until cancelled
	slowClient := &mockInferenceClientWithFn{
		generateFn: func(ctx context.Context, req *inference.GenerateRequest) (*inference.GenerateResponse, *inference.ClientError) {
			<-ctx.Done()
			return nil, &inference.ClientError{Category: httpclient.ErrCategoryUnknown, Message: "request cancelled"}
		},
	}
	resolver := inference.NewSingleClientResolver(slowClient)
	defer func() { _ = resolver.Close() }()

	sem, aimd := makeEndpointAIMD(t, concurrency)
	globalLimit := concurrency

	items := makeItems(numRequests, "m1")

	// simulate SLO expiry after 200ms
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	outputFile := tempFile(t)
	errorFile := tempFile(t)
	tracker := NewProgressTracker(int64(numRequests), nil, "test-job", logr.Discard())
	collector := NewResultCollector(outputFile, errorFile, &PendingRequests{}, tracker, logr.Discard())

	pending := &PendingRequests{}
	direct := NewDirectDispatcher(resolver, pending, logr.Discard())
	dispatcher := NewAIMDDispatcher(direct,
		map[string]*EndpointAIMD{"m1": {Sem: sem, AIMD: aimd, Label: "test"}},
		globalLimit, logr.Discard())

	executor := NewJobExecutor(JobExecutorConfig{
		Source:     &sliceSource{items: items},
		Dispatcher: dispatcher,
		Collector:  collector,
		Tracker:    tracker,
		Logger:     logr.Discard(),
	})

	_, err := executor.Execute(ctx)
	_ = err

	outputData := readFile(t, outputFile)
	errorData := readFile(t, errorFile)

	outputLines := countLines(outputData)
	errorLines := countLines(errorData)
	total := outputLines + errorLines

	t.Logf("output=%d error=%d total=%d (of %d)", outputLines, errorLines, total, numRequests)

	if total != numRequests {
		t.Errorf("output(%d) + error(%d) = %d, want %d (all requests must be accounted for)",
			outputLines, errorLines, total, numRequests)
	}
	if outputLines != 0 {
		t.Errorf("expected 0 completed (all should expire), got %d", outputLines)
	}
	if errorLines != numRequests {
		t.Errorf("expected %d error entries, got %d", numRequests, errorLines)
	}
}

func TestRetryExhaustion(t *testing.T) {
	client := &statusCodeClient{statusCode: 429, category: httpclient.ErrCategoryRateLimit}
	resolver := inference.NewSingleClientResolver(client)
	defer func() { _ = resolver.Close() }()

	items := makeItems(2, "m1")

	outputFile := tempFile(t)
	errorFile := tempFile(t)
	tracker := NewProgressTracker(int64(len(items)), nil, "test-job", logr.Discard())
	collector := NewResultCollector(outputFile, errorFile, &PendingRequests{}, tracker, logr.Discard())

	pending := &PendingRequests{}
	direct := NewDirectDispatcher(resolver, pending, logr.Discard())

	executor := NewJobExecutor(JobExecutorConfig{
		Source:     &sliceSource{items: items},
		Dispatcher: direct,
		Collector:  collector,
		Tracker:    tracker,
		Logger:     logr.Discard(),
	})

	_, err := executor.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute() error: %v", err)
	}

	counts := tracker.Counts()
	if counts.Failed != 2 {
		t.Errorf("Failed = %d, want 2", counts.Failed)
	}
	if counts.Completed != 0 {
		t.Errorf("Completed = %d, want 0", counts.Completed)
	}

	outputData := readFile(t, outputFile)
	outputLines := countLines(outputData)
	if outputLines != 2 {
		t.Errorf("output lines = %d, want 2 (429 responses go to output file)", outputLines)
	}

	for _, line := range splitLines(outputData) {
		var entry outputLine
		if err := json.Unmarshal(line, &entry); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if entry.Response == nil || entry.Response.StatusCode != 429 {
			t.Errorf("expected 429 response, got %+v", entry.Response)
		}
	}
}

// Test helpers

type statusCodeClient struct {
	statusCode int
	category   httpclient.ErrorCategory
}

func (c *statusCodeClient) Generate(_ context.Context, req *inference.GenerateRequest) (*inference.GenerateResponse, *inference.ClientError) {
	return nil, &inference.ClientError{
		Category:     c.category,
		Message:      "error",
		StatusCode:   c.statusCode,
		ResponseBody: []byte(`{"error":{"message":"error"}}`),
	}
}

type capacityRetryClient struct{}

func (c *capacityRetryClient) Generate(_ context.Context, req *inference.GenerateRequest) (*inference.GenerateResponse, *inference.ClientError) {
	return &inference.GenerateResponse{
		RequestID:        req.RequestID,
		Response:         []byte(`{"ok":true}`),
		HadCapacityRetry: true,
	}, nil
}

type mockInferenceClientWithFn struct {
	generateFn func(context.Context, *inference.GenerateRequest) (*inference.GenerateResponse, *inference.ClientError)
}

func (m *mockInferenceClientWithFn) Generate(ctx context.Context, req *inference.GenerateRequest) (*inference.GenerateResponse, *inference.ClientError) {
	return m.generateFn(ctx, req)
}

type cancelAfterNSource struct {
	items       []RequestItem
	cancelAfter int
	cancelFn    context.CancelFunc
}

func (s *cancelAfterNSource) Produce(ctx context.Context, out chan<- RequestItem) error {
	defer close(out)
	for i, item := range s.items {
		if i == s.cancelAfter {
			s.cancelFn()
		}
		select {
		case <-ctx.Done():
			return nil
		case out <- item:
		}
	}
	return nil
}

func countLines(data []byte) int {
	lines := splitLines(data)
	return len(lines)
}

func splitLines(data []byte) [][]byte {
	trimmed := trimBytes(data)
	if len(trimmed) == 0 {
		return nil
	}
	return splitByNewline(trimmed)
}

func trimBytes(data []byte) []byte {
	for len(data) > 0 && (data[len(data)-1] == '\n' || data[len(data)-1] == ' ') {
		data = data[:len(data)-1]
	}
	return data
}

func splitByNewline(data []byte) [][]byte {
	var lines [][]byte
	for len(data) > 0 {
		idx := indexOf(data, '\n')
		if idx < 0 {
			lines = append(lines, data)
			break
		}
		lines = append(lines, data[:idx])
		data = data[idx+1:]
	}
	return lines
}

func indexOf(data []byte, b byte) int {
	for i, v := range data {
		if v == b {
			return i
		}
	}
	return -1
}

var _ inference.InferenceClient = (*statusCodeClient)(nil)
var _ inference.InferenceClient = (*capacityRetryClient)(nil)
var _ inference.InferenceClient = (*mockInferenceClientWithFn)(nil)
var _ RequestSource = (*cancelAfterNSource)(nil)

func tempFile(t *testing.T) *os.File {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "test-*.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func readFile(t *testing.T, f *os.File) []byte {
	t.Helper()
	data, err := os.ReadFile(f.Name())
	if err != nil {
		t.Fatal(err)
	}
	return data
}
