package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/go-logr/logr"

	"github.com/llm-d/llm-d-batch-gateway/pkg/clients/inference"
)

type mockInferenceClient struct {
	response []byte
}

func (m *mockInferenceClient) Generate(_ context.Context, req *inference.GenerateRequest) (*inference.GenerateResponse, *inference.ClientError) {
	return &inference.GenerateResponse{
		RequestID: req.RequestID,
		Response:  m.response,
	}, nil
}

type sliceSource struct {
	items []RequestItem
}

func (s *sliceSource) Produce(_ context.Context, out chan<- RequestItem) error {
	defer close(out)
	for _, item := range s.items {
		out <- item
	}
	return nil
}

func TestJobExecutorEndToEnd(t *testing.T) {
	body := map[string]any{"choices": []any{}, "usage": map[string]any{"prompt_tokens": 10.0, "completion_tokens": 5.0}}
	respBytes, _ := json.Marshal(body)

	client := &mockInferenceClient{response: respBytes}
	resolver := inference.NewSingleClientResolver(client)
	defer func() { _ = resolver.Close() }()

	items := []RequestItem{
		{RequestID: "req-1", CustomID: "c-1", ModelID: "m1", Endpoint: "/v1/chat/completions"},
		{RequestID: "req-2", CustomID: "c-2", ModelID: "m1", Endpoint: "/v1/chat/completions"},
		{RequestID: "req-3", CustomID: "c-3", ModelID: "m1", Endpoint: "/v1/chat/completions"},
	}

	outputFile := tempFile(t)
	errorFile := tempFile(t)
	pending := &PendingRequests{}
	tracker := NewProgressTracker(int64(len(items)), nil, "test-job", logr.Discard())
	collector := NewResultCollector(outputFile, errorFile, pending, tracker, logr.Discard())

	executor := NewJobExecutor(JobExecutorConfig{
		Source:     &sliceSource{items: items},
		Dispatcher: NewDirectDispatcher(resolver, pending, logr.Discard()),
		Collector:  collector,
		Tracker:    tracker,
		Logger:     logr.Discard(),
	})

	counts, err := executor.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute() error: %v", err)
	}

	if counts.Total != 3 {
		t.Errorf("Total = %d, want 3", counts.Total)
	}
	if counts.Completed != 3 {
		t.Errorf("Completed = %d, want 3", counts.Completed)
	}
	if counts.Failed != 0 {
		t.Errorf("Failed = %d, want 0", counts.Failed)
	}

	outputData := readFile(t, outputFile)
	lines := bytes.Split(bytes.TrimSpace(outputData), []byte("\n"))
	if len(lines) != 3 {
		t.Fatalf("output lines = %d, want 3", len(lines))
	}

	for i, line := range lines {
		var out outputLine
		if err := json.Unmarshal(line, &out); err != nil {
			t.Fatalf("line %d: unmarshal error: %v", i, err)
		}
		if out.Response == nil {
			t.Errorf("line %d: response is nil", i)
		} else if out.Response.StatusCode != 200 {
			t.Errorf("line %d: status = %d, want 200", i, out.Response.StatusCode)
		}
	}

	errorData := readFile(t, errorFile)
	if len(bytes.TrimSpace(errorData)) != 0 {
		t.Errorf("error output = %q, want empty", errorData)
	}
}

func TestJobExecutorWithErrors(t *testing.T) {
	resolver := inference.NewPerModelClientResolver(map[string]inference.InferenceClient{
		"m1": &mockInferenceClient{response: []byte(`{}`)},
	})
	defer resolver.Close()

	items := []RequestItem{
		{RequestID: "req-1", CustomID: "c-1", ModelID: "m1", Endpoint: "/v1/chat/completions"},
		{RequestID: "req-bad", CustomID: "c-bad", ModelID: "no-such-model", Endpoint: "/v1/chat/completions"},
	}

	outputFile := tempFile(t)
	errorFile := tempFile(t)
	pending := &PendingRequests{}
	tracker := NewProgressTracker(int64(len(items)), nil, "test-job", logr.Discard())
	collector := NewResultCollector(outputFile, errorFile, pending, tracker, logr.Discard())

	executor := NewJobExecutor(JobExecutorConfig{
		Source:     &sliceSource{items: items},
		Dispatcher: NewDirectDispatcher(resolver, pending, logr.Discard()),
		Collector:  collector,
		Tracker:    tracker,
		Logger:     logr.Discard(),
	})

	counts, err := executor.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute() error: %v", err)
	}

	if counts.Completed+counts.Failed != int64(len(items)) {
		t.Errorf("Completed(%d) + Failed(%d) != Total(%d)", counts.Completed, counts.Failed, counts.Total)
	}

	errorData := readFile(t, errorFile)
	if len(bytes.TrimSpace(errorData)) == 0 {
		t.Error("expected error output for unknown model")
	}

	var errLine outputLine
	if err := json.Unmarshal(bytes.TrimSpace(errorData), &errLine); err != nil {
		t.Fatalf("unmarshal error line: %v", err)
	}
	if errLine.Error == nil {
		t.Fatal("expected error field in error output")
	}
	if errLine.Error.Code != inference.ErrCodeModelNotFound {
		t.Errorf("error code = %q, want %q", errLine.Error.Code, inference.ErrCodeModelNotFound)
	}
}

func TestJobExecutorCancellation(t *testing.T) {
	resolver := inference.NewSingleClientResolver(&mockInferenceClient{response: []byte(`{}`)})
	defer resolver.Close()

	ctx, cancel := context.WithCancel(context.Background())

	source := &cancellingSource{
		items:    []RequestItem{{RequestID: "req-1", CustomID: "c-1", ModelID: "m1"}},
		cancelFn: cancel,
	}

	outputFile := tempFile(t)
	errorFile := tempFile(t)
	pending := &PendingRequests{}
	tracker := NewProgressTracker(10, nil, "test-job", logr.Discard())
	collector := NewResultCollector(outputFile, errorFile, pending, tracker, logr.Discard())

	executor := NewJobExecutor(JobExecutorConfig{
		Source:     source,
		Dispatcher: NewDirectDispatcher(resolver, pending, logr.Discard()),
		Collector:  collector,
		Tracker:    tracker,
		Logger:     logr.Discard(),
	})

	_, err := executor.Execute(ctx)
	if err != nil && err != context.Canceled {
		t.Fatalf("Execute() error: %v", err)
	}
}

// cancellingSource sends one item then cancels the context.
type cancellingSource struct {
	items    []RequestItem
	cancelFn context.CancelFunc
}

func (s *cancellingSource) Produce(_ context.Context, out chan<- RequestItem) error {
	defer close(out)
	for _, item := range s.items {
		out <- item
	}
	s.cancelFn()
	return nil
}

var _ inference.InferenceClient = (*mockInferenceClient)(nil)
var _ RequestSource = (*sliceSource)(nil)
