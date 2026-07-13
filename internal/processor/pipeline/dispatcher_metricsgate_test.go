package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/go-logr/logr"

	"github.com/llm-d/llm-d-batch-gateway/pkg/clients/inference"
)

type fixedBudgetSource struct {
	budget DispatchBudget
}

func (f *fixedBudgetSource) Budget(_ context.Context) (DispatchBudget, error) {
	return f.budget, nil
}

type errorBudgetSource struct{}

func (e *errorBudgetSource) Budget(_ context.Context) (DispatchBudget, error) {
	return 0, errors.New("budget read failed")
}

type sequenceBudgetSource struct {
	values []DispatchBudget
	idx    int
}

func (s *sequenceBudgetSource) Budget(_ context.Context) (DispatchBudget, error) {
	if s.idx < len(s.values) {
		v := s.values[s.idx]
		s.idx++
		return v, nil
	}
	return s.values[len(s.values)-1], nil
}

func TestMetricsGateDispatcher_BudgetAboveBaseline(t *testing.T) {
	client := &mockInferenceClient{response: []byte(`{"ok":true}`)}
	resolver := inference.NewSingleClientResolver(client)
	defer func() { _ = resolver.Close() }()

	direct := NewDirectDispatcher(resolver, logr.Discard())

	budget := &fixedBudgetSource{budget: 0.8}
	gate := NewMetricsGateDispatcher(direct, budget, 0.5, 50*time.Millisecond, logr.Discard())

	items := makeItems(3, "m1")

	outputFile := tempFile(t)
	errorFile := tempFile(t)
	tracker := NewProgressTracker(int64(len(items)), nil, "test-job", 0, logr.Discard())
	collector := NewResultCollector(outputFile, errorFile, NewPendingRequests(int64(len(items))), tracker, logr.Discard())

	executor := NewJobExecutor(JobExecutorConfig{
		Source:     &sliceSource{items: items},
		Dispatcher: gate,
		Collector:  collector,
		Tracker:    tracker,
		Logger:     logr.Discard(),
	})

	counts, err := executor.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute() error: %v", err)
	}
	if counts.Completed != 3 {
		t.Errorf("Completed = %d, want 3", counts.Completed)
	}

	outputData := readFile(t, outputFile)
	lines := splitLines(outputData)
	if len(lines) != 3 {
		t.Errorf("output lines = %d, want 3", len(lines))
	}
}

func TestMetricsGateDispatcher_BudgetBelowBaseline_Cancelled(t *testing.T) {
	client := &mockInferenceClient{response: []byte(`{"ok":true}`)}
	resolver := inference.NewSingleClientResolver(client)
	defer func() { _ = resolver.Close() }()

	direct := NewDirectDispatcher(resolver, logr.Discard())

	budget := &fixedBudgetSource{budget: 0.1}
	gate := NewMetricsGateDispatcher(direct, budget, 0.5, 10*time.Millisecond, logr.Discard())

	items := makeItems(3, "m1")

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(100*time.Millisecond, cancel)

	outputFile := tempFile(t)
	errorFile := tempFile(t)
	tracker := NewProgressTracker(int64(len(items)), nil, "test-job", 0, logr.Discard())
	collector := NewResultCollector(outputFile, errorFile, NewPendingRequests(int64(len(items))), tracker, logr.Discard())

	executor := NewJobExecutor(JobExecutorConfig{
		Source:     &sliceSource{items: items},
		Dispatcher: gate,
		Collector:  collector,
		Tracker:    tracker,
		Logger:     logr.Discard(),
	})

	_, err := executor.Execute(ctx)
	_ = err

	errorData := readFile(t, errorFile)
	errorLines := splitLines(errorData)
	if len(errorLines) == 0 {
		t.Error("expected at least one cancelled request when budget is below baseline")
	}
	for _, line := range errorLines {
		var entry outputLine
		if err := json.Unmarshal(line, &entry); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if entry.Error == nil || entry.Error.Code != "batch_cancelled" {
			t.Errorf("expected batch_cancelled, got %+v", entry.Error)
		}
	}
}

func TestMetricsGateDispatcher_BudgetReadError_AllowsThrough(t *testing.T) {
	client := &mockInferenceClient{response: []byte(`{"ok":true}`)}
	resolver := inference.NewSingleClientResolver(client)
	defer func() { _ = resolver.Close() }()

	direct := NewDirectDispatcher(resolver, logr.Discard())

	budget := &errorBudgetSource{}
	gate := NewMetricsGateDispatcher(direct, budget, 0.5, 10*time.Millisecond, logr.Discard())

	items := makeItems(2, "m1")

	outputFile := tempFile(t)
	errorFile := tempFile(t)
	tracker := NewProgressTracker(int64(len(items)), nil, "test-job", 0, logr.Discard())
	collector := NewResultCollector(outputFile, errorFile, NewPendingRequests(int64(len(items))), tracker, logr.Discard())

	executor := NewJobExecutor(JobExecutorConfig{
		Source:     &sliceSource{items: items},
		Dispatcher: gate,
		Collector:  collector,
		Tracker:    tracker,
		Logger:     logr.Discard(),
	})

	counts, err := executor.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute() error: %v", err)
	}
	if counts.Completed != 2 {
		t.Errorf("Completed = %d, want 2 (budget read errors should allow requests through)", counts.Completed)
	}
}

func TestMetricsGateDispatcher_BudgetRecovery(t *testing.T) {
	client := &mockInferenceClient{response: []byte(`{"ok":true}`)}
	resolver := inference.NewSingleClientResolver(client)
	defer func() { _ = resolver.Close() }()

	direct := NewDirectDispatcher(resolver, logr.Discard())

	budget := &sequenceBudgetSource{
		values: []DispatchBudget{0.8, 0.8, 0.8},
	}
	gate := NewMetricsGateDispatcher(direct, budget, 0.5, 10*time.Millisecond, logr.Discard())

	items := makeItems(3, "m1")

	outputFile := tempFile(t)
	errorFile := tempFile(t)
	tracker := NewProgressTracker(int64(len(items)), nil, "test-job", 0, logr.Discard())
	collector := NewResultCollector(outputFile, errorFile, NewPendingRequests(int64(len(items))), tracker, logr.Discard())

	executor := NewJobExecutor(JobExecutorConfig{
		Source:     &sliceSource{items: items},
		Dispatcher: gate,
		Collector:  collector,
		Tracker:    tracker,
		Logger:     logr.Discard(),
	})

	counts, err := executor.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute() error: %v", err)
	}

	outputData := readFile(t, outputFile)
	lines := bytes.Split(bytes.TrimSpace(outputData), []byte("\n"))
	if int64(len(lines)) != counts.Completed {
		t.Errorf("output lines = %d, completed = %d", len(lines), counts.Completed)
	}
}
