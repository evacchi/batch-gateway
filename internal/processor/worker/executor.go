/*
Copyright 2026 The llm-d Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package worker

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	"github.com/google/uuid"

	"github.com/llm-d/llm-d-batch-gateway/internal/processor/metrics"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
	batch_types "github.com/llm-d/llm-d-batch-gateway/internal/shared/types"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/logging"
	httpclient "github.com/llm-d/llm-d-batch-gateway/pkg/clients/http"
	"github.com/llm-d/llm-d-batch-gateway/pkg/clients/inference"
)

// outputWriters holds the buffered writers and their mutexes for the output and error JSONL files.
// A single instance is created per job and shared across model goroutines.
type outputWriters struct {
	output   *bufio.Writer
	outputMu sync.Mutex
	errors   *bufio.Writer
	errorsMu sync.Mutex
}

// write writes line to the error file if isError is true, otherwise to the output file.
func (w *outputWriters) write(line []byte, isError bool) error {
	if isError {
		w.errorsMu.Lock()
		defer w.errorsMu.Unlock()
		_, err := w.errors.Write(line)
		return err
	}
	w.outputMu.Lock()
	defer w.outputMu.Unlock()
	_, err := w.output.Write(line)
	return err
}

// outputLine represents a single line in the output JSONL file following the OpenAI batch output format.
type outputLine struct {
	ID       string                    `json:"id"`
	CustomID string                    `json:"custom_id"`
	Response *batch_types.ResponseData `json:"response"`
	Error    *outputError              `json:"error"`

	// hadCapacityRetry is true when at least one retry was caused by a
	// capacity-related response (429/5xx). Network-error retries do not
	// set this flag. Used for AIMD signaling.
	hadCapacityRetry bool `json:"-"`
}

type outputError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// isSuccess returns true when the output line represents a fully successful request
// (no non-HTTP error and a 200 HTTP status). HTTP error responses (4xx/5xx) are not
// considered successful even though they populate the Response field.
//
// NOTE: because HTTP errors are written to the output file (not the error file),
// request_counts.failed may be greater than the number of lines in the error file.
// This diverges from OpenAI's documented behavior but aligns with the OpenAPI schema
// (see executeOneRequest for rationale).
func (o *outputLine) isSuccess() bool {
	return o.Error == nil && o.Response != nil && o.Response.StatusCode == 200
}

// progressUpdateInterval is the minimum time between Redis progress updates.
// Updates within this window are skipped — the next update after the interval
// will include all accumulated progress. Declared as var so tests can override.
var progressUpdateInterval = time.Second

// executionProgress tracks per-request progress across goroutines
// and pushes throttled updates to the status store.
type executionProgress struct {
	completed  atomic.Int64
	failed     atomic.Int64
	total      int64
	updater    *StatusUpdater
	jobID      string
	lastUpdate atomic.Int64 // unix nanoseconds of last Redis push
}

// record increments the appropriate counter and pushes a throttled progress
// update to Redis. Updates are skipped if less than progressUpdateInterval
// has elapsed since the last push, reducing Redis writes from O(requests)
// to O(job_duration / interval).
func (ep *executionProgress) record(ctx context.Context, success bool) {
	if success {
		ep.completed.Add(1)
	} else {
		ep.failed.Add(1)
	}
	now := time.Now().UnixNano()
	last := ep.lastUpdate.Load()
	if now-last < int64(progressUpdateInterval) {
		return
	}
	// Best-effort CAS: if another goroutine raced us, skip this update.
	if !ep.lastUpdate.CompareAndSwap(last, now) {
		return
	}
	ep.push(ctx)
}

// flush pushes the final progress to Redis unconditionally, ensuring the
// last update reflects the true counts regardless of throttling.
func (ep *executionProgress) flush(ctx context.Context) {
	ep.push(ctx)
}

func (ep *executionProgress) push(ctx context.Context) {
	if err := ep.updater.UpdateProgressCounts(ctx, ep.jobID, &openai.BatchRequestCounts{
		Total:     ep.total,
		Completed: ep.completed.Load(),
		Failed:    ep.failed.Load(),
	}); err != nil {
		logr.FromContextOrDiscard(ctx).Error(err, "Failed to update progress counts (best-effort)")
	}
}

func (ep *executionProgress) counts() *openai.BatchRequestCounts {
	return &openai.BatchRequestCounts{
		Total:     ep.total,
		Completed: ep.completed.Load(),
		Failed:    ep.failed.Load(),
	}
}

type collector struct {
	ch chan modelResultCollector
}

func newCollector() *collector {
	return &collector{ch: make(chan modelResultCollector)}
}

func (c *collector) send(mp modelResultCollector) {
	c.ch <- mp
}

func (c *collector) close() {
	close(c.ch)
}

func (c *collector) run(ctx context.Context) {
	for mp := range c.ch {
		mp.collect(ctx)
	}
}

type stopReason int

const (
	stopNone         stopReason = iota
	stopExpired                 // SLO deadline exceeded
	stopCancelled               // user-initiated cancel
	stopFailed                  // modelErr (I/O failure)
	stopShutdown                // SIGTERM (mainCtx cancelled)
	stopSiblingAbort            // sibling model's error cancelled requestAbortCtx
)

func resolveStopReason(sloCtx, userCancelCtx, mainCtx, requestAbortCtx context.Context, modelErr error) stopReason {
	switch {
	case errors.Is(sloCtx.Err(), context.DeadlineExceeded):
		return stopExpired
	case userCancelCtx.Err() != nil:
		return stopCancelled
	case modelErr != nil:
		return stopFailed
	case mainCtx.Err() != nil:
		return stopShutdown
	case requestAbortCtx.Err() != nil:
		return stopSiblingAbort
	default:
		return stopNone
	}
}

// executeJob performs execution: reads plan files per model, sends inference
// requests concurrently (one goroutine per model, multiple requests per model), and writes results to
// output.jsonl (successes) and error.jsonl (failures). Returns request counts for finalization.
//
// On success, returns (counts, nil). On interruption or error, output and error writers are
// always flushed (buffered data written to the underlying files) before returning, and partial
// counts are returned alongside the sentinel/cause error:
//   - SLO expired:    (counts, errExpired)   — undispatched drained as batch_expired
//   - User cancel:    (counts, errCancelled) — undispatched drained as batch_cancelled
//   - System error:   (counts, firstErr)     — undispatched drained as batch_failed
//   - Pod shutdown:   (counts, errShutdown)  — caller re-enqueues; counts reflect work done
//     before SIGTERM, flush preserves partial output for startup recovery
//
// requestAbortCtx controls the dispatch loop and all in-flight inference calls: cancelling it
// stops dispatch and aborts in-flight requests. It is derived from sloCtx in runJob, so SLO
// expiry and SIGTERM propagate automatically. User cancel also triggers requestAbortFn via
// context.AfterFunc(userCancelCtx, requestAbortFn) wired in runJob — watchCancel itself only
// calls userCancelFn.
// userCancelCtx is a user-cancel-only signal derived from context.Background; it does not inherit
// SLO expiry or SIGTERM. Its sole purpose is to let the drain phase distinguish user cancel from
// SLO expiry.
func (p *Processor) executeJob(ctx, sloCtx, userCancelCtx, requestAbortCtx context.Context, params *jobExecutionParams) (*openai.BatchRequestCounts, error) {
	logger := logr.FromContextOrDiscard(ctx)
	logger.V(logging.INFO).Info("Starting execution: executing job")

	jobRootDir, err := p.jobRootDir(params.jobInfo.JobID, params.jobInfo.TenantID)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve job root directory: %w", err)
	}

	modelMap, err := readModelMap(jobRootDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read model map: %w", err)
	}

	// Early SLO check: if the deadline already fired before execution begins (e.g. SLO expired
	// during ingestion), skip dispatch entirely. No output file is written since no requests
	// were dispatched, but error.jsonl may already contain model_not_found entries from
	// ingestion. handleExpired will upload whatever files exist.
	if sloCtx.Err() == context.DeadlineExceeded {
		logger.V(logging.INFO).Info("SLO already expired at execution start, skipping dispatch",
			"total", modelMap.LineCount)
		return &openai.BatchRequestCounts{Total: modelMap.LineCount, Failed: modelMap.RejectedCount}, errExpired
	}

	inputFilePath, err := p.jobInputFilePath(params.jobInfo.JobID, params.jobInfo.TenantID)
	if err != nil {
		return nil, err
	}
	inputFile, err := os.Open(inputFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open input file: %w", err)
	}
	defer inputFile.Close()

	outputFilePath, err := p.jobOutputFilePath(params.jobInfo.JobID, params.jobInfo.TenantID)
	if err != nil {
		return nil, err
	}
	outputFile, err := os.OpenFile(outputFilePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, fmt.Errorf("failed to create output file: %w", err)
	}
	defer outputFile.Close()

	errorFilePath, err := p.jobErrorFilePath(params.jobInfo.JobID, params.jobInfo.TenantID)
	if err != nil {
		return nil, err
	}
	// Append mode: ingestion may have already written model_not_found errors.
	errorFile, err := os.OpenFile(errorFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("failed to create error file: %w", err)
	}
	defer errorFile.Close()

	writers := &outputWriters{
		output: bufio.NewWriterSize(outputFile, 1024*1024),
		errors: bufio.NewWriterSize(errorFile, 1024*1024),
	}

	plansDir, err := p.jobPlansDir(params.jobInfo.JobID, params.jobInfo.TenantID)
	if err != nil {
		return nil, err
	}

	// requestAbortCtx and requestAbortFn are set in runJob before watchCancel starts,
	// eliminating the race window where a cancel event could arrive before the fn is assigned.

	progress := &executionProgress{
		total:   modelMap.LineCount,
		updater: params.updater,
		jobID:   params.jobInfo.JobID,
	}
	// Seed with requests already rejected during ingestion (model not found).
	progress.failed.Store(modelMap.RejectedCount)

	pw := newProgressWorker(ctx, writers, progress)
	go pw.run()

	passThroughHeaders := params.jobInfo.PassThroughHeaders
	if len(passThroughHeaders) > 0 {
		headerNames := make([]string, 0, len(passThroughHeaders))
		for k := range passThroughHeaders {
			headerNames = append(headerNames, k)
		}
		logger.V(logging.DEBUG).Info("pass-through headers attached to job", "headerNames", headerNames)
	}

	tenantID := params.jobInfo.TenantID

	processors := make([]modelProcessor, 0, len(modelMap.SafeToModel))
	for safeModelID, modelID := range modelMap.SafeToModel {
		mp := p.makeModelProcessor(pw, inputFile)
		processors = append(processors, mp)

		go func(mp modelProcessor, safeModelID, modelID string) {
			mp.submit(
				requestAbortCtx,
				ctx,
				sloCtx,
				userCancelCtx,
				plansDir, safeModelID, modelID,
				passThroughHeaders,
				tenantID,
			)
			p.collector.send(mp.(modelResultCollector))
		}(mp, safeModelID, modelID)
	}

	var firstErr error
	for _, mp := range processors {
		if err := <-mp.done(); err != nil && firstErr == nil {
			firstErr = err
			if fn := params.requestAbortFn; fn != nil {
				fn()
			}
		}
	}

	pw.close()
	pwErr := <-pw.errCh

	progress.flush(ctx)

	if pwErr != nil && firstErr == nil {
		firstErr = pwErr
	}

	counts := pw.counts()

	if firstErr != nil {
		switch {
		case errors.Is(firstErr, errExpired):
			logger.V(logging.INFO).Info("Execution SLO expired, returning partial counts",
				"total", counts.Total, "completed", counts.Completed, "failed", counts.Failed)
		case errors.Is(firstErr, errCancelled):
			logger.V(logging.INFO).Info("Execution cancelled, returning partial counts",
				"total", counts.Total, "completed", counts.Completed, "failed", counts.Failed)
		default:
			logger.V(logging.INFO).Info("Execution system error, returning partial counts",
				"total", counts.Total, "completed", counts.Completed, "failed", counts.Failed)
		}
		return counts, firstErr
	}

	logger.V(logging.INFO).Info("Execution completed",
		"total", counts.Total, "completed", counts.Completed, "failed", counts.Failed)

	switch {
	case errors.Is(sloCtx.Err(), context.DeadlineExceeded):
		return counts, errExpired
	case userCancelCtx.Err() != nil:
		return counts, errCancelled
	}

	return counts, nil
}

func (p *Processor) makeModelProcessor(pw *progressWorker, inputFile *os.File) modelProcessor {
	if p.asyncInference == nil {
		return &syncModelProcessor{processor: p, pw: pw, inputFile: inputFile, errCh: make(chan error, 1)}
	}
	return &asyncModelProcessor{processor: p, pw: pw, inputFile: inputFile, errCh: make(chan error, 1)}
}

// drainAndFinalize drains undispatched entries based on the stop reason and
// returns the appropriate sentinel error.
func drainAndFinalize(
	inputFile *os.File,
	undispatched []planEntry,
	pw *progressWorker,
	modelErr error,
	logger logr.Logger,
	totalEntries int,
	reason stopReason,
) error {
	var returnErr error
	switch reason {
	case stopExpired:
		if len(undispatched) > 0 {
			logger.V(logging.INFO).Info("SLO expired: draining undispatched entries", "count", len(undispatched))
			drainUnprocessedRequests(inputFile, undispatched, pw, batch_types.ErrCodeBatchExpired)
		}
		returnErr = errExpired

	case stopCancelled:
		if len(undispatched) > 0 {
			logger.V(logging.INFO).Info("Cancelled: draining undispatched entries", "count", len(undispatched))
			drainUnprocessedRequests(inputFile, undispatched, pw, batch_types.ErrCodeBatchCancelled)
		}
		returnErr = errCancelled

	case stopFailed:
		if len(undispatched) > 0 {
			logger.V(logging.INFO).Info("Fatal error: draining undispatched entries", "count", len(undispatched))
			drainUnprocessedRequests(inputFile, undispatched, pw, batch_types.ErrCodeBatchFailed)
		}
		returnErr = modelErr

	case stopShutdown:
		if len(undispatched) > 0 {
			returnErr = errShutdown
		}

	case stopSiblingAbort:
		if len(undispatched) > 0 {
			logger.V(logging.INFO).Info("Sibling abort: draining undispatched entries", "count", len(undispatched))
			drainUnprocessedRequests(inputFile, undispatched, pw, batch_types.ErrCodeBatchFailed)
		}

	case stopNone:
	}

	logger.V(logging.INFO).Info("Finished processing model", "numEntries", totalEntries, "hasError", returnErr != nil, "siblingAbort", reason == stopSiblingAbort)
	return returnErr
}

// drainUnprocessedRequests records undispatched requests in the error file when a job terminates
// mid-execution (SLO expiry, cancellation, or systemic failure). For each plan entry, it reads
// the original request from input.jsonl to extract the custom_id, then writes an error line with
// the given error code and its canonical message.
func drainUnprocessedRequests(
	inputFile *os.File,
	entries []planEntry,
	pw *progressWorker,
	errCode batch_types.BatchErrorCode,
) {
	errMessage := errCode.Message()

	var maxLen uint32
	for _, e := range entries {
		if e.Length > maxLen {
			maxLen = e.Length
		}
	}
	buf := make([]byte, maxLen)

	for _, entry := range entries {
		customID := ""
		if _, err := inputFile.ReadAt(buf[:entry.Length], entry.Offset); err == nil {
			var req batch_types.Request
			if err := json.Unmarshal(bytes.TrimSuffix(buf[:entry.Length], []byte{'\n'}), &req); err == nil {
				customID = req.CustomID
			}
		}

		requestID := uuid.NewString()
		out := newErrorOutputLine(newBatchRequestID(requestID), customID, string(errCode), errMessage)
		pw.send(resultItem{out: out})
	}
}

const (
	sloTTFTMSHeader          = "x-slo-ttft-ms"
	inferenceObjectiveHeader = "x-gateway-inference-objective"
	fairnessIDHeader         = "x-gateway-inference-fairness-id"
)

// mergeInferenceHeaders adds processor-managed headers to the outgoing inference request:
//   - x-slo-ttft-ms: remaining milliseconds until the SLO deadline (>= 0).
//   - x-gateway-inference-objective: name of the InferenceObjective CRD that
//     determines the priority band for this request.
//   - x-gateway-inference-fairness-id: tenant identifier for per-tenant fairness
//     within a priority band.
//
// Headers are only added when the relevant value is available/configured.
// If sloCtx has no deadline, is cancelled, or has an expired deadline, the SLO
// header is not set. If inferenceObjective is empty, the objective header is not set.
// If fairnessID is non-empty, the fairness header is set only when the outgoing
// headers do not already include x-gateway-inference-fairness-id. Unlike SLO and
// objective (which are processor-authoritative and always override), fairness is
// user-overridable: callers can supply a custom flow key (e.g. API key, group ID)
// via pass-through headers, and the processor falls back to tenantID only when no
// override is present.
func mergeInferenceHeaders(headers map[string]string, sloCtx context.Context, inferenceObjective, fairnessID string) map[string]string {
	hasSLO := false
	var sloMs int64
	if sloCtx.Err() == nil {
		if dl, ok := sloCtx.Deadline(); ok {
			ms := time.Until(dl).Milliseconds()
			if ms >= 0 {
				hasSLO = true
				sloMs = ms
			}
		}
	}
	hasObjective := inferenceObjective != ""
	hasFairness := fairnessID != ""
	if hasFairness && headers != nil {
		if _, exists := headers[fairnessIDHeader]; exists {
			hasFairness = false
		}
	}

	if !hasSLO && !hasObjective && !hasFairness {
		return headers
	}
	if headers == nil {
		headers = make(map[string]string)
	}
	if hasSLO {
		headers[sloTTFTMSHeader] = strconv.FormatInt(sloMs, 10)
	}
	if hasObjective {
		headers[inferenceObjectiveHeader] = inferenceObjective
	}
	if hasFairness {
		headers[fairnessIDHeader] = fairnessID
	}
	return headers
}

// readRequestLine reads a single plan entry from the input file, parses it, and
// generates a batch request ID. Returns the parsed request and batch request ID
// on success, an outputLine on parse error, or a fatal error on I/O failure.
func readRequestLine(inputFile *os.File, entry planEntry, logger logr.Logger) (*batch_types.Request, string, *outputLine, error) {
	buf := make([]byte, entry.Length)
	if _, err := inputFile.ReadAt(buf, entry.Offset); err != nil {
		return nil, "", nil, fmt.Errorf("%w at offset %d: %w", errRequestInputRead, entry.Offset, err)
	}
	trimmed := bytes.TrimSuffix(buf, []byte{'\n'})
	batchReqID := newBatchRequestID(uuid.NewString())

	var req batch_types.Request
	if err := json.Unmarshal(trimmed, &req); err != nil {
		logger.Error(err, "failed to parse request line, recording as error")
		return nil, batchReqID, newErrorOutputLine(batchReqID, "", string(httpclient.ErrCategoryParse),
			fmt.Sprintf("failed to parse request line: %v", err)), nil
	}

	return &req, batchReqID, nil, nil
}

func (p *Processor) fairnessID(tenantID string) string {
	if p.cfg.SendFairnessHeader {
		return tenantID
	}
	return ""
}

// executeOneRequest reads a single input line from the input file at the given plan entry offset,
// sends it to the inference gateway, and returns the formatted output line.
func (p *Processor) executeOneRequest(
	ctx context.Context,
	sloCtx context.Context,
	inputFile *os.File,
	entry planEntry,
	modelID string,
	passThroughHeaders map[string]string,
	tenantID string,
) (*outputLine, error) {
	logger := logr.FromContextOrDiscard(ctx)
	req, batchReqID, parseErr, readErr := readRequestLine(inputFile, entry, logger)
	if readErr != nil {
		return nil, readErr
	}
	if parseErr != nil {
		return parseErr, nil
	}

	logger = logger.WithValues("customId", req.CustomID, "requestId", batchReqID)

	inferClient := p.inference.ClientFor(modelID)
	if inferClient == nil {
		logger.V(logging.INFO).Info("ClientFor returned nil during execution (expected rejection at ingestion)",
			"model", modelID)
		metrics.RecordRequestError(modelID)
		return newErrorOutputLine(batchReqID, req.CustomID, inference.ErrCodeModelNotFound,
			fmt.Sprintf("model %q is not configured in any gateway", modelID)), nil
	}

	headers := maps.Clone(passThroughHeaders)
	headers = mergeInferenceHeaders(headers, sloCtx, p.cfg.InferenceObjectiveFor(modelID), p.fairnessID(tenantID))

	inferReq := &inference.GenerateRequest{
		RequestID: batchReqID,
		Endpoint:  req.URL,
		Params:    req.Body,
		Headers:   headers,
	}

	if sloCtx.Err() == context.DeadlineExceeded {
		logger.V(logging.INFO).Info("SLO expired during execution, skipping request", "error", sloCtx.Err())
		result := newErrorOutputLine(batchReqID, req.CustomID,
			string(batch_types.ErrCodeBatchExpired), batch_types.ErrCodeBatchExpired.Message())
		metrics.RecordRequestError(modelID)
		return result, nil
	}

	start := time.Now()
	metrics.IncProcessorInflightRequests()
	metrics.IncModelInflightRequests(modelID)
	logger.V(logging.TRACE).Info("Dispatching inference request")

	inferResp, inferErr := inferClient.Generate(ctx, inferReq)

	metrics.DecModelInflightRequests(modelID)
	metrics.DecProcessorInflightRequests()
	metrics.RecordModelRequestExecutionDuration(time.Since(start), modelID)

	result := buildOutputLine(batchReqID, req.CustomID, modelID, inferReq.RequestID, inferResp, inferErr, logger)
	return result, nil
}

func newErrorOutputLine(batchReqID, customID, code, message string) *outputLine {
	return &outputLine{
		ID:       batchReqID,
		CustomID: customID,
		Error:    &outputError{Code: code, Message: message},
	}
}

type resultItem struct {
	out           *outputLine
	userCancelled bool
}

type progressWorker struct {
	ch       chan resultItem
	errCh    chan error
	writers  *outputWriters
	progress *executionProgress
	ctx      context.Context
}

func newProgressWorker(ctx context.Context, writers *outputWriters, progress *executionProgress) *progressWorker {
	return &progressWorker{
		ch:       make(chan resultItem, 256),
		errCh:    make(chan error, 1),
		writers:  writers,
		progress: progress,
		ctx:      ctx,
	}
}

func (pw *progressWorker) send(item resultItem) {
	pw.ch <- item
}

func (pw *progressWorker) close() {
	close(pw.ch)
}

func (pw *progressWorker) run() {
	var firstErr error
	for item := range pw.ch {
		if firstErr != nil {
			continue
		}
		if err := writeResult(item.out, item.userCancelled, pw.ctx, pw.writers, pw.progress); err != nil {
			firstErr = err
		}
	}
	if err := pw.writers.output.Flush(); err != nil && firstErr == nil {
		firstErr = fmt.Errorf("failed to flush output file: %w", err)
	}
	if err := pw.writers.errors.Flush(); err != nil && firstErr == nil {
		firstErr = fmt.Errorf("failed to flush error file: %w", err)
	}
	pw.errCh <- firstErr
}

func (pw *progressWorker) counts() *openai.BatchRequestCounts {
	return pw.progress.counts()
}

// writeResult applies user-cancel overwrite if needed, records progress, marshals
// the output line, and writes it to the appropriate file. Returns an error only
// for marshal/write failures.
func writeResult(
	out *outputLine,
	userCancelled bool,
	progressCtx context.Context,
	writers *outputWriters,
	progress *executionProgress,
) error {
	if userCancelled {
		out.Response = nil
		out.Error = &outputError{
			Code:    string(batch_types.ErrCodeBatchCancelled),
			Message: "This request was cancelled while in progress.",
		}
		progress.record(progressCtx, false)
	} else {
		progress.record(progressCtx, out.isSuccess())
	}

	lineBytes, err := json.Marshal(out)
	if err != nil {
		return fmt.Errorf("marshal output line for %s: %w", out.ID, err)
	}
	lineBytes = append(lineBytes, '\n')
	if writeErr := writers.write(lineBytes, out.Error != nil); writeErr != nil {
		return fmt.Errorf("write output line for %s: %w", out.ID, writeErr)
	}
	return nil
}

// buildOutputLine converts an inference response and/or error into an outputLine.
// Used by both executeOneRequest (sync path) and processModelAsync (async path).
func buildOutputLine(
	batchReqID, customID, modelID, serverRequestID string,
	inferResp *inference.GenerateResponse,
	inferErr *inference.ClientError,
	logger logr.Logger,
) *outputLine {
	result := &outputLine{
		ID:       batchReqID,
		CustomID: customID,
	}

	// Response handling by case.
	//
	// Design note: HTTP errors (4xx/5xx) are written to the output file with their
	// status code and body, rather than the error file. The OpenAI Batch API guides
	// describe output_file_id as containing "successfully executed requests", but
	// the OpenAPI schema defines the error field as "for requests that failed with a
	// non-HTTP error", implying HTTP errors belong in the response. We follow the
	// schema interpretation here, as it preserves the HTTP status code and body for
	// callers to inspect.
	if inferErr != nil {
		logger.V(logging.DEBUG).Info("Inference request failed", "error", inferErr.Message)
		if inferErr.StatusCode > 0 {
			if inferErr.DroppedReason == httpclient.DroppedReasonTTLExpired {
				result.Error = &outputError{
					Code:    string(batch_types.ErrCodeBatchExpired),
					Message: batch_types.ErrCodeBatchExpired.Message(),
				}
				metrics.RecordRequestError(modelID)
				return result
			}
			// HTTP error (4xx/5xx) — populate response with status code and original body
			// per OpenAI spec, error field is only for non-HTTP errors
			// Ensure body is always a non-nil object to satisfy the OpenAI schema (type: object).
			body := make(map[string]interface{})
			if len(inferErr.ResponseBody) > 0 {
				if err := json.Unmarshal(inferErr.ResponseBody, &body); err != nil {
					// Non-JSON response body cannot be placed directly into a JSON object field,
					// so we wrap it in a synthetic error structure to preserve the content.
					body = map[string]interface{}{
						"error": map[string]interface{}{
							"message": string(inferErr.ResponseBody),
							"type":    inferErr.OpenAIErrorType(),
						},
					}
				}
			}
			result.Response = &batch_types.ResponseData{
				StatusCode: inferErr.StatusCode,
				RequestID:  serverRequestID,
				Body:       body,
			}
		} else {
			// Non-HTTP error (network, timeout, etc.)
			result.Error = &outputError{
				Code:    string(inferErr.Category),
				Message: inferErr.Message,
			}
		}
	} else if inferResp == nil {
		// ok status without error but no response
		err := fmt.Errorf("inference returned no error but response is nil")
		logger.Error(err, "Inference request failed")
		result.Error = &outputError{
			Code:    string(httpclient.ErrCategoryServer),
			Message: err.Error(),
		}
	} else {
		result.hadCapacityRetry = inferResp.HadCapacityRetry
		// success — unmarshal the response body
		var body map[string]interface{}
		if len(inferResp.Response) > 0 {
			if err := json.Unmarshal(inferResp.Response, &body); err != nil {
				// failed to unmarshal the response body
				logger.Error(err, "failed to unmarshal inference response body")
				result.Error = &outputError{
					Code:    string(httpclient.ErrCategoryParse),
					Message: fmt.Sprintf("inference succeeded but response body could not be parsed: %v", err),
				}
			}
		}
		if result.Error == nil {
			logger.V(logging.TRACE).Info("Inference request completed", "serverRequestId", inferResp.RequestID)
			result.Response = &batch_types.ResponseData{
				StatusCode: 200,
				RequestID:  inferResp.RequestID,
				Body:       body,
			}
			recordTokenUsageFromBody(body, modelID, logger)
		}
	}

	if !result.isSuccess() {
		metrics.RecordRequestError(modelID)
	}
	return result
}

// recordTokenUsageFromBody extracts prompt and completion token counts from the
// inference response body and records them as metrics. Skips if the usage object
// is absent, if neither prompt_tokens nor completion_tokens is a valid numeric value,
// or if either one is negative.
func recordTokenUsageFromBody(body map[string]interface{}, model string, logger logr.Logger) {
	usage, ok := body["usage"].(map[string]interface{})
	if !ok {
		logger.V(logging.DEBUG).Info("Inference response missing usage data, skipping token metrics")
		return
	}
	prompt, promptOK := jsonNumericToFloat64(usage["prompt_tokens"])
	completion, completionOK := jsonNumericToFloat64(usage["completion_tokens"])
	if !promptOK && !completionOK {
		logger.V(logging.DEBUG).Info("Inference response usage has no numeric token fields, skipping token metrics")
		return
	}
	// Prometheus Counter.Add() panics on negative values. Guard against non-conforming
	// inference backends that might return negative token counts.
	if prompt < 0 || completion < 0 {
		logger.V(logging.DEBUG).Info("Inference response usage has negative token values, skipping token metrics",
			"prompt_tokens", prompt, "completion_tokens", completion)
		return
	}
	metrics.RecordTokenUsage(prompt, completion, model)
}

func jsonNumericToFloat64(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	default:
		return 0, false
	}
}

// newBatchRequestID formats requestID into the "batch_req_<uuid>" form required by the
// OpenAI Batch API for output/error line IDs. When used in executeOneRequest, the same
// requestID is also passed to the inference client so the two can be correlated in logs.
func newBatchRequestID(requestID string) string {
	return fmt.Sprintf("batch_req_%s", requestID)
}

type pendingRequest struct {
	batchReqID string
	customID   string
}

type modelProcessor interface {
	submit(
		requestAbortCtx context.Context,
		mainCtx context.Context,
		sloCtx context.Context,
		userCancelCtx context.Context,
		plansDir, safeModelID, modelID string,
		passThroughHeaders map[string]string,
		tenantID string,
	) error
	done() chan error
}

type modelResultCollector interface {
	collect(ctx context.Context) error
}

type syncModelProcessor struct {
	processor *Processor
	pw        *progressWorker
	inputFile *os.File
	errCh     chan error
}

func (s *syncModelProcessor) done() chan error { return s.errCh }

func (s *syncModelProcessor) submit(
	requestAbortCtx context.Context,
	mainCtx context.Context,
	sloCtx context.Context,
	userCancelCtx context.Context,
	plansDir, safeModelID, modelID string,
	passThroughHeaders map[string]string,
	tenantID string,
) (retErr error) {
	defer func() { s.errCh <- retErr; close(s.errCh) }()

	p := s.processor
	logger := logr.FromContextOrDiscard(requestAbortCtx).WithValues("model", modelID)
	requestAbortCtx = logr.NewContext(requestAbortCtx, logger)

	planPath := filepath.Join(plansDir, safeModelID+".plan")
	entries, err := readPlanEntries(planPath)
	if err != nil {
		return fmt.Errorf("model setup failed: read plan for model %s: %w", modelID, err)
	}

	logger.V(logging.INFO).Info("Processing requests for a model", "numEntries", len(entries))

	client := p.inference.ClientFor(modelID)
	epLimit := p.endpointLimits[client]
	if epLimit == nil {
		logger.V(logging.INFO).Info("No endpoint limit for model (client not in resolver), draining as model_not_found")
		drainUnprocessedRequests(s.inputFile, entries, s.pw,
			batch_types.BatchErrorCode(inference.ErrCodeModelNotFound))
		return nil
	}
	endpointSem := epLimit.sem

	var (
		wg              sync.WaitGroup
		errOnce         sync.Once
		modelErr        error
		dispatchedCount int
	)

dispatch:
	for i, entry := range entries {
		if requestAbortCtx.Err() != nil {
			break
		}

		if err := endpointSem.Acquire(requestAbortCtx); err != nil {
			break dispatch
		}

		if err := p.globalSem.Acquire(requestAbortCtx); err != nil {
			endpointSem.Release()
			break dispatch
		}

		dispatchedCount = i + 1
		wg.Add(1)
		go func(entry planEntry) {
			defer wg.Done()
			defer endpointSem.Release()
			defer p.globalSem.Release()

			result, execErr := p.executeOneRequest(requestAbortCtx, sloCtx, s.inputFile, entry, modelID, passThroughHeaders, tenantID)

			if epLimit.aimd != nil && execErr == nil && result != nil && result.Response != nil {
				sc := result.Response.StatusCode
				switch {
				case sc == http.StatusTooManyRequests:
					epLimit.aimd.RecordRateLimit(metrics.AIMDSignal429)
					metrics.RecordAIMDDecrease(epLimit.label, metrics.AIMDSignal429)
				case sc >= http.StatusInternalServerError:
					epLimit.aimd.RecordRateLimit(metrics.AIMDSignal5xx)
					metrics.RecordAIMDDecrease(epLimit.label, metrics.AIMDSignal5xx)
				case result.hadCapacityRetry:
					epLimit.aimd.RecordRateLimit(metrics.AIMDSignalCapacityRetry)
					metrics.RecordAIMDDecrease(epLimit.label, metrics.AIMDSignalCapacityRetry)
				default:
					oldLimit := epLimit.aimd.Limit()
					epLimit.aimd.RecordSuccess()
					if epLimit.aimd.Limit() != oldLimit {
						metrics.RecordAIMDIncrease(epLimit.label)
					}
				}
				metrics.SetAIMDConcurrencyLimit(epLimit.label, float64(epLimit.aimd.Limit()))
			}
			if execErr != nil {
				logger.Error(execErr, "Fatal error executing request", "offset", entry.Offset)
				errOnce.Do(func() { modelErr = execErr })
				return
			}

			s.pw.send(resultItem{out: result, userCancelled: sloCtx.Err() == nil && userCancelCtx.Err() != nil})
		}(entry)
	}

	wg.Wait()

	reason := resolveStopReason(sloCtx, userCancelCtx, mainCtx, requestAbortCtx, modelErr)
	return drainAndFinalize(s.inputFile, entries[dispatchedCount:], s.pw, modelErr, logger, len(entries), reason)
}

func (s *syncModelProcessor) collect(ctx context.Context) error {
	return nil
}

type asyncModelProcessor struct {
	processor   *Processor
	pw          *progressWorker
	inputFile   *os.File
	errCh       chan error
	submitErr   error
	asyncClient inference.AsyncInferenceClient
	pending     map[string]*pendingRequest
	entries     []planEntry
	submitCount int
	modelID     string
	logger      logr.Logger
	reason      stopReason
}

func (a *asyncModelProcessor) done() chan error { return a.errCh }

func (a *asyncModelProcessor) submit(
	requestAbortCtx context.Context,
	mainCtx context.Context,
	sloCtx context.Context,
	userCancelCtx context.Context,
	plansDir, safeModelID, modelID string,
	passThroughHeaders map[string]string,
	tenantID string,
) (submitErr error) {
	defer func() { a.submitErr = submitErr }()

	p := a.processor

	logger := logr.FromContextOrDiscard(requestAbortCtx).WithValues("model", modelID)
	requestAbortCtx = logr.NewContext(requestAbortCtx, logger)

	planPath := filepath.Join(plansDir, safeModelID+".plan")
	entries, err := readPlanEntries(planPath)
	if err != nil {
		return fmt.Errorf("model setup failed: read plan for model %s: %w", modelID, err)
	}

	logger.V(logging.INFO).Info("Processing requests for model (async)", "numEntries", len(entries))

	asyncClient := p.asyncInference.ClientFor(modelID)
	if asyncClient == nil {
		logger.V(logging.INFO).Info("No async client for model, draining as model_not_found")
		drainUnprocessedRequests(a.inputFile, entries, a.pw, inference.ErrCodeModelNotFound)
		return nil
	}

	a.asyncClient = asyncClient
	a.entries = entries
	a.modelID = modelID
	a.logger = logger
	a.pending = make(map[string]*pendingRequest)

	for _, entry := range entries {
		if requestAbortCtx.Err() != nil {
			logger.V(logging.INFO).Info("Async submit aborted", "submitted", len(a.pending), "total", len(entries), "reason", requestAbortCtx.Err())
			break
		}

		req, batchReqID, parseErr, readErr := readRequestLine(a.inputFile, entry, logger)
		if readErr != nil {
			return readErr
		}
		if parseErr != nil {
			a.pw.send(resultItem{out: parseErr})
			a.submitCount++
			continue
		}

		if errors.Is(sloCtx.Err(), context.DeadlineExceeded) {
			break
		}

		headers := maps.Clone(passThroughHeaders)
		headers = mergeInferenceHeaders(headers, sloCtx, p.cfg.InferenceObjectiveFor(modelID), p.fairnessID(tenantID))

		inferReq := &inference.GenerateRequest{
			RequestID: batchReqID,
			Endpoint:  req.URL,
			Params:    req.Body,
			Headers:   headers,
		}

		if submitErr := asyncClient.Submit(requestAbortCtx, inferReq); submitErr != nil {
			out := newErrorOutputLine(batchReqID, req.CustomID,
				string(submitErr.Category), submitErr.Message)
			a.pw.send(resultItem{out: out})
			a.submitCount++
			continue
		}

		a.pending[batchReqID] = &pendingRequest{
			batchReqID: batchReqID,
			customID:   req.CustomID,
		}
		a.submitCount++
	}

	logger.V(logging.INFO).Info("Submit phase complete", "submitted", len(a.pending), "total", a.submitCount)
	a.reason = resolveStopReason(sloCtx, userCancelCtx, mainCtx, requestAbortCtx, nil)
	return nil
}

func (a *asyncModelProcessor) collect(ctx context.Context) (retErr error) {
	defer func() { a.errCh <- retErr; close(a.errCh) }()

	if a.submitErr != nil {
		return a.submitErr
	}
	if a.asyncClient == nil {
		return nil
	}
	defer func() {
		if err := a.asyncClient.Close(); err != nil {
			a.logger.Error(err, "Failed to close async client")
		}
	}()

	var modelErr error

	for len(a.pending) > 0 {
		resp, err := a.asyncClient.GetResult(ctx)
		if err != nil {
			if ctx.Err() == nil {
				a.logger.Error(err, "Failed to collect async result", "pendingCount", len(a.pending))
				modelErr = fmt.Errorf("async result collection failed: %w", err)
			}
			break
		}

		pr, ok := a.pending[resp.RequestID]
		if !ok {
			a.logger.V(logging.TRACE).Info("Ignoring result for unknown request", "requestID", resp.RequestID)
			continue
		}

		out := buildOutputLine(pr.batchReqID, pr.customID, a.modelID, resp.RequestID, resp, nil, a.logger)
		a.pw.send(resultItem{out: out, userCancelled: a.reason == stopCancelled})
		delete(a.pending, resp.RequestID)
	}

	for _, pr := range a.pending {
		out := newErrorOutputLine(pr.batchReqID, pr.customID,
			string(batch_types.ErrCodeBatchExpired), "result not collected before deadline")
		a.pw.send(resultItem{out: out})
	}

	reason := a.reason
	if modelErr != nil {
		reason = stopFailed
	}
	return drainAndFinalize(a.inputFile, a.entries[a.submitCount:], a.pw, modelErr, a.logger, len(a.entries), reason)
}
