package worker

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	klog "k8s.io/klog/v2"

	db "github.com/llm-d-incubation/batch-gateway/internal/database/api"
	mockdb "github.com/llm-d-incubation/batch-gateway/internal/database/mock"
	mockfiles "github.com/llm-d-incubation/batch-gateway/internal/files_store/mock"
	"github.com/llm-d-incubation/batch-gateway/internal/processor/config"
	"github.com/llm-d-incubation/batch-gateway/internal/shared/batch_utils"
	"github.com/llm-d-incubation/batch-gateway/internal/shared/openai"
)

const mockFilesRootDir = "/tmp/batch-gateway-files"

// -------------------------
// Spy wrappers (thin)
// -------------------------

type spyPQ struct {
	inner db.BatchPriorityQueueClient
	mu    sync.Mutex
	delN  int
}

func (s *spyPQ) PQEnqueue(ctx context.Context, jobPriority *db.BatchJobPriority) error {
	return s.inner.PQEnqueue(ctx, jobPriority)
}
func (s *spyPQ) PQDequeue(ctx context.Context, timeout time.Duration, maxObjs int) ([]*db.BatchJobPriority, error) {
	return s.inner.PQDequeue(ctx, timeout, maxObjs)
}
func (s *spyPQ) PQDelete(ctx context.Context, jobPriority *db.BatchJobPriority) (int, error) {
	s.mu.Lock()
	s.delN++
	s.mu.Unlock()
	return s.inner.PQDelete(ctx, jobPriority)
}
func (s *spyPQ) GetContext(parentCtx context.Context, timeLimit time.Duration) (context.Context, context.CancelFunc) {
	return s.inner.GetContext(parentCtx, timeLimit)
}
func (s *spyPQ) Close() error { return s.inner.Close() }

func (s *spyPQ) DeleteCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.delN
}

// -------------------------
// Helpers
// -------------------------

func testLoggerCtx() context.Context {
	// stable logger context for unit tests
	l := klog.Background()
	return klog.NewContext(context.Background(), l)
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	return b
}

func makeInputLines(models []string) [][]byte {
	lines := make([][]byte, 0, len(models))
	for i, m := range models {
		// preProcessJob expects:
		// { "body": { "model": "..." ... }, ... }
		// It only reads body.model.
		req := map[string]any{
			"body": map[string]any{
				"model": m,
			},
			"meta": map[string]any{
				"i": i,
			},
		}
		b, _ := json.Marshal(req)
		// Intentionally vary newline handling: add '\n' here; preProcess also appends if missing.
		b = append(b, '\n')
		lines = append(lines, b)
	}
	return lines
}

// Read plan entries from a single plan file
func readPlanEntries(t *testing.T, planPath string) []planEntry {
	t.Helper()
	b, err := os.ReadFile(planPath)
	if err != nil {
		t.Fatalf("read plan file: %v", err)
	}
	if len(b)%12 != 0 {
		t.Fatalf("plan file size not multiple of 12: %d", len(b))
	}

	n := len(b) / 12
	out := make([]planEntry, 0, n)
	for i := 0; i < n; i++ {
		chunk := b[i*12 : (i+1)*12]
		off := int64(binary.LittleEndian.Uint64(chunk[0:8]))
		l := binary.LittleEndian.Uint32(chunk[8:12])
		out = append(out, planEntry{Offset: off, Length: l})
	}
	return out
}

func readAtExact(t *testing.T, f *os.File, off int64, n uint32) []byte {
	t.Helper()
	buf := make([]byte, n)
	readN, err := f.ReadAt(buf, off)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("ReadAt(off=%d,n=%d): %v", off, n, err)
	}
	if uint32(readN) != n {
		t.Fatalf("ReadAt short: got=%d want=%d", readN, n)
	}
	return buf
}

func cleanMockFilesFolder(t *testing.T, folder string) {
	t.Helper()
	target := filepath.Join(mockFilesRootDir, folder)
	_ = os.RemoveAll(target)
	t.Cleanup(func() { _ = os.RemoveAll(target) })
}

func uniqueTestFolder(t *testing.T, base string) string {
	t.Helper()
	testName := strings.ReplaceAll(t.Name(), "/", "_")
	return filepath.Join(base, testName, fmt.Sprintf("%d", time.Now().UnixNano()))
}

// -------------------------
// Test 1: Phase 1
// - local input.jsonl exact copy (line-by-line)
// - plan offsets/lengths are correct (ReadAt matches original line bytes)
// - model_map.json consistency
// -------------------------

func TestPreProcess_BuildsPlansAndModelMap_OffsetsCorrect(t *testing.T) {
	ctx := testLoggerCtx()

	workDir := t.TempDir()
	cfg := config.NewConfig()
	cfg.WorkDir = workDir
	cfg.MaxOpenFiles = 2

	dbClient := mockdb.NewMockBatchDBClient()
	filesClient := mockfiles.NewMockBatchFilesClient()

	// Build remote input in mock files store
	folder := uniqueTestFolder(t, "tenantA/job-inputs")
	cleanMockFilesFolder(t, folder)
	filename := "input.jsonl"
	models := []string{
		"m1", "m2", "m1", "m3",
		"m2", "m2", "m1",
		// include characters requiring sanitization (safe name logic)
		"org/model-A:1",
		"org/model-A?1", // collision candidate with above depending on sanitization
	}

	lines := makeInputLines(models)
	var remoteBuf bytes.Buffer
	for _, ln := range lines {
		remoteBuf.Write(ln)
	}

	if _, err := filesClient.Store(ctx, filename, folder, 0, 0, bytes.NewReader(remoteBuf.Bytes())); err != nil {
		t.Fatalf("files.Store: %v", err)
	}

	// Create DB item for "input file metadata"
	inputFileID := "file-123"
	fileSpec := &batch_utils.FileSpec{Filename: filename, FolderName: folder}
	fileItem := &db.BatchItem{
		ID:   inputFileID,
		Spec: mustJSON(t, fileSpec),
	}
	if _, err := dbClient.DBStore(ctx, fileItem); err != nil {
		t.Fatalf("DBStore file item: %v", err)
	}

	clients := &ProcessorClients{
		database: dbClient,
		files:    filesClient,
		// not needed for preProcessJob:
		priorityQueue: nil,
		status:        nil,
		event:         nil,
		inference:     nil,
	}
	p := NewProcessor(cfg, clients)

	// Build JobInfo (only BatchSpec.InputFileID is used in preProcessJob)
	jobID := "job-abc"
	jobInfo := &batch_utils.JobInfo{
		JobID: jobID,
		BatchJob: &openai.Batch{
			ID: jobID,
			BatchSpec: openai.BatchSpec{
				InputFileID: inputFileID,
			},
			BatchStatusInfo: openai.BatchStatusInfo{
				Status: openai.BatchStatusInProgress,
			},
		},
		TenantID: "tenantA",
	}

	var cancelRequested atomic.Bool
	if err := p.preProcessJob(ctx, jobInfo, &cancelRequested); err != nil {
		t.Fatalf("preProcessJob: %v", err)
	}

	// 1) local input exists and equals remoteBuf
	localInput := p.jobInputFilePath(jobID)
	gotLocal, err := os.ReadFile(localInput)
	if err != nil {
		t.Fatalf("read local input: %v", err)
	}
	if !bytes.Equal(gotLocal, remoteBuf.Bytes()) {
		t.Fatalf("local input != remote input (bytes differ)")
	}

	// 2) model_map.json exists and is consistent
	mapPath := filepath.Join(p.jobRootDir(jobID), "model_map.json")
	mapBytes, err := os.ReadFile(mapPath)
	if err != nil {
		t.Fatalf("read model_map.json: %v", err)
	}
	var mm modelMapFile
	if err := json.Unmarshal(mapBytes, &mm); err != nil {
		t.Fatalf("unmarshal model_map.json: %v", err)
	}

	// quick sanity:
	if mm.LineCount != int64(len(lines)) {
		t.Fatalf("LineCount mismatch: got=%d want=%d", mm.LineCount, len(lines))
	}
	for model, safe := range mm.ModelToSafe {
		back, ok := mm.SafeToModel[safe]
		if !ok || back != model {
			t.Fatalf("model_map not bijective: model=%q safe=%q back=%q ok=%v", model, safe, back, ok)
		}
	}

	// 3) plan files exist and offsets/length map back to exact original line bytes (ReadAt)
	f, err := os.Open(localInput)
	if err != nil {
		t.Fatalf("open local input for ReadAt: %v", err)
	}
	defer f.Close()

	plansDir := filepath.Join(p.jobRootDir(jobID), "plans")
	for safeID := range mm.SafeToModel {
		planPath := filepath.Join(plansDir, safeID+".plan")
		if _, err := os.Stat(planPath); err != nil {
			t.Fatalf("missing plan file for safeID=%q: %v", safeID, err)
		}
		entries := readPlanEntries(t, planPath)

		// For each entry, read input.jsonl slice and ensure it is a valid JSON line ending with '\n'
		for _, e := range entries {
			chunk := readAtExact(t, f, e.Offset, e.Length)
			if len(chunk) == 0 || chunk[len(chunk)-1] != '\n' {
				t.Fatalf("entry does not end with newline: safeID=%q off=%d len=%d", safeID, e.Offset, e.Length)
			}
			trimmed := bytes.TrimSuffix(chunk, []byte{'\n'})
			var req planRequestLine
			if err := json.Unmarshal(trimmed, &req); err != nil {
				t.Fatalf("entry not valid json: safeID=%q off=%d len=%d err=%v", safeID, e.Offset, e.Length, err)
			}
			model := req.Body.Model
			if model == "" {
				t.Fatalf("entry missing body.model: safeID=%q off=%d", safeID, e.Offset)
			}

			// And ensure that this model maps to this safeID in model_map.json
			expectedSafe := mm.ModelToSafe[model]
			if expectedSafe != safeID {
				t.Fatalf("plan safeID mismatch: model=%q expectedSafe=%q gotSafe=%q", model, expectedSafe, safeID)
			}
		}
	}
}

// -------------------------
// Test 2: Cancel integration
// - watchCancel sets cancelRequested and updates status to cancelling exactly once
// - preProcessJob observes cancelRequested and returns ErrCancelled
// - handleCancelled removes jobDir, updates status to cancelled, calls PQDelete (best-effort)
// -------------------------

func TestCancelFlow_DuringPreProcess(t *testing.T) {
	ctx := testLoggerCtx()

	workDir := t.TempDir()
	cfg := config.NewConfig()
	cfg.WorkDir = workDir
	cfg.MaxOpenFiles = 3

	dbClient := mockdb.NewMockBatchDBClient()
	filesClient := mockfiles.NewMockBatchFilesClient()
	statusClient := mockdb.NewMockBatchStatusClient()
	eventClient := mockdb.NewMockBatchEventChannelClient()

	// PQ with spy to assert PQDelete call
	rawPQ := mockdb.NewMockBatchPriorityQueueClient()
	pq := &spyPQ{inner: rawPQ}

	clients := &ProcessorClients{
		database:      dbClient,
		files:         filesClient,
		priorityQueue: pq,
		status:        statusClient,
		event:         eventClient,
		inference:     nil,
	}
	p := NewProcessor(cfg, clients)

	jobID := "job-cancel-1"
	inputFileID := "file-cancel-1"

	// large input for cancellation detect

	models := make([]string, 0, 50000)
	for i := 0; i < 50000; i++ {
		switch {
		case i%3 == 0:
			models = append(models, "mA")
		case i%3 == 1:
			models = append(models, "mB")
		case i%3 == 2:
			models = append(models, "mC")
		}
	}
	lines := makeInputLines(models)

	var remoteBuf bytes.Buffer
	for _, ln := range lines {
		remoteBuf.Write(ln)
	}

	folder := uniqueTestFolder(t, "tenantA/cancel-test")
	cleanMockFilesFolder(t, folder)
	filename := "input.jsonl"
	if _, err := filesClient.Store(ctx, filename, folder, 0, 0, bytes.NewReader(remoteBuf.Bytes())); err != nil {
		t.Fatalf("files.Store: %v", err)
	}

	// DB item for input file spec
	fileSpec := &batch_utils.FileSpec{Filename: filename, FolderName: folder}
	if _, err := dbClient.DBStore(ctx, &db.BatchItem{
		ID:   inputFileID,
		Spec: mustJSON(t, fileSpec),
	}); err != nil {
		t.Fatalf("DBStore file item: %v", err)
	}

	// DB item for job (needed for status updater)
	// StatusUpdater unmarshals dbJob.Status to BatchStatusInfo
	initialStatus := openai.BatchStatusInfo{Status: openai.BatchStatusInProgress}
	jobItem := &db.BatchItem{
		ID:     jobID,
		Spec:   mustJSON(t, openai.BatchSpec{InputFileID: inputFileID}),
		Status: mustJSON(t, initialStatus),
		Tags: db.Tags{
			"tenant": "tenantA",
		},
	}
	if _, err := dbClient.DBStore(ctx, jobItem); err != nil {
		t.Fatalf("DBStore job item: %v", err)
	}

	jobInfo := &batch_utils.JobInfo{
		JobID: jobID,
		BatchJob: &openai.Batch{
			ID: jobID,
			BatchSpec: openai.BatchSpec{
				InputFileID: inputFileID,
			},
			BatchStatusInfo: openai.BatchStatusInfo{
				Status: openai.BatchStatusInProgress,
			},
		},
		TenantID: "tenantA",
	}

	// Create updater used by watchCancel/handleCancelled
	updater := NewStatusUpdater(dbClient, statusClient)

	// Event watcher
	evCh, err := eventClient.ECConsumerGetChannel(ctx, jobID)
	if err != nil {
		t.Fatalf("ECConsumerGetChannel: %v", err)
	}
	defer evCh.CloseFn()

	var cancelRequested atomic.Bool
	var cancellingOnce sync.Once

	// Start watching cancel in background
	go p.watchCancel(ctx, evCh, updater, jobItem, &cancelRequested, &cancellingOnce)

	// Start preProcess in goroutine
	errCh := make(chan error, 1)
	go func() {
		errCh <- p.preProcessJob(ctx, jobInfo, &cancelRequested)
	}()

	// Send cancel quickly (but give preProcess a moment to start and create job dir)
	time.Sleep(10 * time.Millisecond)
	_, _ = eventClient.ECProducerSendEvents(ctx, []db.BatchEvent{
		{ID: jobID, Type: db.BatchEventCancel},
	})
	// Send cancel again to ensure sync.Once behavior (should still update cancelling only once)
	_, _ = eventClient.ECProducerSendEvents(ctx, []db.BatchEvent{
		{ID: jobID, Type: db.BatchEventCancel},
	})

	// Wait for preProcess to return (should be ErrCancelled)
	select {
	case e := <-errCh:
		if !errors.Is(e, ErrCancelled) {
			t.Fatalf("expected ErrCancelled, got: %v", e)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for preProcessJob to return")
	}

	// Ensure DB status was updated to cancelling at least once.
	// We can't count DBUpdate calls without another spy; instead check final stored status becomes cancelling
	// BEFORE handleCancelled runs (watchCancel did it).
	jobs, _, _, err := dbClient.DBGet(ctx, &db.BatchDBQuery{IDs: []string{jobID}}, true, 0, 1)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("DBGet job: err=%v len=%d", err, len(jobs))
	}
	var st openai.BatchStatusInfo
	if err := json.Unmarshal(jobs[0].Status, &st); err != nil {
		t.Fatalf("unmarshal status: %v", err)
	}
	if st.Status != openai.BatchStatusCancelling {
		t.Fatalf("expected status=cancelling after cancel event, got=%s", st.Status)
	}

	// Now handleCancelled should remove job dir + set cancelled + attempt PQDelete
	task := &db.BatchJobPriority{ID: jobID}
	if err := p.handleCancelled(ctx, jobItem, updater, task); err != nil {
		t.Fatalf("handleCancelled: %v", err)
	}

	// job dir should be removed (best-effort but should normally succeed)
	jobDir := p.jobRootDir(jobID)
	if _, err := os.Stat(jobDir); err == nil {
		t.Fatalf("expected job dir removed, still exists: %s", jobDir)
	}

	// status should now be cancelled in DB
	jobs, _, _, err = dbClient.DBGet(ctx, &db.BatchDBQuery{IDs: []string{jobID}}, true, 0, 1)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("DBGet job after cancel: err=%v len=%d", err, len(jobs))
	}
	st = openai.BatchStatusInfo{}
	if err := json.Unmarshal(jobs[0].Status, &st); err != nil {
		t.Fatalf("unmarshal status after cancel: %v", err)
	}
	if st.Status != openai.BatchStatusCancelled {
		t.Fatalf("expected status=cancelled, got=%s", st.Status)
	}
	if st.CancelledAt == nil {
		t.Fatalf("expected CancelledAt to be set")
	}

	// PQDelete should have been attempted exactly once by handleCancelled
	if pq.DeleteCalls() != 1 {
		t.Fatalf("expected PQDelete called once, got=%d", pq.DeleteCalls())
	}
}
