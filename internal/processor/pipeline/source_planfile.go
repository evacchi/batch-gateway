package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/go-logr/logr"
	"github.com/google/uuid"

	"encoding/binary"
	"io"

	"github.com/llm-d/llm-d-batch-gateway/internal/processor/config"
	batch_types "github.com/llm-d/llm-d-batch-gateway/internal/shared/types"
	"github.com/llm-d/llm-d-batch-gateway/pkg/clients/inference"
)

const (
	sloTTFTMSHeader          = "x-slo-ttft-ms"
	inferenceObjectiveHeader = "x-gateway-inference-objective"
	fairnessIDHeader         = "x-gateway-inference-fairness-id"
)

// PlanEntry is a single entry in a plan file (16 bytes).
type PlanEntry struct {
	Offset     int64
	Length     uint32
	PrefixHash uint32
}

const planEntrySize = 16

func readPlanEntries(path string) ([]PlanEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open plan file %s: %w", path, err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat plan file: %w", err)
	}
	if info.Size()%planEntrySize != 0 {
		return nil, fmt.Errorf("plan file size %d not a multiple of %d", info.Size(), planEntrySize)
	}

	entries := make([]PlanEntry, 0, info.Size()/planEntrySize)
	var buf [planEntrySize]byte
	for {
		_, err := io.ReadFull(f, buf[:])
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read plan entry: %w", err)
		}
		entries = append(entries, PlanEntry{
			Offset:     int64(binary.LittleEndian.Uint64(buf[0:8])),
			Length:     binary.LittleEndian.Uint32(buf[8:12]),
			PrefixHash: binary.LittleEndian.Uint32(buf[12:16]),
		})
	}
	return entries, nil
}

// ModelMap maps model IDs to safe filenames and tracks counts.
type ModelMap struct {
	SafeToModel   map[string]string `json:"safe_to_model"`
	LineCount     int64             `json:"line_count"`
	RejectedCount int64             `json:"rejected_count"`
}

// UnsentEntry represents a request that was never dispatched.
type UnsentEntry struct {
	RequestID string
	CustomID  string
}

// PlanFileSource reads plan files and input JSONL to produce RequestItems.
// After Produce returns, call Unproduced() to get the count of entries
// that were not produced (e.g. due to context cancellation).
type PlanFileSource struct {
	inputFile          *os.File
	plansDir           string
	modelMap           *ModelMap
	resolver           *inference.GatewayResolver
	cfg                *config.ProcessorConfig
	passThroughHeaders map[string]string
	sloDeadline        time.Time
	hasSLO             bool
	tenantID           string
	logger             logr.Logger
}

var _ RequestSource = (*PlanFileSource)(nil)

type PlanFileSourceConfig struct {
	InputFile          *os.File
	PlansDir           string
	ModelMap           *ModelMap
	Resolver           *inference.GatewayResolver
	Cfg                *config.ProcessorConfig
	PassThroughHeaders map[string]string
	SLODeadline        time.Time
	HasSLO             bool
	TenantID           string
	Logger             logr.Logger
}

func NewPlanFileSource(cfg PlanFileSourceConfig) *PlanFileSource {
	return &PlanFileSource{
		inputFile:          cfg.InputFile,
		plansDir:           cfg.PlansDir,
		modelMap:           cfg.ModelMap,
		resolver:           cfg.Resolver,
		cfg:                cfg.Cfg,
		passThroughHeaders: cfg.PassThroughHeaders,
		sloDeadline:        cfg.SLODeadline,
		hasSLO:             cfg.HasSLO,
		tenantID:           cfg.TenantID,
		logger:             cfg.Logger,
	}
}

func (s *PlanFileSource) Produce(_ context.Context, out chan<- RequestItem) error {
	defer close(out)

	for safeModelID, modelID := range s.modelMap.SafeToModel {
		planPath := filepath.Join(s.plansDir, safeModelID+".plan")
		entries, err := readPlanEntries(planPath)
		if err != nil {
			return fmt.Errorf("read plan for model %s: %w", modelID, err)
		}

		for _, entry := range entries {
			item, err := s.readEntry(entry, modelID)
			if err != nil {
				return err
			}
			if item == nil {
				continue
			}
			out <- *item
		}
	}

	return nil
}

func (s *PlanFileSource) readEntry(entry PlanEntry, modelID string) (*RequestItem, error) {
	buf := make([]byte, entry.Length)
	if _, err := s.inputFile.ReadAt(buf, entry.Offset); err != nil {
		return nil, fmt.Errorf("read input at offset %d: %w", entry.Offset, err)
	}

	trimmed := bytes.TrimSuffix(buf, []byte{'\n'})
	var req batch_types.Request
	if err := json.Unmarshal(trimmed, &req); err != nil {
		s.logger.Error(err, "Failed to parse request line, skipping")
		return nil, nil
	}

	headers := maps.Clone(s.passThroughHeaders)
	headers = s.mergeHeaders(headers, modelID)

	return &RequestItem{
		RequestID: fmt.Sprintf("batch_req_%s", uuid.NewString()),
		CustomID:  req.CustomID,
		ModelID:   modelID,
		Endpoint:  req.URL,
		Body:      req.Body,
		Headers:   headers,
	}, nil
}

func (s *PlanFileSource) mergeHeaders(headers map[string]string, modelID string) map[string]string {
	if headers == nil {
		headers = make(map[string]string)
	}

	if s.hasSLO {
		ms := time.Until(s.sloDeadline).Milliseconds()
		if ms >= 0 {
			headers[sloTTFTMSHeader] = strconv.FormatInt(ms, 10)
		}
	}

	if obj := s.cfg.InferenceObjectiveFor(modelID); obj != "" {
		headers[inferenceObjectiveHeader] = obj
	}

	if s.cfg.SendFairnessHeader && s.tenantID != "" {
		if _, exists := headers[fairnessIDHeader]; !exists {
			headers[fairnessIDHeader] = s.tenantID
		}
	}

	return headers
}
