package pipeline

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// PendingRequests tracks in-flight requests by RequestID.
// The dispatcher stores entries before dispatching; the collector
// resolves them when results arrive.
type PendingRequests struct {
	m     sync.Map
	count atomic.Int64
}

func (p *PendingRequests) Store(msg RequestItem) {
	p.m.Store(msg.RequestID, msg)
	p.count.Add(1)
}

// Resolve enriches a result with request metadata. Returns true if the
// result is accepted: either it already has metadata (cancels, inline errors)
// or it was found in the pending map (async inference results).
// Returns false only for broadcast results that belong to another job.
func (p *PendingRequests) Resolve(result *ResultItem) bool {
	if result.CustomID != "" {
		return true
	}
	val, ok := p.m.LoadAndDelete(result.RequestID)
	if !ok {
		return false
	}
	p.count.Add(-1)
	msg := val.(RequestItem)
	result.CustomID = msg.CustomID
	result.ModelID = msg.ModelID
	return true
}

// Wait blocks until all pending entries are resolved or ctx is cancelled.
func (p *PendingRequests) Wait(ctx context.Context) {
	for p.count.Load() > 0 {
		select {
		case <-ctx.Done():
			return
		case <-time.After(10 * time.Millisecond):
		}
	}
}
