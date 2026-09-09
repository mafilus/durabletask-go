package backend

import (
	"context"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// WithRequireCompletionTokens rejects workers that do not echo the work item's
// completion token. By default empty tokens remain accepted for legacy workers;
// their stale responses cannot be distinguished from a current response.
func WithRequireCompletionTokens() grpcExecutorOptions {
	return func(g *grpcExecutor) { g.requireCompletionTokens = true }
}

// transportAttempt serializes terminal transitions for one dispatch. The map
// reservation lasts until Execute returns and its backend waiter has finished,
// so a late cleanup cannot cancel a successor registered under the same key.
// Backend calls hold only this attempt's lock, never the executor lifecycle lock.
type transportAttempt struct {
	mu       sync.Mutex
	token    string
	streamID atomic.Pointer[string]
	terminal atomic.Bool
	cancel   context.CancelFunc
}

func (p *transportAttempt) stop() {
	p.terminal.Store(true)
	p.cancel()
}

// finish keeps the map reservation alive until an in-flight backend completion
// returns. Cancellation itself never waits on a backend RPC, so stream cleanup
// and Shutdown remain responsive even if that backend ignores cancellation.
func (p *transportAttempt) finish() {
	p.stop()
	p.mu.Lock()
	p.mu.Unlock()
}

func (p *transportAttempt) stopForStream(id string) {
	if owner := p.streamID.Load(); owner != nil && *owner == id {
		p.stop()
	}
}

func (p *transportAttempt) claim(streamID, token string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.terminal.Load() || p.token != token {
		return false
	}
	p.streamID.Store(&streamID)
	return true
}

func (p *transportAttempt) complete(token string, strict bool, deliver func() error) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.terminal.Load() || p.streamID.Load() == nil {
		return status.Error(codes.NotFound, "attempt is no longer pending or has not been dispatched")
	}
	if (token == "" && strict) || (token != "" && token != p.token) {
		return status.Error(codes.FailedPrecondition, "invalid completion token")
	}
	if err := deliver(); err != nil {
		return err
	}
	p.terminal.Store(true)
	return nil
}

func (g *grpcExecutor) initShutdown() {
	g.shutdownOnce.Do(func() { g.shutdownCh = make(chan struct{}) })
}

func (g *grpcExecutor) registerAttempt(registry *sync.Map, key, attempt any) error {
	g.lifecycleMu.Lock()
	defer g.lifecycleMu.Unlock()
	g.initShutdown()
	if g.shuttingDown {
		return errShuttingDown
	}
	if _, loaded := registry.LoadOrStore(key, attempt); loaded {
		return status.Error(codes.AlreadyExists, "an execution attempt is already pending")
	}
	return nil
}
