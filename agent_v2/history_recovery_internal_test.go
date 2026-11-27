package agent_v2

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/schema"
	"crew-go/store"
)

type historyRecoveryFault struct {
	HistoryTailAppender
	fail atomic.Bool
}

func (f *historyRecoveryFault) HistoryAppendOwner() HistoryStore { return f }

func (f *historyRecoveryFault) AppendAfter(ctx context.Context, key HistoryKey, after HistoryCursor, msgs []*schema.Message, boundary int) (HistoryCursor, bool, error) {
	if f.fail.Load() {
		return "", false, io.ErrUnexpectedEOF
	}
	return f.HistoryTailAppender.AppendAfter(ctx, key, after, msgs, boundary)
}

func TestHistoryRecoveryKeepsMessagesWhenCompactionTailChanged(t *testing.T) {
	ctx := t.Context()
	key := HistoryKey{Owner: "tenant", ConvID: "compaction"}
	base := NewInMemoryHistoryStore()
	fault := &historyRecoveryFault{HistoryTailAppender: base}
	journal := store.NewInMemoryStore()
	recoverable := NewRecoverableHistoryStore(fault, journal, "history")
	seed, ok, err := base.AppendAfter(ctx, key, "", []*schema.Message{schema.UserMessage("original")}, -1)
	if err != nil || !ok {
		t.Fatalf("seed: %q %v %v", seed, ok, err)
	}
	fault.fail.Store(true)
	coverage := &historyCoverage{store: recoverable, tail: seed, valid: true}
	batch := historyWrite{
		msgs:     []*schema.Message{schema.SystemMessage("stale summary"), schema.AssistantMessage("original reply")},
		coverage: coverage, boundary: 0,
	}
	if err = batch.append(ctx, recoverable, key); !errors.Is(err, ErrHistoryWritePending) {
		t.Fatalf("expected staged batch, got %v", err)
	}
	if err = base.Append(ctx, key, []*schema.Message{schema.UserMessage("concurrent input")}); err != nil {
		t.Fatal(err)
	}
	fault.fail.Store(false)
	page, err := recoverable.Load(ctx, key, HistoryQuery{})
	if err != nil || len(page.Messages) != 3 || len(page.VerifiedBoundaries) != 0 ||
		page.Messages[1].Content != "concurrent input" || page.Messages[2].Content != "original reply" {
		t.Fatalf("stale boundary affected committed messages: %+v err=%v", page, err)
	}
}

type historyProofDeclined struct{ *InMemoryHistoryStore }

func (s *historyProofDeclined) HistoryAppendOwner() HistoryStore           { return s }
func (s *historyProofDeclined) HistoryUnverifiedAppendOwner() HistoryStore { return s }

func (s *historyProofDeclined) AppendAfter(context.Context, HistoryKey, HistoryCursor, []*schema.Message, int) (HistoryCursor, bool, error) {
	return "", false, nil
}

func (s *historyProofDeclined) AppendUnverifiedAfter(ctx context.Context, key HistoryKey, tail HistoryCursor, msgs []*schema.Message) (HistoryCursor, bool, error) {
	return s.InMemoryHistoryStore.AppendAfter(ctx, key, tail, msgs, -1)
}

func TestHistoryRecoveryKeepsMessagesWhenBackendDeclinesProof(t *testing.T) {
	ctx := t.Context()
	key := HistoryKey{Owner: "tenant", ConvID: "proof-declined"}
	base := &historyProofDeclined{NewInMemoryHistoryStore()}
	hs := NewRecoverableHistoryStore(base, store.NewInMemoryStore(), "history")
	coverage := &historyCoverage{store: hs, valid: true}
	batch := historyWrite{
		msgs:     []*schema.Message{schema.SystemMessage("candidate summary"), schema.AssistantMessage("concrete reply")},
		coverage: coverage, boundary: 0,
	}
	if err := batch.append(ctx, hs, key); err != nil {
		t.Fatal(err)
	}
	page, err := hs.Load(ctx, key, HistoryQuery{})
	if err != nil || coverage.valid || len(page.Messages) != 1 || page.Messages[0].Content != "concrete reply" ||
		len(page.VerifiedBoundaries) != 0 {
		t.Fatalf("declined proof changed the ordinary append: %+v err=%v", page, err)
	}
}

type stagedHistoryJournal struct {
	store.Store
	staged  chan struct{}
	release chan struct{}
}

func (s *stagedHistoryJournal) Put(ctx context.Context, ns store.Namespace, key string, value any, opts ...store.PutOption) error {
	if err := s.Store.Put(ctx, ns, key, value, opts...); err != nil {
		return err
	}
	close(s.staged)
	select {
	case <-s.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestHistoryRecoveryReturnsBatchCursorAfterPeerAppends(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	key := HistoryKey{Owner: "tenant", ConvID: "cursor"}
	base, journal := NewInMemoryHistoryStore(), store.NewInMemoryStore()
	barrier := &stagedHistoryJournal{Store: journal, staged: make(chan struct{}), release: make(chan struct{})}
	first := NewRecoverableHistoryStore(base, barrier, "history")
	peer := NewRecoverableHistoryStore(base, journal, "history")
	type outcome struct {
		cursor HistoryCursor
		ok     bool
		err    error
	}
	result := make(chan outcome, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		cursor, ok, err := first.AppendAfter(ctx, key, "", []*schema.Message{schema.UserMessage("first"), nil}, -1)
		result <- outcome{cursor: cursor, ok: ok, err: err}
	}()
	defer func() {
		cancel()
		<-done
	}()
	select {
	case <-barrier.staged:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := peer.Recover(ctx, key); err != nil {
		t.Fatal(err)
	}
	page, err := base.Load(ctx, key, HistoryQuery{Limit: 1})
	if err != nil || page.Start == "" {
		t.Fatalf("missing committed tail: %+v err=%v", page, err)
	}
	if err := peer.Append(ctx, key, []*schema.Message{schema.UserMessage("next")}); err != nil {
		t.Fatal(err)
	}
	close(barrier.release)
	got := <-result
	if got.err != nil || !got.ok || got.cursor != page.Start {
		t.Fatalf("expected the original batch cursor %q, got %+v", page.Start, got)
	}
	all, err := base.Load(ctx, key, HistoryQuery{})
	if err != nil || len(all.Messages) != 3 || all.Messages[1] != nil ||
		all.Messages[0].Content != "first" || all.Messages[2].Content != "next" {
		t.Fatalf("peer recovery duplicated or reordered history: %+v err=%v", all, err)
	}
}
