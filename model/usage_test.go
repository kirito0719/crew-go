package model_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"crew-go/model"
	"crew-go/schema"
)

func TestFailureUsagePreservesErrorAndSnapshot(t *testing.T) {
	u := &schema.MessageUsage{InputTokens: 10, OutputTokens: 6, ThinkTokens: 4, TotalTokens: 16}
	base := &model.APIError{Provider: "provider", Err: model.ErrContentFiltered}
	err := model.WithFailureUsage(base, u, "provider", "actual-model")
	u.InputTokens = 999
	var api *model.APIError
	if !errors.Is(err, model.ErrContentFiltered) || !errors.As(err, &api) || api != base || err.Error() != base.Error() {
		t.Fatalf("error contract lost: %v", err)
	}
	got := model.UsageFromError(fmt.Errorf("wrapped: %w", err))
	if got == nil || got.Usage.InputTokens != 10 || got.Usage.OutputTokens != 6 || got.Model != "actual-model" {
		t.Fatalf("usage snapshot: %+v", got)
	}
	got.Usage.InputTokens = 888
	if model.UsageFromError(err).Usage.InputTokens != 10 || model.WithFailureUsage(err, u, "other", "other") != err {
		t.Fatal("snapshot mutated or cumulative snapshot replaced")
	}
	if model.WithFailureUsage(nil, u, "", "") != nil || model.WithFailureUsage(base, nil, "", "") != base || model.UsageFromError(base) != nil {
		t.Fatal("unknown usage must remain unknown")
	}
}

func TestFailureUsageReceiptAndRetryIsolation(t *testing.T) {
	err := model.WithFailureUsage(context.Canceled, &schema.MessageUsage{InputTokens: 3}, "p", "m")
	var first, second atomic.Int32
	ctx := model.WithFailedUsageObserver(t.Context(), func(_ context.Context, u model.FailedUsage) { first.Add(u.Usage.InputTokens) })
	ctx = model.WithFailedUsageObserver(ctx, func(_ context.Context, u model.FailedUsage) { second.Add(u.Usage.InputTokens) })
	reported := model.ReportFailedUsage(ctx, err)
	reported = model.ReportFailedUsage(ctx, fmt.Errorf("outer: %w", reported))
	if !errors.Is(reported, context.Canceled) || first.Load() != 3 || second.Load() != 3 {
		t.Fatal("same attempt counted twice")
	}
	model.ReportFailedUsage(ctx, err)
	if first.Load() != 6 || second.Load() != 6 {
		t.Fatal("new attempt reusing the same error was lost")
	}
	model.ReportFailedUsage(model.ResetFailedUsageObserver(ctx), err)
	if first.Load() != 6 {
		t.Fatal("nested model scope leaked")
	}
	if !errors.Is(model.ReportFailedUsage(t.Context(), err), context.Canceled) {
		t.Fatal("no observer lost error chain")
	}
	if model.ReportFailedUsage(ctx, nil) != nil {
		t.Fatal("nil error changed")
	}
}

func TestFailureUsageParallelObservers(t *testing.T) {
	var tokens atomic.Int32
	ctx := model.WithFailedUsageObserver(t.Context(), func(_ context.Context, u model.FailedUsage) { tokens.Add(u.Usage.InputTokens) })
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			model.ReportFailedUsage(ctx, model.WithFailureUsage(errors.New("failed"), &schema.MessageUsage{InputTokens: 2}, "", ""))
		})
	}
	wg.Wait()
	if tokens.Load() != 32 {
		t.Fatalf("parallel usage = %d", tokens.Load())
	}
}

func TestFailureUsageObserverExtensionAndChildReceipt(t *testing.T) {
	var a, b int32
	ctxA := model.WithFailedUsageObserver(t.Context(), func(_ context.Context, u model.FailedUsage) { a += u.Usage.InputTokens })
	err := model.ReportFailedUsage(ctxA, model.WithFailureUsage(errors.New("failed"), &schema.MessageUsage{InputTokens: 3}, "p", "m"))
	ctxB := model.WithFailedUsageObserver(ctxA, func(_ context.Context, u model.FailedUsage) { b += u.Usage.InputTokens })
	err = model.ReportFailedUsage(ctxB, err)
	model.ReportFailedUsage(ctxA, err)
	if a != 3 || b != 3 {
		t.Fatalf("one failure delivered a=%d b=%d", a, b)
	}
	parent := model.WithFailedUsageObserver(model.ResetFailedUsageObserver(t.Context()), func(_ context.Context, u model.FailedUsage) { a += u.Usage.InputTokens })
	model.ReportFailedUsage(parent, err)
	if a != 3 {
		t.Fatal("child attempt was charged again by parent")
	}
}
