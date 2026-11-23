package observability_test

import (
	"context"
	"errors"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/observability"
	"crew-go/schema"
)

type chargedFailureModel struct {
	calls      int
	alwaysFail bool
}

func (m *chargedFailureModel) Generate(context.Context, []*schema.Message, ...model.Option) (*schema.Message, error) {
	m.calls++
	if m.calls == 1 || m.alwaysFail {
		return nil, model.WithFailureUsage(errors.New("request failed"), &schema.MessageUsage{InputTokens: 31, OutputTokens: 9, ThinkTokens: 4, CachedInputTokens: 7, TotalTokens: 40}, "mock", "failed-model")
	}
	out := schema.AssistantMessage("done")
	out.Metadata = &schema.MessageMetadata{System: "mock", Model: "success-model", Usage: &schema.MessageUsage{InputTokens: 5, OutputTokens: 2, TotalTokens: 7}}
	return out, nil
}
func (m *chargedFailureModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m *chargedFailureModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

func TestFailedUsageCountedOnErrorAndRetrySuccess(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "retry-success", true: "terminal-error"}[fail], func(t *testing.T) {
			h := newHarness(t)
			m := &chargedFailureModel{alwaysFail: fail}
			ctx, opts := h.obs.Install(t.Context(), agent_v2.WithName("charged"), agent_v2.WithModel(m), agent_v2.WithModelRetry(compose_v2.SimpleRetry(1)))
			_, err := agent_v2.NewBasicAgent(opts...).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("x"))
			if (err != nil) != fail {
				t.Fatalf("unexpected outcome: %v", err)
			}
			wantIn, wantOut, wantThink := int64(36), int64(11), int64(4)
			if fail {
				wantIn, wantOut, wantThink = 62, 18, 8
			}
			rm := h.collect(t)
			if n := sumInt(t, rm, observability.MetricUsageInputTokens, nil); n != wantIn {
				t.Fatalf("input=%d want=%d", n, wantIn)
			}
			if n := sumInt(t, rm, observability.MetricUsageOutputTokens, nil); n != wantOut {
				t.Fatalf("output=%d want=%d", n, wantOut)
			}
			if n := sumInt(t, rm, observability.MetricUsageOutputTokens, map[string]string{"gen_ai.response.model": "failed-model", "gen_ai.token.modality": "unknown"}); n != wantThink/4*9 {
				t.Fatalf("failed output model/modality total=%d", n)
			}
			if n := sumInt(t, rm, observability.MetricUsageReasoningTokens, nil); n != wantThink {
				t.Fatalf("reasoning=%d want=%d", n, wantThink)
			}
			found := 0
			for _, span := range h.spans() {
				for _, event := range span.Events() {
					if event.Name == observability.EventFailedModelUsage {
						found++
					}
				}
			}
			wantEvents := 1
			if fail {
				wantEvents = 2
			}
			if found != wantEvents {
				t.Fatalf("failed usage events=%d want=%d", found, wantEvents)
			}
			h.assertAllEnded(t)
		})
	}
}
