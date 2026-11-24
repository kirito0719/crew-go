package agui

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/internal/toolresult"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

type softLandingDisplayModel struct{}

func (softLandingDisplayModel) Generate(context.Context, []*schema.Message, ...model.Option) (*schema.Message, error) {
	return schema.AssistantToolCallMessage([]schema.ToolCall{{
		ID: "provider", Type: "function", Function: schema.FunctionCall{Name: "noop", Arguments: `{}`},
	}}), nil
}

func (softLandingDisplayModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}

func (m softLandingDisplayModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) {
	return m, nil
}

type softLandingDisplayHook struct{ hook.NoOpHooks }

func (softLandingDisplayHook) OnToolStart(ctx context.Context, info *hook.AgentInfo, _ *schema.ToolInput) (context.Context, error) {
	if info.Round == 2 {
		return ctx, errors.New("soft landing unexpectedly entered the business tool hook")
	}
	return ctx, nil
}

func TestSoftLandingToolResultMatchesPreviouslyDisplayedCall(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		name := "fresh_run"
		if legacy {
			name = "legacy_checkpoint"
		}
		t.Run(name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			sse, ok := NewSSEWriter(recorder)
			if !ok {
				t.Fatal("SSE recorder missing flusher")
			}
			adapter := newSSEAdapter(sse, "wire-run", nil)
			cp := compose_v2.NewInMemoryCheckpointer()
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			ctx = toolresult.WithObserver(compose_v2.WithCheckpointerCtx(ctx, cp), &adapter.resultObserver)

			var calls atomic.Int32
			tk := tool.NewToolkit()
			tk.Register(tool.NewFuncTool("noop", "noop", map[string]any{"type": "object"}, func(context.Context, map[string]any) (*tool.Result, error) {
				calls.Add(1)
				return tool.Success("done"), nil
			}))
			agent := agent_v2.NewBasicAgent(agent_v2.WithName("soft_landing_display"), agent_v2.WithModel(softLandingDisplayModel{}),
				agent_v2.WithToolkit(tk), agent_v2.WithMaxRounds(1), agent_v2.WithHooks(hook.NewChainHooks(adapter, softLandingDisplayHook{})))

			wantStarts := 1
			var wantID string
			wantResult := "done"
			var err error
			if legacy {
				// 基线 05f5bb9 在第二轮展示工具后、业务确认暂停时保存的真实存档。
				var fixture struct {
					CallID     string          `json:"call_id"`
					Checkpoint json.RawMessage `json:"checkpoint"`
				}
				wire, readErr := os.ReadFile("testdata/soft_landing_display.json")
				if readErr != nil {
					t.Fatal(readErr)
				}
				if decodeErr := json.Unmarshal(wire, &fixture); decodeErr != nil {
					t.Fatal(decodeErr)
				}
				saved, decodeErr := compose_v2.UnmarshalCheckpoint(fixture.Checkpoint)
				if decodeErr != nil || saved == nil || len(saved.PendingInterrupts) != 1 {
					t.Fatalf("应取回复用的待答存档，错误 %v", decodeErr)
				}
				if saveErr := cp.SaveNS(ctx, saved.ID, "", saved); saveErr != nil {
					t.Fatal(saveErr)
				}
				ctx = compose_v2.WithSessionIDCtx(ctx, saved.ID)
				answer := agent_v2.WithResumeUserInputs(map[string]*schema.Message{
					saved.PendingInterrupts[0].InterruptID: schema.UserMessage("yes"),
				})
				_, err = agent.Resume(ctx, saved.ID, answer)
				wantStarts, wantID, wantResult = 0, fixture.CallID, "tool error: round_budget_exhausted"
			} else {
				_, err = agent.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("go"))
			}
			var exhausted *compose_v2.MaxIterationsError
			if !errors.As(err, &exhausted) {
				t.Fatalf("软降落应以轮次耗尽结束，实际 %v", err)
			}
			starts, results := 0, 0
			for _, line := range strings.Split(recorder.Body.String(), "\n") {
				if body, ok := strings.CutPrefix(line, "data: "); ok {
					var event struct {
						Type    EventType `json:"type"`
						CallID  string    `json:"toolCallId"`
						Content string    `json:"content"`
					}
					if err := json.Unmarshal([]byte(body), &event); err != nil {
						t.Fatal(err)
					}
					switch event.Type {
					case EventTypeToolCallStart:
						starts++
						if !legacy {
							wantID = event.CallID
						}
					case EventTypeToolCallResult:
						results++
						if wantID == "" || event.CallID != wantID || event.Content != wantResult {
							t.Errorf("工具结果实际 %+v，期望 ID %q，内容 %q", event, wantID, wantResult)
						}
					}
				}
			}
			if starts != wantStarts || results != 1 || int(calls.Load()) != wantStarts {
				t.Fatalf("预计展示 %d 次、提交结果一次、实际执行 %d 次，实际展示 %d 次、结果 %d 次、执行 %d 次",
					wantStarts, wantStarts, starts, results, calls.Load())
			}
		})
	}
}
