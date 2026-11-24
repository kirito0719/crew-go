package agent_v2_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/tool"
)

// alwaysInterruptModel 第 1 次 Generate 即触发 interrupt（通过 hook）。
// 但更可靠的方式是构造一个 sub-agent，其 hook 立刻 throw InterruptError。
type interruptingAgent struct {
	name string
}

func (a *interruptingAgent) Name() string        { return a.name }
func (a *interruptingAgent) Description() string { return "always interrupts" }
func (a *interruptingAgent) Run(_ context.Context, _ *agent_v2.RunSession, _ *schema.Message, _ ...agent_v2.RunOption) ([]*agent_v2.AgentEvent, error) {
	return nil, &compose_v2.InterruptError{
		Reason: "sub-agent forced interrupt",
		Query: &compose_v2.AskQuery{
			InterruptID: "sub_ask",
			Question:    "sub-agent says hi",
		},
	}
}
func (a *interruptingAgent) Resume(_ context.Context, _ string, _ ...agent_v2.ResumeOption) ([]*agent_v2.AgentEvent, error) {
	return nil, fmt.Errorf("not impl")
}

var _ agent_v2.Agent = (*interruptingAgent)(nil)

// TestAgentAsTool_PropagatesInterruptError 验证 audit Bug #1 修复：
// sub-agent 抛 InterruptError 时 AgentAsTool.Call 必须 propagate 而非吞为 tool.Failure。
//
// 链路：
//   - parent BasicAgent 调 ask_sub tool（包了 interruptingAgent）
//   - sub-agent.Run 抛 InterruptError
//   - AgentAsTool.Call 之前 fix：返 tool.Failure 静默吞 → parent 当 tool 成功继续 ReAct
//   - 修复后：返 (nil, InterruptError) → parent 框架 SaveNS cp + propagate 给 caller
//
// sanity-revert：把 agent_tool.go IsInterrupt 检查去掉，本测试应 fail——
// parent.Run 返 nil err 而非 InterruptError。
func TestAgentAsTool_PropagatesInterruptError(t *testing.T) {
	subAgent := &interruptingAgent{name: "sub_interrupting"}
	tk := tool.NewToolkit()
	tk.Register(agent_v2.AgentAsTool(subAgent, agent_v2.WithAgentToolDescription("always interrupts")))

	// parent BasicAgent：用 stub model 调 ask_sub tool 一次
	tc := schema.ToolCall{ID: "tc_1", Function: schema.FunctionCall{Name: "sub_interrupting", Arguments: `{"input":"go"}`}}
	resp1 := schema.AssistantToolCallMessage([]schema.ToolCall{tc})
	parentModel := &stubModel{responses: []*schema.Message{resp1}}

	parent := agent_v2.NewBasicAgent(
		agent_v2.WithName("parent"),
		agent_v2.WithModel(parentModel),
		agent_v2.WithToolkit(tk),
		agent_v2.WithMaxRounds(3),
	)

	cper := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(context.Background(), cper)
	sess := agent_v2.NewRunSession()

	_, err := parent.Run(ctx, sess, schema.UserMessage("delegate"))
	if err == nil {
		t.Fatal("parent.Run 应返 error（sub-agent InterruptError 应 propagate）")
	}
	ie, isInt := agent_v2.IsInterrupt(err)
	if !isInt {
		t.Fatalf("parent.Run 返的 err 应能 unwrap 为 InterruptError，实际 %v", err)
	}
	if ie.Query == nil || ie.Query.InterruptID == "" {
		t.Fatalf("InterruptError 应含 Query.InterruptID，实际 %+v", ie)
	}

	// 验证 cp 已 SaveNS（HITL 端到端不变量）
	saved, _ := cper.LoadNS(context.Background(), sess.RunID, "")
	if saved == nil {
		t.Fatalf("sub-agent InterruptError 后 parent 应 SaveNS cp，实际未保存\n" +
			"sanity-revert：agent_tool.go 去掉 IsInterrupt 检查，本断言 fail")
	}
}

func TestAgentAsToolKeepsRuntimeErrorsOutOfModelInput(t *testing.T) {
	private := errors.New("dependency error with private connection details")
	for _, mapped := range []bool{false, true} {
		t.Run(fmt.Sprintf("mapped=%t", mapped), func(t *testing.T) {
			const name = "inspect"
			child := mkEchoGraph(t, name, func(_ context.Context, in *schema.Message) (*schema.Message, error) {
				if in.Content != "run inspection" {
					t.Fatalf("子任务收到 %q", in.Content)
				}
				return nil, private
			})
			opts := []agent_v2.AgentAsToolOption{agent_v2.WithAgentToolDescription("核验检查项")}
			want := "子 Agent 未能完成调用"
			mappedCalls := 0
			if mapped {
				want = "暂时无法核验，请选择其他记录"
				opts = append(opts, agent_v2.WithAgentToolErrorMapper(func(_ context.Context, err error) *tool.Error {
					if !errors.Is(err, private) {
						t.Fatalf("业务映射器未收到原错误，实际 %v", err)
					}
					mappedCalls++
					return &tool.Error{Code: "inspection_unavailable", Message: want}
				}))
			}
			kit := tool.NewToolkit()
			kit.Register(agent_v2.AgentAsTool(child, opts...))
			m := &promptRecorder{resp: []*schema.Message{schema.AssistantToolCallMessage([]schema.ToolCall{
				{ID: "inspection", Function: schema.FunctionCall{Name: name, Arguments: `{"input":"run inspection"}`}},
			}), schema.AssistantMessage("已收到工具失败")}}
			parent := agent_v2.NewBasicAgent(agent_v2.WithName("parent"), agent_v2.WithModel(m), agent_v2.WithToolkit(kit))
			events, err := parent.Run(t.Context(), agent_v2.NewRunSession(), schema.UserMessage("核验"))
			if err != nil || agent_v2.FinalMessage(events) == nil {
				t.Fatalf("父 agent 应收到工具失败后继续，错误 %v", err)
			}
			for _, msg := range m.call(1) {
				if schema.GetToolResultID(msg) == "inspection" {
					if !strings.Contains(msg.Content, want) || strings.Contains(msg.Content, private.Error()) || mapped && mappedCalls != 1 {
						t.Fatalf("模型收到 %q，映射器调用 %d 次", msg.Content, mappedCalls)
					}
					return
				}
			}
			t.Fatal("父模型没有拿到工具结果")
		})
	}
}
