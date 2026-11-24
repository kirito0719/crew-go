package agent_v2_test

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

type toolIDEncodingHooks struct {
	hook.NoOpHooks
	rewrite *schema.Message
	ends    atomic.Int32
	starts  atomic.Int32
}

func (h *toolIDEncodingHooks) OnModelEnd(ctx context.Context, _ *hook.AgentInfo, out *schema.ModelOutput) (context.Context, error) {
	h.ends.Add(1)
	if h.rewrite != nil {
		out.Message = h.rewrite
	}
	return ctx, nil
}

func (h *toolIDEncodingHooks) OnToolStart(ctx context.Context, _ *hook.AgentInfo, _ *schema.ToolInput) (context.Context, error) {
	h.starts.Add(1)
	return ctx, nil
}

func TestToolCallIDEncodingRejectsInvalidBatch(t *testing.T) {
	for _, path := range []string{"generate", "generate-auto", "single-chunk", "multi-chunk", "model-end-rewrite"} {
		t.Run(path, func(t *testing.T) {
			body := &recordTool{name: "check"}
			tk := tool.NewToolkit()
			tk.Register(body)
			var calls []schema.ToolCall
			for _, id := range []string{"先行", "id\xff", "id\xfe", "id\uFFFD"} {
				calls = append(calls, schema.ToolCall{ID: id, Function: schema.FunctionCall{Name: body.name, Arguments: "{}"}})
			}
			bad := schema.AssistantToolCallMessage(calls)
			hooks := &toolIDEncodingHooks{}
			mode := agent_v2.ApprovalAsk
			var llm model.ChatModel
			switch path {
			case "generate":
				llm = newStubModel([]*schema.Message{bad})
			case "generate-auto":
				mode = agent_v2.ApprovalAuto
				llm = newStubModel([]*schema.Message{bad, schema.AssistantMessage("完成")})
			case "single-chunk":
				llm = &streamProbeModel{reader: schema.NewSingleChunkReader(bad)}
			case "multi-chunk":
				r, send := schema.NewStreamReader[*schema.Message](2)
				send(schema.AssistantMessage("请确认"))
				send(bad)
				schema.CloseSend(r)
				llm = &streamProbeModel{reader: r}
			case "model-end-rewrite":
				llm = &streamProbeModel{reader: schema.NewSingleChunkReader(schema.AssistantMessage("原始答复"))}
				hooks.rewrite = bad
			}
			cp := compose_v2.NewInMemoryCheckpointer()
			hs := agent_v2.NewInMemoryHistoryStore()
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			ctx = compose_v2.WithCheckpointerCtx(compose_v2.WithThreadIDCtx(ctx, "encoding-conv"), cp)
			ag := agent_v2.NewBasicAgent(agent_v2.WithName("encoding"), agent_v2.WithModel(llm),
				agent_v2.WithToolkit(tk), agent_v2.WithHooks(hooks), agent_v2.WithApprovalMode(mode), agent_v2.WithHistory(hs, "tests"))
			sess := agent_v2.NewRunSession()
			log := &eventLog{}
			_, err := ag.Run(ctx, sess, schema.UserMessage("运行"), agent_v2.WithOrchHooks(log))
			if !errors.Is(err, compose_v2.ErrRunAborted) || !strings.Contains(strings.ToLower(err.Error()), "utf-8") {
				t.Errorf("期望 UTF-8 协议错误终止本次运行，实际 %v", err)
			}
			var wantEnds int32
			if path == "model-end-rewrite" {
				wantEnds = 1
			}
			if hooks.ends.Load() != wantEnds || hooks.starts.Load() != 0 || body.calls.Load() != 0 {
				t.Errorf("模型结束钩子 %d 次（期望 %d），工具开始钩子 %d 次，工具执行 %d 次",
					hooks.ends.Load(), wantEnds, hooks.starts.Load(), body.calls.Load())
			}
			if path == "multi-chunk" || path == "model-end-rewrite" {
				content := "请确认"
				if path == "model-end-rewrite" {
					content = "原始答复"
				}
				runID, resets := resetsOf(log, content)
				if runID == "" || len(resets) != 1 || resets[runID] != 1 {
					t.Errorf("作废片段的 run_id 为 %q，撤回次数 %v", runID, resets)
				}
			}
			if pending, err := cp.ListPending(ctx, sess.RunID); err != nil || len(pending) != 0 {
				t.Errorf("非法调用遗留 %d 个待答存档，查询错误 %v", len(pending), err)
			}
			rows, err := agent_v2.LoadAllHistory(ctx, hs, agent_v2.HistoryKey{Owner: "tests", ConvID: "encoding-conv"})
			if err != nil {
				t.Fatal(err)
			}
			for _, row := range rows {
				if len(schema.GetToolCalls(row)) != 0 {
					t.Fatal("整批应在写入历史之前被拒绝")
				}
			}
		})
	}
}

func TestToolCallIDEncodingRetractsOnlyRejectedRound(t *testing.T) {
	body := &recordTool{name: "check"}
	tk := tool.NewToolkit()
	tk.Register(body)
	llm := &seqStreamModel{next: func(round int) (*schema.Message, error) {
		msg := oneToolCall(body.name)
		switch round {
		case 1:
			msg.Content = "第一轮获准"
		case 2:
			msg.Content = "第二轮作废"
			msg.ContentBlocks[0].Toolcall.Id = "id\xff"
		default:
			return schema.AssistantMessage("完成"), nil
		}
		return msg, nil
	}}
	log := &eventLog{}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("encoding"), agent_v2.WithModel(llm), agent_v2.WithToolkit(tk))
	_, err := ag.Run(t.Context(), agent_v2.NewRunSession(), schema.UserMessage("运行"), agent_v2.WithOrchHooks(log))
	first, _ := resetsOf(log, "第一轮获准")
	second, resets := resetsOf(log, "第二轮作废")
	if !errors.Is(err, compose_v2.ErrRunAborted) || body.calls.Load() != 1 {
		t.Fatalf("错误 %v，工具实际执行 %d 次", err, body.calls.Load())
	}
	if first == "" || second == "" || first == second || len(resets) != 1 || resets[second] != 1 {
		t.Fatalf("应只撤回第二轮一次，第一轮 %q，第二轮 %q，撤回次数 %v", first, second, resets)
	}
}

func TestToolCallIDEncodingColdApproval(t *testing.T) {
	// 合法的字面替换字符、多字节 Unicode 和省略 ID 都要沿用模型协议中的原值。
	want := map[string]string{"allow": "id\uFFFD", "deny": "id中文", "no_id": ""}
	tk := tool.NewToolkit()
	bodies := make(map[string]*recordTool, len(want))
	var calls []schema.ToolCall
	for _, name := range []string{"allow", "deny", "no_id"} {
		body := &recordTool{name: name}
		tk.Register(body)
		bodies[name] = body
		calls = append(calls, schema.ToolCall{ID: want[name], Function: schema.FunctionCall{Name: name, Arguments: "{}"}})
	}
	build := func(m model.ChatModel) *agent_v2.BasicAgent {
		return agent_v2.NewBasicAgent(agent_v2.WithName("encoding"), agent_v2.WithToolkit(tk), agent_v2.WithModel(m), agent_v2.WithApprovalMode(agent_v2.ApprovalAsk))
	}
	base, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx := compose_v2.WithCheckpointerCtx(base, cp)
	session := agent_v2.NewRunSession()
	_, err := build(newStubModel([]*schema.Message{schema.AssistantToolCallMessage(calls)})).Run(ctx, session, schema.UserMessage("运行"))
	request, ok := compose_v2.IsInterrupt(err)
	if !ok || len(request.AllQueries()) != len(want) {
		t.Fatalf("应有 %d 个独立的待答项，实际 %v", len(want), err)
	}
	answers := make(map[string]*schema.Message, len(want))
	original := make(map[string]string, len(want))
	for _, q := range request.AllQueries() {
		id, found := want[q.ToolName]
		if !found || q.ToolCallID != id || q.InterruptID == "" || answers[q.InterruptID] != nil {
			t.Fatalf("首轮审批工具和寻址不匹配：%+v", q)
		}
		answer := agent_v2.ApprovalOptionApprove
		if q.ToolName == "deny" {
			answer = agent_v2.ApprovalOptionDeny
		}
		answers[q.InterruptID] = schema.UserMessage(answer)
		original[q.ToolName] = q.InterruptID
	}
	saved, err := cp.LoadNS(ctx, session.RunID, "")
	if err != nil || saved == nil {
		t.Fatalf("读取原始存档失败：%v", err)
	}
	wire, err := compose_v2.MarshalCheckpoint(saved)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := compose_v2.UnmarshalCheckpoint(wire)
	if err != nil {
		t.Fatal(err)
	}
	stored := make(map[string]string, len(want))
	for _, q := range restored.PendingInterrupts {
		if id, found := want[q.ToolName]; !found || id != q.ToolCallID {
			t.Fatalf("持久存档改变了工具调用 ID：%+v", q)
		}
		stored[q.ToolName] = q.InterruptID
	}
	if !maps.Equal(stored, original) {
		t.Fatalf("首轮寻址 %v 和存档寻址 %v 不一致", original, stored)
	}
	next := compose_v2.NewInMemoryCheckpointer()
	if err := next.SaveNS(base, session.RunID, "", restored); err != nil {
		t.Fatal(err)
	}
	llm := &scriptModel{steps: []func([]*schema.Message) (*schema.Message, error){func(in []*schema.Message) (*schema.Message, error) {
		paired := make(map[string]int, len(want))
		for _, m := range in {
			if m.Role == schema.RoleTool {
				paired[schema.GetToolResultID(m)]++
			}
		}
		for _, id := range want {
			if paired[id] != 1 {
				return nil, fmt.Errorf("工具调用 %q 的模型结果有 %d 份", id, paired[id])
			}
		}
		return schema.AssistantMessage("完成"), nil
	}}}
	events, err := build(llm).Resume(compose_v2.WithCheckpointerCtx(base, next), session.RunID, agent_v2.WithResumeUserInputs(answers))
	final := agent_v2.FinalMessage(events)
	if err != nil || final == nil || final.Content != "完成" {
		t.Fatalf("按首轮 ID 冷恢复失败：%v", err)
	}
	if bodies["allow"].calls.Load() != 1 || bodies["deny"].calls.Load() != 0 || bodies["no_id"].calls.Load() != 1 {
		t.Fatal("原审批答案没有兑现到各自的工具")
	}
}
