package agent_v2_test

import (
	"context"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/mcp"
	"crew-go/schema"
	"crew-go/tool"
)

type sharedPermissionCatalog struct{ calls atomic.Int32 }

func (*sharedPermissionCatalog) Name() string                     { return "catalog" }
func (*sharedPermissionCatalog) Initialize(context.Context) error { return nil }
func (*sharedPermissionCatalog) Close() error                     { return nil }
func (*sharedPermissionCatalog) ListTools(context.Context) ([]mcp.ToolSpec, error) {
	ro := true
	var specs []mcp.ToolSpec
	for _, name := range []string{"alpha", "beta", "shared"} {
		specs = append(specs, mcp.ToolSpec{Name: name, Description: "bulletin " + name + " label", Annotations: &mcp.ToolAnnotations{ReadOnlyHint: &ro}})
	}
	return specs, nil
}
func (c *sharedPermissionCatalog) CallTool(_ context.Context, name string, _ map[string]any) (*mcp.CallResult, error) {
	c.calls.Add(1)
	return &mcp.CallResult{Content: []mcp.Block{{Type: "text", Text: "used " + name}}}, nil
}

func catalogSearch(id, query string) *schema.Message {
	return schema.AssistantToolCallMessage([]schema.ToolCall{{ID: id, Function: schema.FunctionCall{Name: "tool_search", Arguments: `{"query":"` + query + `"}`}}})
}

func catalogSearchReceipt(m *captureToolsModel, id string) string {
	for _, round := range m.inputs {
		for _, msg := range round {
			if schema.GetToolResultID(msg) == id {
				return msg.Content
			}
		}
	}
	return ""
}

func TestHandoffSharedMCPDirectoryUsesMemberToolPolicy(t *testing.T) {
	for _, trusted := range []bool{false, true} {
		t.Run(map[bool]string{true: "trusted", false: "untrusted"}[trusted], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
			defer cancel()
			cp := compose_v2.NewInMemoryCheckpointer()
			ctx = compose_v2.WithCheckpointerCtx(ctx, cp)
			client := &sharedPermissionCatalog{}
			opt := mcp.WithClient(client)
			if trusted {
				opt = mcp.WithTrustedClient(client)
			}
			hub := mcp.NewHub(opt)
			if err := hub.Start(ctx); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := hub.Close(); err != nil {
					t.Error(err)
				}
			})
			const shared = "mcp_catalog_shared"
			first := &captureToolsModel{responses: []*schema.Message{catalogSearch("list-a", "bulletin"), toolCallsMessage(shared), transferToolCall("transfer", "second")}}
			second := &captureToolsModel{responses: []*schema.Message{catalogSearch("list-b", "bulletin"), toolCallsMessage(shared), schema.AssistantMessage("complete")}}
			member := func(m *captureToolsModel, own string) *agent_v2.BasicAgent {
				return agent_v2.NewBasicAgent(agent_v2.WithName("member"), agent_v2.WithModel(m), agent_v2.WithToolkit(hub.Toolkit()),
					agent_v2.WithToolFilter(func(context.Context, *schema.Message) []string { return []string{shared, own} }),
					agent_v2.WithToolSearch(&agent_v2.ToolSearchConfig{Enabled: true, Deferred: mcp.IsRemoteTool}),
					agent_v2.WithHandoffTransfer(), agent_v2.WithApprovalMode(agent_v2.ApprovalAskWrites), agent_v2.WithMaxRounds(6))
			}
			sess := agent_v2.NewRunSession()
			h := agent_v2.NewHandoffAgent(agent_v2.WithHandoffName("catalog-members"), agent_v2.WithHandoffInitial("first"),
				agent_v2.WithHandoffTargets("first", "second"), agent_v2.WithHandoffTargets("second")).
				AddAgent("first", member(first, "mcp_catalog_alpha")).AddAgent("second", member(second, "mcp_catalog_beta"))
			events, err := h.Run(ctx, sess, schema.UserMessage("check the shared bulletin"))
			var ids []string
			if !trusted {
				for i, decision := range []string{agent_v2.ApprovalOptionApproveAlwaysTool, agent_v2.ApprovalOptionDeny} {
					ie, ok := compose_v2.IsInterrupt(err)
					if !ok || len(ie.AllQueries()) != 1 {
						t.Fatalf("第 %d 次应该审批一次: %v", i, err)
					}
					q := ie.AllQueries()[0]
					if q.ToolName != shared || q.Reason != agent_v2.ApprovalInterruptReason || q.InterruptID == "" || slices.Contains(ids, q.InterruptID) || client.calls.Load() != int32(i) {
						t.Fatalf("审批归属/次数异常: %v calls=%d", q, client.calls.Load())
					}
					ids = append(ids, q.InterruptID)
					events, err = h.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess), agent_v2.WithResumeUserInputs(map[string]*schema.Message{q.InterruptID: schema.UserMessage(decision)}))
				}
			}
			if err != nil || agent_v2.FinalMessage(events) == nil || agent_v2.FinalMessage(events).Content != "complete" {
				t.Fatalf("最终结果 %v, %v", agent_v2.FinalMessage(events), err)
			}
			want := int32(1)
			if trusted {
				want = 2
			}
			if got := client.calls.Load(); got != want {
				t.Fatalf("得到 %d 次，期望 %d", got, want)
			}
			for i, model := range []*captureToolsModel{first, second} {
				n := []string{"mcp_catalog_alpha", "mcp_catalog_beta"}
				id := []string{"list-a", "list-b"}[i]
				if len(model.bound) < 2 || !slices.Contains(model.bound[1], shared) || !slices.Contains(model.bound[1], n[i]) || slices.Contains(model.bound[1], n[1-i]) {
					t.Fatalf("成员 %d 的模型绑定 %v", i, model.bound)
				}
				rec := catalogSearchReceipt(model, id)
				if !strings.Contains(rec, n[i]) || strings.Contains(rec, n[1-i]) || strings.Contains(rec, "bulletin "+[]string{"beta", "alpha"}[i]+" label") {
					t.Fatalf("成员 %d 检索回执 %q", i, rec)
				}
			}
			if slices.Contains(second.bound[0], agent_v2.HandoffTransferToolName) || !slices.Contains(first.bound[0], agent_v2.HandoffTransferToolName) {
				t.Fatalf("转交授权工具跨成员: A=%v B=%v", first.bound[0], second.bound[0])
			}
			t.Logf("信任=%t, 两人检索及绑定各自允许的 2 种、首次绑定转交控制限 A；审批=%d，实际调用=%d", trusted, len(ids), client.calls.Load())
		})
	}
}

func TestToolSearchDoesNotChargeFilteredCandidates(t *testing.T) {
	client := &sharedPermissionCatalog{}
	hub := mcp.NewHub(mcp.WithClient(client))
	if err := hub.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	defer hub.Close()
	const allowed = "mcp_catalog_beta"
	m := &captureToolsModel{responses: []*schema.Message{catalogSearch("catalog", "bulletin"), catalogSearch("permitted", "beta"), schema.AssistantMessage("complete")}}
	a := agent_v2.NewBasicAgent(agent_v2.WithName("cap"), agent_v2.WithModel(m), agent_v2.WithToolkit(hub.Toolkit()),
		agent_v2.WithToolFilter(func(context.Context, *schema.Message) []string { return []string{allowed} }),
		agent_v2.WithToolSearch(&agent_v2.ToolSearchConfig{Enabled: true, Deferred: mcp.IsRemoteTool, MaxRevealed: 1}), agent_v2.WithMaxRounds(5))
	if _, err := a.Run(t.Context(), agent_v2.NewRunSession(), schema.UserMessage("check bulletin")); err != nil {
		t.Fatal(err)
	}
	if rec := catalogSearchReceipt(m, "catalog"); !strings.Contains(rec, allowed) || strings.Contains(rec, "mcp_catalog_alpha") {
		t.Fatalf("首次检索应只揭示允许的候选，实际 %q", rec)
	}
	if rec := catalogSearchReceipt(m, "permitted"); strings.Contains(rec, "最多解锁 1 个") {
		t.Fatalf("同一允许候选已经占用额度，重复查找仍可见，实际 %q", rec)
	}
	if len(m.bound) != 3 || slices.Contains(m.bound[0], allowed) || !slices.Contains(m.bound[1], allowed) || !slices.Contains(m.bound[2], allowed) || client.calls.Load() != 0 {
		t.Fatalf("应在下轮绑定 beta，目录中的实际工具调用次数为 %d，绑定 %v", client.calls.Load(), m.bound)
	}
}

func TestToolSearchChangingFilterKeepsEarlierAuthorizedQuota(t *testing.T) {
	const a, b = "mcp_catalog_alpha", "mcp_catalog_beta"
	client := &sharedPermissionCatalog{}
	hub := mcp.NewHub(mcp.WithClient(client))
	if err := hub.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	defer hub.Close()
	m := &captureToolsModel{responses: []*schema.Message{catalogSearch("one", "alpha"), catalogSearch("two", "beta"), toolCallsMessage(a), schema.AssistantMessage("complete")}}
	calls := 0
	modelBound := [][]string{{"tool_search"}, {"tool_search"}, {a, "tool_search"}, {a, "tool_search"}}
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("changing"), agent_v2.WithToolkit(hub.Toolkit()), agent_v2.WithModel(m),
		agent_v2.WithToolFilter(func(context.Context, *schema.Message) []string {
			calls++
			if calls == 2 {
				return []string{b}
			}
			return []string{a}
		}),
		agent_v2.WithToolSearch(&agent_v2.ToolSearchConfig{Enabled: true, Deferred: mcp.IsRemoteTool, MaxRevealed: 1}), agent_v2.WithMaxRounds(6))
	evs, err := ag.Run(t.Context(), agent_v2.NewRunSession(), schema.UserMessage("lookup"))
	if err != nil || agent_v2.FinalMessage(evs) == nil || agent_v2.FinalMessage(evs).Content != "complete" {
		t.Fatalf("最终 %v %v", agent_v2.FinalMessage(evs), err)
	}
	if calls != len(modelBound) || len(m.bound) != len(modelBound) || client.calls.Load() != 1 {
		t.Fatalf("filter=%d bound=%v calls=%d", calls, m.bound, client.calls.Load())
	}
	for i, bound := range m.bound {
		if !slices.Equal(bound, modelBound[i]) {
			t.Fatalf("第 %d 轮绑定 %v，期望 %v", i, bound, modelBound[i])
		}
	}
	if first, second := catalogSearchReceipt(m, "one"), catalogSearchReceipt(m, "two"); !strings.Contains(first, a) || !strings.Contains(second, "最多解锁 1 个") {
		t.Fatalf("检索两轮 %q / %q", first, second)
	}
	t.Logf("四轮调用四次 filter，第一次合法解锁在第二轮隐藏时仍占任务额度，第三轮重新允许后真实运行 1 次；每轮绑定 %v", m.bound)
}

func TestToolSearchResumeKeepsModelTurnCandidateScope(t *testing.T) {
	const beta = "mcp_catalog_beta"
	const alpha = "mcp_catalog_alpha"
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cp := compose_v2.NewInMemoryCheckpointer()
	ctx = compose_v2.WithCheckpointerCtx(ctx, cp)
	hub := mcp.NewHub(mcp.WithClient(&sharedPermissionCatalog{}))
	if err := hub.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := hub.Close(); err != nil {
			t.Error(err)
		}
	})
	m := &captureToolsModel{responses: []*schema.Message{catalogSearch("search", "bulletin"), schema.AssistantMessage("done")}}
	current := beta
	filterCalls := 0
	ag := agent_v2.NewBasicAgent(agent_v2.WithName("scope"), agent_v2.WithToolkit(hub.Toolkit()), agent_v2.WithModel(m),
		agent_v2.WithHooks(askOnToolsRound{round: 1}),
		agent_v2.WithToolFilter(func(context.Context, *schema.Message) []string {
			filterCalls++
			return []string{current}
		}), agent_v2.WithToolSearch(&agent_v2.ToolSearchConfig{Enabled: true, Deferred: mcp.IsRemoteTool,
			Retriever: func(_ context.Context, _ string, _ []tool.Tool) []string {
				return []string{alpha, beta}
			}}))
	sess := agent_v2.NewRunSession()
	_, err := ag.Run(ctx, sess, schema.UserMessage("lookup bulletin"))
	q := partitionQuestionID(t, err)
	if filterCalls != 1 {
		t.Fatalf("生成这一轮应该只调用一次过滤器，实际 %d", filterCalls)
	}
	cp = reopenDirectChildStore(t, ctx, cp, sess.RunID)
	ctx = compose_v2.WithCheckpointerCtx(ctx, cp)
	current = alpha
	events, err := ag.Resume(ctx, sess.RunID,
		agent_v2.WithResumeUserInputs(map[string]*schema.Message{q: schema.UserMessage("继续")}))
	if err != nil || agent_v2.FinalMessage(events) == nil || filterCalls != 2 {
		t.Fatalf("恢复后仅新轮计算过滤器，次数 %d，错误 %v", filterCalls, err)
	}
	if got := catalogSearchReceipt(m, "search"); !strings.Contains(got, beta) || strings.Contains(got, alpha) {
		t.Fatalf("回执应沿用原轮候选并复核 Retriever 的候选外结果，实际 %q", got)
	}
}
