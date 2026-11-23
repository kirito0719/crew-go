package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/schema"
	"crew-go/tool"
)

type observations struct {
	Route        string             `json:"route"`
	Outcome      string             `json:"outcome"`
	Calls        []invocation       `json:"calls"`
	Approvals    []string           `json:"approvals"`
	Final        string             `json:"final"`
	InputTokens  int                `json:"input_tokens"`
	OutputTokens int                `json:"output_tokens"`
	Rounds       []roundObservation `json:"rounds"`
}

type roundObservation struct {
	Preamble      []*schema.Message   `json:"preamble,omitempty"`
	Tools         []*schema.ToolInfo  `json:"tools,omitempty"`
	RequestConfig *schema.ModelConfig `json:"request_config,omitempty"`
	ElapsedMS     float64             `json:"elapsed_ms"`
	started       time.Time
	Agent         string               `json:"agent"`
	Round         int32                `json:"round"`
	Input         []*schema.Message    `json:"input"`
	Output        *schema.Message      `json:"output,omitempty"`
	Usage         *schema.MessageUsage `json:"usage,omitempty"`
	FailedUsage   []model.FailedUsage  `json:"failed_usage,omitempty"`
	Error         string               `json:"error,omitempty"`
	Reused        bool                 `json:"reused,omitempty"`
}

type roundKey struct{}

type recorder struct {
	hook.NoOpHooks
	mu sync.Mutex
	o  observations
}

func (r *recorder) OnAgentStart(ctx context.Context, info *hook.AgentInfo, _ *hook.AgentInput) (context.Context, error) {
	r.mu.Lock()
	r.o.Route = info.Name
	r.mu.Unlock()
	return ctx, nil
}

func (r *recorder) OnModelStart(ctx context.Context, info *hook.AgentInfo, in *schema.ModelInput) (context.Context, error) {
	encoded, err := json.Marshal(in)
	if err != nil {
		return ctx, fmt.Errorf("record model input: %w", err)
	}
	var snapshot schema.ModelInput
	if err := json.Unmarshal(encoded, &snapshot); err != nil {
		return ctx, fmt.Errorf("decode model input snapshot: %w", err)
	}
	r.mu.Lock()
	index := len(r.o.Rounds)
	r.o.Rounds = append(r.o.Rounds, roundObservation{Agent: info.Name, Round: info.Round, Input: snapshot.Messages, Preamble: snapshot.Preamble, Tools: snapshot.Tools, RequestConfig: snapshot.Config, started: time.Now(),
		Reused: hook.IsModelResponseReused(in.Extra)})
	r.mu.Unlock()
	ctx = context.WithValue(ctx, roundKey{}, index)
	return model.WithFailedUsageObserver(ctx, func(_ context.Context, u model.FailedUsage) {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.o.InputTokens += int(u.Usage.InputTokens)
		r.o.OutputTokens += int(u.Usage.OutputTokens)
		r.o.Rounds[index].FailedUsage = append(r.o.Rounds[index].FailedUsage, u)
	}), nil
}

func (r *recorder) OnModelEnd(ctx context.Context, _ *hook.AgentInfo, out *schema.ModelOutput) (context.Context, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if out != nil {
		if index, ok := ctx.Value(roundKey{}).(int); ok {
			r.o.Rounds[index].ElapsedMS = float64(time.Since(r.o.Rounds[index].started)) / float64(time.Millisecond)
			r.o.Rounds[index].Output = out.Message
			r.o.Rounds[index].Usage = out.Usage
		}
		if out.Usage != nil && !hook.IsModelResponseReused(out.Extra) {
			r.o.InputTokens += int(out.Usage.InputTokens)
			r.o.OutputTokens += int(out.Usage.OutputTokens)
		}
	}
	return ctx, nil
}

func (r *recorder) OnError(ctx context.Context, _ *hook.AgentInfo, err *schema.CommError) context.Context {
	r.mu.Lock()
	defer r.mu.Unlock()
	if index, ok := ctx.Value(roundKey{}).(int); ok && err != nil {
		r.o.Rounds[index].ElapsedMS = float64(time.Since(r.o.Rounds[index].started)) / float64(time.Millisecond)
		r.o.Rounds[index].Error = err.Message
	}
	return ctx
}

const lookupTool = "lookup_asset"

func buildWorkflow(llm model.ChatModel, rec *recorder) agent_v2.Agent {
	llm = recordingModel{inner: llm, rec: rec}
	kit := tool.NewToolkit()
	kit.Register(tool.NewFuncTool(lookupTool, "Look up the status of an asset by key", map[string]any{
		"type": "object", "properties": map[string]any{"key": map[string]any{"type": "string"}},
		"required": []string{"key"},
	}, func(_ context.Context, args map[string]any) (*tool.Result, error) {
		rec.mu.Lock()
		rec.o.Calls = append(rec.o.Calls, invocation{Name: lookupTool, Args: maps.Clone(args)})
		rec.mu.Unlock()
		key, _ := args["key"].(string)
		if key == "" {
			return nil, errors.New("asset key is required")
		}
		return tool.Success("asset " + key + "=ready"), nil
	}))
	lookup := agent_v2.NewBasicAgent(agent_v2.WithName("lookup"), agent_v2.WithModel(llm),
		agent_v2.WithToolkit(kit), agent_v2.WithApprovalMode(agent_v2.ApprovalAsk), agent_v2.WithHooks(rec),
		agent_v2.WithSystemPrompt("For input 'lookup <key>', call lookup_asset once with that key. If it succeeds, reply with the exact asset status from the tool. If denied, reply denied."),
		agent_v2.WithMaxRounds(3))
	help := agent_v2.NewBasicAgent(agent_v2.WithName("help"), agent_v2.WithModel(llm), agent_v2.WithHooks(rec),
		agent_v2.WithSystemPrompt("For a help request, reply exactly: use lookup <key>"))
	return agent_v2.NewRoutingAgent(agent_v2.WithRoutingName("trajectory"),
		agent_v2.WithRouter(func(_ context.Context, msg *schema.Message) (compose_v2.NodeID, error) {
			if strings.HasPrefix(msg.Content, "lookup ") {
				return "lookup", nil
			}
			return "help", nil
		})).AddBranch("lookup", lookup).AddBranch("help", help)
}

func runFixture(ctx context.Context, c fixture, llm model.ChatModel) (observations, error) {
	var rec recorder
	workflow := buildWorkflow(llm, &rec)
	ctx = compose_v2.WithCheckpointerCtx(ctx, compose_v2.NewInMemoryCheckpointer())
	sess := agent_v2.NewRunSession()
	events, err := workflow.Run(ctx, sess, schema.UserMessage(c.Input))
	remaining := c.Answers
	for {
		if err == nil {
			rec.o.Outcome = "success"
			if final := agent_v2.FinalMessage(events); final != nil {
				rec.o.Final = final.Content
			}
			break
		}
		interrupt, ok := compose_v2.IsInterrupt(err)
		if !ok {
			rec.o.Outcome = "error"
			return rec.o, fmt.Errorf("workflow execution: %w", err)
		}
		rec.o.Outcome = "interrupt"
		queries := interrupt.AllQueries()
		for _, query := range queries {
			rec.o.Approvals = append(rec.o.Approvals, query.ToolName)
		}
		if len(remaining) == 0 {
			rec.o.Outcome = "interrupt"
			break
		}
		if len(queries) == 0 || len(remaining) < len(queries) {
			return rec.o, errors.New("fixture answers do not cover the pending approval batch")
		}
		responses := make(map[string]*schema.Message, len(queries))
		for i, query := range queries {
			answer := remaining[i]
			if answer.Tool != query.ToolName {
				return rec.o, fmt.Errorf("approval %q does not match answer for %q", query.ToolName, answer.Tool)
			}
			responses[query.InterruptID] = schema.UserMessage(answer.Value)
		}
		remaining = remaining[len(queries):]
		events, err = workflow.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess), agent_v2.WithResumeUserInputs(responses))
	}
	if len(remaining) != 0 {
		return rec.o, fmt.Errorf("%d fixture answers were unused", len(remaining))
	}
	return rec.o, compare(c.Expect, rec.o)
}

func compare(want expectations, got observations) error {
	var errs []error
	if got.Route != want.Route || got.Outcome != want.Outcome {
		errs = append(errs, fmt.Errorf("route/outcome got %s/%s, want %s/%s", got.Route, got.Outcome, want.Route, want.Outcome))
	}
	if !slices.Equal(want.Approvals, got.Approvals) {
		errs = append(errs, fmt.Errorf("approvals got %v, want %v", got.Approvals, want.Approvals))
	}
	if !slices.EqualFunc(want.Calls, got.Calls, func(a, b invocation) bool {
		return a.Name == b.Name && reflect.DeepEqual(a.Args, b.Args)
	}) {
		errs = append(errs, fmt.Errorf("tool calls got %v, want %v", got.Calls, want.Calls))
	}
	if !strings.Contains(got.Final, want.FinalContains) {
		errs = append(errs, fmt.Errorf("final output %q does not contain %q", got.Final, want.FinalContains))
	}
	return errors.Join(errs...)
}

type mockModel struct{}

func (mockModel) Generate(_ context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	for i := len(msgs) - 1; i >= 0; i-- {
		msg := msgs[i]
		switch msg.Role {
		case schema.RoleTool:
			if strings.Contains(msg.Content, "=ready") {
				return schema.AssistantMessage(msg.Content), nil
			}
			return schema.AssistantMessage("denied"), nil
		case schema.RoleUser:
			key, lookup := strings.CutPrefix(msg.Content, "lookup ")
			if !lookup {
				return schema.AssistantMessage("use lookup <key>"), nil
			}
			args, err := json.Marshal(map[string]string{"key": key})
			if err != nil {
				return nil, err
			}
			return schema.AssistantToolCallMessage([]schema.ToolCall{{
				ID: "lookup-1", Type: "function", Function: schema.FunctionCall{Name: lookupTool, Arguments: string(args)},
			}}), nil
		}
	}
	return nil, errors.New("mock model did not receive a user message")
}

func (m mockModel) Stream(ctx context.Context, msgs []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	msg, err := m.Generate(ctx, msgs, opts...)
	if err != nil {
		return nil, err
	}
	return schema.NewSingleChunkReader(msg), nil
}

func (m mockModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

// recordingModel 在实际调用点记录本轮绑定后的工具表，避免把钩子之前的候选表当成实际 schema。
type recordingModel struct {
	inner model.ChatModel
	rec   *recorder
	tools []*schema.ToolInfo
}

func (m recordingModel) ModelName() string {
	if named, ok := m.inner.(model.ModelNamer); ok {
		return named.ModelName()
	}
	return ""
}
func (m recordingModel) WithTools(tools []*schema.ToolInfo) (model.ChatModel, error) {
	bound, err := m.inner.WithTools(tools)
	if err != nil || bound == nil {
		return bound, err
	}
	data, err := json.Marshal(tools)
	if err != nil {
		return nil, fmt.Errorf("record bound tools: %w", err)
	}
	var snapshot []*schema.ToolInfo
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return nil, fmt.Errorf("decode bound tools: %w", err)
	}
	return recordingModel{inner: bound, rec: m.rec, tools: snapshot}, nil
}
func (m recordingModel) recordTools(ctx context.Context) {
	m.rec.mu.Lock()
	defer m.rec.mu.Unlock()
	if index, ok := ctx.Value(roundKey{}).(int); ok {
		m.rec.o.Rounds[index].Tools = m.tools
	}
}
func (m recordingModel) Generate(ctx context.Context, in []*schema.Message, opts ...model.Option) (*schema.Message, error) {
	m.recordTools(ctx)
	return m.inner.Generate(ctx, in, opts...)
}
func (m recordingModel) Stream(ctx context.Context, in []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	m.recordTools(ctx)
	return m.inner.Stream(ctx, in, opts...)
}
