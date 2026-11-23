package observability_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"crew-go/agent_v2"
	"crew-go/compose_v2"
	"crew-go/decision"
	"crew-go/fallback"
	"crew-go/hook"
	"crew-go/model"
	"crew-go/model/openai"
	"crew-go/observability"
	"crew-go/schema"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

type failedUsageRt func(*http.Request) (*http.Response, error)

func (r failedUsageRt) RoundTrip(q *http.Request) (*http.Response, error) { return r(q) }
func failedUsageResponseClient(body string) *openai.ResponsesClient {
	return openai.NewResponsesClient("key", "test", openai.WithHTTPClient(&http.Client{Transport: failedUsageRt(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(body))}, nil
	})}))
}

const failedUsageCompleted = `{"status":"completed","model":"billed-model","output":[{"type":"message","content":[{"type":"output_text","text":"bar"}]}],"usage":{"input_tokens":31,"output_tokens":9,"total_tokens":40}}`

func TestTerminalProtocolErrorsPreserveUsage(t *testing.T) {
	for name, body := range map[string]string{
		"mismatching-deltas": "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"foo\"}\n\nevent: response.completed\ndata: {\"type\":\"response.completed\",\"response\":" + failedUsageCompleted + "}\n\n",
		"unfinished-items":   "event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"id\":\"i\",\"type\":\"message\"}}\n\nevent: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"model\":\"billed\",\"usage\":{\"input_tokens\":31,\"output_tokens\":9,\"total_tokens\":40}}}\n\n",
	} {
		t.Run(name, func(t *testing.T) {
			sr, err := failedUsageResponseClient(body).Stream(t.Context(), []*schema.Message{schema.UserMessage("x")})
			if err != nil {
				t.Fatal(err)
			}
			defer sr.Close()
			for err == nil {
				_, err = sr.Recv(t.Context())
			}
			u := model.UsageFromError(err)
			want := "deltas disagree"
			if name == "unfinished-items" {
				want = "unfinished output items"
			}
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("wrong protocol rejection: %v", err)
			}
			if errors.Is(err, io.EOF) || err == nil {
				t.Fatalf("not rejected: %v", err)
			}
			if u == nil || u.Usage.InputTokens != 31 {
				t.Fatalf("reported usage lost: usage=%+v err=%v", u, err)
			}
		})
	}
}

type failedUsageFailModel struct{ calls int }

func (m *failedUsageFailModel) Generate(context.Context, []*schema.Message, ...model.Option) (*schema.Message, error) {
	m.calls++
	return nil, model.WithFailureUsage(errors.New("billed failure"), &schema.MessageUsage{InputTokens: 31, OutputTokens: 9}, "p", "m")
}
func (m *failedUsageFailModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m *failedUsageFailModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }
func failedUsageMetricSum(t *testing.T, r *sdkmetric.ManualReader, name string) int64 {
	t.Helper()
	var all metricdata.ResourceMetrics
	if err := r.Collect(t.Context(), &all); err != nil {
		t.Fatal(err)
	}
	var sum int64
	for _, scope := range all.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name == name {
				if s, ok := m.Data.(metricdata.Sum[int64]); ok {
					for _, dp := range s.DataPoints {
						sum += dp.Value
					}
				}
			}
		}
	}
	return sum
}
func TestRoutingFailedUsageCount(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	defer tp.Shutdown(context.Background())
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer mp.Shutdown(context.Background())
	obs := observability.New(observability.WithTracerProvider(tp), observability.WithMeterProvider(mp))
	m := &failedUsageFailModel{}
	ctx, _ := obs.Install(t.Context())
	r := agent_v2.NewRoutingAgent(agent_v2.WithRoutingName("route"), agent_v2.WithRoutingHooks(obs.AgentHooks()), agent_v2.WithRouteDecider("sample", decision.NewLLMRouteDecider(m, func(decision.LLMRouteSample) (float64, bool) { return 1, true }), 0))
	r.AddBranch("worker", agent_v2.NewBasicAgent(agent_v2.WithName("worker")))
	_, err := r.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("x"))
	if err == nil || m.calls != 1 {
		t.Fatalf("expected one billed failure: calls=%d err=%v", m.calls, err)
	}
	got := failedUsageMetricSum(t, reader, observability.MetricUsageInputTokens)
	if got != 31 {
		t.Fatalf("routing billed tokens=%d want=31 (chat spans=%d)", got, len(rec.Ended()))
	}
}

func TestObserverChainChangeDedup(t *testing.T) {
	var a, b int32
	ctxA := model.WithFailedUsageObserver(t.Context(), func(_ context.Context, u model.FailedUsage) { a += u.Usage.InputTokens })
	err := model.ReportFailedUsage(ctxA, model.WithFailureUsage(errors.New("failed"), &schema.MessageUsage{InputTokens: 3}, "p", "m"))
	ctxB := model.WithFailedUsageObserver(ctxA, func(_ context.Context, u model.FailedUsage) { b += u.Usage.InputTokens })
	err = model.ReportFailedUsage(ctxB, err)
	_ = model.ReportFailedUsage(ctxA, err)
	if a != 3 || b != 3 {
		t.Fatalf("one failed attempt delivered a=%d b=%d want each 3", a, b)
	}
}

type failedUsageNestedModel struct{ child *agent_v2.BasicAgent }

func (m failedUsageNestedModel) Generate(ctx context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	_, err := agent_v2.RunSubAgent(ctx, "nested", m.child, msgs[len(msgs)-1])
	return nil, err
}
func (m failedUsageNestedModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m failedUsageNestedModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

type failedUsageCounterHook struct {
	hook.NoOpHooks
	counts map[string]int32
}

func (h *failedUsageCounterHook) OnModelStart(ctx context.Context, info *hook.AgentInfo, _ *schema.ModelInput) (context.Context, error) {
	name := info.Name
	return model.WithFailedUsageObserver(ctx, func(_ context.Context, u model.FailedUsage) { h.counts[name] += u.Usage.InputTokens }), nil
}
func TestNestedAgentFailureDedup(t *testing.T) {
	h := &failedUsageCounterHook{counts: map[string]int32{}}
	child := agent_v2.NewBasicAgent(agent_v2.WithName("child"), agent_v2.WithModel(&failedUsageFailModel{}), agent_v2.WithHooks(h))
	parent := agent_v2.NewBasicAgent(agent_v2.WithName("parent"), agent_v2.WithModel(failedUsageNestedModel{child}), agent_v2.WithHooks(h))
	_, err := parent.Run(t.Context(), agent_v2.NewRunSession(), schema.UserMessage("x"))
	if err == nil {
		t.Fatal("expected failure")
	}
	if h.counts["child"] != 31 || h.counts["parent"] != 0 {
		t.Fatalf("one request counted across model scopes: %+v", h.counts)
	}
}

type failedUsageForeignHook struct{ hook.NoOpHooks }

func (failedUsageForeignHook) OnModelStart(context.Context, *hook.AgentInfo, *schema.ModelInput) (context.Context, error) {
	return context.Background(), nil
}
func TestForeignHookContextPreservesFailedMetrics(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	defer tp.Shutdown(context.Background())
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer mp.Shutdown(context.Background())
	obs := observability.New(observability.WithTracerProvider(tp), observability.WithMeterProvider(mp))
	m := &failedUsageFailModel{}
	ctx, opts := obs.Install(t.Context(), agent_v2.WithName("foreign"), agent_v2.WithModel(m), agent_v2.WithHooks(failedUsageForeignHook{}))
	_, err := agent_v2.NewBasicAgent(opts...).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("x"))
	if err == nil {
		t.Fatal("expected failure")
	}
	got := failedUsageMetricSum(t, reader, observability.MetricUsageInputTokens)
	if got != 31 {
		t.Fatalf("foreign hook failed tokens=%d want=31 spans=%d/%d", got, len(rec.Ended()), len(rec.Started()))
	}
}

type failedUsageCancelReader struct {
	r      io.Reader
	cancel context.CancelFunc
}

func (r *failedUsageCancelReader) Read(p []byte) (int, error) {
	n, e := r.r.Read(p)
	r.cancel()
	return n, e
}
func TestChatCancellationPreservesLastReportedUsage(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	body := "data: {\"model\":\"billed\",\"choices\":[],\"usage\":{\"prompt_tokens\":31,\"completion_tokens\":9,\"total_tokens\":40}}\n\n"
	client := openai.NewClient("key", "test", openai.WithHTTPClient(&http.Client{Transport: failedUsageRt(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: io.NopCloser(&failedUsageCancelReader{strings.NewReader(body), cancel})}, nil
	})}))
	sr, err := client.Stream(ctx, []*schema.Message{schema.UserMessage("x")})
	if err != nil {
		t.Fatal(err)
	}
	defer sr.Close()
	var seen *schema.Message
	for err == nil {
		var m *schema.Message
		m, err = sr.Recv(context.Background())
		if m != nil {
			seen = m
		}
	}
	if seen == nil || seen.Metadata == nil || seen.Metadata.Usage == nil || seen.Metadata.Usage.InputTokens != 31 {
		t.Fatalf("expected reported usage chunk, got %+v err=%v", seen, err)
	}
	u := model.UsageFromError(err)
	if !errors.Is(err, context.Canceled) || u == nil || u.Usage.InputTokens != 31 {
		t.Fatalf("cancelled error lost usage=%+v err=%v", u, err)
	}
}

func TestResponsesCancellationAtReportedTerminalUsage(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	body := "event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"status\":\"failed\",\"model\":\"billed\",\"error\":{\"code\":\"server_error\"},\"usage\":{\"input_tokens\":31,\"output_tokens\":9,\"total_tokens\":40}}}\n\n"
	client := openai.NewResponsesClient("key", "test", openai.WithHTTPClient(&http.Client{Transport: failedUsageRt(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: io.NopCloser(&failedUsageCancelReader{strings.NewReader(body), cancel})}, nil
	})}))
	sr, err := client.Stream(ctx, []*schema.Message{schema.UserMessage("x")})
	if err != nil {
		t.Fatal(err)
	}
	defer sr.Close()
	for err == nil {
		_, err = sr.Recv(context.Background())
	}
	u := model.UsageFromError(err)
	if !errors.Is(err, context.Canceled) || u == nil || u.Usage.InputTokens != 31 {
		t.Fatalf("decoded failed terminal lost usage=%+v err=%v", u, err)
	}
}

func TestChatErrorEventReportedUsage(t *testing.T) {
	body := "event: error\ndata: {\"model\":\"billed\",\"error\":{\"message\":\"failure\"},\"usage\":{\"prompt_tokens\":31,\"completion_tokens\":9,\"total_tokens\":40}}\n\n"
	client := openai.NewClient("key", "test", openai.WithHTTPClient(&http.Client{Transport: failedUsageRt(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(body))}, nil
	})}))
	sr, err := client.Stream(t.Context(), []*schema.Message{schema.UserMessage("x")})
	if err != nil {
		t.Fatal(err)
	}
	defer sr.Close()
	for err == nil {
		_, err = sr.Recv(t.Context())
	}
	u := model.UsageFromError(err)
	if errors.Is(err, io.EOF) || u == nil || u.Usage.InputTokens != 31 {
		t.Fatalf("reported error event usage=%+v err=%v", u, err)
	}
}

type failedUsageUsageStreamModel struct{}

func (failedUsageUsageStreamModel) Generate(context.Context, []*schema.Message, ...model.Option) (*schema.Message, error) {
	return nil, errors.New("not used")
}
func (m failedUsageUsageStreamModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) {
	return m, nil
}
func (failedUsageUsageStreamModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	sr, send := schema.NewStreamReader[*schema.Message](2)
	send(&schema.Message{Role: schema.RoleAssistant, Content: "partial", Metadata: &schema.MessageMetadata{Model: "m", System: "p", Usage: &schema.MessageUsage{InputTokens: 31, OutputTokens: 9}}})
	schema.SendError(sr, errors.New("stream broken"))
	schema.CloseSend(sr)
	return sr, nil
}
func TestAggregatorPreservesReportedUsageOnStreamError(t *testing.T) {
	h := &failedUsageCounterHook{counts: map[string]int32{}}
	a := agent_v2.NewBasicAgent(agent_v2.WithName("stream"), agent_v2.WithModel(failedUsageUsageStreamModel{}), agent_v2.WithHooks(h))
	_, err := a.Run(t.Context(), agent_v2.NewRunSession(), schema.UserMessage("x"))
	if err == nil {
		t.Fatal("expected failure")
	}
	if h.counts["stream"] != 31 {
		t.Fatalf("metadata usage lost on stream failure: counts=%+v err=%v", h.counts, err)
	}
}

type failedUsageRateModel struct{ calls int }

func (m *failedUsageRateModel) Generate(context.Context, []*schema.Message, ...model.Option) (*schema.Message, error) {
	m.calls++
	return nil, model.WithFailureUsage(&model.APIError{Provider: "p", StatusCode: 429, RetryAfter: 2 * time.Minute}, &schema.MessageUsage{InputTokens: 31}, "p", "m")
}
func (m *failedUsageRateModel) Stream(ctx context.Context, in []*schema.Message, opts ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	_, err := m.Generate(ctx, in, opts...)
	return nil, err
}
func (m *failedUsageRateModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }
func TestRateGateDoesNotRebillPriorFailure(t *testing.T) {
	m := &failedUsageRateModel{}
	f := fallback.New([]model.ChatModel{m})
	h := &failedUsageCounterHook{counts: map[string]int32{}}
	a := agent_v2.NewBasicAgent(agent_v2.WithName("rate"), agent_v2.WithModel(f), agent_v2.WithHooks(h))
	for range 2 {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		_, err := a.Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("x"))
		cancel()
		if err == nil {
			t.Fatal("expected failure")
		}
	}
	if m.calls != 1 || h.counts["rate"] != 31 {
		t.Fatalf("calls=%d billed=%d want calls=1 billed=31", m.calls, h.counts["rate"])
	}
}

type failedUsageRetryModel struct{ calls int }

func (m *failedUsageRetryModel) Generate(context.Context, []*schema.Message, ...model.Option) (*schema.Message, error) {
	m.calls++
	if m.calls == 1 {
		return nil, model.WithFailureUsage(errors.New("billed failure"), &schema.MessageUsage{InputTokens: 31, OutputTokens: 9, ThinkTokens: 4}, "p", "bad")
	}
	out := schema.AssistantMessage("done")
	out.Metadata = &schema.MessageMetadata{System: "p", Model: "good", Usage: &schema.MessageUsage{InputTokens: 5, OutputTokens: 2}}
	return out, nil
}
func (m *failedUsageRetryModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m *failedUsageRetryModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

type failedUsageAskHook struct {
	hook.NoOpHooks
	seen int
}

func (h *failedUsageAskHook) OnModelEnd(ctx context.Context, _ *hook.AgentInfo, _ *schema.ModelOutput) (context.Context, error) {
	h.seen++
	_, err := compose_v2.Ask(ctx, &compose_v2.AskQuery{Question: "approve?"})
	return ctx, err
}
func TestRetryThenResumeCountsOnce(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	defer tp.Shutdown(context.Background())
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer mp.Shutdown(context.Background())
	obs := observability.New(observability.WithTracerProvider(tp), observability.WithMeterProvider(mp))
	m := &failedUsageRetryModel{}
	asker := &failedUsageAskHook{}
	ctx, opts := obs.Install(compose_v2.WithCheckpointerCtx(t.Context(), compose_v2.NewInMemoryCheckpointer()), agent_v2.WithName("resume"), agent_v2.WithModel(m), agent_v2.WithHooks(asker), agent_v2.WithModelRetry(compose_v2.SimpleRetry(1)))
	a := agent_v2.NewBasicAgent(opts...)
	sess := agent_v2.NewRunSession()
	ctx = compose_v2.WithSessionIDCtx(ctx, sess.RunID)
	_, err := a.Run(ctx, sess, schema.UserMessage("x"))
	ie, ok := compose_v2.IsInterrupt(err)
	if !ok {
		t.Fatalf("expected interrupt got %v", err)
	}
	if _, err = a.Resume(ctx, sess.RunID, agent_v2.WithResumeSession(sess), agent_v2.WithResumeUserInputs(map[string]*schema.Message{ie.Query.InterruptID: schema.UserMessage("yes")})); err != nil {
		t.Fatal(err)
	}
	if m.calls != 2 || asker.seen != 2 {
		t.Fatalf("calls=%d end hooks=%d", m.calls, asker.seen)
	}
	for name, want := range map[string]int64{observability.MetricUsageInputTokens: 36, observability.MetricUsageOutputTokens: 11, observability.MetricUsageReasoningTokens: 4} {
		if got := failedUsageMetricSum(t, reader, name); got != want {
			t.Errorf("%s=%d want=%d", name, got, want)
		}
	}
	n := 0
	for _, s := range rec.Ended() {
		for _, e := range s.Events() {
			if e.Name == observability.EventFailedModelUsage {
				n++
			}
		}
	}
	if n != 1 {
		t.Errorf("failure events=%d want=1", n)
	}
	if len(rec.Started()) != len(rec.Ended()) {
		t.Fatalf("unfinished spans %d/%d", len(rec.Ended()), len(rec.Started()))
	}
}

type failedUsageNestedRouteModel struct{ route *agent_v2.RoutingAgent }

func (m failedUsageNestedRouteModel) Generate(ctx context.Context, msgs []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	_, err := agent_v2.RunSubAgent(ctx, "router", m.route, msgs[len(msgs)-1])
	return nil, err
}
func (m failedUsageNestedRouteModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}
func (m failedUsageNestedRouteModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) {
	return m, nil
}
func TestNestedDirectRouteUsageIsolation(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	defer tp.Shutdown(context.Background())
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer mp.Shutdown(context.Background())
	obs := observability.New(observability.WithTracerProvider(tp), observability.WithMeterProvider(mp))
	m := &failedUsageFailModel{}
	r := agent_v2.NewRoutingAgent(agent_v2.WithRoutingName("route"), agent_v2.WithRoutingHooks(obs.AgentHooks()), agent_v2.WithRouteDecider("sample", decision.NewLLMRouteDecider(m, func(decision.LLMRouteSample) (float64, bool) { return 1, true }), 0))
	r.AddBranch("worker", agent_v2.NewBasicAgent(agent_v2.WithName("worker")))
	ctx, opts := obs.Install(t.Context(), agent_v2.WithName("parent"), agent_v2.WithModel(failedUsageNestedRouteModel{r}))
	_, err := agent_v2.NewBasicAgent(opts...).Run(ctx, agent_v2.NewRunSession(), schema.UserMessage("x"))
	if err == nil || m.calls != 1 {
		t.Fatalf("calls=%d err=%v", m.calls, err)
	}
	if got := failedUsageMetricSum(t, reader, observability.MetricUsageInputTokens); got != 31 {
		t.Fatalf("nested routing tokens=%d want31", got)
	}
	n := 0
	for _, s := range rec.Ended() {
		for _, e := range s.Events() {
			if e.Name == observability.EventFailedModelUsage {
				n++
			}
		}
	}
	if n != 1 {
		t.Errorf("nested routing events=%d want1", n)
	}
}
