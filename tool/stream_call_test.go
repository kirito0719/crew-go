package tool_test

import (
	"context"
	"errors"
	"io"
	"testing"

	"crew-go/schema"
	"crew-go/tool"
)

type streamCallTool struct {
	*tool.FuncTool
	stream func(context.Context, map[string]any) (*schema.StreamReader[*tool.Result], error)
}

func (s streamCallTool) StreamCall(ctx context.Context, args map[string]any) (*schema.StreamReader[*tool.Result], error) {
	return s.stream(ctx, args)
}

func TestToolkitStreamCallDispatchAndFallback(t *testing.T) {
	ctx, args := t.Context(), map[string]any{"city": "成都"}
	tk := tool.NewToolkit()
	calls, streams := 0, 0
	invoke := func(got context.Context, arguments map[string]any) {
		t.Helper()
		if got != ctx || arguments["city"] != "成都" {
			t.Fatalf("工具收到 ctx=%v，参数 %v", got, arguments)
		}
	}
	whole := tool.Success("完整结果")
	syncTool := tool.NewFuncTool("search", "查天气", nil, func(c context.Context, a map[string]any) (*tool.Result, error) {
		calls++
		invoke(c, a)
		return whole, nil
	})
	first, second := tool.Success("开始"), tool.Success("完成")
	tk.Register(streamCallTool{syncTool, func(c context.Context, a map[string]any) (*schema.StreamReader[*tool.Result], error) {
		streams++
		invoke(c, a)
		reader, send := schema.NewStreamReader[*tool.Result](2)
		send(first)
		send(second)
		schema.CloseSend(reader)
		return reader, nil
	}})
	check := func(want ...*tool.Result) {
		t.Helper()
		r, err := tk.StreamCall(ctx, "search", args)
		if err != nil || r == nil {
			t.Fatalf("打开工具流失败，err=%v", err)
		}
		defer r.Close()
		for _, result := range want {
			got, err := r.Recv(ctx)
			if err != nil || got != result {
				t.Fatalf("期望结果 %v，实际 %v err=%v", result, got, err)
			}
		}
		if _, err := r.Recv(ctx); !errors.Is(err, io.EOF) {
			t.Fatalf("读取末尾应为 EOF，实际 %v", err)
		}
	}
	check(first, second)
	if calls != 0 || streams != 1 {
		t.Fatalf("支持流式的工具调用次数 stream=%d sync=%d", streams, calls)
	}
	tk.Register(syncTool)
	check(whole)
	if calls != 1 || streams != 1 {
		t.Fatalf("同步工具退化后的调用次数 stream=%d sync=%d", streams, calls)
	}
}

func TestToolkitStreamCallPropagatesFailure(t *testing.T) {
	want := errors.New("获取结果失败")
	tk := tool.NewToolkit()
	syncTool := tool.NewFuncTool("sync", "同步", nil, func(context.Context, map[string]any) (*tool.Result, error) {
		return nil, want
	})
	tk.Register(syncTool)
	tk.Register(streamCallTool{tool.NewFuncTool("native", "流式", nil, syncTool.Call), func(context.Context, map[string]any) (*schema.StreamReader[*tool.Result], error) {
		return nil, want
	}})
	for _, name := range []string{"sync", "native"} {
		if r, err := tk.StreamCall(t.Context(), name, nil); r != nil || !errors.Is(err, want) {
			t.Errorf("%s 返回的流或错误不符，流=%v err=%v", name, r, err)
		}
	}
	if r, err := tk.StreamCall(t.Context(), "missing", nil); r != nil || err == nil {
		t.Errorf("未知工具返回的流或错误不符，流=%v err=%v", r, err)
	}
}

func TestToolkitStreamCallRejectsMissingResult(t *testing.T) {
	tk := tool.NewToolkit()
	empty := func(context.Context, map[string]any) (*tool.Result, error) { return nil, nil }
	tk.Register(tool.NewFuncTool("sync", "同步", nil, empty))
	tk.Register(streamCallTool{tool.NewFuncTool("native", "流式", nil, empty), func(context.Context, map[string]any) (*schema.StreamReader[*tool.Result], error) {
		return nil, nil
	}})
	for _, name := range []string{"sync", "native"} {
		if r, err := tk.StreamCall(t.Context(), name, nil); err == nil || r != nil {
			if r != nil {
				r.Close()
			}
			t.Errorf("%s 缺少结果应在调用处返回错误，流=%v err=%v", name, r != nil, err)
		}
	}
}
