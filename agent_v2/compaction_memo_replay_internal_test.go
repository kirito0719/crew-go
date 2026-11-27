package agent_v2

import (
	"encoding/json"
	"testing"

	"crew-go/internal/modelreplay"
	"crew-go/schema"
)

func TestCompactionMemoTracksProviderReplay(t *testing.T) {
	for _, key := range []string{modelreplay.AnthropicAssistantContent, modelreplay.OpenAIResponsesItems} {
		t.Run(key, func(t *testing.T) {
			msg := schema.AssistantMessage("可见答复")
			msg.Annotations = map[string]any{key: "provider-snapshot-A"}
			orig := []*schema.Message{msg, schema.UserMessage("当前输入")}
			memo := nextCompactionMemo(orig, 0, 0, len(orig), orig, 1)

			for _, tc := range []struct {
				name        string
				annotations map[string]any
				reuse       bool
			}{
				{name: "removed"},
				{name: "replaced", annotations: map[string]any{key: "provider-snapshot-B"}},
				{name: "unrelated", reuse: true, annotations: map[string]any{key: "provider-snapshot-A", "other": "changed"}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					current := *msg
					current.Annotations = tc.annotations
					in := []*schema.Message{&current, orig[1]}
					out, upto, _ := memo.apply(in)
					if (upto == 1) != tc.reuse {
						t.Fatalf("备忘复用有误，应用的原前缀条数 %d", upto)
					}
					if !tc.reuse && out[0] != &current {
						t.Fatal("模型应拿到本轮改写后的消息")
					}
				})
			}
		})
	}
}

func TestLegacyCompactionMemoDoesNotRestoreRemovedReplay(t *testing.T) {
	current := schema.AssistantMessage("恢复后仍有可见答复")
	original := *current
	original.Annotations = map[string]any{modelreplay.AnthropicAssistantContent: "旧的提供方快照"}
	in := []*schema.Message{current}
	// 旧指纹与剥除回放标注后的输入相同。存量 Head 可含旧快照。
	legacy := compactionMemo{Upto: len(in), Anchor: prefixAnchor(in), Head: []*schema.Message{&original}}
	payload, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	var restored compactionMemo
	if err := json.Unmarshal(payload, &restored); err != nil {
		t.Fatal(err)
	}
	if out, upto, _ := restored.apply(in); upto != 0 || out[0] != current {
		t.Fatal("旧备忘不能重新引入被移除的回放快照")
	}
	restored.Head = []*schema.Message{schema.UserMessage("旧摘要")}
	if out, upto, _ := restored.apply(in); upto != len(in) || out[0].Content != "旧摘要" {
		t.Fatal("与当前输入匹配且无回放快照的旧备忘仍可复用")
	}
}
