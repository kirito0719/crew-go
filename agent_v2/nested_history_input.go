package agent_v2

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// nestedInputSettled 是旧标记，也是成员自行接管输入历史时的标记。
type nestedInputSettled bool

// nestedHistoryInput 保存调用边界已经接管的输入。历史异步写入，入队不证明已提交。
type nestedHistoryInput struct {
	Key     HistoryKey      `json:"key"`
	Message json.RawMessage `json:"message"`
}

func init() {
	compose_v2.RegisterValueType[nestedInputSettled]("agent_v2.nestedInputSettled")
	compose_v2.RegisterValueType[nestedHistoryInput]("agent_v2.nestedHistoryInput")
}

func settledInputStepKey(call nestedCall) string {
	return "agent_v2.nested_call_input:" + identityHash(call.identity)
}

func saveNestedHistoryInput(ctx context.Context, call nestedCall, key HistoryKey, stamped *schema.Message) {
	var record any = nestedInputSettled(true)
	if stamped != nil {
		wire, err := json.Marshal(stamped)
		if err != nil {
			slog.WarnContext(ctx, "agent_v2: 无法保存挂起调用的历史输入快照", "ns", call.ns, "err", err)
		} else {
			record = nestedHistoryInput{Key: key, Message: wire}
		}
	}
	if err := compose_v2.SaveStepResult(ctx, settledInputStepKey(call), record, compose_v2.WithStreams(nil)); err != nil {
		slog.WarnContext(ctx, "agent_v2: 无法保存挂起调用的历史输入记录", "ns", call.ns, "err", err)
	}
}

// restoreNestedHistoryInput 返回输入结算是否已经由存档接管。跨次重造的调用参数
// 可以有不同 ID 或内容，补写只使用首次入队的原始快照。
func restoreNestedHistoryInput(ctx, subCtx context.Context, call nestedCall) bool {
	value, _ := compose_v2.GetStepResult[any](ctx, settledInputStepKey(call))
	switch record := value.(type) {
	case nestedInputSettled:
		return bool(record)
	case nestedHistoryInput:
		record.restore(subCtx)
		return true
	default:
		return false
	}
}

func (r nestedHistoryInput) restore(ctx context.Context) {
	w := historyWriterFrom(ctx)
	if w == nil || w.key != r.Key {
		return
	}
	var msg schema.Message
	decoder := json.NewDecoder(bytes.NewReader(r.Message))
	decoder.UseNumber()
	if err := decoder.Decode(&msg); err != nil {
		slog.WarnContext(ctx, "agent_v2: 无法解码挂起调用的历史输入快照", "key", w.key.String(), "err", err)
		return
	}
	agent, _ := msg.Annotations[HistoryAgentAnnotation].(string)
	task, _ := msg.Annotations[HistoryTaskAnnotation].(string)
	trail, _ := msg.Annotations[historyScopeAnnotation].(string)
	if msg.Role != schema.RoleUser || isDerivedInput(&msg) || msg.ID == "" || agent != HistorySlot(ctx) || task == "" || task != w.task ||
		trail == "" || trail != historyScopeTrail(ctx) {
		return
	}
	w.waitDrained(ctx)
	if ctx.Err() != nil {
		return
	}
	found := false
	source := historySource{store: w.store, key: w.key, task: w.task}
	query := HistoryQuery{MessageID: msg.ID, Agent: agent, Task: task}
	if err := source.scanBackward(ctx, query, historyPageMin, true, func(saved *schema.Message, _ HistoryCursor) bool {
		found = saved.Role == schema.RoleUser && saved.Annotations[historyScopeAnnotation] == trail
		return found
	}, nil); err != nil {
		slog.WarnContext(ctx, "agent_v2: 核对挂起调用的历史输入失败", "key", w.key.String(), "err", err)
		return
	}
	if !found && w.claimRecordHere(agent, &msg) {
		msg.Annotations[historyRecoveredInputAnnotation] = true
		msgs := []*schema.Message{&msg}
		w.markRecorded(agent, msgs)
		registerHistoryOutputs(ctx, msgs)
		w.submit(msgs)
	}
}
