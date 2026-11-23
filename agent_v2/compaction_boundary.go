package agent_v2

// AutoCompact 生成摘要后，模型节点把受保护的组、摘要和保留的原文组成跨 Run 的候选边界。
// 模型节点只把自身消息流产生的候选交给历史写入器。写入器以 preamble 读取历史前的尾游标为起点，
// 逐批条件追加本次调用的记录。每次成功都推进游标；遇到并发写入或不确定的回执就放弃后续候选。
//
// 后端只对成功批次中的指定行保存位置证明。historyPrior 可在同归属、同视图的已确认行停止向前翻页。
// 旧边界、重新追加的副本及损坏的替代历史都跳过，继续读取更早的原始记录或已确认边界。
// 会话原始记录按追加顺序保留，可通过 LoadAllHistory 回看。

import (
	"encoding/json"
	"strconv"

	"crew-go/schema"
)

// HistoryCompactionAnnotation 标「这条历史记录是一条压缩边界」，值是边界那一刻完整替代历史的 JSON
// （[]*schema.Message）。
//
// 边界记录的正文是那次的摘要，供人回看；框架读回 prior 时用的是这个替代历史，见 compaction_boundary.go。
// 业务回看会话时可以按这个键跳过它，或把它当作「此前的摘要」展示。
const HistoryCompactionAnnotation = "crew_history_compaction"

// historyCompactionModeAnnotation 记边界写下时的视图模式（[HandoffHistoryMode] 的十进制）。
const historyCompactionModeAnnotation = "crew_history_compaction_mode"

// isCompactionBoundary 报这条历史记录是不是压缩边界。
func isCompactionBoundary(m *schema.Message) bool {
	if m == nil || m.Annotations == nil {
		return false
	}
	_, ok := m.Annotations[HistoryCompactionAnnotation]
	return ok
}

// buildCompactionBoundary 用这一轮的压缩结果拼边界记录。msgs 是这一轮交给 modelFn 的消息流（钩子改写
// 之后、套用备忘之前），memo 是这一轮新记下的结果。替代历史 = Head 去掉长期记忆注入 + 消息流第 Upto 条
// 之后（去掉派生输入）。
func buildCompactionBoundary(memo *compactionMemo, msgs []*schema.Message, mode HandoffHistoryMode) *schema.Message {
	if memo == nil || memo.Upto > len(msgs) {
		return nil
	}
	replacement := make([]*schema.Message, 0, len(memo.Head)+len(msgs)-memo.Upto)
	summary := ""
	for _, m := range memo.Head {
		if m == nil || isStoreInjected(m) {
			continue
		}
		if summary == "" && isCompactSummary(m) {
			summary = m.Content
		}
		replacement = append(replacement, m)
	}
	for _, m := range msgs[memo.Upto:] {
		if m == nil || isDerivedInput(m) || isStoreInjected(m) {
			continue
		}
		replacement = append(replacement, m)
	}
	if summary == "" {
		return nil // 没有摘要的「压缩结果」不该成为边界
	}
	data, err := json.Marshal(replacement)
	if err != nil {
		return nil
	}
	b := schema.UserMessage(summary)
	b.Annotations = map[string]any{
		HistoryCompactionAnnotation:     string(data),
		historyCompactionModeAnnotation: strconv.Itoa(int(mode)),
	}
	return b
}

// decodeCompactionBoundary 取边界记录里的替代历史；视图模式对不上或解不出来时 ok=false。
func decodeCompactionBoundary(m *schema.Message, mode HandoffHistoryMode) ([]*schema.Message, bool) {
	if m.Annotations[historyCompactionModeAnnotation] != strconv.Itoa(int(mode)) {
		return nil, false
	}
	raw, _ := m.Annotations[HistoryCompactionAnnotation].(string)
	var replacement []*schema.Message
	if err := json.Unmarshal([]byte(raw), &replacement); err != nil || len(replacement) == 0 {
		return nil, false
	}
	return replacement, true
}

// isCompactSummary 报这条消息是不是 AutoCompact 产出的摘要。
func isCompactSummary(m *schema.Message) bool {
	if m == nil || m.Annotations == nil {
		return false
	}
	v, ok := m.Annotations[AutoCompactSummaryAnnotation].(bool)
	return ok && v
}

// isStoreInjected 报这条消息是不是长期记忆注入（preamble 每次 Run 重新检索注入）。
func isStoreInjected(m *schema.Message) bool {
	if m == nil || m.Annotations == nil {
		return false
	}
	v, ok := m.Annotations[StoreInjectedAnnotation].(bool)
	return ok && v
}
