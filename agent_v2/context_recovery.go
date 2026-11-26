package agent_v2

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"crew-go/compose_v2"
	"crew-go/schema"
	"crew-go/store"
	"crew-go/tool"
)

// ReadContextToolName 是按定位符取回已折叠上下文的框架工具。
const ReadContextToolName = "read_context"

// ContextRecoveryNoteAnnotation 是业务在 assistant 消息上提供的简短笔记。
// 折叠时优先使用它；未提供时从正文生成有界预览，不额外调用摘要模型。
const ContextRecoveryNoteAnnotation = "crew_context_note"

// ContextRecoveryAnnotation 标记折叠视图，值为原文定位符。
const ContextRecoveryAnnotation = "crew_context_recovery"

const contextRecoveryScopeAnnotation = "crew_context_recovery_scope"

// ContextRecoveryConfig 配置 Folio 式的笔记、调用清单和原文恢复。
// 折叠只改变送模视图，原始消息仍保存在 channel 与历史中。
type ContextRecoveryConfig struct {
	// MaxTokens 是对话估算 token 的触发门槛与目标，<=0 用 16000。前言与工具定义不计入。
	MaxTokens int
	// KeepRecentRounds 是至少完整保留的最近 API 消息组数，<=0 用 4。当前用户输入始终保留。
	KeepRecentRounds int
	// NoteBytes 是单组笔记的字节上限，<=0 用 256，最多 2000。
	NoteBytes int
	// Store 保存原文；nil 时使用当前 ctx 的 Store。没有存储或写入失败时保留原文。
	Store store.Store
	// TTL 在支持 TTL 的 backend 上控制原文寿命；<=0 不过期。过期后 read_context 返回 not_found。
	TTL time.Duration
}

// WithContextRecovery 启用可取回的上下文折叠；nil 使用默认配置。
// 配置会复制，存储依赖仍由调用方管理。恢复按 owner、会话与 agent 图位置隔离。
func WithContextRecovery(cfg *ContextRecoveryConfig) Option {
	return func(c *Config) {
		copy := ContextRecoveryConfig{}
		if cfg != nil {
			copy = *cfg
			if cfg.Store != nil && isFrameworkNil(cfg.Store) {
				panic("WithContextRecovery: typed nil Store")
			}
		}
		c.ContextRecovery = &copy
	}
}

func (c *ContextRecoveryConfig) budget() int {
	if c.MaxTokens > 0 {
		return c.MaxTokens
	}
	return 16000
}

func (c *ContextRecoveryConfig) recent() int {
	if c.KeepRecentRounds > 0 {
		return c.KeepRecentRounds
	}
	return 4
}

func (c *ContextRecoveryConfig) noteBytes() int {
	if c.NoteBytes > 0 {
		return min(c.NoteBytes, 2000)
	}
	return 256
}

func (c *ContextRecoveryConfig) storage(ctx context.Context) store.Store {
	st := c.Store
	if isFrameworkNil(st) {
		st = store.FromContext(ctx)
	}
	if isFrameworkNil(st) {
		return nil
	}
	return st
}

func recoveryNamespace(ctx context.Context, agent string) store.Namespace {
	owner, conv := conversationScope(ctx)
	return store.NS("_context_recovery", owner, conv, shortHash(historyOwner(ctx, agent)))
}

func recoveryScope(ctx context.Context, agent string) string {
	return shortHash(strings.Join(recoveryNamespace(ctx, agent), "\x00"))
}

func isRecoveryView(m *schema.Message) bool {
	if m == nil {
		return false
	}
	id, _ := m.Annotations[ContextRecoveryAnnotation].(string)
	return id != ""
}

// recoveryViewsMatch 防止旧备忘把别的身份下的定位符带入本次调用。
func recoveryViewsMatch(ctx context.Context, agent string, msgs []*schema.Message) bool {
	for _, m := range msgs {
		if isRecoveryView(m) && m.Annotations[contextRecoveryScopeAnnotation] != recoveryScope(ctx, agent) {
			return false
		}
	}
	return true
}

// recoveryMessage 只归档模型可见字段，不暴露应用 annotations、计费元数据或前言。
type recoveryMessage struct {
	Role      schema.MessageRole     `json:"role"`
	Content   string                 `json:"content,omitempty"`
	Reasoning string                 `json:"reasoning,omitempty"`
	Refusal   string                 `json:"refusal,omitempty"`
	Blocks    []*schema.ContentBlock `json:"blocks,omitempty"`
}

func recoveryArchive(group []*schema.Message) (string, error) {
	messages := make([]recoveryMessage, 0, len(group))
	for _, m := range group {
		if m != nil {
			messages = append(messages, recoveryMessage{m.Role, m.Content, m.ReasoningContent, m.Refusal, m.ContentBlocks})
		}
	}
	data, err := json.Marshal(struct {
		Version  int               `json:"version"`
		Messages []recoveryMessage `json:"messages"`
	}{1, messages})
	return string(data), err
}

// incompleteRecoveryGroups 在完整送模视图中配对，标记有未配对工具块或配对跨组的组。
func incompleteRecoveryGroups(groups [][]*schema.Message) []bool {
	messages := compose_v2.FlattenGroups(groups)
	groupAt := make([]int, 0, len(messages))
	incomplete := make([]bool, len(groups))
	unmatchedResults := make([]int, len(groups))
	for i, group := range groups {
		for _, m := range group {
			groupAt = append(groupAt, i)
			if m == nil || m.Role != schema.RoleTool {
				continue
			}
			ids := toolResultIDs(m)
			incomplete[i] = incomplete[i] || len(ids) == 0 || slices.Contains(ids, "")
			unmatchedResults[i] += len(ids)
		}
	}

	for _, pair := range pairToolResults(messages) {
		callGroup := groupAt[pair.declaredAt]
		if pair.result == nil || groupAt[pair.resultAt] != callGroup {
			incomplete[callGroup] = true
			continue
		}
		unmatchedResults[callGroup]--
	}
	for i, n := range unmatchedResults {
		incomplete[i] = incomplete[i] || n != 0
	}
	return incomplete
}

func recoveryPlaceholder(ctx context.Context, agent string, cfg *ContextRecoveryConfig, group []*schema.Message, id string) *schema.Message {
	var note, manifest strings.Builder
	for _, m := range group {
		if m == nil {
			continue
		}
		if value, _ := m.Annotations[ContextRecoveryNoteAnnotation].(string); value != "" {
			if note.Len() < cfg.noteBytes() {
				note.WriteString(truncateWithEllipsis(value, cfg.noteBytes()-note.Len()))
			}
		}
		if manifest.Len() < 1000 {
			fmt.Fprintf(&manifest, "%s", m.Role)
			for _, tc := range schema.GetToolCalls(m) {
				fmt.Fprintf(&manifest, " call=%s(%s)", truncateWithEllipsis(tc.Function.Name, 80), truncateWithEllipsis(tc.ID, 80))
			}
			if m.Role == schema.RoleTool {
				for _, result := range toolResultIDs(m) {
					fmt.Fprintf(&manifest, " result=%s", truncateWithEllipsis(result, 80))
				}
			}
			manifest.WriteByte('\n')
		}
	}
	if note.Len() == 0 {
		for _, m := range group {
			if m != nil && m.Content != "" && note.Len() < cfg.noteBytes() {
				note.WriteString(truncateWithEllipsis(m.Content, cfg.noteBytes()-note.Len()))
			}
		}
	}
	content := fmt.Sprintf("[此前上下文已归档，以下是笔记与调用清单，原文可用 %s 按字节分页读取，id=%q]\n笔记：%s\n清单：\n%s",
		ReadContextToolName, id, note.String(), truncateWithEllipsis(manifest.String(), 1000))
	msg := schema.UserMessage(content)
	msg.Annotations = map[string]any{ContextRecoveryAnnotation: id, contextRecoveryScopeAnnotation: recoveryScope(ctx, agent)}
	return msg
}

// foldContextRecovery 返回有界视图和未改动后缀的长度。控制错误由调用方继续传播。
func foldContextRecovery(ctx context.Context, agent string, cfg *ContextRecoveryConfig, msgs []*schema.Message) ([]*schema.Message, int) {
	tokens := compose_v2.EstimateMessageTokens(msgs)
	if tokens <= cfg.budget() {
		return msgs, -1
	}
	st := cfg.storage(ctx)
	if st == nil {
		return msgs, -1
	}
	groups := compose_v2.GroupMessagesByAPIRound(msgs)
	if len(groups) <= cfg.recent() {
		return msgs, -1
	}
	incomplete := incompleteRecoveryGroups(groups)
	lastUser := lastUserGroupIndex(groups, true)
	currentUser := lastUserGroupIndex(groups, false)
	pendingStart := len(groups)
	if lastUser >= 0 && allPendingUserInput(groups[lastUser]) {
		pendingStart = lastUser
		for pendingStart > 0 && allPendingUserInput(groups[pendingStart]) {
			pendingStart--
		}
	}
	out := make([]*schema.Message, 0, len(msgs))
	consumed, lastFold := 0, 0
	for i, group := range groups {
		consumed += len(group)
		protected := isProtectedGroup(group) || groupContainsProtectedMarker(group) || groupContainsSystem(group)
		if tokens <= cfg.budget() || i >= len(groups)-cfg.recent() || i == lastUser || i == currentUser || i >= pendingStart || protected || incomplete[i] {
			out = append(out, group...)
			continue
		}
		archive, err := recoveryArchive(group)
		if err != nil {
			out = append(out, group...)
			continue
		}
		id := "cx_" + shortHash(archive)
		view := recoveryPlaceholder(ctx, agent, cfg, group, id)
		saved := compose_v2.EstimateMessageTokens(group) - compose_v2.EstimateMessageTokens([]*schema.Message{view})
		if saved <= 0 {
			out = append(out, group...)
			continue
		}
		var opts []store.PutOption
		if cfg.TTL > 0 {
			if ttl, ok := st.(store.TTLAware); ok && ttl.SupportsTTL() {
				opts = append(opts, store.WithTTL(cfg.TTL))
			}
		}
		if err := st.Put(ctx, recoveryNamespace(ctx, agent), id, archive, opts...); err != nil {
			slog.WarnContext(ctx, "context archive failed, retain original messages", "agent", agent, "err", err)
			out = append(out, group...)
			continue
		}
		out = append(out, view)
		tokens -= saved
		lastFold = consumed
	}
	if lastFold == 0 {
		return msgs, -1
	}
	return out, len(msgs) - lastFold
}

func newReadContextTool(cfg *ContextRecoveryConfig, agent string) tool.Tool {
	return markMinted(tool.NewFuncTool(ReadContextToolName,
		"取回已归档的上下文原文（JSON）。id 来自上下文笔记；offset 从 0 开始，limit 默认 2000、最多 2000 字节；query 可查找文本。按 next_offset 继续读取。",
		map[string]any{"type": "object", "properties": map[string]any{
			"id": map[string]any{"type": "string"}, "offset": map[string]any{"type": "integer"},
			"limit": map[string]any{"type": "integer"}, "query": map[string]any{"type": "string"}}, "required": []string{"id"}},
		func(ctx context.Context, args map[string]any) (*tool.Result, error) {
			return readContext(ctx, cfg, agent, args), nil
		}))
}

func readContext(ctx context.Context, cfg *ContextRecoveryConfig, agent string, args map[string]any) *tool.Result {
	id, _ := args["id"].(string)
	if len(id) != 35 || !strings.HasPrefix(id, "cx_") {
		return tool.Failure("invalid_arguments", "需要笔记中的上下文 id")
	}
	st := cfg.storage(ctx)
	if st == nil {
		return tool.Failure("unavailable", "上下文存储不可用")
	}
	item, err := st.Get(ctx, recoveryNamespace(ctx, agent), id)
	if err != nil {
		return tool.Failure("unavailable", fmt.Sprintf("读取上下文失败：%v", err))
	}
	if item == nil {
		return tool.Failure("not_found", "上下文不存在、已过期或不属于当前会话与 agent")
	}
	content, ok := item.Value.(string)
	if !ok {
		return tool.Failure("invalid_archive", "上下文存档格式无效")
	}
	offset, limit := intArg(args, "offset", 0), intArg(args, "limit", 2000)
	if offset < 0 || offset > len(content) || offset < len(content) && !utf8.RuneStart(content[offset]) || limit <= 0 {
		return tool.Failure("invalid_arguments", "offset 需要是原文中的 UTF-8 字节边界，limit 需要为正数")
	}
	if query, _ := args["query"].(string); query != "" {
		if len(query) > 256 {
			return tool.Failure("invalid_arguments", "query 最多 256 字节")
		}
		index := strings.Index(content[offset:], query)
		if index < 0 {
			return tool.Failure("not_found", "此位置之后没有匹配文本")
		}
		offset += index
	}
	end := min(len(content), offset+min(limit, 2000))
	for end > offset && end < len(content) && !utf8.RuneStart(content[end]) {
		end--
	}
	if end == offset && end < len(content) {
		return tool.Failure("invalid_arguments", "limit 不足以容纳下一个 UTF-8 字符")
	}
	page, err := json.Marshal(struct {
		ID     string `json:"id"`
		Offset int    `json:"offset"`
		Next   int    `json:"next_offset"`
		Total  int    `json:"total_bytes"`
		EOF    bool   `json:"eof"`
		Text   string `json:"text"`
	}{id, offset, end, len(content), end == len(content), content[offset:end]})
	if err != nil {
		return tool.Failure("invalid_archive", "上下文分页编码失败")
	}
	return tool.Success(string(page))
}
