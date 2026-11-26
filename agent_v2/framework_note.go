package agent_v2

import (
	"slices"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// IsFrameworkNote 报 m 是不是框架以 user 角色加入模型视图的提示。
// 包括后台任务通知、工具重复调用提醒、Skill 正文、上下文恢复视图和超窗重试提示。
// 压缩和 [WithToolFilter] 用它识别真正的用户输入。框架合成的 assistant 记账文本见 [IsUnresolvedCallsRecord]。
func IsFrameworkNote(m *schema.Message) bool {
	if m == nil || m.Role != schema.RoleUser {
		return false
	}
	if IsBackgroundTaskNotification(m) || isRecoveryView(m) {
		return true
	}
	if reminder, _ := m.Annotations[RepeatToolReminderAnnotation].(bool); reminder {
		return true
	}
	retry, _ := m.Annotations[reactiveCompactNote].(bool)
	body, _ := m.Annotations[SkillBodyAnnotation].(bool)
	reference, _ := m.Annotations[skillReferenceMessage].(bool)
	return retry || body || reference
}

// lastConversationGroup 找到最近含业务对话的组。退到首组时保留整个后缀。
// 追加输入、框架提示和 system 消息之间的分组边界不切断它们与前一轮业务消息的联系。
func lastConversationGroup(groups [][]*schema.Message) int {
	for i := len(groups) - 1; i > 0; i-- {
		for _, m := range groups[i] {
			if m != nil && m.Role != schema.RoleSystem && !IsFrameworkNote(m) && !isPendingUserInput(m) {
				return i
			}
		}
	}
	return 0
}

// pendingUserTailStart 找到尚未回应的追加输入及其引用的业务组。
// start 是需要查找追加输入的首组。被引用的业务组也可以位于此前的备忘里。
func pendingUserTailStart(groups [][]*schema.Message, start int) int {
	waiting := false
	for i := len(groups) - 1; i >= 0; i-- {
		if i < start && !waiting {
			break
		}
		group := groups[i]
		for j := len(group) - 1; j >= 0; j-- {
			m := group[j]
			if m == nil {
				continue
			}
			user := m.Role == schema.RoleUser && !IsFrameworkNote(m)
			if user && isPendingUserInput(m) {
				waiting = true
				continue
			}
			reply := m.Role == schema.RoleAssistant && !IsUnresolvedCallsRecord(m)
			if user || reply {
				if waiting {
					return keepToolPairsInTail(groups, i)
				}
				return len(groups)
			}
		}
	}
	if waiting {
		return 0
	}
	return len(groups)
}

// keepToolPairsInTail 将待答后缀向前扩展到其中回执的声明处。
func keepToolPairsInTail(groups [][]*schema.Message, start int) int {
	if start == 0 {
		return start
	}
	messages := compose_v2.FlattenGroups(groups)
	groupAt := make([]int, 0, len(messages))
	for i, group := range groups {
		for range group {
			groupAt = append(groupAt, i)
		}
	}
	// 按声明位置倒扫。起点往前移后，尚未扫描的更早声明仍可进一步扩展后缀。
	for _, pair := range slices.Backward(pairToolResults(messages)) {
		group := groupAt[pair.declaredAt]
		if group < start && pair.result != nil && groupAt[pair.resultAt] >= start {
			start = group
		}
	}
	return start
}
