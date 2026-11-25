package agent_v2

import "crew-go/schema"

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
