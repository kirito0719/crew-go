package agent_v2

import (
	"context"

	"crew-go/schema"
)

// historyInvocation 是持久历史中的一次确定调用。仅从同时具备任务、成员和调用轨迹的记录关联输入。
type historyInvocation struct {
	task  string
	agent string
	trail string
}

func historyInvocationOf(message *schema.Message) historyInvocation {
	if message == nil {
		return historyInvocation{}
	}
	task, _ := message.Annotations[HistoryTaskAnnotation].(string)
	agent, _ := message.Annotations[HistoryAgentAnnotation].(string)
	trail, _ := message.Annotations[historyScopeAnnotation].(string)
	if task == "" || agent == "" || trail == "" {
		return historyInvocation{}
	}
	return historyInvocation{task: task, agent: agent, trail: trail}
}

func invocationInput(message *schema.Message) bool {
	return message != nil && message.Role == schema.RoleUser && !IsFrameworkNote(message) &&
		!isCallerHistory(message) && !isDerivedInput(message)
}

// retainedInvocations 只从实际保留的工具结果和产出找输入，保持首次产出的顺序。
func retainedInvocations(kept []*schema.Message) []historyInvocation {
	var needed []historyInvocation
	seen := make(map[historyInvocation]bool)
	for _, message := range kept {
		if message == nil || isPriorProtected(message) || (message.Role != schema.RoleAssistant && message.Role != schema.RoleTool) {
			continue
		}
		key := historyInvocationOf(message)
		if key != (historyInvocation{}) && !seen[key] {
			needed = append(needed, key)
			seen[key] = true
		}
	}
	return needed
}

// locateInvocationInputs 只定位尾部产出缺少的调用输入。正式翻页仍按原顺序处理消息和已验证的摘要边界。
func (s historySource) locateInvocationInputs(ctx context.Context, prior []*schema.Message, owner string, limit int, before HistoryCursor) (map[historyInvocation]string, error) {
	kept := truncatePriorPreservingMarkers(prior, limit)
	keys := retainedInvocations(kept)
	if len(keys) == 0 {
		return nil, nil
	}
	present := make(map[historyInvocation]bool)
	for _, message := range kept {
		if invocationInput(message) {
			present[historyInvocationOf(message)] = true
		}
	}
	byTask := make(map[string]map[historyInvocation]string)
	for _, key := range keys {
		if key.agent == owner && !present[key] {
			if byTask[key.task] == nil {
				byTask[key.task] = make(map[historyInvocation]string)
			}
			byTask[key.task][key] = ""
		}
	}
	found := make(map[historyInvocation]string)
	for task, missing := range byTask {
		err := s.scanBackward(ctx, HistoryQuery{Task: task, Agent: owner, Before: before}, historyPageMin, true, func(message *schema.Message, _ HistoryCursor) bool {
			if invocationInput(message) && message.ID != "" {
				key := historyInvocationOf(message)
				if _, wanted := missing[key]; wanted {
					// 从新到旧扫描，定位该次调用最早记录的实际输入。
					missing[key] = message.ID
				}
			}
			return false
		}, nil)
		if err != nil {
			return nil, err
		}
		for key, id := range missing {
			if id != "" {
				found[key] = id
			}
		}
	}
	return found, nil
}
