// Package modelreplay 保存模型适配器和上下文压缩共同使用的请求回放标注。
package modelreplay

const (
	// AnthropicAssistantContent 保存 Claude 的 assistant 内容块及签名。
	AnthropicAssistantContent = "anthropic.assistant_content"
	// OpenAIResponsesItems 保存 Responses 请求可回放的原始输出项。
	OpenAIResponsesItems = "openai_responses_items"
)
