package anthropic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"

	"crew-go/model"
	"crew-go/schema"
)

// CountInputTokens 调用 Claude 的 token counting API，返回服务端估计的输入 token 数。
// 计数使用与生成相同的输入投影，包括合并 ExtraBody 后的工具、系统消息和签名 thinking。
// 该调用与生成分离。端点不支持的输入由 API 拒绝，实际计费用量以生成响应为准。
func (c *Client) CountInputTokens(ctx context.Context, messages []*schema.Message, opts ...model.Option) (int64, error) {
	req, err := c.buildRequest(ctx, messages, model.Apply(opts...))
	if err != nil {
		return 0, err
	}
	body := req.body
	if body == nil {
		body, err = json.Marshal(req.params)
		if err != nil {
			return 0, fmt.Errorf("anthropic: encode token counting input: %w", err)
		}
	}

	var generated map[string]json.RawMessage
	if err := json.Unmarshal(body, &generated); err != nil {
		return 0, fmt.Errorf("anthropic: decode token counting input: %w", err)
	}
	// 计数端点只接收这些请求字段。保留嵌套 JSON 值，避免经浮点数重编码工具参数。
	input := make(map[string]json.RawMessage)
	for _, key := range []string{
		"model", "messages", "system", "tools", "tool_choice", "thinking", "cache_control",
		"context_management", "compaction", "output_config", "output_format", "mcp_servers", "speed",
	} {
		if raw, ok := generated[key]; ok {
			input[key] = raw
		}
	}
	count := anthropic.BetaMessageCountTokensParams{Betas: slices.DeleteFunc(slices.Clone(req.params.Betas), func(beta anthropic.AnthropicBeta) bool {
		return beta == anthropic.AnthropicBetaServerSideFallback2026_07_01
	})}
	if err := json.Unmarshal(input["model"], &count.Model); err != nil || count.Model == "" {
		return 0, errors.New("anthropic: token counting requires a model name")
	}
	body, err = json.Marshal(input)
	if err != nil {
		return 0, fmt.Errorf("anthropic: encode token counting request: %w", err)
	}
	resp, err := c.sdk.Beta.Messages.CountTokens(ctx, count, option.WithRequestBody("application/json", body))
	if err != nil {
		return 0, c.requestError(err)
	}
	if resp == nil || !resp.JSON.InputTokens.Valid() || resp.InputTokens < 0 {
		return 0, &model.APIError{Provider: ProviderName, Err: errors.New("invalid token counting response")}
	}
	return resp.InputTokens, nil
}
