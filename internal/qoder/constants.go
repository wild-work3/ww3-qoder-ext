package qoder

// 上游接口路径（与旧版 constants.go 一致）。
const (
	// EpChat 是流式对话端点。Encode=1 表示请求体经 qoderEncode 编码。
	EpChat = "/algo/api/v2/service/pro/sse/agent_chat_generation?FetchKeys=llm_model_result&AgentId=agent_common&Encode=1"
	// EpModels 是模型列表端点。COSY 签名时 body 必须传空串（传 "{}" 会 403 Signature invalid）。
	EpModels = "/algo/api/v2/model/list?Encode=1"
	// EpDTRefresh 是 device token 刷新端点。
	EpDTRefresh = "/api/v1/deviceToken/refresh"
	// EpQuotaUsage 是额度查询端点。
	EpQuotaUsage = "/api/v2/quota/usage"
	// EpUserInfo 是用户信息端点。
	EpUserInfo = "/api/v1/userinfo"
	// EpPlan 是套餐信息端点。
	EpPlan = "/api/v2/user/plan"
	// EpCampaigns 是活动签到端点（仅 qodercn / qodercom）。
	EpCampaigns = "/sash/api/v1/me/campaigns"
	// EpCheckinStatus / EpCheckinClaim 是 qoder 区的旧每日签到端点（已无活动）。
	EpCheckinStatus = "/sash/api/v1/me/daily-check-in/status"
	EpCheckinClaim  = "/sash/api/v1/me/daily-check-in/claim"
	// EpDevicePoll 是 device_poll 轮询端点（query 参数由调用方补全）。
	EpDevicePoll = "/api/v1/deviceToken/poll"
)

// clientUA 与官方客户端保持一致。
const clientUA = "Go-http-client/2.0"

// defaultContextWindow 是上游默认上下文窗口。
const defaultContextWindow = 180000

// 模型表默认字段。
const (
	defaultFormat = "openai"
	defaultSource = "system"
)

// staticModelOrder 是 qoder 区静态模型表的稳定顺序。
var staticModelOrder = []string{
	"auto",
	"qwen3.8-max",
	"qwen3.7-max",
	"qwen3.7-plus",
	"qwen3.7-flash",
	"qwen3.6-flash",
	"deepseek-v4-pro",
	"deepseek-v4-flash",
	"glm-5.3",
	"glm-5.2",
	"kimi-k2.7-code",
	"minimax-m2.7",
}

// staticModelKeys 是 qoder 区的固定模型表：对外展示名 → 上游 key。
var staticModelKeys = map[string]string{
	"auto":              "auto",
	"qwen3.8-max":       "qmodel_38max",
	"qwen3.7-max":       "qmodel_latest",
	"qwen3.7-plus":      "qmodel",
	"qwen3.7-flash":     "q37fmodel",
	"qwen3.6-flash":     "q36fmodel",
	"deepseek-v4-pro":   "dmodel",
	"deepseek-v4-flash": "dfmodel",
	"glm-5.3":           "gmodel",
	"glm-5.2":           "gm51model",
	"kimi-k2.7-code":    "kmodel",
	"minimax-m2.7":      "mmodel",
}

// StaticModels 返回 qoder 区的静态模型表（其余区域返回 nil，走动态拉取）。
func StaticModels(region string) []DynamicModel {
	if NormalizeRegion(region) != RegionQoder {
		return nil
	}
	out := make([]DynamicModel, 0, len(staticModelOrder))
	for _, name := range staticModelOrder {
		out = append(out, DynamicModel{
			Key:           staticModelKeys[name],
			DisplayName:   name,
			Enable:        true,
			Format:        defaultFormat,
			Source:        defaultSource,
			ContextWindow: defaultContextWindow,
		})
	}
	return out
}

// StaticModelKey 在静态表里查展示名对应的上游 key；找不到返回空串。
func StaticModelKey(region, name string) string {
	if NormalizeRegion(region) != RegionQoder {
		return ""
	}
	return staticModelKeys[name]
}
