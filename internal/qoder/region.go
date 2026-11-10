// Package qoder 是 Qoder 三个区变体（qoder / qodercn / qodercom）共用的上游实现。
//
// 单实现 + Region 参数化：一个 region 字段切 host、session_type、COSY 头族与模型表，
// 避免像旧版那样把三个区各写一份重复代码（见迁移前的 qoder/qodercn/qodercom 三套包）。
package qoder

import (
	"strings"
)

// 区域标识。它同时是模型命名空间前缀（对外模型 ID 形如 "<region>/<upstream>"）
// 和凭据文件名前缀（auths/<region>-<uid>.json）。
const (
	// RegionQoder 是国内正式版。
	RegionQoder = "qoder"
	// RegionQoderCN 是国内活动版，与 RegionQoder 同 host，但走 campaigns 签到。
	RegionQoderCN = "qodercn"
	// RegionQoderCOM 是国际版。
	RegionQoderCOM = "qodercom"
)

// 各区域上游基址。
const (
	openAPIQoder    = "https://openapi.qoder.com.cn"
	gatewayQoder    = "https://gateway.qoder.com.cn"
	openAPIQoderCOM = "https://openapi.qoder.sh"
	gatewayQoderCOM = "https://api1.qoder.sh"
	modelsQoderCOM  = "https://api2.qoder.sh"
)

// 各区域 OAuth 客户端标识（以旧版 constants.go 为准）。
const (
	clientIDQoder    = "1c5e33e1-364d-4ce6-b02c-acaa81274a5c"
	clientIDQoderInt = "e883ade2-e6e3-4d6d-adf7-f92ceff5fdcb"
)

// Endpoints 是一次请求 / 登录流程用到的端点与协议参数集合。
type Endpoints struct {
	Region      string // 区域标识（= 模型前缀 = 凭据文件名前缀）
	OpenAPI     string // Bearer 接口基址（deviceToken refresh / quota / userinfo / poll）
	Gateway     string // COSY 签名接口基址（对话）
	Models      string // 模型列表基址（国际版与 Gateway 不同）
	SessionType string // 对话 body 里的 session_type
	CosyVersion string // COSY 头与 payload 里的 cosyVersion
	ClientID    string // device_poll 的 OAuth client_id
	RedirectURI string // 仅 qoder 区在授权 URL 上带 redirect_uri
	Domain      string // 凭据里记录的站点域名
}

// EndpointsFor 按区域返回端点集合，未知区域归一为 qoder 区。
func EndpointsFor(region string) Endpoints {
	switch NormalizeRegion(region) {
	case RegionQoderCN:
		return Endpoints{
			Region:      RegionQoderCN,
			OpenAPI:     openAPIQoder,
			Gateway:     gatewayQoder,
			Models:      gatewayQoder,
			SessionType: "qoder",
			CosyVersion: "1.0.10",
			ClientID:    clientIDQoderInt,
			Domain:      "qoder.com.cn",
		}
	case RegionQoderCOM:
		return Endpoints{
			Region:      RegionQoderCOM,
			OpenAPI:     openAPIQoderCOM,
			Gateway:     gatewayQoderCOM,
			Models:      modelsQoderCOM,
			SessionType: "qoder",
			CosyVersion: "1.0.10",
			ClientID:    clientIDQoderInt,
			Domain:      "qoder.com",
		}
	default:
		return Endpoints{
			Region:      RegionQoder,
			OpenAPI:     openAPIQoder,
			Gateway:     gatewayQoder,
			Models:      gatewayQoder,
			SessionType: "qodercli",
			CosyVersion: "0.1.43",
			ClientID:    clientIDQoder,
			RedirectURI: "qoder-work-cn://",
			Domain:      "qoder.com.cn",
		}
	}
}

// SupportsCheckin 报告该区域是否具备可用的签到活动（campaigns 仅 qodercn）。
func (e Endpoints) SupportsCheckin() bool { return e.Region == RegionQoderCN }

// IsCLI 报告该区域是否使用 qodercli 头族（仅 qoder 区）。
func (e Endpoints) IsCLI() bool { return e.Region == RegionQoder }

// Site 返回设备授权展示页所在站点域（openapi 只用于 poll）。
func (e Endpoints) Site() string {
	switch e.Region {
	case RegionQoderCOM:
		return "https://qoder.com"
	default:
		return "https://qoder.com.cn"
	}
}

// NormalizeRegion 把输入（区域名或带前缀的模型名）归一到三个合法区域之一。
//
// 未知输入默认落到 qoder 区，与旧版 qoder 包行为一致。
func NormalizeRegion(region string) string {
	r := strings.ToLower(strings.TrimSpace(region))
	if i := strings.IndexByte(r, '/'); i > 0 {
		r = r[:i]
	}
	switch r {
	case RegionQoderCN:
		return RegionQoderCN
	case RegionQoderCOM:
		return RegionQoderCOM
	default:
		return RegionQoder
	}
}

// OverrideBase 用 base 覆盖所有上游基址（联调 / 自建反代用）。
func (e Endpoints) OverrideBase(base string) Endpoints {
	if base = strings.TrimRight(strings.TrimSpace(base), "/"); base != "" {
		e.OpenAPI = base
		e.Gateway = base
		e.Models = base
	}
	return e
}

// PublicModelID 拼接对外模型 ID：<前缀>/<上游模型名>。
func PublicModelID(prefix, upstream string) string { return prefix + "/" + upstream }

// ModelPrefixOf 从对外模型名里取出命名空间前缀（"qodercn/auto" → "qodercn"）。
func ModelPrefixOf(model string) string {
	if i := strings.IndexByte(model, '/'); i > 0 {
		return model[:i]
	}
	return ""
}

// UpstreamModelName 去掉对外模型名的命名空间前缀。
func UpstreamModelName(model string) string {
	if i := strings.IndexByte(model, '/'); i > 0 {
		return model[i+1:]
	}
	return model
}

// AuthName 是凭据文件名（不含 .json）：<region>-<uid>。
func AuthName(region, uid string) string { return NormalizeRegion(region) + "-" + uid }
