// qoder-ext 是 Qoder 渠道插件（单插件多区：qoder / qodercn / qodercom）。
//
// 子命令：
//
//	call      协议一次性调用（core/宿主使用；也是所有子命令的真实 IPC 通道）
//	stdio     协议常连模式
//	manifest  打印插件元数据
//	ping      逐个探活协议方法
//	login     执行设备码登录（device_poll）
//	status    查询登录流程状态
//	accounts  列出账号
//	models    列出模型
//	chat      发起一次对话（流式打印到 stdout）
//
// 数据目录 <data>（--data / $WW3_DATA / 可执行文件所在目录）：
//
//	<data>/auths/qoder-<uid>.json     凭据
//	<data>/auths/flows/<flow_id>.json 登录流程
//	<data>/logs/qoder.log             日志
//	<data>/stat/                      统计
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"wild-work3/qoder-ext/internal/channel"
	"wild-work3/qoder-ext/internal/protocol/authflow"
	"wild-work3/qoder-ext/internal/protocol/chat"
	"wild-work3/qoder-ext/internal/protocol/cli"
	"wild-work3/qoder-ext/internal/protocol/plugin"
	"wild-work3/qoder-ext/internal/qoder"
)

// dataDirGlobal 是本进程解析出的数据目录。
var dataDirGlobal string

func main() {
	app := cli.NewApp("qoder-ext", "Qoder 渠道插件（三区 qoder/qodercn/qodercom，COSY 签名 + 嵌套 SSE）", "call")

	cCall := app.Cmd("call", "协议一次性调用")
	pCall := cCall.Str("data", "", "数据目录（--data / $WW3_DATA / 可执行文件目录）")

	cStdio := app.Cmd("stdio", "协议常连模式")
	pStdio := cStdio.Str("data", "", "数据目录")

	cManifest := app.Cmd("manifest", "打印插件元数据")
	pManOut := cManifest.Str("out", "", "写入文件（默认 stdout）")

	cPing := app.Cmd("ping", "逐个探活协议方法")
	nPing := addNetFlags(cPing)

	cLogin := app.Cmd("login", "执行设备码登录")
	nLogin := addNetFlags(cLogin)
	loginWait := cLogin.Int("wait", 0, "等待登录完成的秒数（0 = 一直等到超时）")

	cStatus := app.Cmd("status", "查询登录流程状态")
	nStatus := addNetFlags(cStatus)
	statusFlow := cStatus.Str("flow", "", "登录流程 ID")

	cAccounts := app.Cmd("accounts", "列出账号")
	nAccounts := addNetFlags(cAccounts)

	cModels := app.Cmd("models", "列出模型")
	nModels := addNetFlags(cModels)

	cChat := app.Cmd("chat", "发起一次对话（流式）")
	nChat := addNetFlags(cChat)
	chatModel := cChat.Str("model", os.Getenv("WW3_MODEL"), "模型 ID（如 qoder/qwen3.7-plus）")
	chatPrompt := cChat.Str("prompt", "", "用户提示词（--raw 提供完整请求体时忽略）")
	chatRaw := cChat.Str("raw", "", "完整 chat.generate 请求体（JSON）或 @file")

	cmd, err := app.Parse(os.Args[1:])
	if err != nil {
		if err == cli.ErrHelp {
			return
		}
		fmt.Fprintf(os.Stderr, "参数错误: %v\n", err)
		os.Exit(2)
	}

	dataFlag := ""
	switch cmd.CmdName() {
	case plugin.ModeCall:
		dataFlag = *pCall
	case plugin.ModeStdio:
		dataFlag = *pStdio
	case "ping":
		dataFlag = *nPing.data
	case "login":
		dataFlag = *nLogin.data
	case "status":
		dataFlag = *nStatus.data
	case "accounts":
		dataFlag = *nAccounts.data
	case "models":
		dataFlag = *nModels.data
	case "chat":
		dataFlag = *nChat.data
	}
	dataDirGlobal = cli.ResolveData(dataFlag)
	channel.Log, _ = cli.NewLogger("qoder", cli.LogPath(dataDirGlobal, "qoder"))
	if channel.Log != nil {
		defer channel.Log.Close()
	}

	switch cmd.CmdName() {
	case plugin.ModeCall, plugin.ModeStdio:
		if err := channel.Serve(context.Background(), cmd.CmdName(), dataDirGlobal, os.Stdin, os.Stdout); err != nil {
			fail("%s 退出: %v", cmd.CmdName(), err)
		}
	case "manifest":
		b, err := json.MarshalIndent(channel.Manifest(), "", "  ")
		if err != nil {
			fail("序列化 manifest 失败: %v", err)
		}
		if *pManOut != "" {
			if err := os.WriteFile(*pManOut, append(b, '\n'), 0o644); err != nil {
				fail("写入 %s 失败: %v", *pManOut, err)
			}
			return
		}
		fmt.Println(string(b))
	case "login":
		runLogin(nLogin.cfg(), *loginWait)
	case "status":
		runStatus(nStatus.cfg(), *statusFlow)
	case "accounts":
		fmt.Println(cli.RedactJSON(callOnce("accounts.list", map[string]any{"config": nAccounts.cfg()})))
	case "models":
		fmt.Println(cli.RedactJSON(callOnce("models.list", map[string]any{"config": nModels.cfg()})))
	case "chat":
		runChat(nChat.cfg(), *nChat.auth, *chatModel, *chatPrompt, *chatRaw)
	case "ping":
		runPing(nPing.cfg())
	default:
		fail("未知命令: %s", cmd.CmdName())
	}
}

// ---- 命令行开关 ----

type netFlags struct {
	data    *string
	config  *string
	region  *string
	auth    *string
	proxy   *string
	baseURL *string
	timeout *int
}

func addNetFlags(c *cli.Command) *netFlags {
	return &netFlags{
		data:    c.Str("data", "", "数据目录"),
		config:  c.Str("config", "", "渠道配置 JSON 或 @file"),
		region:  c.Str("region", os.Getenv("WW3_REGION"), "区域（qoder / qodercn / qodercom）"),
		auth:    c.Str("auth", os.Getenv("WW3_AUTH"), "账号标识（auths/ 文件名）"),
		proxy:   c.Str("proxy", os.Getenv("WW3_PROXY"), "HTTP 代理"),
		baseURL: c.Str("base-url", os.Getenv("WW3_BASE_URL"), "上游基址覆盖"),
		timeout: c.Int("timeout", 300, "超时（秒）"),
	}
}

func (n *netFlags) cfg() map[string]any {
	cfg := map[string]any{}
	if n.config != nil && *n.config != "" {
		if err := json.Unmarshal([]byte(cli.ConfigArg(*n.config)), &cfg); err != nil {
			fail("解析 --config 失败: %v", err)
		}
	}
	if n.region != nil && *n.region != "" {
		cfg["region"] = *n.region
	}
	if n.baseURL != nil && *n.baseURL != "" {
		cfg["base_url"] = *n.baseURL
	}
	if n.proxy != nil && *n.proxy != "" {
		cfg["proxy"] = *n.proxy
	}
	if n.timeout != nil && *n.timeout > 0 {
		cfg["timeout"] = *n.timeout
	}
	return cfg
}

// ---- 真实 IPC（起自身 call 子进程，无 CLI 专用捷径）----

func callOnce(method string, params any) string {
	raw, err := spawnInvoke(method, params)
	if err != nil {
		fail("%s 失败: %v", method, err)
	}
	return raw
}

func spawnInvoke(method string, params any) (string, error) {
	exe, err := os.Executable()
	if err != nil {
		return "", err
	}
	args := []string{plugin.ModeCall}
	if dataDirGlobal != "" {
		args = append(args, "--data", dataDirGlobal)
	}
	p, err := cli.Spawn("qoder-call", exe, args, channel.Log)
	if err != nil {
		return "", err
	}
	defer p.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	res, err := p.InvokeSimple(ctx, method, params)
	if err != nil {
		return "", err
	}
	return string(res.Raw), nil
}

// ---- 各命令实现 ----

// runLogin 执行设备码登录：auth.begin 拉起流程 → 打印授权地址 → 轮询 auth.status。
func runLogin(cfg map[string]any, waitSec int) {
	raw := callOnce("auth.begin", map[string]any{"method": "device", "region": cfg["region"], "config": cfg})
	var begin authflow.BeginResult
	if err := json.Unmarshal([]byte(raw), &begin); err != nil {
		fail("解析 auth.begin 结果失败: %v", err)
	}
	fmt.Fprintf(os.Stderr, "请在浏览器打开以下地址完成授权：\n\n  %s\n\n", begin.AuthURL)
	fmt.Fprintf(os.Stderr, "登录流程 %s 已创建，正在轮询…\n", begin.FlowID)

	deadline := time.Now().Add(10 * time.Minute)
	if waitSec > 0 {
		deadline = time.Now().Add(time.Duration(waitSec) * time.Second)
	}
	for {
		if time.Now().After(deadline) {
			fail("登录超时（流程 %s）", begin.FlowID)
		}
		time.Sleep(2 * time.Second)
		var st authflow.StatusResult
		if err := json.Unmarshal([]byte(callOnce("auth.status", map[string]any{"flow_id": begin.FlowID})), &st); err != nil {
			fail("解析 auth.status 结果失败: %v", err)
		}
		switch st.State {
		case authflow.StateSuccess:
			if st.Account != nil {
				fmt.Printf("登录成功：uid=%s nickname=%s region=%s\n", st.Account.UID, st.Account.Nickname, st.Account.Region)
			} else {
				fmt.Println("登录成功")
			}
			return
		case authflow.StateFailed, authflow.StateExpired, authflow.StateCancelled:
			msg := string(st.State)
			if st.Error != nil && st.Error.Message != "" {
				msg = st.Error.Message
			}
			fail("登录未完成: %s", msg)
		}
		if st.Hint != "" {
			fmt.Fprintf(os.Stderr, "  %s\n", st.Hint)
		}
	}
}

func runStatus(cfg map[string]any, flowID string) {
	if flowID == "" {
		fail("缺少 --flow")
	}
	fmt.Fprintln(os.Stderr, cli.RedactJSON(callOnce("auth.status", map[string]any{
		"flow_id": flowID,
		"config":  cfg,
	})))
}

func runPing(cfg map[string]any) {
	probes := []string{
		"config.schema",
		"auth.methods",
		"accounts.list",
		"credits.get",
		"accounts.resource_detail",
		"accounts.refresh",
		"stats.get",
	}
	for _, m := range probes {
		raw, err := spawnInvoke(m, map[string]any{"config": cfg})
		if err != nil {
			fmt.Printf("✗ %-26s %v\n", m, err)
			continue
		}
		fmt.Printf("✓ %-26s %s\n", m, truncateStr(cli.RedactJSON(raw), 160))
	}
}

func runChat(cfg map[string]any, authName, model, prompt, rawBody string) {
	if model == "" {
		fail("缺少 --model（如 qoder/qwen3.7-plus）；也可用 $WW3_MODEL")
	}
	if pfx := qoder.ModelPrefixOf(model); pfx != "" && pfx != qoder.NormalizeRegion(fmt.Sprint(cfg["region"])) {
		fail("模型 %s 属于 %s 区，当前 --region=%v 不一致；请改用对应区的模型", model, pfx, cfg["region"])
	}
	req := chat.GenerateRequest{
		Model:          model,
		Stream:         true,
		AccountUID:     authName,
		ConversationID: fmt.Sprintf("cli-%d", os.Getpid()),
	}
	if rawBody != "" {
		if err := json.Unmarshal([]byte(cli.ConfigArg(rawBody)), &req); err != nil {
			fail("解析 --raw 失败: %v", err)
		}
		req.Stream = true
		req.AccountUID = authName
	} else {
		if prompt == "" {
			fail("缺少 --prompt 或 --raw")
		}
		req.Messages = []chat.ChatMessage{{Role: "user", Content: prompt}}
	}
	if req.ChannelConfig == nil {
		if b, err := json.Marshal(cfg); err == nil {
			req.ChannelConfig = b
		}
	}

	exe, err := os.Executable()
	if err != nil {
		fail("定位可执行文件失败: %v", err)
	}
	args := []string{plugin.ModeCall}
	if dataDirGlobal != "" {
		args = append(args, "--data", dataDirGlobal)
	}
	p, err := cli.Spawn("qoder-call", exe, args, channel.Log)
	if err != nil {
		fail("启动 IPC 子进程失败: %v", err)
	}
	defer p.Close()
	p.Out = os.Stdout

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	res, err := p.Invoke(ctx, cli.Step{Method: "chat.generate", Params: req, Stream: true})
	if err != nil {
		fail("chat.generate 失败: %v", err)
	}
	if res == nil || !res.OK {
		os.Exit(2)
	}
}

func truncateStr(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}

func fail(format string, a ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", a...)
	os.Exit(2)
}
