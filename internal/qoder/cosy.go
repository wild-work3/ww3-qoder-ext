package qoder

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// serverPubKeyPEM 是上游 COSY 加密用的 1024-bit RSA 公钥。
const serverPubKeyPEM = `-----BEGIN PUBLIC KEY-----
MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDA8iMH5c02LilrsERw9t6Pv5Nc4
k6Pz1EaDicBMpdpxKduSZu5OANqUq8er4GM95omAGIOPOh+Nx0spthYA2BqGz+l6
HRkPJ7S236FZz73In/KVuLnwI8JJ2CbuJap8kvheCCZpmAWpb/cPx/3Vr/J6I17X
cW+ML9FoCI6AOvOzwIDAQAB
-----END PUBLIC KEY-----`

// serverPubKey 是解析后的 COSY 公钥（包初始化时解析，失败即 panic——公钥是编译期常量）。
var serverPubKey = mustParsePubKey(serverPubKeyPEM)

// nowUnix 是 COSY 签名时间戳来源，必须与 cosy-date 头同源。
var nowUnix = func() int64 { return time.Now().Unix() }

func mustParsePubKey(pemText string) *rsa.PublicKey {
	block, _ := pem.Decode([]byte(pemText))
	if block == nil {
		panic("qoder: 无法解码 COSY 服务端公钥 PEM")
	}
	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		panic("qoder: 无法解析 COSY 服务端公钥: " + err.Error())
	}
	rk, ok := pub.(*rsa.PublicKey)
	if !ok {
		panic("qoder: COSY 服务端公钥不是 RSA 公钥")
	}
	return rk
}

// CosySession 保存一次请求所需的 COSY 加密材料与机器指纹。
type CosySession struct {
	tempKey      []byte
	cosyKey      string
	info         string
	MachineID    string
	MachineToken string
	MachineType  string
}

// NewCosySession 构造一次请求用的 COSY 会话。
//
// userType 为空时按区域取默认值：qoder 区用 personal_professional_trial，
// 其余区域用 personal_standard（与旧版三套包的行为一致）。
func NewCosySession(ep Endpoints, machineID, machineToken, machineType, nickname, uid, accessToken, refreshToken, userType string) (*CosySession, error) {
	if machineID == "" || machineToken == "" || machineType == "" {
		return nil, errors.New("missing machine fingerprint (need MachineID/Token/Type)")
	}
	if userType == "" {
		if ep.IsCLI() {
			userType = "personal_professional_trial"
		} else {
			userType = "personal_standard"
		}
	}
	tempKey := []byte(hexShort(16))
	enc, err := rsa.EncryptPKCS1v15(rand.Reader, serverPubKey, tempKey)
	if err != nil {
		return nil, fmt.Errorf("cosy 临时密钥加密失败: %w", err)
	}
	identity := map[string]string{
		"name":                 nickname,
		"aid":                  uid,
		"uid":                  uid,
		"yx_uid":               "",
		"organization_id":      "",
		"organization_name":    "",
		"user_type":            userType,
		"security_oauth_token": accessToken,
		"refresh_token":        refreshToken,
	}
	plain, err := json.Marshal(identity)
	if err != nil {
		return nil, err
	}
	sealed, err := aesCBCEncrypt(plain, tempKey)
	if err != nil {
		return nil, err
	}
	return &CosySession{
		tempKey:      tempKey,
		cosyKey:      base64.StdEncoding.EncodeToString(enc),
		info:         base64.StdEncoding.EncodeToString(sealed),
		MachineID:    machineID,
		MachineToken: machineToken,
		MachineType:  machineType,
	}, nil
}

// aesCBCEncrypt 用 AES-128-CBC 加密并按 PKCS#7 填充；key 同时充当 IV（与旧版一致）。
func aesCBCEncrypt(plain, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	pad := aes.BlockSize - len(plain)%aes.BlockSize
	out := make([]byte, len(plain)+pad)
	copy(out, plain)
	for i := len(plain); i < len(out); i++ {
		out[i] = byte(pad)
	}
	cipher.NewCBCEncrypter(block, key).CryptBlocks(out, out)
	return out, nil
}

// authHeader 计算 COSY 授权头与配套的 cosy-date 值。
//
// 签名输入是 payloadB64 + "\n" + cosyKey + "\n" + date + "\n" + body + "\n" + path，
// 其中 path 去掉了 "/algo" 前缀；摘要用 MD5（上游约定）。
func (s *CosySession) authHeader(ep Endpoints, body, rawURL string) (auth, date string, err error) {
	u, perr := url.Parse(rawURL)
	if perr != nil {
		return "", "", perr
	}
	payload, err := json.Marshal(map[string]string{
		"cosyVersion": ep.CosyVersion,
		"ideVersion":  "",
		"info":        s.info,
		"requestId":   uuid4(),
		"version":     "v1",
	})
	if err != nil {
		return "", "", err
	}
	payloadB64 := base64.StdEncoding.EncodeToString(payload)
	date = fmt.Sprintf("%d", nowUnix())
	pathSig := strings.TrimPrefix(u.Path, "/algo")
	sum := md5.Sum([]byte(payloadB64 + "\n" + s.cosyKey + "\n" + date + "\n" + body + "\n" + pathSig))
	auth = "Bearer COSY." + payloadB64 + "." + hex.EncodeToString(sum[:])
	return auth, date, nil
}

// ApplyHeaders 把 COSY 头族写入请求。
//
// 三区头族差异：
//   - qoder：cosy-data-policy "AGREE"、cosy-clienttype "5"、cosy-clientip、
//     accept-encoding identity、accept 恒为 text/event-stream。
//   - qodercn / qodercom：cosy-data-policy "agree"（小写）、无 clientip / accept-encoding、
//     accept 由调用方给定（accept 参数）、cache-control 恒为 no-cache，
//     并额外带 cosy-scene / cosy-business-product / cosy-business-type。
//
// modelKey 非空时附带 x-model-key 与 x-model-source。
func (s *CosySession) ApplyHeaders(req *http.Request, ep Endpoints, body, rawURL, uid, accept string, sse bool, modelKey string) error {
	auth, date, err := s.authHeader(ep, body, rawURL)
	if err != nil {
		return err
	}
	h := req.Header
	h.Set("user-agent", clientUA)
	h.Set("login-version", "v2")
	h.Set("cosy-machinetype", s.MachineType)
	h.Set("cosy-machineid", s.MachineID)
	h.Set("cosy-machinetoken", s.MachineToken)
	h.Set("cosy-version", ep.CosyVersion)
	h.Set("cosy-date", date)
	h.Set("cosy-user", uid)
	h.Set("cosy-key", s.cosyKey)
	h.Set("authorization", auth)
	h.Set("content-type", "application/json")

	if ep.IsCLI() {
		h.Set("accept", "text/event-stream")
		h.Set("cosy-clienttype", "5")
		h.Set("cosy-clientip", "169.254.198.161")
		h.Set("accept-encoding", "identity")
		h.Set("cosy-data-policy", "AGREE")
		if sse {
			h.Set("cache-control", "no-cache")
		}
	} else {
		h.Set("accept", accept)
		h.Set("cache-control", "no-cache")
		h.Set("cosy-data-policy", "agree")
		h.Set("cosy-scene", "assistant")
		h.Set("cosy-business-product", "ide")
		h.Set("cosy-business-type", "agent")
	}

	if modelKey != "" {
		h.Set("x-model-key", modelKey)
		h.Set("x-model-source", "system")
	}
	return nil
}

// ApplyBearer 把普通 Bearer 头写入请求（device token 刷新 / 额度等非 COSY 接口）。
func ApplyBearer(req *http.Request, token string) {
	req.Header.Set("user-agent", clientUA)
	req.Header.Set("accept", "application/json")
	req.Header.Set("content-type", "application/json")
	if token != "" {
		req.Header.Set("authorization", "Bearer "+token)
	}
}

// uuid4 生成 RFC 4122 v4 UUID。
func uuid4() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return ""
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// hexShort 生成 n 位十六进制随机串。
func hexShort(n int) string {
	if n <= 0 {
		return ""
	}
	b := make([]byte, (n+1)/2)
	if _, err := rand.Read(b); err != nil {
		return strings.Repeat("0", n)
	}
	return hex.EncodeToString(b)[:n]
}

// newSimpleID 生成时间前缀 + 随机后缀的短 ID（cn/com 登录的 nonce 用）。
func newSimpleID() string {
	return fmt.Sprintf("%d%s", time.Now().UnixNano()/int64(time.Millisecond), hexShort(8))
}

// EnsureFingerprint 补齐凭据里缺失的机器指纹。
//
// 指纹在登录时生成并随凭据落盘，之后固定不变——上游会把它与账号绑定。
func EnsureFingerprint(a *Auth) bool {
	changed := false
	if a.MachineID == "" {
		a.MachineID = uuid4()
		changed = true
	}
	if a.MachineToken == "" {
		a.MachineToken = base64.RawURLEncoding.EncodeToString([]byte(uuid4() + uuid4()))
		if len(a.MachineToken) > 50 {
			a.MachineToken = a.MachineToken[:50]
		}
		changed = true
	}
	if a.MachineType == "" {
		a.MachineType = strings.ReplaceAll(uuid4(), "-", "")
		if len(a.MachineType) > 18 {
			a.MachineType = a.MachineType[:18]
		}
		changed = true
	}
	return changed
}
