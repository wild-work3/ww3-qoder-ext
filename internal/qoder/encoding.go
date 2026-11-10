package qoder

import "encoding/base64"

// customAlphabet 是 Qoder 自定义 base64 字母表（上游 Encode=1 约定的乱序表）。
const customAlphabet = "_doRTgHZBKcGVjlvpC,@aFSx#DPuNJme&i*MzLOEn)sUrthbf%Y^w.(kIQyXqWA!"

// customPad 是自定义字母表里的填充符（标准 base64 的 '=' 被换成 '$'）。
const customPad = '$'

const stdAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"

// stdToCustom 把标准 base64 字符映射到自定义字母表。
var stdToCustom = func() [128]byte {
	var m [128]byte
	for i := 0; i < len(stdAlphabet) && i < len(customAlphabet); i++ {
		m[stdAlphabet[i]] = customAlphabet[i]
	}
	return m
}()

// qoderEncode 把明文编码成上游 Encode=1 期望的字符串形态。
//
// 步骤：标准 base64 → 按 a=len/3 把结果切成三段重排（尾段 + 中段 + 首段）→
// 标准字母表逐字符映射到自定义字母表，'=' 替换为 '$'。
// 三段重排是上游的弱混淆，解码端按同样规则还原。
func qoderEncode(plain []byte) string {
	std := base64.StdEncoding.EncodeToString(plain)
	n := len(std)
	if n == 0 {
		return ""
	}
	a := n / 3
	rearranged := std[n-a:] + std[a:n-a] + std[:a]
	out := make([]byte, len(rearranged))
	for i := 0; i < len(rearranged); i++ {
		c := rearranged[i]
		if c == '=' {
			out[i] = customPad
			continue
		}
		if int(c) < len(stdToCustom) && stdToCustom[c] != 0 {
			out[i] = stdToCustom[c]
			continue
		}
		out[i] = c
	}
	return string(out)
}
