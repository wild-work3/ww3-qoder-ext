package qoder

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"wild-work3/qoder-ext/internal/protocol/errkind"
	"wild-work3/qoder-ext/internal/protocol/meta"
)

// campaign 是 /sash/api/v1/me/campaigns 返回的单条签到活动。
type campaign struct {
	ID          string  `json:"id"`
	Name        string  `json:"name"`
	Title       string  `json:"title"`
	Description string  `json:"description"`
	Status      string  `json:"status"`
	Claimed     bool    `json:"claimed"`
	Claimable   bool    `json:"claimable"`
	CanClaim    bool    `json:"canClaim"`
	Reward      float64 `json:"reward"`
	Credits     float64 `json:"credits"`
	Unit        string  `json:"unit"`
}

type campaignsResp struct {
	Campaigns []campaign `json:"campaigns"`
	Data      []campaign `json:"data"`
	List      []campaign `json:"list"`
}

func (r campaignsResp) list() []campaign {
	switch {
	case len(r.Campaigns) > 0:
		return r.Campaigns
	case len(r.Data) > 0:
		return r.Data
	default:
		return r.List
	}
}

func (c *Client) sashCall(ctx context.Context, a *Auth, path string, body []byte) (int, []byte, error) {
	url := c.base() + path
	var reader io.Reader
	if len(body) > 0 {
		reader = strings.NewReader(string(body))
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, reader)
	if err != nil {
		return 0, nil, err
	}
	ApplyBearer(req, a.AccessToken)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", clientUA)
	resp, err := c.streamHTTP().Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return resp.StatusCode, raw, nil
}

// Checkin 返回当前可领取的签到活动列表（仅 qodercn 有 campaigns 活动）。
func (c *Client) Checkin(ctx context.Context, a *Auth) ([]meta.ResourceItem, error) {
	if !c.Endpoints.SupportsCheckin() {
		return nil, &Error{Kind: errkind.NotFound, Msg: fmt.Sprintf("%s 区暂无签到活动", c.Endpoints.Region)}
	}
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	status, raw, err := c.sashCall(ctx, a, EpCampaigns, []byte("{}"))
	if err != nil {
		return nil, err
	}
	if status >= 400 {
		return nil, &Error{Kind: Classify(status, string(raw)), Msg: fmt.Sprintf("http %d: %s", status, truncate(string(raw), 200))}
	}
	var parsed campaignsResp
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, fmt.Errorf("parse campaigns: %w", err)
	}
	list := parsed.list()
	items := make([]meta.ResourceItem, 0, len(list))
	for _, cp := range list {
		name := cp.Title
		if name == "" {
			name = cp.Name
		}
		if name == "" {
			name = cp.ID
		}
		reward := cp.Reward
		if reward == 0 {
			reward = cp.Credits
		}
		unit := cp.Unit
		if unit == "" {
			unit = "credits"
		}
		usable := cp.Claimable || cp.CanClaim || (!cp.Claimed && cp.Status != "claimed")
		items = append(items, meta.ResourceItem{
			Name:     name,
			Total:    &reward,
			Remain:   &reward,
			Unit:     unit,
			Usable:   usable,
			InfoOnly: !usable,
			Key:      cp.ID,
		})
	}
	return items, nil
}

// ClaimCheckin 领取指定活动，返回是否成功。
func (c *Client) ClaimCheckin(ctx context.Context, a *Auth, id string) (bool, error) {
	if !c.Endpoints.SupportsCheckin() {
		return false, &Error{Kind: errkind.NotFound, Msg: fmt.Sprintf("%s 区暂无签到活动", c.Endpoints.Region)}
	}
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	path := EpCampaigns
	if strings.TrimSpace(id) != "" {
		path = EpCampaigns + "/" + id + "/claim"
	}
	body, _ := json.Marshal(map[string]any{"campaignId": id})
	status, raw, err := c.sashCall(ctx, a, path, body)
	if err != nil {
		return false, err
	}
	if status >= 400 {
		return false, &Error{Kind: Classify(status, string(raw)), Msg: fmt.Sprintf("http %d: %s", status, truncate(string(raw), 200))}
	}
	return true, nil
}
