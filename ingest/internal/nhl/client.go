package nhl

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

const defaultBaseURL = "https://api-web.nhle.com/v1"

type Client struct {
	baseURL    string
	httpClient *http.Client
}

func NewClient() *Client {
	return NewClientWithBaseURL(defaultBaseURL)
}

func NewClientWithBaseURL(baseURL string) *Client {
	return &Client{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// Schedule fetches the games scheduled for a given date.
// The NHL API endpoint is /score/{date} (not /schedule/{date}, which returns a week).
func (c *Client) Schedule(ctx context.Context, date string) ([]byte, error) {
	return c.get(ctx, "/score/"+date)
}

func (c *Client) PlayByPlay(ctx context.Context, gameID int64) ([]byte, error) {
	return c.get(ctx, fmt.Sprintf("/gamecenter/%d/play-by-play", gameID))
}

func (c *Client) get(ctx context.Context, path string) ([]byte, error) {
	url := c.baseURL + path
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("new request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: status %d", path, resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}

	return body, nil
}

type Game struct {
	ID     int64 `json:"id"`
	Season int64 `json:"season"`
}

func ParseGames(scheduleBody []byte) ([]Game, error) {
	var sr struct {
		Games *[]Game `json:"games"`
	}
	if err := json.Unmarshal(scheduleBody, &sr); err != nil {
		return nil, fmt.Errorf("unmarshal schedule: %w", err)
	}
	if sr.Games == nil {
		return nil, fmt.Errorf("schedule response missing 'games' field")
	}
	return *sr.Games, nil
}
