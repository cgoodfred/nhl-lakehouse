package nhl

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"golang.org/x/time/rate"
)

const (
	defaultBaseURL    = "https://api-web.nhle.com/v1"
	defaultRate       = rate.Limit(2) // requests per second sustained
	defaultBurst      = 5
	defaultMaxRetries = 6
	maxBackoffDelay   = 60 * time.Second
)

type Client struct {
	baseURL    string
	httpClient *http.Client
	limiter    *rate.Limiter
	maxRetries int
	backoff    func(attempt int, retryAfter string) time.Duration
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
		limiter:    rate.NewLimiter(defaultRate, defaultBurst),
		maxRetries: defaultMaxRetries,
		backoff:    backoffDelay,
	}
}

// newClientForTest builds a Client with custom rate/retry/backoff settings so
// tests don't burn wall-clock time on the production defaults.
func newClientForTest(baseURL string, r rate.Limit, burst, maxRetries int, backoff func(int, string) time.Duration) *Client {
	return &Client{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		limiter:    rate.NewLimiter(r, burst),
		maxRetries: maxRetries,
		backoff:    backoff,
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

	for attempt := 0; ; attempt++ {
		if err := c.limiter.Wait(ctx); err != nil {
			return nil, fmt.Errorf("rate limit wait: %w", err)
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, fmt.Errorf("new request: %w", err)
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("do request: %w", err)
		}

		body, readErr := io.ReadAll(resp.Body)
		retryAfter := resp.Header.Get("Retry-After")
		status := resp.StatusCode
		resp.Body.Close()

		if status == http.StatusOK {
			if readErr != nil {
				return nil, fmt.Errorf("read body: %w", readErr)
			}
			return body, nil
		}

		if status == http.StatusTooManyRequests && attempt < c.maxRetries {
			delay := c.backoff(attempt, retryAfter)
			select {
			case <-time.After(delay):
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		return nil, fmt.Errorf("GET %s: status %d", path, status)
	}
}

// backoffDelay returns the wait duration before the next retry. Honors a
// Retry-After header value (interpreted as seconds) when present, otherwise
// uses exponential backoff capped at maxBackoffDelay: 1s, 2s, 4s, 8s, 16s,
// 32s, 60s, 60s...
func backoffDelay(attempt int, retryAfter string) time.Duration {
	if retryAfter != "" {
		if secs, err := strconv.Atoi(retryAfter); err == nil && secs > 0 {
			return time.Duration(secs) * time.Second
		}
	}
	delay := time.Duration(1<<attempt) * time.Second
	if delay > maxBackoffDelay {
		return maxBackoffDelay
	}
	return delay
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
