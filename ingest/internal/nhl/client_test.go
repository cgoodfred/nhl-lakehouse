package nhl

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

// instantBackoff returns 0 for every attempt so tests don't sleep.
func instantBackoff(int, string) time.Duration { return 0 }

func TestParseGames(t *testing.T) {
	tests := []struct {
		name      string
		input     []byte
		wantLen   int
		wantErr   bool
		wantFirst Game
	}{
		{
			name:      "single game",
			input:     []byte(`{"games": [{"id": 2025020740, "season": 20252026}]}`),
			wantLen:   1,
			wantErr:   false,
			wantFirst: Game{ID: 2025020740, Season: 20252026},
		},
		{
			name:    "empty games",
			input:   []byte(`{"games": []}`),
			wantLen: 0,
			wantErr: false,
		},
		{
			name:      "multiple games",
			input:     []byte(`{"games": [{"id": 2025020741, "season": 20252026}, {"id": 2026020742, "season": 20252026}]}`),
			wantLen:   2,
			wantErr:   false,
			wantFirst: Game{ID: 2025020741, Season: 20252026},
		},
		{
			name:    "bad json",
			input:   []byte(`{"games": [sand, blast]}`),
			wantLen: 0,
			wantErr: true,
		},
		{
			name:    "missing games field",
			input:   []byte(`{}`),
			wantLen: 0,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseGames(tt.input)
			if (err != nil) != tt.wantErr {
				t.Fatalf("got err=%v wantErr=%v", err, tt.wantErr)
			}
			if len(got) != tt.wantLen {
				t.Errorf("got %d games, want %d", len(got), tt.wantLen)
			}
			if tt.wantLen > 0 && got[0] != tt.wantFirst {
				t.Errorf("first game: got %+v, want %+v", got[0], tt.wantFirst)
			}
		})
	}
}

func TestClient_Schedule(t *testing.T) {
	const wantPath = "/score/2026-01-15"
	const wantBody = `{"games": [{"id": 2025020740, "season": 20252026}]}`

	t.Run("happy path", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet {
				t.Errorf("got method %s, want GET", r.Method)
			}
			if r.URL.Path != wantPath {
				t.Errorf("got path %s, want %s", r.URL.Path, wantPath)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(wantBody))
		}))
		defer srv.Close()

		client := NewClientWithBaseURL(srv.URL)
		body, err := client.Schedule(context.Background(), "2026-01-15")
		if err != nil {
			t.Fatalf("Schedule: %v", err)
		}
		if string(body) != wantBody {
			t.Errorf("got body %q, want %q", body, wantBody)
		}
	})

	t.Run("non-200 returns error", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "not found", http.StatusNotFound)
		}))
		defer srv.Close()

		client := NewClientWithBaseURL(srv.URL)
		_, err := client.Schedule(context.Background(), "2026-01-15")
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !strings.Contains(err.Error(), "404") {
			t.Errorf("error should mention status 404, got: %v", err)
		}
	})
}

func TestClient_PlayByPlay(t *testing.T) {
	const wantPath = "/gamecenter/2025020740/play-by-play"
	const wantBody = `{"id": 2025020740, "season": 20252026}`

	t.Run("happy path", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet {
				t.Errorf("got method %s, want GET", r.Method)
			}
			if r.URL.Path != wantPath {
				t.Errorf("got path %s, want %s", r.URL.Path, wantPath)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(wantBody))
		}))
		defer srv.Close()

		client := NewClientWithBaseURL(srv.URL)
		body, err := client.PlayByPlay(context.Background(), 2025020740)
		if err != nil {
			t.Fatalf("PlayByPlay: %v", err)
		}
		if string(body) != wantBody {
			t.Errorf("got body %q, want %q", body, wantBody)
		}
	})

	t.Run("non-200 returns error", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "not found", http.StatusNotFound)
		}))
		defer srv.Close()

		client := NewClientWithBaseURL(srv.URL)
		_, err := client.PlayByPlay(context.Background(), 2025020740)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !strings.Contains(err.Error(), "404") {
			t.Errorf("error should mention status 404, got: %v", err)
		}
	})
}

func TestClient_RetriesOn429(t *testing.T) {
	var calls atomic.Int32
	const wantBody = `{"games": []}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			http.Error(w, "slow down", http.StatusTooManyRequests)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(wantBody))
	}))
	defer srv.Close()

	client := newClientForTest(srv.URL, rate.Inf, 100, 3, instantBackoff)
	body, err := client.Schedule(context.Background(), "2026-01-15")
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	if string(body) != wantBody {
		t.Errorf("got body %q, want %q", body, wantBody)
	}
	if calls.Load() != 2 {
		t.Errorf("expected 2 calls (1 fail + 1 success), got %d", calls.Load())
	}
}

func TestClient_429ExhaustedReturnsError(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "slow down", http.StatusTooManyRequests)
	}))
	defer srv.Close()

	const maxRetries = 2
	client := newClientForTest(srv.URL, rate.Inf, 100, maxRetries, instantBackoff)
	_, err := client.Schedule(context.Background(), "2026-01-15")
	if err == nil {
		t.Fatal("expected error after exhausting retries, got nil")
	}
	if !strings.Contains(err.Error(), "429") {
		t.Errorf("error should mention 429, got: %v", err)
	}
	wantCalls := int32(maxRetries + 1) // initial attempt + N retries
	if calls.Load() != wantCalls {
		t.Errorf("expected %d calls, got %d", wantCalls, calls.Load())
	}
}

func TestBackoffDelay(t *testing.T) {
	tests := []struct {
		name       string
		attempt    int
		retryAfter string
		want       time.Duration
	}{
		{name: "no header attempt 0", attempt: 0, want: 1 * time.Second},
		{name: "no header attempt 1", attempt: 1, want: 2 * time.Second},
		{name: "no header attempt 2", attempt: 2, want: 4 * time.Second},
		{name: "no header attempt 3", attempt: 3, want: 8 * time.Second},
		{name: "no header attempt 5 below cap", attempt: 5, want: 32 * time.Second},
		{name: "no header attempt 6 hits cap", attempt: 6, want: 60 * time.Second},
		{name: "no header attempt 10 stays capped", attempt: 10, want: 60 * time.Second},
		{name: "valid header overrides backoff", attempt: 2, retryAfter: "5", want: 5 * time.Second},
		{name: "header can exceed cap", attempt: 0, retryAfter: "120", want: 120 * time.Second},
		{name: "invalid header falls back", attempt: 1, retryAfter: "abc", want: 2 * time.Second},
		{name: "empty header falls back", attempt: 0, retryAfter: "", want: 1 * time.Second},
		{name: "negative header falls back", attempt: 0, retryAfter: "-1", want: 1 * time.Second},
		{name: "zero header falls back", attempt: 0, retryAfter: "0", want: 1 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := backoffDelay(tt.attempt, tt.retryAfter)
			if got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}
