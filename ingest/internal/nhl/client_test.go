package nhl

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestParseGames(t *testing.T) {
	tests := []struct {
		name    string
		input   []byte
		wantLen int
		wantErr bool
	}{
		{
			name:    "single game",
			input:   []byte(`{"games": [{"id": 2025020740, "season": 20252026}]}`),
			wantLen: 1,
			wantErr: false,
		},
		{
			name:    "empty games",
			input:   []byte(`{"games": []}`),
			wantLen: 0,
			wantErr: false,
		},
		{
			name:    "multiple games",
			input:   []byte(`{"games": [{"id": 2025020741, "season": 20252026}, {"id": 2026020742, "season": 20252026}]}`),
			wantLen: 2,
			wantErr: false,
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
			wantErr: false,
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
			// TODO: add field-level assertions (e.g. got[0].ID, got[0].Season) where useful
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
