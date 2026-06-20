package bronze

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// newTestWriter wires up an s3.Client pointed at the given httptest server URL
// with static fake credentials. SeaweedFS-style path-style addressing is enabled.
func newTestWriter(serverURL, bucket string) *Writer {
	client := s3.New(s3.Options{
		Region:                     "us-east-1",
		BaseEndpoint:               aws.String(serverURL),
		UsePathStyle:               true,
		Credentials:                credentials.NewStaticCredentialsProvider("test", "test", ""),
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
	})
	return NewWriterFromClient(client, bucket)
}

func TestWriter_WriteSchedule(t *testing.T) {
	const (
		bucket  = "nhl-bronze"
		date    = "2026-01-15"
		body    = `{"games": []}`
		wantKey = "schedule/date=2026-01-15/schedule.json"
	)

	t.Run("happy path", func(t *testing.T) {
		var gotPath string
		var gotBody string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotPath = r.URL.Path
			buf, _ := io.ReadAll(r.Body)
			gotBody = string(buf)
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()

		writer := newTestWriter(srv.URL, bucket)
		if err := writer.WriteSchedule(context.Background(), date, []byte(body)); err != nil {
			t.Fatalf("WriteSchedule: %v", err)
		}

		wantPath := "/" + bucket + "/" + wantKey
		if gotPath != wantPath {
			t.Errorf("got path %q, want %q", gotPath, wantPath)
		}
		if gotBody != body {
			t.Errorf("got body %q, want %q", gotBody, body)
		}
	})

	t.Run("non-2xx returns error", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "forbidden", http.StatusForbidden)
		}))
		defer srv.Close()

		writer := newTestWriter(srv.URL, bucket)
		err := writer.WriteSchedule(context.Background(), date, []byte(body))
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !strings.Contains(err.Error(), wantKey) {
			t.Errorf("error should mention key %q, got: %v", wantKey, err)
		}
	})
}

func TestWriter_WriteRunFailures(t *testing.T) {
	const (
		bucket  = "nhl-bronze"
		runID   = "20260619T143012Z-a7b3c1d4"
		body    = `[{"date":"2023-11-04","game_id":2023020234,"stage":"pbp_fetch","error":"oops"}]`
		wantKey = "_runs/run=20260619T143012Z-a7b3c1d4/failures.json"
	)

	t.Run("happy path", func(t *testing.T) {
		var gotPath string
		var gotBody string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotPath = r.URL.Path
			buf, _ := io.ReadAll(r.Body)
			gotBody = string(buf)
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()

		writer := newTestWriter(srv.URL, bucket)
		if err := writer.WriteRunFailures(context.Background(), runID, []byte(body)); err != nil {
			t.Fatalf("WriteRunFailures: %v", err)
		}

		wantPath := "/" + bucket + "/" + wantKey
		if gotPath != wantPath {
			t.Errorf("got path %q, want %q", gotPath, wantPath)
		}
		if gotBody != body {
			t.Errorf("got body %q, want %q", gotBody, body)
		}
	})

	t.Run("non-2xx returns error", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "internal", http.StatusInternalServerError)
		}))
		defer srv.Close()

		writer := newTestWriter(srv.URL, bucket)
		err := writer.WriteRunFailures(context.Background(), runID, []byte(body))
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !strings.Contains(err.Error(), wantKey) {
			t.Errorf("error should mention key %q, got: %v", wantKey, err)
		}
	})
}

func TestWriter_WritePlayByPlay(t *testing.T) {
	const (
		bucket  = "nhl-bronze"
		season  = int64(20252026)
		date    = "2026-01-15"
		gameID  = int64(2025020740)
		body    = `{"id": 2025020740, "plays": []}`
		wantKey = "play-by-play/season=20252026/date=2026-01-15/game_2025020740.json"
	)

	t.Run("happy path", func(t *testing.T) {
		var gotPath string
		var gotBody string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotPath = r.URL.Path
			buf, _ := io.ReadAll(r.Body)
			gotBody = string(buf)
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()

		writer := newTestWriter(srv.URL, bucket)
		if err := writer.WritePlayByPlay(context.Background(), season, date, gameID, []byte(body)); err != nil {
			t.Fatalf("WritePlayByPlay: %v", err)
		}

		wantPath := "/" + bucket + "/" + wantKey
		if gotPath != wantPath {
			t.Errorf("got path %q, want %q", gotPath, wantPath)
		}
		if gotBody != body {
			t.Errorf("got body %q, want %q", gotBody, body)
		}
	})

	t.Run("non-2xx returns error", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "internal", http.StatusInternalServerError)
		}))
		defer srv.Close()

		writer := newTestWriter(srv.URL, bucket)
		err := writer.WritePlayByPlay(context.Background(), season, date, gameID, []byte(body))
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !strings.Contains(err.Error(), wantKey) {
			t.Errorf("error should mention key %q, got: %v", wantKey, err)
		}
	})
}
