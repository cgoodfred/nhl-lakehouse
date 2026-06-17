package bronze

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
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
		wantKey = "schedule/date=2026-01-15.json"
	)

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

	// TODO: add a subtest where the mock server returns an error status (e.g. 403)
	// and verify WriteSchedule returns an error whose message includes the key.
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

	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
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

	// TODO: add a subtest where the body asserts the uploaded content equals the input.
	// TODO: add a subtest covering a non-2xx response.
}
