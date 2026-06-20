package manifest

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestRunID(t *testing.T) {
	tests := []struct {
		name   string
		time   time.Time
		wantID string
	}{
		{
			name:   "utc time",
			time:   time.Date(2026, time.June, 19, 14, 30, 12, 0, time.UTC),
			wantID: "20260619T143012Z",
		},
		{
			name:   "non-utc converts to utc",
			time:   time.Date(2026, time.June, 19, 10, 30, 12, 0, time.FixedZone("EST", -5*3600)),
			wantID: "20260619T153012Z",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id := RunID(tt.time)
			if id != tt.wantID {
				t.Errorf("got %q, want %q", id, tt.wantID)
			}
		})
	}
}

func TestUniqueRunID(t *testing.T) {
	at := time.Date(2026, time.June, 19, 14, 30, 12, 0, time.UTC)
	const wantPrefix = "20260619T143012Z-"

	id, err := UniqueRunID(at)
	if err != nil {
		t.Fatalf("UniqueRunID: %v", err)
	}
	if !strings.HasPrefix(id, wantPrefix) {
		t.Errorf("id %q does not start with %q", id, wantPrefix)
	}
	suffix := strings.TrimPrefix(id, wantPrefix)
	if len(suffix) != 8 {
		t.Errorf("suffix %q: got len %d, want 8", suffix, len(suffix))
	}
	if _, err := hex.DecodeString(suffix); err != nil {
		t.Errorf("suffix %q is not hex: %v", suffix, err)
	}

	id2, err := UniqueRunID(at)
	if err != nil {
		t.Fatalf("UniqueRunID (second call): %v", err)
	}
	if id == id2 {
		t.Errorf("two calls produced the same id %q; suffix is not unique", id)
	}
}

func TestMarshal(t *testing.T) {
	failures := []Failure{
		{Date: "2023-11-04", GameID: 2023020234, Stage: StagePBPFetch, Error: "GET /gamecenter/2023020234/play-by-play: status 503"},
		{Date: "2023-11-05", Stage: StageScheduleFetch, Error: "context deadline exceeded"},
	}

	body, err := Marshal(failures)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var got []Failure
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("unmarshal round-trip: %v", err)
	}
	if !reflect.DeepEqual(got, failures) {
		t.Errorf("round-trip mismatch:\n got=%+v\nwant=%+v", got, failures)
	}

	if !bytes.Contains(body, []byte("\n")) {
		t.Errorf("expected pretty-printed output with newlines, got: %s", body)
	}
}
