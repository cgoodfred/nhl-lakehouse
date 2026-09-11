package window

import (
	"testing"
	"time"
)

func TestDates(t *testing.T) {
	newYork, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name          string
		now           time.Time
		lookback      int
		lookahead     int
		wantStartDate string
		wantEndDate   string
	}{
		{
			name:          "normal day",
			now:           time.Date(2026, time.January, 15, 12, 0, 0, 0, time.UTC),
			lookback:      7,
			lookahead:     1,
			wantStartDate: "2026-01-08",
			wantEndDate:   "2026-01-16",
		},
		{
			name:          "new york midnight",
			now:           time.Date(2026, time.January, 15, 4, 59, 0, 0, time.UTC),
			lookback:      0,
			lookahead:     0,
			wantStartDate: "2026-01-14",
			wantEndDate:   "2026-01-14",
		},
		{
			name:          "new york after midnight",
			now:           time.Date(2026, time.January, 15, 5, 1, 0, 0, time.UTC),
			lookback:      0,
			lookahead:     0,
			wantStartDate: "2026-01-15",
			wantEndDate:   "2026-01-15",
		},
		{
			name:          "daylight saving transition",
			now:           time.Date(2026, time.March, 8, 16, 0, 0, 0, time.UTC),
			lookback:      1,
			lookahead:     1,
			wantStartDate: "2026-03-07",
			wantEndDate:   "2026-03-09",
		},
		{
			name:          "year boundary",
			now:           time.Date(2026, time.January, 1, 12, 0, 0, 0, newYork),
			lookback:      1,
			lookahead:     1,
			wantStartDate: "2025-12-31",
			wantEndDate:   "2026-01-02",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start, end, err := Dates(tt.now, newYork, tt.lookback, tt.lookahead)
			if err != nil {
				t.Fatal(err)
			}
			if got := FormatDate(start); got != tt.wantStartDate {
				t.Errorf("start = %s, want %s", got, tt.wantStartDate)
			}
			if got := FormatDate(end); got != tt.wantEndDate {
				t.Errorf("end = %s, want %s", got, tt.wantEndDate)
			}
		})
	}
}

func TestDatesRejectNegativeWindows(t *testing.T) {
	location, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := Dates(time.Now(), location, -1, 0); err == nil {
		t.Fatal("expected negative lookback to fail")
	}
	if _, _, err := Dates(time.Now(), location, 0, -1); err == nil {
		t.Fatal("expected negative lookahead to fail")
	}
}
