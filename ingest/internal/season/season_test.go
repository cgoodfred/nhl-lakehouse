package season

import (
	"testing"
	"time"
)

func TestDates(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantStart time.Time
		wantEnd   time.Time
		wantErr   bool
	}{
		{
			name:      "regular season",
			input:     "20232024",
			wantStart: time.Date(2023, time.September, 1, 0, 0, 0, 0, time.UTC),
			wantEnd:   time.Date(2024, time.July, 1, 0, 0, 0, 0, time.UTC),
		},
		{name: "too short", input: "2023", wantErr: true},
		{name: "too long", input: "202320242025", wantErr: true},
		{name: "invalid characters", input: "2027abcd", wantErr: true},
		{name: "non consecutive", input: "20212024", wantErr: true},
		{name: "same year", input: "20222022", wantErr: true},
		{name: "reversed years", input: "20232022", wantErr: true},
		{name: "empty string", input: "", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotStart, gotEnd, err := Dates(tt.input)
			if (err != nil) != tt.wantErr {
				t.Fatalf("got err=%v wantErr=%v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if !gotStart.Equal(tt.wantStart) {
				t.Errorf("start: got %v, want %v", gotStart, tt.wantStart)
			}
			if !gotEnd.Equal(tt.wantEnd) {
				t.Errorf("end: got %v, want %v", gotEnd, tt.wantEnd)
			}
		})
	}
}
