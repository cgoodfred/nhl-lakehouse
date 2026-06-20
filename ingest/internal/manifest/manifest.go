package manifest

import (
	"encoding/json"
	"time"
)

type Failure struct {
	Date   string `json:"date"`
	GameID int64  `json:"game_id"`
	Stage  string `json:"stage"`
	Error  string `json:"error"`
}

const (
	StageScheduleFetch = "schedule_fetch"
	StageScheduleWrite = "schedule_write"
	StageScheduleParse = "schedule_parse"
	StagePBPFetch      = "pbp_fetch"
	StagePBPWrite      = "pbp_write"
)

func RunID(t time.Time) string {
	return t.UTC().Format("20060102T150405Z")
}

func Marshal(failures []Failure) ([]byte, error) {
	return json.MarshalIndent(failures, "", "  ")
}
