package manifest

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"time"
)

type Failure struct {
	Date   string `json:"date"`
	GameID int64  `json:"game_id"`
	Stage  string `json:"stage"`
	Error  string `json:"error"`
}

// Impact describes the bounded set of bronze data written by one ingest run.
// It is control-plane metadata for downstream incremental processing; bronze
// objects remain the source of truth.
type Impact struct {
	RunID       string       `json:"run_id"`
	WindowStart string       `json:"window_start"`
	WindowEnd   string       `json:"window_end"`
	Objects     []string     `json:"objects"`
	Games       []GameImpact `json:"games"`
}

type GameImpact struct {
	Date        string `json:"date"`
	GameID      int64  `json:"game_id"`
	Season      int64  `json:"season"`
	State       string `json:"state"`
	PBPStatus   string `json:"pbp_status"`
	PBPError    string `json:"pbp_error,omitempty"`
	ShiftStatus string `json:"shift_status"`
	ShiftError  string `json:"shift_error,omitempty"`
}

const (
	StageScheduleFetch    = "schedule_fetch"
	StageScheduleWrite    = "schedule_write"
	StageScheduleParse    = "schedule_parse"
	StagePBPFetch         = "pbp_fetch"
	StagePBPWrite         = "pbp_write"
	StageShiftFetch       = "shift_fetch"
	StageShiftWrite       = "shift_write"
	StageShiftUnavailable = "shift_unavailable"
)

func RunID(t time.Time) string {
	return t.UTC().Format("20060102T150405Z")
}

func UniqueRunID(t time.Time) (string, error) {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return RunID(t) + "-" + hex.EncodeToString(b[:]), nil
}

func Marshal(failures []Failure) ([]byte, error) {
	return json.MarshalIndent(failures, "", "  ")
}

func MarshalImpact(impact Impact) ([]byte, error) {
	return json.MarshalIndent(impact, "", "  ")
}
