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
