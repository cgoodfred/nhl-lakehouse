package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/cgoodfred/nhl-lakehouse/ingest/internal/bronze"
	"github.com/cgoodfred/nhl-lakehouse/ingest/internal/manifest"
	"github.com/cgoodfred/nhl-lakehouse/ingest/internal/nhl"
	"github.com/cgoodfred/nhl-lakehouse/ingest/internal/season"
	"github.com/cgoodfred/nhl-lakehouse/ingest/internal/window"
)

const (
	dateLayout = "2006-01-02"
	opTimeout  = 30 * time.Second
)

func main() {
	startFlag := flag.String("start", "", "start date (YYYY-MM-DD, inclusive)")
	endFlag := flag.String("end", "", "end date (YYYY-MM-DD, inclusive)")
	rollingFlag := flag.Bool("rolling", false, "use a rolling local-calendar date window")
	lookbackFlag := flag.Int("lookback-days", 7, "rolling window days before today (inclusive)")
	lookaheadFlag := flag.Int("lookahead-days", 1, "rolling window days after today (inclusive)")
	timezoneFlag := flag.String("timezone", "America/New_York", "IANA timezone for rolling window")
	endpointFlag := flag.String("s3-endpoint", "", "S3-compatible endpoint URL (credentials read from AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY env vars via the AWS SDK default chain)")
	bucketFlag := flag.String("s3-bucket", "", "S3 bucket to write bronze data to")
	seasonFlag := flag.String("season", "", "Season in the format YYYYYYYY such as 20242025")
	flag.Parse()

	var start, end time.Time
	switch {
	case *rollingFlag && (*seasonFlag != "" || *startFlag != "" || *endFlag != ""):
		log.Fatalf("--rolling, --season, and --start/--end are mutually exclusive")
	case *seasonFlag != "" && (*startFlag != "" || *endFlag != ""):
		log.Fatalf("--season and --start/--end can't be used together")
	case *rollingFlag:
		location, err := time.LoadLocation(*timezoneFlag)
		if err != nil {
			log.Fatalf("load timezone: %v", err)
		}
		start, end, err = window.Dates(time.Now(), location, *lookbackFlag, *lookaheadFlag)
		if err != nil {
			log.Fatalf("compute rolling window: %v", err)
		}
		log.Printf("rolling window start=%s end=%s timezone=%s", window.FormatDate(start), window.FormatDate(end), *timezoneFlag)
	case *seasonFlag != "":
		var err error
		start, end, err = season.Dates(*seasonFlag)
		if err != nil {
			log.Fatalf("parse --season: %v", err)
		}
	case *startFlag != "" && *endFlag != "":
		var err error
		start, err = time.Parse(dateLayout, *startFlag)
		if err != nil {
			log.Fatalf("parse --start: %v", err)
		}
		end, err = time.Parse(dateLayout, *endFlag)
		if err != nil {
			log.Fatalf("parse --end: %v", err)
		}
	default:
		log.Fatalf("must specify --season or both --start and --end")
	}

	if end.Before(start) {
		log.Fatalf("end (%s) is before start (%s)", end.Format(dateLayout), start.Format(dateLayout))
	}
	if *endpointFlag == "" || *bucketFlag == "" {
		log.Fatalf("--s3-endpoint and --s3-bucket are required")
	}

	ctx := context.Background()
	runID, err := manifest.UniqueRunID(time.Now())
	if err != nil {
		log.Fatalf("generate run id: %v", err)
	}

	writer, err := bronze.NewWriter(ctx, bronze.Config{
		Endpoint: *endpointFlag,
		Bucket:   *bucketFlag,
	})
	if err != nil {
		log.Fatalf("init bronze writer: %v", err)
	}

	client := nhl.NewClient()

	var schedulesOK, scheduleFailures int
	var gamesOK, gameFailures int
	var totalBytes int
	var failures []manifest.Failure

	for d := start; !d.After(end); d = d.AddDate(0, 0, 1) {
		date := d.Format(dateLayout)

		scheduleCtx, cancel := context.WithTimeout(ctx, opTimeout)
		scheduleBody, err := client.Schedule(scheduleCtx, date)
		cancel()
		if err != nil {
			log.Printf("date=%s schedule fetch error=%v", date, err)
			failures = append(failures, manifest.Failure{
				Date: date, Stage: manifest.StageScheduleFetch, Error: err.Error(),
			})
			scheduleFailures++
			continue
		}

		writeCtx, cancel := context.WithTimeout(ctx, opTimeout)
		err = writer.WriteSchedule(writeCtx, date, scheduleBody)
		cancel()
		if err != nil {
			log.Printf("date=%s schedule write error=%v", date, err)
			failures = append(failures, manifest.Failure{
				Date: date, Stage: manifest.StageScheduleWrite, Error: err.Error(),
			})
			scheduleFailures++
			continue
		}
		totalBytes += len(scheduleBody)

		games, err := nhl.ParseGames(scheduleBody)
		if err != nil {
			log.Printf("date=%s schedule parse error=%v", date, err)
			failures = append(failures, manifest.Failure{
				Date: date, Stage: manifest.StageScheduleParse, Error: err.Error(),
			})
			scheduleFailures++
			continue
		}
		schedulesOK++

		var datePBPBytes, datePBPFailures int
		var dateShiftOK, dateShiftFailures int
		for _, g := range games {
			if !knownGameState(g.GameState) {
				log.Printf("date=%s game=%d unknown game state=%q; no game data eligibility granted", date, g.ID, g.GameState)
			}

			if g.PBPEligible() {
				pbpCtx, cancel := context.WithTimeout(ctx, opTimeout)
				pbpBody, err := client.PlayByPlay(pbpCtx, g.ID)
				cancel()
				if err != nil {
					log.Printf("date=%s game=%d pbp fetch error=%v", date, g.ID, err)
					failures = append(failures, manifest.Failure{
						Date: date, GameID: g.ID, Stage: manifest.StagePBPFetch, Error: err.Error(),
					})
					datePBPFailures++
					gameFailures++
				} else {
					writeCtx, cancel := context.WithTimeout(ctx, opTimeout)
					err = writer.WritePlayByPlay(writeCtx, g.Season, date, g.ID, pbpBody)
					cancel()
					if err != nil {
						log.Printf("date=%s game=%d pbp write error=%v", date, g.ID, err)
						failures = append(failures, manifest.Failure{
							Date: date, GameID: g.ID, Stage: manifest.StagePBPWrite, Error: err.Error(),
						})
						datePBPFailures++
						gameFailures++
					} else {
						gamesOK++
						datePBPBytes += len(pbpBody)
						totalBytes += len(pbpBody)
					}
				}
			}

			if g.ShiftEligible() {
				shiftCtx, cancel := context.WithTimeout(ctx, opTimeout)
				shiftBody, err := client.ShiftCharts(shiftCtx, g.ID)
				cancel()
				if err != nil {
					log.Printf("date=%s game=%d shift fetch error=%v", date, g.ID, err)
					failures = append(failures, manifest.Failure{
						Date: date, GameID: g.ID, Stage: manifest.StageShiftFetch, Error: err.Error(),
					})
					dateShiftFailures++
				} else {
					writeCtx, cancel := context.WithTimeout(ctx, opTimeout)
					err = writer.WriteShiftCharts(writeCtx, g.Season, date, g.ID, shiftBody)
					cancel()
					if err != nil {
						log.Printf("date=%s game=%d shift write error=%v", date, g.ID, err)
						failures = append(failures, manifest.Failure{
							Date: date, GameID: g.ID, Stage: manifest.StageShiftWrite, Error: err.Error(),
						})
						dateShiftFailures++
					} else {
						dateShiftOK++
						totalBytes += len(shiftBody)
					}
				}
			}
		}

		fmt.Printf("date=%s schedule_bytes=%d games=%d pbp_bytes=%d pbp_failures=%d shifts_ok=%d shift_failures=%d\n",
			date, len(scheduleBody), len(games), datePBPBytes, datePBPFailures, dateShiftOK, dateShiftFailures)
	}

	fmt.Printf("done schedules_ok=%d schedule_failures=%d games_ok=%d game_failures=%d total_bytes=%d\n",
		schedulesOK, scheduleFailures, gamesOK, gameFailures, totalBytes)

	if len(failures) > 0 {
		body, err := manifest.Marshal(failures)
		if err != nil {
			log.Printf("marshal failure manifest: %v", err)
		} else {
			writeCtx, cancel := context.WithTimeout(ctx, opTimeout)
			err = writer.WriteRunFailures(writeCtx, runID, body)
			cancel()
			if err != nil {
				log.Printf("write failure manifest: %v", err)
			} else {
				fmt.Printf("failure manifest written run=%s count=%d\n", runID, len(failures))
			}
		}
	}

	// A schedule failure means a day's source snapshot was not persisted and is
	// blocking. Individual PBP/shift failures are partial: their manifest and
	// logs make them visible while Spark can still process successful objects.
	if scheduleFailures > 0 {
		os.Exit(1)
	}
}

func knownGameState(state string) bool {
	switch state {
	case nhl.GameStateFuture, nhl.GameStatePreview, nhl.GameStateLive, nhl.GameStateCritical, nhl.GameStateFinal, nhl.GameStateOff, nhl.GameStateOver:
		return true
	default:
		return false
	}
}
